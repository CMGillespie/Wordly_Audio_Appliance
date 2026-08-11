# src/wss.py
# v1.0 — WSSStreamer: WebSocket connection to Wordly /present endpoint
# Unchanged from Tkinter POC — just extracted to its own module.
#
# GOTCHA #8: split command = {"type":"split"} — confirmed by Jim Firby (CTO)
# GOTCHA #9: use "type" not "command". Include connectionCode "9005" in every connect.
#            Result responses use "text" field, only process when final=True.
# GOTCHA #10: disconnect must block before teardown. Send end:true.

import asyncio
import json
import queue
import threading
import logging
import websockets

log = logging.getLogger(__name__)

WSS_ENDPOINT    = "wss://endpoint.wordly.ai/present"
CONNECTION_CODE = "9005"
SAMPLE_RATE     = 16000


class WSSStreamer:
    def __init__(self, session_id, passcode, on_status, on_transcript, name=""):
        self.session_id    = session_id
        self.passcode      = passcode
        self.name          = name
        self.on_status     = on_status
        self.on_transcript = on_transcript
        self.ws            = None
        self.running       = False
        self.connected     = False
        self.audio_q       = queue.Queue()
        self.loop          = None
        self._thread       = None
        self.context       = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def send_audio(self, chunk: bytes):
        if self.connected:
            self.audio_q.put(chunk)

    def send_split(self):
        """GOTCHA #8 — instant transcript boundary, no reconnect needed."""
        self.send_control({"type": "split"})
        log.info("WSS split sent — transcript boundary created")

    def send_control(self, msg: dict):
        async def _send():
            if self.ws:
                await self.ws.send(json.dumps(msg))
                log.info(f"Control sent: {msg}")
        if self.loop:
            asyncio.run_coroutine_threadsafe(_send(), self.loop)

    def stop(self):
        self.running   = False
        self.connected = False
        self.audio_q.put(None)

    def disconnect(self):
        # Schedule disconnect messages then stop — don't block
        if self.loop and self.ws:
            asyncio.run_coroutine_threadsafe(self._disconnect(), self.loop)
            import time; time.sleep(0.8)  # give coroutine time to send
        self.stop()

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._connect_loop())

    async def _connect_loop(self):
        backoff = 2
        while self.running:
            try:
                self.on_status("connecting")
                async with websockets.connect(WSS_ENDPOINT) as ws:
                    self.ws     = ws
                    backoff     = 2
                    await self._handshake()
                    await self._stream_loop()
            except Exception as e:
                if not self.running:
                    break
                log.warning(f"WSS error: {e} — retry in {backoff}s")
                self.connected = False
                self.on_status("error")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _handshake(self):
        connect_msg = {
            "type":             "connect",
            "presentationCode": self.session_id,
            "accessKey":        self.passcode,
            "connectionCode":   CONNECTION_CODE,
        }
        if self.name:
            connect_msg["name"] = self.name
        if self.context:
            connect_msg["context"] = self.context
        await self.ws.send(json.dumps(connect_msg))
        log.info(f"Sent connect: presentationCode={self.session_id}")

        resp = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=10))
        log.info(f"Connect response: {resp}")
        if not resp.get("success", False):
            raise Exception(f"Rejected: {resp.get('message', 'unknown')}")

        await self.ws.send(json.dumps({
            "type":         "start",
            "languageCode": "en",
            "sampleRate":   SAMPLE_RATE
        }))
        self.connected = True
        self.on_status("connected")
        log.info("WSS handshake complete — streaming active")

    async def _stream_loop(self):
        r = asyncio.create_task(self._recv())
        s = asyncio.create_task(self._send())
        done, pending = await asyncio.wait([r, s], return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()

    async def _send(self):
        loop = asyncio.get_event_loop()
        while self.running and self.connected:
            chunk = await loop.run_in_executor(None, self._get_chunk)
            if chunk is None:
                break
            if chunk and self.ws and self.connected:
                await self.ws.send(chunk)

    def _get_chunk(self):
        try:
            return self.audio_q.get(timeout=1.0)
        except queue.Empty:
            return b""

    async def _recv(self):
        async for msg in self.ws:
            if isinstance(msg, str):
                try:
                    data     = json.loads(msg)
                    msg_type = data.get("type", "")
                    if msg_type == "result":
                        self.context = data.get("context")
                        t = data.get("text", "")
                        if t and data.get("final", False):
                            self.on_transcript(t)
                            log.info(f"Transcript (final): {t}")
                    elif msg_type == "status":
                        log.info(f"Status update: {data}")
                    elif msg_type == "error":
                        log.warning(f"Server error: {data.get('message')}")
                    elif msg_type == "end":
                        log.info("Session ended by server")
                        self.on_status("ended")
                except Exception as e:
                    log.warning(f"Recv error: {e}")

    async def _disconnect(self):
        if self.ws:
            try:
                await self.ws.send(json.dumps({"type": "stop"}))
                await asyncio.sleep(0.3)
                await self.ws.send(json.dumps({"type": "disconnect", "end": True}))
                await asyncio.sleep(0.3)
                log.info("WSS disconnect sent cleanly")
            except Exception as e:
                log.warning(f"Disconnect error: {e}")
