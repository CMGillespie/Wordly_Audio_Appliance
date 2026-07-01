# wordly_appliance/src/app.py
# v0.3 — Wordly Audio Appliance — Mac POC
# Phase 1: Mac simulation with Tkinter UI
# Change log:
#   v0.1 — initial build
#   v0.2 — full UX rewrite: idle/streaming/error states, setup panels,
#           split workaround (stop+500ms+start), background diagnostics,
#           mute toggle, timer, red/green full-screen states
#   v0.3 — replace split workaround with confirmed WSS command {"type":"split"}
#           per Jim Firby (CTO). No disconnect needed. Instant transcript boundary.

import tkinter as tk
from tkinter import ttk, messagebox
import threading
import asyncio
import json
import re
import time
import queue
import socket
import subprocess
import sounddevice as sd
import numpy as np
import websockets
import logging
import os
from datetime import datetime

# ── LOGGING ───────────────────────────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/appliance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

WSS_ENDPOINT  = "wss://endpoint.wordly.ai/present"
SAMPLE_RATE   = 16000
CHANNELS      = 1
CHUNK_MS      = 100
CHUNK_FRAMES  = int(SAMPLE_RATE * CHUNK_MS / 1000)
# GOTCHA #8: split command confirmed by Jim Firby (CTO) — {"type":"split"} sent as text frame
# No disconnect or delay required. Instant transcript boundary.

# ── COLORS ────────────────────────────────────────────────────────────────────

WORDLY_BLUE    = "#1B3A6B"
WORDLY_BLUE_LT = "#2A5298"
ACCENT         = "#4A90D9"
TEXT_WHITE     = "#F0F0F0"
TEXT_DIM       = "#8899AA"
BG_INPUT       = "#162d52"
GREEN_BG       = "#1B5E20"
GREEN_LIVE     = "#2E7D32"
GREEN_MUTED    = "#4A7C59"
GREEN_PULSE1   = "#3A6B47"
GREEN_PULSE2   = "#2E5C3A"
RED_BG         = "#7F0000"
RED_DARK       = "#5C0000"
AMBER          = "#FFC107"
BTN_DARK       = "#0D2140"
BTN_HOVER      = "#1B3A6B"
BTN_SPLIT      = "#1A4A2E"
BTN_END        = "#4A0000"
BTN_MUTE       = "#3A5A3A"
BTN_UNMUTE     = "#1A3A1A"

APP_W = 600
APP_H = 700

# ── SESSION ID ────────────────────────────────────────────────────────────────

def normalize_session_id(raw: str) -> str:
    cleaned = raw.upper().replace(" ", "").replace("_", "")
    if "-" not in cleaned and len(cleaned) == 8:
        cleaned = cleaned[:4] + "-" + cleaned[4:]
    return cleaned if re.match(r'^[A-Z]{4}-\d{4}$', cleaned) else ""

def format_session_input(raw: str) -> str:
    cleaned = re.sub(r'[^A-Za-z0-9]', '', raw).upper()
    if len(cleaned) > 4:
        cleaned = cleaned[:4] + "-" + cleaned[4:8]
    return cleaned

# ── NETWORK DIAGNOSTICS ───────────────────────────────────────────────────────

def diagnose_connection() -> str:
    """Returns a plain-English best-guess error message."""
    # 1. Check gateway reachability
    try:
        gw = _get_default_gateway()
        if gw:
            result = subprocess.run(["ping", "-c", "1", "-W", "1000", gw],
                                     capture_output=True, timeout=3)
            if result.returncode != 0:
                return f"Network issue — cannot reach gateway ({gw}). Check cable or WiFi."
    except Exception:
        return "Network issue — cannot determine gateway. Check connection."

    # 2. DNS check
    try:
        socket.getaddrinfo("wordly.ai", 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return "DNS failure — network connected but cannot resolve wordly.ai. Check DNS or proxy."

    # 3. TCP port check
    try:
        s = socket.create_connection(("endpoint.wordly.ai", 443), timeout=4)
        s.close()
    except Exception:
        return "Cannot reach Wordly servers — internet connected but wordly.ai is unreachable."

    # 4. Audio device check
    try:
        devs = [d for d in sd.query_devices() if d['max_input_channels'] > 0]
        if not devs:
            return "No audio input device found — check USB audio interface connection."
    except Exception:
        return "Audio system error — cannot enumerate devices."

    return "Connection lost — Wordly WSS dropped. Attempting to reconnect..."

def _get_default_gateway() -> str:
    try:
        result = subprocess.run(["route", "-n", "get", "default"],
                                 capture_output=True, text=True, timeout=2)
        for line in result.stdout.splitlines():
            if "gateway" in line.lower():
                return line.split()[-1]
    except Exception:
        pass
    return "8.8.8.8"  # fallback ping target

# ── AUDIO ENGINE ──────────────────────────────────────────────────────────────

class AudioEngine:
    def __init__(self, device_index: int, on_chunk):
        self.device_index = device_index
        self.on_chunk = on_chunk
        self.stream = None
        self.running = False
        self.peak = 0.0

    def start(self):
        self.running = True
        self.stream = sd.InputStream(
            device=self.device_index,
            samplerate=SAMPLE_RATE,
            channels=CHANNELS,
            dtype='int16',
            blocksize=CHUNK_FRAMES,
            callback=self._cb
        )
        self.stream.start()
        log.info(f"Audio started — device {self.device_index} @ {SAMPLE_RATE}Hz mono 16-bit")

    def stop(self):
        self.running = False
        if self.stream:
            self.stream.stop()
            self.stream.close()
            self.stream = None

    def _cb(self, indata, frames, time_info, status):
        if status:
            log.warning(f"Audio: {status}")
        if self.running:
            self.peak = float(np.abs(indata).max()) / 32767.0
            self.on_chunk(indata.copy().tobytes())

# ── WSS STREAMER ──────────────────────────────────────────────────────────────

class WSSStreamer:
    def __init__(self, session_id, passcode, on_status, on_transcript):
        self.session_id  = session_id
        self.passcode    = passcode
        self.on_status   = on_status
        self.on_transcript = on_transcript
        self.ws          = None
        self.running     = False
        self.connected   = False
        self.audio_q     = queue.Queue()
        self.loop        = None
        self._thread     = None
        self.context     = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def send_audio(self, chunk: bytes):
        if self.connected:
            self.audio_q.put(chunk)

    def send_split(self):
        """Transcript boundary. GOTCHA #8 — confirmed by Jim Firby (CTO).
        Sends {"type":"split"} as a text frame. No disconnect needed."""
        async def _split():
            if self.ws and self.connected:
                await self.ws.send(json.dumps({"type": "split"}))
                log.info("WSS split sent — transcript boundary created")
        if self.loop:
            asyncio.run_coroutine_threadsafe(_split(), self.loop)

    def stop(self):
        self.running = False
        self.connected = False
        self.audio_q.put(None)

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
                    self.ws = ws
                    backoff = 2
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
        await self.ws.send(json.dumps({
            "command": "connect",
            "presentationCode": self.session_id,
            "accessKey": self.passcode
        }))
        resp = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=10))
        log.info(f"Connect response: {resp}")
        if not resp.get("success", False):
            raise Exception(f"Rejected: {resp.get('message', 'unknown')}")
        await self.ws.send(json.dumps({
            "command": "start",
            "languageCode": "en",
            "sampleRate": SAMPLE_RATE
        }))
        self.connected = True
        self.on_status("connected")
        log.info("WSS handshake complete")

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
                    data = json.loads(msg)
                    if data.get("command") == "result":
                        self.context = data.get("context")
                        t = data.get("transcript", "")
                        if t:
                            self.on_transcript(t)
                except Exception as e:
                    log.warning(f"Recv error: {e}")

    async def _disconnect(self):
        if self.ws and self.connected:
            try:
                await self.ws.send(json.dumps({"command": "stop"}))
                await self.ws.send(json.dumps({"command": "disconnect"}))
            except Exception as e:
                log.warning(f"Disconnect: {e}")

    def disconnect(self):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self._disconnect(), self.loop)
        self.stop()

# ── MAIN APP ──────────────────────────────────────────────────────────────────

class WordlyAppliance(tk.Tk):

    # App state machine:
    # idle → connecting → streaming ↔ muted
    #                  ↘ error → (auto-retry) → streaming
    # streaming → ended → idle

    def __init__(self):
        super().__init__()
        self.title("Wordly Audio Appliance")
        self.geometry(f"{APP_W}x{APP_H}")
        self.resizable(False, False)
        self.configure(bg=WORDLY_BLUE)

        # Config state
        self.cfg = {
            "session_id":  "",
            "passcode":    "",
            "presenter":   "",
            "device_idx":  None,
            "device_name": "",
            "wifi_ssid":   "",
            "wifi_pass":   "",
        }

        # Runtime state
        self.state       = "idle"   # idle|connecting|streaming|muted|error
        self.audio_eng   = None
        self.wss         = None
        self.muted       = False
        self.timer_start = None
        self.timer_id    = None
        self.pulse_id    = None
        self.pulse_state = False
        self.error_msg   = ""

        # Panels (setup overlays)
        self.panel       = None

        self._build_idle()
        self.protocol("WM_DELETE_WINDOW", self._on_quit)

    # ═══════════════════════════════════════════════════════════════════════════
    # IDLE SCREEN
    # ═══════════════════════════════════════════════════════════════════════════

    def _build_idle(self):
        self._clear()
        self.configure(bg=WORDLY_BLUE)
        self.state = "idle"

        # ── Header ──
        hdr = tk.Frame(self, bg=WORDLY_BLUE)
        hdr.pack(fill="x", padx=30, pady=(30, 0))
        tk.Label(hdr, text="WORDLY", font=("Arial", 36, "bold"),
                 bg=WORDLY_BLUE, fg=TEXT_WHITE).pack(side="left")
        tk.Label(hdr, text="  Audio Appliance", font=("Arial", 18),
                 bg=WORDLY_BLUE, fg=ACCENT).pack(side="left", pady=6)

        # ── Session summary card ──
        card = tk.Frame(self, bg=WORDLY_BLUE_LT, padx=24, pady=20)
        card.pack(fill="x", padx=30, pady=(30, 0))

        if self.cfg["session_id"]:
            tk.Label(card, text=self.cfg["session_id"],
                     font=("Courier", 32, "bold"), bg=WORDLY_BLUE_LT, fg=TEXT_WHITE).pack(anchor="w")
            if self.cfg["presenter"]:
                tk.Label(card, text=self.cfg["presenter"],
                         font=("Arial", 16), bg=WORDLY_BLUE_LT, fg=TEXT_DIM).pack(anchor="w")
            dev = self.cfg["device_name"] or "No audio device selected"
            tk.Label(card, text=f"🎙  {dev}",
                     font=("Arial", 13), bg=WORDLY_BLUE_LT, fg=TEXT_DIM).pack(anchor="w", pady=(8, 0))
        else:
            tk.Label(card, text="Not configured", font=("Arial", 20),
                     bg=WORDLY_BLUE_LT, fg=TEXT_DIM).pack(anchor="w")
            tk.Label(card, text="Use Wordly Setup below to enter session details.",
                     font=("Arial", 13), bg=WORDLY_BLUE_LT, fg=TEXT_DIM).pack(anchor="w", pady=(6, 0))

        # WiFi status
        wifi_txt = f"WiFi: {self.cfg['wifi_ssid']}" if self.cfg["wifi_ssid"] else "WiFi: not configured (using OS connection)"
        tk.Label(card, text=f"🌐  {wifi_txt}",
                 font=("Arial", 13), bg=WORDLY_BLUE_LT, fg=TEXT_DIM).pack(anchor="w", pady=(4, 0))

        # ── BIG START ──
        ready = bool(self.cfg["session_id"] and self.cfg["passcode"] and self.cfg["device_idx"] is not None)
        start_color = ACCENT if ready else "#334466"
        start_fg    = TEXT_WHITE if ready else "#556688"

        self.start_btn = tk.Button(
            self, text="START SESSION",
            font=("Arial", 22, "bold"),
            bg=start_color, fg=start_fg,
            relief="flat", padx=0, pady=24,
            cursor="hand2" if ready else "arrow",
            state="normal" if ready else "disabled",
            command=self._on_start
        )
        self.start_btn.pack(fill="x", padx=30, pady=(40, 0))

        if not ready:
            tk.Label(self, text="Configure session and audio device before starting.",
                     font=("Arial", 11), bg=WORDLY_BLUE, fg=TEXT_DIM).pack(pady=(6, 0))

        # ── Bottom setup buttons ──
        bot = tk.Frame(self, bg=WORDLY_BLUE)
        bot.pack(side="bottom", fill="x", padx=30, pady=30)

        self._small_btn(bot, "🌐  Network Setup", self._open_network_setup).pack(
            side="left", expand=True, fill="x", padx=(0, 10))
        self._small_btn(bot, "🎙  Wordly Setup", self._open_wordly_setup).pack(
            side="left", expand=True, fill="x")

    # ═══════════════════════════════════════════════════════════════════════════
    # SETUP PANELS (slide over idle screen)
    # ═══════════════════════════════════════════════════════════════════════════

    def _open_network_setup(self):
        self._close_panel()
        p = self._make_panel("🌐  Network Setup")

        tk.Label(p, text="WiFi SSID", font=("Arial", 11), bg=BTN_DARK, fg=TEXT_DIM).pack(anchor="w")
        ssid_var = tk.StringVar(value=self.cfg["wifi_ssid"])
        tk.Entry(p, textvariable=ssid_var, font=("Arial", 14),
                 bg=BG_INPUT, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                 relief="flat", width=30).pack(fill="x", pady=(2, 12))

        tk.Label(p, text="WiFi Password", font=("Arial", 11), bg=BTN_DARK, fg=TEXT_DIM).pack(anchor="w")
        pass_var = tk.StringVar(value=self.cfg["wifi_pass"])
        tk.Entry(p, textvariable=pass_var, font=("Arial", 14),
                 bg=BG_INPUT, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                 relief="flat", show="•", width=30).pack(fill="x", pady=(2, 12))

        # Connection test result
        test_result = tk.Label(p, text="", font=("Arial", 12),
                                bg=BTN_DARK, fg=AMBER, wraplength=480, justify="left")
        test_result.pack(anchor="w", pady=(0, 8))

        def do_test():
            test_result.config(text="Testing...", fg=AMBER)
            p.update()
            msg = diagnose_connection()
            ok  = "Cannot" not in msg and "issue" not in msg.lower() and "fail" not in msg.lower()
            test_result.config(text=msg, fg="#66FF66" if ok else RED_BG)

        self._small_btn(p, "Test Connection", do_test).pack(anchor="w", pady=(0, 16))

        note = ("Phase 1 note: On Mac, WiFi is managed by the OS.\n"
                "SSID/password fields are stored for Pi deployment in Phase 2.")
        tk.Label(p, text=note, font=("Arial", 10), bg=BTN_DARK,
                 fg=TEXT_DIM, justify="left", wraplength=480).pack(anchor="w")

        def save():
            self.cfg["wifi_ssid"] = ssid_var.get().strip()
            self.cfg["wifi_pass"] = pass_var.get().strip()
            self._close_panel()
            self._build_idle()

        self._small_btn(p, "✓  Save & Close", save, fg=GREEN_LIVE).pack(
            side="bottom", fill="x", pady=(16, 0))

    def _open_wordly_setup(self):
        self._close_panel()
        p = self._make_panel("🎙  Wordly Setup")

        # Session ID
        tk.Label(p, text="Session ID", font=("Arial", 11), bg=BTN_DARK, fg=TEXT_DIM).pack(anchor="w")
        sid_var = tk.StringVar(value=self.cfg["session_id"])

        def on_sid_type(*_):
            raw = sid_var.get()
            fmt = format_session_input(raw)
            if fmt != raw:
                sid_var.set(fmt)

        sid_var.trace_add("write", on_sid_type)
        tk.Entry(p, textvariable=sid_var, font=("Courier", 18, "bold"),
                 bg=BG_INPUT, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                 relief="flat", width=12, justify="center").pack(pady=(2, 4))
        tk.Label(p, text="Format: ABCD-1234", font=("Arial", 10),
                 bg=BTN_DARK, fg=TEXT_DIM).pack(anchor="w", pady=(0, 12))

        # Passcode
        tk.Label(p, text="Passcode", font=("Arial", 11), bg=BTN_DARK, fg=TEXT_DIM).pack(anchor="w")
        pc_var = tk.StringVar(value=self.cfg["passcode"])
        tk.Entry(p, textvariable=pc_var, font=("Arial", 14),
                 bg=BG_INPUT, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                 relief="flat", show="•", width=24).pack(fill="x", pady=(2, 12))

        # Presenter
        tk.Label(p, text="Presenter / Speaker Name", font=("Arial", 11),
                 bg=BTN_DARK, fg=TEXT_DIM).pack(anchor="w")
        pres_var = tk.StringVar(value=self.cfg["presenter"])
        tk.Entry(p, textvariable=pres_var, font=("Arial", 14),
                 bg=BG_INPUT, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                 relief="flat", width=30).pack(fill="x", pady=(2, 12))

        # Audio device
        tk.Label(p, text="Audio Input Device", font=("Arial", 11),
                 bg=BTN_DARK, fg=TEXT_DIM).pack(anchor="w")
        devices = [(i, d['name']) for i, d in enumerate(sd.query_devices())
                   if d['max_input_channels'] > 0]
        dev_names = [f"{i}: {n}" for i, n in devices] or ["No input devices found"]
        dev_var = tk.StringVar()
        # Pre-select current if set
        if self.cfg["device_idx"] is not None:
            match = [x for x in dev_names if x.startswith(str(self.cfg["device_idx"]) + ":")]
            if match:
                dev_var.set(match[0])
        if not dev_var.get():
            dev_var.set(dev_names[0])

        ttk.Combobox(p, textvariable=dev_var, values=dev_names,
                     state="readonly", font=("Arial", 12), width=36).pack(pady=(2, 8))

        # Audio test
        test_lbl = tk.Label(p, text="", font=("Arial", 11), bg=BTN_DARK, fg=AMBER)
        test_lbl.pack(anchor="w", pady=(0, 8))

        def do_audio_test():
            sel = dev_var.get()
            if "No input" in sel:
                test_lbl.config(text="No device to test.", fg=RED_BG)
                return
            idx = int(sel.split(":")[0])
            test_lbl.config(text="Recording 2s...", fg=AMBER)
            p.update()
            try:
                rec = sd.rec(int(2 * SAMPLE_RATE), samplerate=SAMPLE_RATE,
                              channels=1, dtype='int16', device=idx)
                sd.wait()
                peak = float(np.abs(rec).max()) / 32767.0
                db   = 20 * np.log10(peak) if peak > 1e-6 else -99
                if peak < 0.001:
                    test_lbl.config(text="⚠  No signal detected. Check mic/cable.", fg=AMBER)
                else:
                    test_lbl.config(text=f"✓  Signal detected: {db:.1f} dBFS", fg="#66FF66")
            except Exception as e:
                test_lbl.config(text=f"Error: {e}", fg=RED_BG)

        self._small_btn(p, "Test Audio (2s)", do_audio_test).pack(anchor="w", pady=(0, 8))

        def save():
            sid = normalize_session_id(sid_var.get())
            if not sid:
                messagebox.showerror("Invalid Session ID", "Must be ABCD-1234 format.", parent=p)
                return
            if not pc_var.get().strip():
                messagebox.showerror("Missing Passcode", "Passcode is required.", parent=p)
                return
            sel = dev_var.get()
            if "No input" in sel:
                messagebox.showerror("No Device", "Select a valid audio input device.", parent=p)
                return
            idx = int(sel.split(":")[0])
            dev_name = sel.split(":", 1)[1].strip()
            self.cfg.update({
                "session_id":  sid,
                "passcode":    pc_var.get().strip(),
                "presenter":   pres_var.get().strip(),
                "device_idx":  idx,
                "device_name": dev_name,
            })
            self._close_panel()
            self._build_idle()

        self._small_btn(p, "✓  Save & Close", save, fg=GREEN_LIVE).pack(
            side="bottom", fill="x", pady=(16, 0))

    def _make_panel(self, title: str) -> tk.Frame:
        """Creates a full-screen overlay panel on top of idle screen."""
        self.panel = tk.Frame(self, bg=BTN_DARK)
        self.panel.place(x=0, y=0, width=APP_W, height=APP_H)

        hdr = tk.Frame(self.panel, bg=WORDLY_BLUE, height=60)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text=title, font=("Arial", 16, "bold"),
                 bg=WORDLY_BLUE, fg=TEXT_WHITE).pack(side="left", padx=20, pady=12)
        tk.Button(hdr, text="✕  Close", font=("Arial", 12),
                  bg=WORDLY_BLUE, fg=TEXT_DIM, relief="flat",
                  cursor="hand2", command=self._close_panel).pack(side="right", padx=20)

        body = tk.Frame(self.panel, bg=BTN_DARK, padx=30, pady=20)
        body.pack(fill="both", expand=True)
        return body

    def _close_panel(self):
        if self.panel:
            self.panel.destroy()
            self.panel = None

    # ═══════════════════════════════════════════════════════════════════════════
    # STREAMING SCREEN  (full green)
    # ═══════════════════════════════════════════════════════════════════════════

    def _build_streaming(self):
        self._clear()
        self.configure(bg=GREEN_LIVE)
        self.state = "streaming"

        # Top info bar
        top = tk.Frame(self, bg=GREEN_BG, padx=20, pady=10)
        top.pack(fill="x")
        tk.Label(top, text=self.cfg["session_id"],
                 font=("Courier", 16, "bold"), bg=GREEN_BG, fg=TEXT_WHITE).pack(side="left")
        if self.cfg["presenter"]:
            tk.Label(top, text=f"  ·  {self.cfg['presenter']}",
                     font=("Arial", 14), bg=GREEN_BG, fg="#AADDAA").pack(side="left")

        # Big status label
        self.status_lbl = tk.Label(self, text="● LIVE",
                                    font=("Arial", 48, "bold"),
                                    bg=GREEN_LIVE, fg=TEXT_WHITE)
        self.status_lbl.pack(pady=(50, 10))

        # Timer
        self.timer_lbl = tk.Label(self, text="00:00:00",
                                   font=("Courier", 36),
                                   bg=GREEN_LIVE, fg="#CCFFCC")
        self.timer_lbl.pack()

        # Audio meter
        meter_frame = tk.Frame(self, bg=GREEN_LIVE, pady=10)
        meter_frame.pack(fill="x", padx=60)
        tk.Label(meter_frame, text="AUDIO", font=("Arial", 10),
                 bg=GREEN_LIVE, fg="#AADDAA").pack(anchor="w")
        self.meter_bar = ttk.Progressbar(meter_frame, orient="horizontal",
                                          length=480, mode="determinate", maximum=100)
        self.meter_bar.pack(fill="x")

        # Mute button (big center)
        self.mute_btn = tk.Button(self, text="MUTE",
                                   font=("Arial", 20, "bold"),
                                   bg=BTN_MUTE, fg=TEXT_WHITE,
                                   relief="flat", padx=40, pady=16,
                                   cursor="hand2", command=self._on_mute)
        self.mute_btn.pack(pady=(30, 0))

        # Bottom: Split + End
        bot = tk.Frame(self, bg=GREEN_LIVE)
        bot.pack(side="bottom", fill="x", padx=30, pady=24)

        self._small_btn(bot, "✂  Split Session", self._on_split,
                         bg=BTN_SPLIT).pack(side="left", expand=True, fill="x", padx=(0, 10))
        self._small_btn(bot, "■  End Session", self._on_end,
                         bg=BTN_END).pack(side="left", expand=True, fill="x")

        # Start timer and meter
        self.timer_start = time.time()
        self._tick_timer()
        self._tick_meter()

    # ═══════════════════════════════════════════════════════════════════════════
    # ERROR SCREEN  (full red)
    # ═══════════════════════════════════════════════════════════════════════════

    def _build_error(self, msg: str):
        self._clear()
        self.configure(bg=RED_BG)
        self.state = "error"

        tk.Label(self, text="⚠", font=("Arial", 72),
                 bg=RED_BG, fg=TEXT_WHITE).pack(pady=(60, 0))

        tk.Label(self, text="CONNECTION LOST",
                 font=("Arial", 28, "bold"),
                 bg=RED_BG, fg=TEXT_WHITE).pack(pady=(10, 0))

        tk.Label(self, text=msg,
                 font=("Arial", 14),
                 bg=RED_BG, fg="#FFAAAA",
                 wraplength=500, justify="center").pack(pady=(16, 0))

        tk.Label(self, text="Attempting to reconnect automatically...",
                 font=("Arial", 12),
                 bg=RED_BG, fg="#CC8888").pack(pady=(24, 0))

        self._small_btn(self, "■  Give Up / End Session", self._on_end,
                         bg=RED_DARK).pack(pady=(40, 0), padx=60, fill="x")

    # ═══════════════════════════════════════════════════════════════════════════
    # CONTROLS
    # ═══════════════════════════════════════════════════════════════════════════

    def _on_start(self):
        self._build_streaming()
        # Start WSS
        self.wss = WSSStreamer(
            session_id  = self.cfg["session_id"],
            passcode    = self.cfg["passcode"],
            on_status   = self._on_wss_status,
            on_transcript = self._noop_transcript
        )
        self.wss.start()
        # Start audio
        self.audio_eng = AudioEngine(
            device_index = self.cfg["device_idx"],
            on_chunk     = self._on_chunk
        )
        self.audio_eng.start()

    def _on_chunk(self, chunk: bytes):
        if self.state == "streaming" and self.wss:
            self.wss.send_audio(chunk)

    def _on_mute(self):
        if self.state == "streaming":
            self.state = "muted"
            self.mute_btn.config(text="UNMUTE", bg=BTN_UNMUTE)
            self.status_lbl.config(text="⏸ MUTED")
            self._start_pulse()
        elif self.state == "muted":
            self.state = "streaming"
            self.mute_btn.config(text="MUTE", bg=BTN_MUTE)
            self.status_lbl.config(text="● LIVE")
            self.configure(bg=GREEN_LIVE)
            self._stop_pulse()
            self._refresh_streaming_bg(GREEN_LIVE)

    def _on_split(self):
        # GOTCHA #8 — WSS split command confirmed by Jim Firby (CTO)
        # {"type":"split"} = instant transcript boundary, no reconnect, no audio interruption
        if not messagebox.askyesno("Split Session",
                                    "Split will close the current transcript and immediately start a new one.\n\nAudio will not be interrupted.\n\nProceed?"):
            return
        if self.wss:
            self.wss.send_split()
            log.info("Split command sent — transcript boundary created")

    def _on_end(self):
        if not messagebox.askyesno("End Session", "End streaming and return to setup?"):
            return
        self._stop_all()
        self._build_idle()

    def _on_wss_status(self, status: str):
        self.after(0, lambda: self._handle_wss_status(status))

    def _handle_wss_status(self, status: str):
        if status == "error" and self.state in ("streaming", "muted", "connecting"):
            # Run diagnostics in background then show error screen
            def _diag():
                msg = diagnose_connection()
                self.after(0, lambda: self._build_error(msg))
            threading.Thread(target=_diag, daemon=True).start()
        elif status == "connected" and self.state == "error":
            # Auto-recover
            self._build_streaming()

    def _noop_transcript(self, text):
        log.info(f"Transcript: {text}")

    # ═══════════════════════════════════════════════════════════════════════════
    # TIMER + METER + PULSE
    # ═══════════════════════════════════════════════════════════════════════════

    def _tick_timer(self):
        if self.state in ("streaming", "muted") and self.timer_start:
            elapsed = int(time.time() - self.timer_start)
            h, rem  = divmod(elapsed, 3600)
            m, s    = divmod(rem, 60)
            if hasattr(self, 'timer_lbl'):
                self.timer_lbl.config(text=f"{h:02d}:{m:02d}:{s:02d}")
        if self.state in ("streaming", "muted"):
            self.timer_id = self.after(1000, self._tick_timer)

    def _tick_meter(self):
        if self.state in ("streaming", "muted") and self.audio_eng:
            pct = min(self.audio_eng.peak * 300, 100)
            if hasattr(self, 'meter_bar'):
                self.meter_bar['value'] = pct
        if self.state in ("streaming", "muted"):
            self.after(80, self._tick_meter)

    def _start_pulse(self):
        def _pulse():
            if self.state != "muted":
                return
            self.pulse_state = not self.pulse_state
            color = GREEN_PULSE1 if self.pulse_state else GREEN_PULSE2
            self.configure(bg=color)
            if hasattr(self, 'status_lbl'):
                self.status_lbl.config(bg=color)
            if hasattr(self, 'timer_lbl'):
                self.timer_lbl.config(bg=color)
            if hasattr(self, 'mute_btn'):
                pass  # button keeps its own color
            self.pulse_id = self.after(800, _pulse)
        _pulse()

    def _stop_pulse(self):
        if self.pulse_id:
            self.after_cancel(self.pulse_id)
            self.pulse_id = None

    def _refresh_streaming_bg(self, color):
        """Re-apply bg color to all children after unmute."""
        for w in self.winfo_children():
            try:
                if isinstance(w, (tk.Label, tk.Frame)):
                    w.config(bg=color)
            except Exception:
                pass

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════════════

    def _stop_all(self):
        self._stop_pulse()
        if self.timer_id:
            self.after_cancel(self.timer_id)
            self.timer_id = None
        if self.audio_eng:
            self.audio_eng.stop()
            self.audio_eng = None
        if self.wss:
            self.wss.disconnect()
            self.wss = None
        self.state = "idle"

    def _clear(self):
        self._close_panel()
        for w in self.winfo_children():
            w.destroy()

    def _small_btn(self, parent, text, cmd, bg=BTN_DARK, fg=TEXT_WHITE):
        btn = tk.Button(parent, text=text, font=("Arial", 13, "bold"),
                         bg=bg, fg=fg, relief="flat",
                         padx=12, pady=12, cursor="hand2", command=cmd)
        def enter(e): btn.config(bg=self._lighten(bg))
        def leave(e): btn.config(bg=bg)
        btn.bind("<Enter>", enter)
        btn.bind("<Leave>", leave)
        return btn

    def _lighten(self, hex_color: str) -> str:
        """Lighten a hex color by ~20 for hover effect."""
        hex_color = hex_color.lstrip("#")
        r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
        r, g, b = min(r + 30, 255), min(g + 30, 255), min(b + 30, 255)
        return f"#{r:02X}{g:02X}{b:02X}"

    def _on_quit(self):
        self._stop_all()
        self.destroy()

# ── ENTRY ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = WordlyAppliance()
    app.mainloop()
