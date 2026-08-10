# src/app.py
# v1.0 — Wordly Audio Appliance — Flask/SocketIO server
# Replaces Tkinter UI with browser-based UI served on localhost:5000
# Change log:
#   v1.0 — Flask + SocketIO rewrite. Python owns audio + WSS. Browser owns UI.

import os
import re
import json
import socket
import subprocess
import logging
from datetime import datetime

import sounddevice as sd
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit

from audio import AudioEngine, list_input_devices, SAMPLE_RATE
from wss import WSSStreamer

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

# ── FLASK + SOCKETIO ──────────────────────────────────────────────────────────

app = Flask(__name__)
app.config['SECRET_KEY'] = 'wordly-appliance-secret'
sio = SocketIO(app, async_mode='eventlet', cors_allowed_origins='*')

# ── STATE ─────────────────────────────────────────────────────────────────────

state = {
    "status":      "idle",   # idle|connecting|connected|streaming|muted|error|ended
    "session_id":  "",
    "passcode":    "",
    "presenter":   "",
    "device_idx":  None,
    "device_name": "",
    "wifi_ssid":   "",
    "wifi_pass":   "",
    "error_msg":   "",
}

audio_eng  = None
wss        = None
streaming  = False

# ── HELPERS ───────────────────────────────────────────────────────────────────

def normalize_session_id(raw: str) -> str:
    cleaned = raw.upper().replace(" ", "").replace("_", "")
    if "-" not in cleaned and len(cleaned) == 8:
        cleaned = cleaned[:4] + "-" + cleaned[4:]
    return cleaned if re.match(r'^[A-Z]{4}-\d{4}$', cleaned) else ""

def parse_join_link(raw: str):
    match = re.search(r'join\.wordly\.ai/join/([A-Za-z0-9]{4}-?[0-9]{4})', raw)
    if not match:
        return None, None
    session_id = normalize_session_id(match.group(1))
    key_match  = re.search(r'[?&]key=([^&\s]+)', raw)
    passcode   = key_match.group(1) if key_match else ""
    return session_id, passcode

def get_network_status() -> dict:
    info = {"ip": "", "interface": "", "ssid": "", "connected": False}
    try:
        result = subprocess.run(["ip", "route", "get", "8.8.8.8"],
                                 capture_output=True, text=True, timeout=3)
        parts = result.stdout.split()
        if "dev" in parts:
            info["interface"] = parts[parts.index("dev") + 1]
        ip_result = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=2)
        ips = ip_result.stdout.strip().split()
        if ips:
            info["ip"]        = ips[0]
            info["connected"] = True
        if info["interface"].startswith("wl"):
            ssid_result = subprocess.run(["iwgetid", "-r"],
                                          capture_output=True, text=True, timeout=2)
            info["ssid"] = ssid_result.stdout.strip()
    except Exception:
        pass
    if not info["ip"]:
        try:
            info["ip"]        = socket.gethostbyname(socket.gethostname())
            info["connected"] = bool(info["ip"])
        except Exception:
            pass
    return info

def diagnose_connection() -> str:
    try:
        result = subprocess.run(["ping", "-c", "1", "-W", "1000", "8.8.8.8"],
                                 capture_output=True, timeout=3)
        if result.returncode != 0:
            return "Network issue — cannot reach internet. Check cable or WiFi."
    except Exception:
        return "Network issue — cannot determine connectivity."
    try:
        socket.getaddrinfo("wordly.ai", 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return "DNS failure — network connected but cannot resolve wordly.ai."
    try:
        s = socket.create_connection(("endpoint.wordly.ai", 443), timeout=4)
        s.close()
    except Exception:
        return "Cannot reach Wordly servers — internet connected but wordly.ai is unreachable."
    devs = list_input_devices()
    if not devs:
        return "No audio input device found — check USB audio interface connection."
    return "All systems OK — network and Wordly reachable."

def push_status(status: str, extra: dict = None):
    """Push status update to all connected browser clients."""
    state["status"] = status
    payload = {"status": status, "peak": audio_eng.peak if audio_eng else 0.0}
    if extra:
        payload.update(extra)
    sio.emit("status", payload)

# ── AUDIO + WSS CALLBACKS ─────────────────────────────────────────────────────

def on_audio_chunk(chunk: bytes):
    if streaming and wss:
        wss.send_audio(chunk)

_peak_counter = 0
def on_peak(peak: float):
    global _peak_counter
    _peak_counter += 1
    if _peak_counter % 3 != 0:  # emit every 3rd chunk (~300ms)
        return
    sio.emit("peak", {"peak": round(peak, 4)})

def on_wss_status(status: str):
    if status == "connected":
        push_status("connected")
    elif status == "error":
        msg = diagnose_connection()
        state["error_msg"] = msg
        push_status("error", {"message": msg})
    elif status == "ended":
        push_status("ended", {"reason": "portal"})
        _stop_all()
    elif status == "connecting":
        push_status("connecting")

def on_transcript(text: str):
    sio.emit("transcript", {"text": text})

def _stop_all():
    global audio_eng, wss, streaming
    streaming = False
    # Stop audio immediately
    if audio_eng:
        audio_eng.stop()
        audio_eng = None
    # Disconnect WSS in background — don't block the UI thread
    _wss = wss
    if _wss:
        def _bg_disconnect():
            try:
                _wss.disconnect()
            except Exception as e:
                log.warning(f"Background disconnect error: {e}")
        import threading
        threading.Thread(target=_bg_disconnect, daemon=True).start()
    globals()['wss'] = None

# ── HTTP ROUTES ───────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/state')
def api_state():
    net = get_network_status()
    return jsonify({
        **state,
        "devices":  list_input_devices(),
        "network":  net,
    })

@app.route('/api/config', methods=['POST'])
def api_config():
    data = request.json or {}

    # Handle join link or raw session ID
    raw = data.get("session_id", "").strip()
    sid, key = parse_join_link(raw)
    if sid:
        state["session_id"] = sid
        if key and not data.get("passcode", "").strip():
            state["passcode"] = key
        else:
            state["passcode"] = data.get("passcode", state["passcode"]).strip()
    else:
        sid = normalize_session_id(raw)
        if not sid:
            return jsonify({"ok": False, "error": "Invalid session ID format. Use ABCD-1234 or paste a join link."}), 400
        state["session_id"] = sid
        state["passcode"]   = data.get("passcode", "").strip()

    if not state["passcode"]:
        return jsonify({"ok": False, "error": "Passcode is required."}), 400

    state["presenter"]   = data.get("presenter", "").strip()
    state["device_idx"]  = data.get("device_idx")
    state["device_name"] = data.get("device_name", "")

    if state["device_idx"] is None:
        return jsonify({"ok": False, "error": "No audio device selected."}), 400

    log.info(f"Config saved: session={state['session_id']} device={state['device_name']}")
    return jsonify({"ok": True})

@app.route('/api/network', methods=['POST'])
def api_network():
    data = request.json or {}
    state["wifi_ssid"] = data.get("ssid", "").strip()
    state["wifi_pass"] = data.get("password", "").strip()
    return jsonify({"ok": True})

@app.route('/api/network/status')
def api_network_status():
    return jsonify(get_network_status())

@app.route('/api/network/test')
def api_network_test():
    msg = diagnose_connection()
    ok  = "OK" in msg
    return jsonify({"ok": ok, "message": msg})

@app.route('/api/devices')
def api_devices():
    return jsonify(list_input_devices())

@app.route('/api/audio/test')
def api_audio_test():
    idx = request.args.get('device', type=int)
    if idx is None:
        return jsonify({"ok": False, "message": "No device specified."})
    try:
        import numpy as np
        rec  = sd.rec(int(2 * SAMPLE_RATE), samplerate=SAMPLE_RATE,
                      channels=1, dtype='int16', device=idx)
        sd.wait()
        peak = float(np.abs(rec).max()) / 32767.0
        db   = 20 * __import__('math').log10(peak) if peak > 1e-6 else -99
        if peak < 0.001:
            return jsonify({"ok": False, "message": "No signal detected — check mic/cable."})
        return jsonify({"ok": True, "message": f"Signal OK: {db:.1f} dBFS"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})

@app.route('/api/quit', methods=['POST'])
def api_quit():
    import threading
    def _quit():
        import time, os, signal
        time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGTERM)
    threading.Thread(target=_quit, daemon=True).start()
    return jsonify({"ok": True})

# ── SOCKETIO EVENTS ───────────────────────────────────────────────────────────

@sio.on('connect')
def on_connect():
    log.info("Browser connected")
    net = get_network_status()
    emit("status", {
        "status":  state["status"],
        "peak":    0.0,
        "network": net,
    })

@sio.on('start')
def on_start():
    global audio_eng, wss, streaming
    if not state["session_id"] or not state["passcode"] or state["device_idx"] is None:
        emit("error", {"message": "Not configured. Set session ID and audio device first."})
        return

    log.info(f"Starting — session={state['session_id']} device={state['device_idx']}")

    wss = WSSStreamer(
        session_id    = state["session_id"],
        passcode      = state["passcode"],
        name          = state["presenter"],
        on_status     = on_wss_status,
        on_transcript = on_transcript,
    )
    wss.start()

    audio_eng = AudioEngine(
        device_index = state["device_idx"],
        on_chunk     = on_audio_chunk,
        on_peak      = on_peak,
    )
    audio_eng.start()
    streaming = True
    push_status("streaming")

@sio.on('mute')
def on_mute():
    global streaming
    streaming = False          # stop feeding chunks to WSS queue
    if wss:
        # Drain any queued audio before telling Wordly to stop
        try:
            while not wss.audio_q.empty():
                wss.audio_q.get_nowait()
        except Exception:
            pass
        wss.send_control({"type": "stop"})
    push_status("muted")

@sio.on('unmute')
def on_unmute():
    global streaming
    if wss:
        wss.send_control({"type": "start", "languageCode": "en", "sampleRate": SAMPLE_RATE})
    streaming = True           # resume feeding chunks
    push_status("streaming")

@sio.on('split')
def on_split():
    if wss:
        wss.send_split()
    emit("split_ack", {})

@sio.on('end')
def on_end():
    push_status("ended", {"reason": "local"})  # UI responds immediately
    _stop_all()                                  # cleanup in background

@sio.on('leave')
def on_leave():
    """Disconnect WSS without ending session for attendees (end=false)."""
    global audio_eng, wss, streaming
    streaming = False
    if audio_eng:
        audio_eng.stop()
        audio_eng = None
    # Push status immediately — don't wait for WSS cleanup
    push_status("ended", {"reason": "leave"})
    _wss = wss
    globals()['wss'] = None
    if _wss:
        import asyncio, json as _json, threading
        def _bg_leave():
            async def _do():
                if _wss.ws and _wss.connected:
                    try:
                        await _wss.ws.send(_json.dumps({"type": "stop"}))
                        await asyncio.sleep(0.2)
                        await _wss.ws.send(_json.dumps({"type": "disconnect", "end": False}))
                        log.info("WSS leave sent — session continues for attendees")
                    except Exception as e:
                        log.warning(f"Leave error: {e}")
                _wss.stop()
            if _wss.loop:
                asyncio.run_coroutine_threadsafe(_do(), _wss.loop)
        threading.Thread(target=_bg_leave, daemon=True).start()

@sio.on('disconnect')
def on_disconnect():
    log.info("Browser disconnected")

# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import eventlet
    import eventlet.wsgi
    log.info("Wordly Audio Appliance starting on http://localhost:5000")
    sio.run(app, host='0.0.0.0', port=5000, debug=False)
