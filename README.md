# Wordly Audio Appliance — Phase 1 POC

Mac-based simulation of the Wordly Audio Appliance hardware device.
Tkinter UI → Python audio capture → WSS → Wordly /present endpoint.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python3 src/app.py
```

## Usage

1. Enter Session ID (auto-formats to ABCD-1234)
2. Enter passcode
3. Enter presenter name (optional)
4. Select audio input device from dropdown
5. Click CONNECT TO SESSION
6. Wait for green "Connected" status
7. Click START to begin streaming
8. PAUSE suspends audio without disconnecting WSS
9. END disconnects and returns to setup screen

## Audio spec

- Sample rate: 16000 Hz
- Bit depth: 16-bit PCM
- Channels: Mono
- Buffer: ~100ms chunks
- Protocol: binary WebSocket frames

## Auth

- NO API key on WSS
- Session ID + passcode only
- Session ID always normalized to ABCD-1234 format

## Logs

Session logs written to `logs/` directory with timestamp.

## Phase 1 scope

Mac only. WiFi managed by OS — no captive portal handling yet (Phase 2).
