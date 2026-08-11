# src/audio.py
# v1.0 — AudioEngine: USB audio capture → PCM 16-bit mono 16kHz chunks
# Unchanged from Tkinter POC — just extracted to its own module.

import sounddevice as sd
import numpy as np
import logging

log = logging.getLogger(__name__)

SAMPLE_RATE  = 16000
CHANNELS     = 1
CHUNK_MS     = 100
CHUNK_FRAMES = int(SAMPLE_RATE * CHUNK_MS / 1000)


def list_input_devices():
    """Return list of (index, name) for all input-capable devices."""
    devices = []
    try:
        for i, d in enumerate(sd.query_devices()):
            if d['max_input_channels'] > 0:
                devices.append({"index": i, "name": d['name']})
    except Exception as e:
        log.warning(f"Device enumeration error: {e}")
    return devices


class AudioEngine:
    def __init__(self, device_index: int, on_chunk, on_peak=None):
        self.device_index = device_index
        self.on_chunk     = on_chunk
        self.on_peak      = on_peak
        self.stream       = None
        self.running      = False
        self.peak         = 0.0

    def start(self):
        self.running = True
        self.stream  = sd.InputStream(
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
        log.info("Audio stopped")

    def _cb(self, indata, frames, time_info, status):
        if status:
            log.warning(f"Audio status: {status}")
        if self.running:
            self.peak = float(np.abs(indata).max()) / 32767.0
            self.on_chunk(indata.copy().tobytes())
            if self.on_peak:
                self.on_peak(self.peak)
