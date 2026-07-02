# Wordly Audio Appliance — Gotcha Log
# Living document. Add entries immediately when discovered. Never wait.
# Format: What | Symptom | Fix | Source

---

## GOTCHA #1 — API Version Header
**What:** Do NOT include `api-version` header when calling Wordly REST API.
**Symptom:** Auth failure even though Swagger says to include it.
**Fix:** Omit the header entirely.
**Source:** Jim Firby, CTO

---

## GOTCHA #2 — Session ID Format
**What:** Session ID must always be normalized to ABCD-1234 before passing to any endpoint.
**Symptom:** Endpoint rejects malformed IDs silently or with unhelpful errors.
**Fix:** Normalize on save/submit, not on keypress. Live formatting in Tkinter causes trace re-entry cursor bugs (see GOTCHA #7).
**Source:** Wordly API spec + field experience

---

## GOTCHA #3 — Auth Split
**What:** REST API and WSS endpoints use completely different auth.
**Symptom:** Passing API key to WSS endpoint causes auth failure. Passing session ID to REST causes auth failure.
**Fix:** REST API → `apikey` header. WSS `/attend`, `/present`, `/session` → NO apikey. Session ID + passcode only.
**Source:** Wordly API spec + Jim Firby, CTO

---

## GOTCHA #4 — Language List CORS
**What:** `languages.json` at `assets.wordly.ai` has CORS issues in browser-based web apps.
**Symptom:** Fetch blocked in browser, works fine server-side.
**Fix:** Use server-side fetch or proxy. Not an issue for Python appliance.
**Source:** Field experience

---

## GOTCHA #5 — Python Version
**What:** Python 3.11 is the target. 3.13 breaks Firebase and common libraries in PyInstaller builds.
**Symptom:** PyInstaller builds fail or produce broken executables on 3.13.
**Fix:** Pin to 3.11. Check `python3 --version` before building.
**Source:** Prior project experience

---

## GOTCHA #6 — Portal UI Changes
**What:** Wordly portal updates break scrapers.
**Symptom:** Playwright scripts fail after portal deployments.
**Fix:** Re-inspect and document new selectors immediately after any portal update.
**Source:** Field experience

---

## GOTCHA #7 — Tkinter Live Trace Re-entry (Mac)
**What:** Using `trace_add("write", callback)` to live-format a StringVar on Mac Tkinter causes cursor to jump mid-typing.
**Symptom:** Typing `DTFO2103` produces `DTFO-1032` — hyphen inserted at wrong position, characters reordered.
**Fix:** Remove live trace. Normalize/format on Save button click only. Never rewrite StringVar contents from within a write trace callback.
**Source:** Phase 1 POC development

---

## GOTCHA #8 — Session Split / Transcript Boundary
**What:** Wordly only generates a transcript when a session ends. All-day captures produce one undivided transcript.
**Symptom:** No transcript boundaries in multi-session day-long deployments.
**The command:** Send `{"type": "split"}` as a TEXT frame on the active WSS connection.
**Effect:** Immediate transcript boundary — end + start without disconnecting or interrupting audio.
**Fix:** `send_control({"type": "split"})` on the live WSSStreamer.
**Note:** This command is newer than EndpointServices_1.3.pdf — not in the docs.
**Source:** Jim Firby, CTO (confirmed directly)

---

## GOTCHA #9 — WSS Protocol: 'type' not 'command', connectionCode required
**What:** Two separate issues discovered together when WSS returned `Unexpected present request: ""`.
**Issue A:** All WSS messages use `"type"` as the key, NOT `"command"`. Initial code used `"command"` throughout.
**Issue B:** `connectionCode` is a required field in every connect message. Value `"9005"` supplied by Wordly. Appears in Wordly logs for support tracing.
**Issue C:** Result responses use `"text"` field for transcript content, NOT `"transcript"`. Only fire on `"final": true`.
**Symptom:** WSS connects but Wordly returns `Unexpected present request: ""` — sees empty presentationCode.
**Fix:**
- All messages: `{"type": "connect"}` not `{"command": "connect"}`
- Add `"connectionCode": "9005"` to every connect message
- Read `data["text"]` not `data["transcript"]` from result responses
- Only process results where `data["final"] == True`
**Source:** EndpointServices_1.3.pdf + field testing

---

## GOTCHA #10 — WSS Disconnect Must Block Before Teardown
**What:** `disconnect()` fired the async coroutine then immediately tore down the streamer thread, so stop/disconnect messages never actually sent.
**Symptom:** Hitting End returned to idle screen but Wordly session stayed open. Reconnecting resumed the same session seamlessly (confirmed: Wordly holds state).
**Fix:** Use `future.result(timeout=2.0)` to block until the disconnect coroutine completes before calling `stop()`. Add `"end": True` to disconnect message per spec.
**Correct disconnect sequence:**
1. `{"type": "stop"}`
2. sleep 0.2s
3. `{"type": "disconnect", "end": True}`
4. sleep 0.3s
5. then tear down thread
**Source:** Phase 1 POC testing

---

## GOTCHA #11 — macOS Tkinter: tk.Button ignores bg/fg
**What:** On macOS, `tk.Button` renders with native Aqua widget styling. `bg`, `fg`, `relief="flat"` are all ignored.
**Symptom:** Buttons appear as grey system buttons regardless of color settings. Text may be invisible against background.
**Fix:** Replace all `tk.Button` with `tk.Label` and bind `<Button-1>` for click, `<Enter>`/`<Leave>` for hover. Full color control.
**Source:** Phase 1 POC development

---

## GOTCHA #12 — macOS Tkinter: messagebox appears behind app window
**What:** On macOS, `messagebox.askyesno()` and similar dialogs can render behind the calling Tkinter window, especially when called from a fullscreen-colored screen.
**Symptom:** End/Split button pressed, nothing visible happens, app appears frozen.
**Fix:**
```python
self.lift()
self.focus_force()
self.after(100, self._confirm_callback)  # delay lets lift complete

def _confirm_callback(self):
    self.attributes("-topmost", True)
    self.update()
    result = messagebox.askyesno("Title", "Message", parent=self)
    self.attributes("-topmost", False)
```
**Source:** Phase 1 POC development

---

## GOTCHA #13 — Mute Should Use WSS stop/start, Not Client-Side Suppression
**What:** Muting by simply not sending audio chunks is incorrect per the spec.
**Symptom:** Wordly ASR engine may timeout or behave unexpectedly waiting for audio that stopped without a stop signal.
**Fix:** On mute: send `{"type": "stop"}`. On unmute: send `{"type": "start", "languageCode": "en", "sampleRate": 16000}`. Per EndpointServices spec: "Stop and start requests can be sent at any time to pause and resume transcription, for example, when a mute button is pressed."
**Source:** EndpointServices_1.3.pdf, page 11

---

*Last updated: July 2, 2026 — Phase 1 POC complete*
