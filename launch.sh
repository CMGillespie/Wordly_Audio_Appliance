#!/bin/bash
# launch.sh — Start Wordly Audio Appliance
# Used by autostart and manual launch

cd "$(dirname "$0")"
source venv/bin/activate

# Start Flask server in background
python3 src/app.py &
SERVER_PID=$!

# Wait for server to be ready
sleep 2

# Launch Chromium in kiosk mode
if [ -n "$DISPLAY" ]; then
    chromium-browser \
        --kiosk \
        --noerrdialogs \
        --disable-infobars \
        --no-first-run \
        --disable-session-crashed-bubble \
        --disable-restore-session-state \
        --app=http://localhost:5000 \
        &
fi

wait $SERVER_PID
