#!/bin/bash
# Stops the API and PostgreSQL

export PATH="/usr/lib/postgresql/17/bin:$PATH"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Shutdown Notification ─────────────────────────────────────────────────────
curl -s -X POST http://localhost:3001/send \
    -H "Content-Type: application/json" \
    -d '{"message":"🔴 yad2 scraper: im down"}' > /dev/null 2>&1 || true

SCRAPER_PID_FILE="$SCRIPT_DIR/scraper.pid"
if [ -f "$SCRAPER_PID_FILE" ] && kill -0 "$(cat "$SCRAPER_PID_FILE")" 2>/dev/null; then
    kill "$(cat "$SCRAPER_PID_FILE")"
    rm -f "$SCRAPER_PID_FILE"
    echo "[stop] Scraper stopped"
else
    echo "[stop] Scraper not running"
fi

PID_FILE="$SCRIPT_DIR/gunicorn.pid"
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    kill "$(cat "$PID_FILE")"
    rm -f "$PID_FILE"
    echo "[stop] API stopped"
else
    echo "[stop] API not running"
fi

WA_PID_FILE="/root/whatsapp-sender/whatsapp.pid"
if [ -f "$WA_PID_FILE" ] && kill -0 "$(cat "$WA_PID_FILE")" 2>/dev/null; then
    kill "$(cat "$WA_PID_FILE")"
    rm -f "$WA_PID_FILE"
    echo "[stop] WhatsApp sender stopped"
else
    echo "[stop] WhatsApp sender not running"
fi

if pg_isready -q 2>/dev/null; then
    pg_ctl -D /root/pgdata stop
    echo "[stop] PostgreSQL stopped"
else
    echo "[stop] PostgreSQL not running"
fi
