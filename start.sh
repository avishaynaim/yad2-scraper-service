#!/bin/bash
# Starts PostgreSQL + API + optionally the scraper


export PATH="/usr/lib/postgresql/17/bin:$PATH"
export DATABASE_URL="postgresql://username@localhost/yad2"
export TELEGRAM_BOT_TOKEN="8273771765:AAHuqjBbh5jXq3Zkm77m6R52Iv6iB3lX-OA"
export TELEGRAM_CHAT_ID="2018339906"
export WHATSAPP_NUMBERS="972504334994,972556628280"
export PORT=8000

PG_DATA="/root/pgdata"
PG_LOG="$PG_DATA/pg.log"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── PostgreSQL ───────────────────────────────────────────────────────────────

if ! pg_isready -q 2>/dev/null; then
    echo "[start] Starting PostgreSQL..."
    mkdir -p /var/run/postgresql
    sudo -u username /usr/lib/postgresql/17/bin/pg_ctl -D "$PG_DATA" -l "$PG_LOG" start
    sleep 2
else
    echo "[start] PostgreSQL already running"
fi

# Ensure DB exists
psql -U username -lqt 2>/dev/null | grep -q yad2 || createdb yad2 -U username
echo "[start] Database: OK"

# ── Schema ───────────────────────────────────────────────────────────────────

cd "$SCRIPT_DIR"
python3 -c "from scraper import init_db; init_db()" 2>/dev/null
echo "[start] Schema: OK"

# ── API ──────────────────────────────────────────────────────────────────────

PID_FILE="$SCRIPT_DIR/gunicorn.pid"
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "[start] API already running (pid $(cat "$PID_FILE"))"
else
    gunicorn api:app \
        --bind 0.0.0.0:$PORT \
        --timeout 120 \
        --workers 2 \
        --daemon \
        --log-file "$SCRIPT_DIR/gunicorn.log" \
        --access-logfile "$SCRIPT_DIR/access.log" \
        --pid "$PID_FILE"
    echo "[start] API started → http://localhost:$PORT"
fi

# ── Scraper ──────────────────────────────────────────────────────────────────

SCRAPER_PID_FILE="$SCRIPT_DIR/scraper.pid"
if [ -f "$SCRAPER_PID_FILE" ] && kill -0 "$(cat "$SCRAPER_PID_FILE")" 2>/dev/null; then
    echo "[start] Scraper already running (pid $(cat "$SCRAPER_PID_FILE"))"
else
    nohup env \
        DATABASE_URL="$DATABASE_URL" \
        TELEGRAM_BOT_TOKEN="$TELEGRAM_BOT_TOKEN" \
        TELEGRAM_CHAT_ID="$TELEGRAM_CHAT_ID" \
        SCRAPE_INTERVAL_SECONDS=3600 \
        PAGES_PER_SESSION=5 \
        python3 "$SCRIPT_DIR/scraper.py" \
        >> "$SCRIPT_DIR/scraper.log" 2>&1 &
    echo $! > "$SCRAPER_PID_FILE"
    echo "[start] Scraper started (pid $!)"
fi

# ── WhatsApp Sender ──────────────────────────────────────────────────────────

WA_DIR="/root/whatsapp-sender"
WA_PID_FILE="$WA_DIR/whatsapp.pid"
if [ -f "$WA_PID_FILE" ] && kill -0 "$(cat "$WA_PID_FILE")" 2>/dev/null; then
    echo "[start] WhatsApp sender already running (pid $(cat "$WA_PID_FILE"))"
else
    # Kill anything holding port 3001 before starting
    fuser -k 3001/tcp 2>/dev/null || true
    sleep 1
    nohup node "$WA_DIR/index.js" >> "$WA_DIR/whatsapp.log" 2>&1 &
    echo $! > "$WA_PID_FILE"
    echo "[start] WhatsApp sender started (pid $!)"
fi

# ── WhatsApp Watchdog ─────────────────────────────────────────────────────────

WA_WATCHDOG_PID="$WA_DIR/watchdog.pid"
if [ -f "$WA_WATCHDOG_PID" ] && kill -0 "$(cat "$WA_WATCHDOG_PID")" 2>/dev/null; then
    echo "[start] WhatsApp watchdog already running (pid $(cat "$WA_WATCHDOG_PID"))"
else
    nohup "$WA_DIR/watchdog.sh" >> "$WA_DIR/watchdog.log" 2>&1 &
    echo $! > "$WA_WATCHDOG_PID"
    echo "[start] WhatsApp watchdog started (pid $!)"
fi

echo ""
echo "  Dashboard : http://localhost:$PORT/dashboard"
echo "  API       : http://localhost:$PORT/api"
echo "  Health    : http://localhost:$PORT/health"
echo "  WhatsApp  : http://localhost:3001/health"
echo "  Logs      : tail -f $SCRIPT_DIR/scraper.log"
echo ""
echo "To stop everything: $SCRIPT_DIR/stop.sh"

# ── Startup Notification ─────────────────────────────────────────────────────
(sleep 5 && curl -s -X POST http://localhost:3001/send \
    -H "Content-Type: application/json" \
    -d '{"message":"🟢 yad2 scraper: im up"}' > /dev/null 2>&1) &
