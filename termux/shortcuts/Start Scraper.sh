#!/data/data/com.termux/files/usr/bin/bash
# One-tap: start yad2 scraper in a persistent tmux session

SESSION="yad2"
SCRAPER_DIR="/home/avishay/yad2-scraper-service"
LOG="$SCRAPER_DIR/scraper_local.log"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    termux-toast "Scraper already running"
    exit 0
fi

tmux new-session -d -s "$SESSION" \
    "set -a; source $SCRAPER_DIR/.env.local; set +a; \
     /data/data/com.termux/files/usr/bin/python3 $SCRAPER_DIR/scraper.py \
     2>&1 | tee -a $LOG"

termux-toast "Scraper started"
