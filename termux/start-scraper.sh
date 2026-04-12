#!/data/data/com.termux/files/usr/bin/bash
# Auto-start yad2 scraper on device boot (requires Termux:Boot app)

sleep 15  # wait for network

SESSION="yad2"
SCRAPER_DIR="/home/avishay/yad2-scraper-service"
LOG="$SCRAPER_DIR/scraper_local.log"

termux-wake-lock

tmux new-session -d -s "$SESSION" \
    "set -a; source $SCRAPER_DIR/.env.local; set +a; \
     /data/data/com.termux/files/usr/bin/python3 $SCRAPER_DIR/scraper.py \
     2>&1 | tee -a $LOG"
