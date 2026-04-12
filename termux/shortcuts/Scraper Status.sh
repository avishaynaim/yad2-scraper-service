#!/data/data/com.termux/files/usr/bin/bash
# One-tap: show scraper status

SESSION="yad2"
SCRAPER_DIR="/home/avishay/yad2-scraper-service"
LOG="$SCRAPER_DIR/scraper_local.log"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    LAST=$(tail -3 "$LOG" 2>/dev/null | tr '\n' ' ')
    termux-toast -d 5 "RUNNING | $LAST"
else
    LAST=$(tail -1 "$LOG" 2>/dev/null)
    termux-toast -d 5 "STOPPED | Last: $LAST"
fi
