#!/data/data/com.termux/files/usr/bin/bash
# One-tap: stop yad2 scraper

SESSION="yad2"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux kill-session -t "$SESSION"
    termux-toast "Scraper stopped"
else
    termux-toast "Scraper not running"
fi
