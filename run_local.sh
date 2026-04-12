#!/data/data/com.termux/files/usr/bin/bash
# Run yad2 scraper locally, connecting to Railway Postgres
set -a
source "$(dirname "$0")/.env.local"
set +a
exec /data/data/com.termux/files/usr/bin/python3 "$(dirname "$0")/scraper.py"
