# Running the scraper locally on Android (Termux)

## Why local instead of Railway
Railway's datacenter IPs are blocked by yad2.co.il's ShieldSquare bot protection.
Running from a home/mobile IP bypasses this. The PostgreSQL database stays on Railway.

## Prerequisites
- Termux (from F-Droid, not Play Store)
- **Termux:Widget** app — for home screen one-tap buttons
- **Termux:Boot** app — for auto-start on device reboot

## First-time setup

### 1. Clone the repo (inside Termux, NOT in PRoot)
```bash
git clone https://github.com/avishaynaim/yad2-scraper-service.git ~/yad2-scraper-service
cd ~/yad2-scraper-service
git checkout termux-local
```

### 2. Install dependencies
```bash
pkg install python tmux
pip install curl-cffi pg8000
```

### 3. Create your .env.local with the DB credentials
```bash
cp .env.local.example .env.local
# Edit .env.local and fill in DATABASE_URL with the Railway public Postgres URL
```
Railway public DB URL is in the Railway dashboard under the Postgres service → Variables → `DATABASE_PUBLIC_URL`.

### 4. Install the home screen shortcuts
```bash
mkdir -p ~/.shortcuts ~/.termux/boot
cp termux/shortcuts/*.sh ~/.shortcuts/
cp termux/start-scraper.sh ~/.termux/boot/
chmod +x ~/.shortcuts/*.sh ~/.termux/boot/start-scraper.sh
```

### 5. Add the Termux:Widget to your home screen
Long-press home screen → Widgets → Termux → Widget (2x1)
You'll see Start / Stop / Status buttons.

### 6. Open Termux:Boot once to activate auto-start on reboot.

## Usage
- **Start**: tap "Start Scraper" widget — starts a tmux session named `yad2`
- **Stop**: tap "Stop Scraper" widget
- **Status**: tap "Scraper Status" widget — shows last log line as a toast
- **View live logs**: open Termux and run: `tmux attach -t yad2`
- **Log file**: `~/yad2-scraper-service/scraper_local.log`

## If your IP gets blocked
The scraper will log `Cannot create initial session`. Just:
1. Switch WiFi / toggle airplane mode to get a new IP
2. Tap "Start Scraper"

## Files in this folder
```
termux/
  SETUP.md              — this file
  start-scraper.sh      → copy to ~/.termux/boot/   (auto-start on boot)
  shortcuts/
    Start Scraper.sh    → copy to ~/.shortcuts/
    Stop Scraper.sh     → copy to ~/.shortcuts/
    Scraper Status.sh   → copy to ~/.shortcuts/
```
