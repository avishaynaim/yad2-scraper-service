# Yad2 Scraper Service

Scraper service for yad2.co.il real estate rentals. Runs **locally** — the scraper, the REST API, and PostgreSQL all run on the same host.

> ⚠️ **This used to run on Railway. It does not anymore — there is no cloud component.**
> If this machine is off, asleep, or has no internet, **nothing scrapes.** A "scrape" only
> happens while `scraper.py` is running on this host with a working connection.

## Ecosystem

This repo is part of a two-service system:

| Repo | Role |
|------|------|
| **yad2-scraper-service** (this repo) | Scrapes Yad2.co.il, stores listings in PostgreSQL, exposes REST API |
| [**yad2-whatsapp-bot**](https://github.com/avishaynaim/yad2-whatsapp-bot) | WhatsApp bot — listens to a group, answers questions about listings using Claude AI + the shared PostgreSQL DB |

Both services share the same PostgreSQL database. The bot calls Claude (via CLI) with a system prompt that queries the DB directly using `psql`.

## Features

- **Full site scraping**: Scrapes ALL rental listings from yad2.co.il/realestate/rent
- **Scheduled runs**: Configurable interval (default: hourly)
- **PostgreSQL storage**: Persistent storage with full history
- **REST API**: Query listings with filters
- **Anti-bot bypass**: Implements session rotation to bypass Radware Bot Manager

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│  scraper.py     │     │    api.py       │
│  (background)   │────▶│  (web service)  │
│                 │     │                 │
│  - Scheduled    │     │  - REST API     │
│  - Batch scrape │     │  - Filters      │
│  - Session mgmt │     │  - Stats        │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
    ┌─────────────────────────────────┐
    │         PostgreSQL              │
    │                                 │
    │  - listings (main data)         │
    │  - scrape_runs (history)        │
    └─────────────────────────────────┘
```

## Running (local)

Everything runs on this host. Use the bundled scripts:

```bash
./start.sh    # starts PostgreSQL, the API (gunicorn), the scraper loop, and the WhatsApp sender
./stop.sh     # stops them
```

`start.sh` launches:
- **PostgreSQL** (local) and ensures the `yad2` database + schema exist
- **API** — `gunicorn api:app` (dashboard + REST endpoints)
- **scraper.py** — a long-running process that scrapes every `SCRAPE_INTERVAL_SECONDS` (default: hourly)
- **WhatsApp sender** + watchdog (local Baileys service) for notifications

### How scheduling actually works
`scraper.py` runs an infinite loop: scrape all subscribed cities → `sleep(SCRAPE_INTERVAL_SECONDS)` → repeat.
It is **not** wall-clock cron. It is connectivity-aware: if there's no connection to Yad2 it **waits** for
the connection to return and then scrapes, instead of logging an hour of failed runs (see below).

### "Last scrape" vs "last *successful* scrape" — important
A failed cycle still writes a row to `scrape_runs` with a `finished_at` timestamp (status `failed`,
0 pages). **Do not treat `MAX(finished_at)` as "last successful scrape"** — that's how the WhatsApp bot
came to report a failed attempt as if it succeeded. Query `WHERE status = 'completed'`, or read the
`last_successful_scrape` key from the `scraper_state` table, which the scraper now sets only on real scrapes.

> The Railway deploy flow was removed. `railway.toml` / `Procfile` / `nixpacks.toml` may still exist
> in the repo but are no longer used.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | PostgreSQL connection string |
| `SCRAPE_INTERVAL_SECONDS` | `3600` | Time between scrapes (1 hour) |
| `PAGES_PER_SESSION` | `5` | Pages before session rotation |
| `PORT` | `8000` | API server port |

## Dashboard

Access the interactive dashboard at `/dashboard`:
- **Listings table**: Filter by city, neighborhood, price range, rooms, etc.
- **Price changes**: Track price drops and raises across all listings
- **Scrape history**: Monitor scraper performance
- **Cities overview**: See listings distribution by city
- **Saved filters**: Save and load filter presets (stored in browser localStorage)

## API Endpoints

### Dashboard
```
GET /dashboard
```
Interactive dashboard UI with filterable tables.

### Health Check
```
GET /health
```

### List Listings
```
GET /listings
    ?city=תל אביב
    ?neighborhood=לב העיר
    ?min_price=3000
    ?max_price=10000
    ?min_rooms=3
    ?max_rooms=5
    ?is_merchant=false
    ?active_only=true
    ?limit=100
    ?offset=0
    ?sort_by=price_numeric
    ?sort_order=asc
```

### Get Single Listing
```
GET /listings/<id>
```

### Statistics
```
GET /stats
```

### Scrape History
```
GET /runs
```

### List Cities
```
GET /cities
```

### List Neighborhoods
```
GET /neighborhoods?city=תל אביב
```

## Database Schema

### listings
| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(50) | Primary key (Yad2 ID) |
| ad_number | VARCHAR(50) | Yad2 ad number |
| street | VARCHAR(255) | Street address |
| city | VARCHAR(100) | City name |
| neighborhood | VARCHAR(100) | Neighborhood |
| price | VARCHAR(50) | Price string |
| price_numeric | INTEGER | Price as number |
| rooms | VARCHAR(20) | Number of rooms |
| floor | VARCHAR(20) | Floor number |
| size_sqm | VARCHAR(50) | Size in sqm |
| is_merchant | BOOLEAN | From agency |
| amenities | JSONB | Parking, AC, etc |
| first_seen_at | TIMESTAMP | First scraped |
| last_seen_at | TIMESTAMP | Last scraped |
| is_active | BOOLEAN | Still listed |

### scrape_runs
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Run ID |
| started_at | TIMESTAMP | Start time |
| finished_at | TIMESTAMP | End time |
| total_pages | INTEGER | Pages found |
| pages_scraped | INTEGER | Pages completed |
| listings_found | INTEGER | Total listings |
| listings_new | INTEGER | New this run |
| status | VARCHAR(50) | completed/failed |

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set database URL
export DATABASE_URL="postgresql://user:pass@localhost/yad2"

# Run scraper (one-time)
python scraper.py

# Run API
python api.py
```

## Bypass Strategy

Based on investigation findings:
- Radware Bot Manager limits ~6-7 requests per session
- Solution: Rotate sessions every 5 pages
- Warmup via homepage to collect tracking cookies
- Random delays (3-6s) within batch, longer (12-25s) between batches

## Expected Performance

| Total Pages | Time | Listings |
|-------------|------|----------|
| ~50-100 | ~15-30 min | ~2,000-4,000 |

Full site typically has 50-100 pages with ~40 listings each.
