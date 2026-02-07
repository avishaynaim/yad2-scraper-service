#!/usr/bin/env python3
"""
Yad2 Full Site Scraper Service
==============================
Scrapes ALL rental listings from yad2.co.il/realestate/rent
Stores in PostgreSQL, runs on Railway with scheduled intervals.

Features:
- Smart scraping: stops early when no new listings found
- State persistence: survives restarts/deploys, resumes from last page
- Session rotation with browser impersonation
"""

import json
import os
import random
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import urllib.request
import urllib.parse

import psycopg2
from psycopg2.extras import execute_values
from curl_cffi import requests as curl_requests

# ─── CONFIG ─────────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL")
SCRAPE_INTERVAL_SECONDS = int(os.environ.get("SCRAPE_INTERVAL_SECONDS", "3600"))  # default 1 hour
PAGES_PER_SESSION = int(os.environ.get("PAGES_PER_SESSION", "5"))
DELAY_WITHIN_BATCH = (3, 6)
DELAY_BETWEEN_BATCHES = (12, 25)
MAX_RETRIES = 3
SMART_STOP_THRESHOLD = int(os.environ.get("SMART_STOP_THRESHOLD", "10"))
FULL_SCRAPE_EVERY = int(os.environ.get("FULL_SCRAPE_EVERY", "6"))

# Telegram notifications (optional)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
try:
    TELEGRAM_MIN_DROP_PERCENT = float(os.environ.get("TELEGRAM_MIN_DROP_PERCENT", "5"))
except (ValueError, TypeError):
    TELEGRAM_MIN_DROP_PERCENT = 5.0

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Browser profiles for rotation
BROWSER_PROFILES = [
    ("chrome131", "Windows"),
    ("chrome131", "Mac"),
    ("chrome124", "Linux"),
    ("firefox133", "Windows"),
]


# ─── DATABASE ───────────────────────────────────────────────────────────────────

def get_db_connection(retries=5, delay=3):
    """Get database connection with retry logic."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                logger.warning(f"DB connection failed (attempt {attempt+1}/{retries}): {e}")
                time.sleep(delay)
            else:
                raise


def init_db():
    """Initialize database tables."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # Main listings table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id VARCHAR(50) PRIMARY KEY,
                ad_number VARCHAR(50),
                link_token VARCHAR(50),
                street VARCHAR(255),
                property_type VARCHAR(100),
                description_line TEXT,
                city VARCHAR(100),
                neighborhood VARCHAR(100),
                price VARCHAR(50),
                price_numeric INTEGER,
                currency VARCHAR(10),
                rooms VARCHAR(20),
                floor VARCHAR(20),
                size_sqm VARCHAR(50),
                date_added TIMESTAMP,
                updated_at VARCHAR(100),
                contact_name VARCHAR(255),
                is_merchant BOOLEAN DEFAULT FALSE,
                merchant_name VARCHAR(255),
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                image_url TEXT,
                images_count INTEGER DEFAULT 0,
                amenities JSONB,
                raw_data JSONB,
                first_seen_at TIMESTAMP DEFAULT NOW(),
                last_seen_at TIMESTAMP DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            );

            CREATE INDEX IF NOT EXISTS idx_listings_city ON listings(city);
            CREATE INDEX IF NOT EXISTS idx_listings_neighborhood ON listings(neighborhood);
            CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price_numeric);
            CREATE INDEX IF NOT EXISTS idx_listings_rooms ON listings(rooms);
            CREATE INDEX IF NOT EXISTS idx_listings_last_seen ON listings(last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_listings_is_active ON listings(is_active);
        """)

        # Price history table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id SERIAL PRIMARY KEY,
                listing_id VARCHAR(50) NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
                price VARCHAR(50),
                price_numeric INTEGER,
                recorded_at TIMESTAMP DEFAULT NOW(),
                scrape_run_id INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_price_history_listing ON price_history(listing_id);
            CREATE INDEX IF NOT EXISTS idx_price_history_recorded ON price_history(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_price_history_listing_recorded ON price_history(listing_id, recorded_at DESC);
        """)

        # Scrape runs table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scrape_runs (
                id SERIAL PRIMARY KEY,
                started_at TIMESTAMP DEFAULT NOW(),
                finished_at TIMESTAMP,
                total_pages INTEGER,
                pages_scraped INTEGER DEFAULT 0,
                pages_failed INTEGER DEFAULT 0,
                listings_found INTEGER DEFAULT 0,
                listings_new INTEGER DEFAULT 0,
                listings_updated INTEGER DEFAULT 0,
                price_changes INTEGER DEFAULT 0,
                status VARCHAR(50) DEFAULT 'running',
                error_message TEXT,
                run_type VARCHAR(20) DEFAULT 'full',
                last_page_scraped INTEGER DEFAULT 0,
                scraped_pages JSONB DEFAULT '[]'
            );
        """)

        # Add new columns if table already exists without them
        for col, coltype, default in [
            ("run_type", "VARCHAR(20)", "'full'"),
            ("last_page_scraped", "INTEGER", "0"),
            ("scraped_pages", "JSONB", "'[]'"),
            ("price_changes", "INTEGER", "0"),
        ]:
            cur.execute(f"ALTER TABLE scrape_runs ADD COLUMN IF NOT EXISTS {col} {coltype} DEFAULT {default};")

        # Scraper state table — persists run_counter across restarts
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scraper_state (
                key VARCHAR(50) PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            );
        """)

        conn.commit()
        cur.close()
    finally:
        conn.close()
    logger.info("Database initialized")


# ─── STATE PERSISTENCE ──────────────────────────────────────────────────────────

def get_state(key: str, default: str = "0") -> str:
    """Get a persisted state value."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM scraper_state WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else default
    finally:
        conn.close()


def set_state(key: str, value: str):
    """Persist a state value."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO scraper_state (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (key, value))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def cleanup_stale_runs():
    """Mark all 'running' runs with 0 pages scraped as 'abandoned'."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE scrape_runs
            SET status = 'abandoned', finished_at = NOW(),
                error_message = 'Abandoned: no pages scraped before restart'
            WHERE status = 'running' AND (pages_scraped IS NULL OR pages_scraped = 0)
        """)
        abandoned = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        conn.close()
    if abandoned > 0:
        logger.info(f"Cleaned up {abandoned} stale runs with 0 pages scraped")


def get_interrupted_run() -> Optional[dict]:
    """Check for an interrupted scrape run (status='running') that has actual progress."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, run_type, total_pages, pages_scraped, pages_failed,
                   listings_found, listings_new, listings_updated, price_changes,
                   last_page_scraped, scraped_pages, started_at
            FROM scrape_runs
            WHERE status = 'running' AND pages_scraped > 0
            ORDER BY id DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        return None

    scraped_pages = row[10] if row[10] else []
    if isinstance(scraped_pages, str):
        scraped_pages = json.loads(scraped_pages)

    return {
        "run_id": row[0],
        "run_type": row[1] or "full",
        "total_pages": row[2] or 0,
        "pages_scraped": row[3] or 0,
        "pages_failed": row[4] or 0,
        "listings_found": row[5] or 0,
        "listings_new": row[6] or 0,
        "listings_updated": row[7] or 0,
        "price_changes": row[8] or 0,
        "last_page_scraped": row[9] or 0,
        "scraped_pages": set(scraped_pages),
        "started_at": row[11],
    }


def update_scrape_progress(run_id: int, pages_scraped: int, pages_failed: int,
                           listings_found: int, listings_new: int,
                           listings_updated: int, price_changes: int,
                           last_page_scraped: int, scraped_pages: set,
                           total_pages: int = None):
    """Save current scrape progress to DB (called after each page)."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        pages_list = json.dumps(sorted(scraped_pages))
        if total_pages is not None:
            cur.execute("""
                UPDATE scrape_runs SET
                    total_pages = %s, pages_scraped = %s, pages_failed = %s,
                    listings_found = %s, listings_new = %s, listings_updated = %s,
                    price_changes = %s, last_page_scraped = %s, scraped_pages = %s
                WHERE id = %s
            """, (total_pages, pages_scraped, pages_failed, listings_found,
                  listings_new, listings_updated, price_changes,
                  last_page_scraped, pages_list, run_id))
        else:
            cur.execute("""
                UPDATE scrape_runs SET
                    pages_scraped = %s, pages_failed = %s,
                    listings_found = %s, listings_new = %s, listings_updated = %s,
                    price_changes = %s, last_page_scraped = %s, scraped_pages = %s
                WHERE id = %s
            """, (pages_scraped, pages_failed, listings_found,
                  listings_new, listings_updated, price_changes,
                  last_page_scraped, pages_list, run_id))
        conn.commit()
        cur.close()
    finally:
        conn.close()


# ─── TELEGRAM NOTIFICATIONS ─────────────────────────────────────────────────────

def send_telegram(message: str):
    """Send a message via Telegram bot. Silent no-op if not configured."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true"
        }).encode()
        req = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


def notify_price_drop(listing_id: str, city: str, neighborhood: str, street: str,
                      old_price: int, new_price: int, rooms: str, link_token: str):
    """Send Telegram notification for a significant price drop."""
    drop = old_price - new_price
    if old_price == 0:
        return
    pct = round(drop / old_price * 100, 1)
    if pct < TELEGRAM_MIN_DROP_PERCENT:
        return

    link = f"https://www.yad2.co.il/item/{link_token or listing_id}"
    location = ", ".join(filter(None, [city, neighborhood, street]))
    msg = (
        f"<b>Price Drop {pct}%</b>\n"
        f"{location}\n"
        f"{rooms or '?'} rooms\n"
        f"{old_price:,} -> <b>{new_price:,}</b> (-{drop:,})\n"
        f"<a href=\"{link}\">View on Yad2</a>"
    )
    send_telegram(msg)


def notify_scrape_summary(run_id: int, run_type: str, pages: int, total_pages: int,
                          new_count: int, updated: int, price_changes: int, duration_min: int):
    """Send a summary after each scrape completes."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    msg = (
        f"<b>Scrape #{run_id} Complete</b>\n"
        f"Type: {run_type} | Pages: {pages}/{total_pages}\n"
        f"New: {new_count} | Updated: {updated} | Price changes: {price_changes}\n"
        f"Duration: {duration_min}m"
    )
    send_telegram(msg)


# ─── LISTINGS DB ────────────────────────────────────────────────────────────────

def save_listings(listings: list[dict], run_id: int) -> tuple[int, int, int]:
    """Save listings to database. Returns (new_count, updated_count, price_changes)."""
    if not listings:
        return 0, 0, 0

    conn = get_db_connection()
    try:
        cur = conn.cursor()

        now = datetime.now(timezone.utc)
        new_count = 0
        updated_count = 0
        price_changes = 0

        for listing in listings:
            # Parse price to numeric
            price_numeric = None
            price_str = listing.get("price", "")
            if price_str:
                try:
                    price_numeric = int(price_str.replace(",", "").replace("₪", "").replace(" ", ""))
                except ValueError:
                    pass

            # Parse date_added
            date_added = None
            date_str = listing.get("date_added", "")
            if date_str:
                try:
                    date_added = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass

            # Extract coordinates
            coords = listing.get("coordinates", {})
            lat = coords.get("latitude") if coords else None
            lon = coords.get("longitude") if coords else None

            # Check if exists and get current price
            cur.execute("SELECT id, price_numeric FROM listings WHERE id = %s", (listing["id"],))
            existing = cur.fetchone()

            if existing:
                old_price = existing[1]

                # Check if price changed
                if price_numeric is not None and old_price is not None and price_numeric != old_price:
                    # Record price change in history
                    cur.execute("""
                        INSERT INTO price_history (listing_id, price, price_numeric, recorded_at, scrape_run_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (listing["id"], price_str, price_numeric, now, run_id))
                    price_changes += 1
                    logger.info(f"Price change for {listing['id']}: {old_price} -> {price_numeric}")

                    # Notify on price drops
                    if price_numeric < old_price:
                        notify_price_drop(
                            listing["id"],
                            listing.get("city", ""),
                            listing.get("neighborhood", ""),
                            listing.get("street", ""),
                            old_price, price_numeric,
                            listing.get("rooms", ""),
                            listing.get("link_token", "")
                        )

                # Update existing
                cur.execute("""
                    UPDATE listings SET
                        price = %s,
                        price_numeric = %s,
                        updated_at = %s,
                        last_seen_at = %s,
                        is_active = TRUE,
                        raw_data = %s
                    WHERE id = %s
                """, (
                    listing.get("price"),
                    price_numeric,
                    listing.get("updated_at"),
                    now,
                    json.dumps(listing),
                    listing["id"]
                ))
                updated_count += 1
            else:
                # Insert new listing
                cur.execute("""
                    INSERT INTO listings (
                        id, ad_number, link_token, street, property_type,
                        description_line, city, neighborhood, price, price_numeric,
                        currency, rooms, floor, size_sqm, date_added, updated_at,
                        contact_name, is_merchant, merchant_name, latitude, longitude,
                        image_url, images_count, amenities, raw_data, first_seen_at,
                        last_seen_at, is_active
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, TRUE
                    )
                """, (
                    listing["id"],
                    listing.get("ad_number"),
                    listing.get("link_token"),
                    listing.get("street"),
                    listing.get("property_type"),
                    listing.get("description_line"),
                    listing.get("city"),
                    listing.get("neighborhood"),
                    listing.get("price"),
                    price_numeric,
                    listing.get("currency"),
                    listing.get("rooms"),
                    listing.get("floor"),
                    listing.get("size_sqm"),
                    date_added,
                    listing.get("updated_at"),
                    listing.get("contact_name"),
                    listing.get("is_merchant", False),
                    listing.get("merchant_name"),
                    lat,
                    lon,
                    listing.get("image_url"),
                    listing.get("images_count", 0),
                    json.dumps(listing.get("amenities", {})),
                    json.dumps(listing),
                    now,
                    now
                ))
                new_count += 1

                # Record initial price in history
                if price_numeric is not None:
                    cur.execute("""
                        INSERT INTO price_history (listing_id, price, price_numeric, recorded_at, scrape_run_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (listing["id"], price_str, price_numeric, now, run_id))

        conn.commit()
        cur.close()
    finally:
        conn.close()

    return new_count, updated_count, price_changes


def mark_inactive_listings_by_run(run_started_at):
    """Mark listings not seen during a full scrape as inactive.
    Uses the run's start time to find which listings were updated during this run."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE listings
            SET is_active = FALSE
            WHERE is_active = TRUE AND last_seen_at < %s
        """, (run_started_at,))
        affected = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        conn.close()

    if affected > 0:
        logger.info(f"Marked {affected} listings as inactive (not seen since {run_started_at})")


def start_scrape_run(run_type: str = "full") -> int:
    """Create a new scrape run record."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO scrape_runs (started_at, status, run_type) VALUES (NOW(), 'running', %s) RETURNING id",
            (run_type,)
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return run_id
    finally:
        conn.close()


def get_completed_run_count() -> int:
    """Get number of completed scrape runs."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM scrape_runs WHERE status = 'completed'")
        count = cur.fetchone()[0]
        cur.close()
        return count
    finally:
        conn.close()


def finish_scrape_run(run_id: int, total_pages: int, pages_scraped: int,
                       pages_failed: int, listings_found: int,
                       listings_new: int, listings_updated: int,
                       price_changes: int = 0,
                       status: str = "completed", error: str = None):
    """Update scrape run with final stats."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE scrape_runs SET
                finished_at = NOW(),
                total_pages = %s,
                pages_scraped = %s,
                pages_failed = %s,
                listings_found = %s,
                listings_new = %s,
                listings_updated = %s,
                price_changes = %s,
                status = %s,
                error_message = %s
            WHERE id = %s
        """, (total_pages, pages_scraped, pages_failed, listings_found,
              listings_new, listings_updated, price_changes, status, error, run_id))
        conn.commit()
        cur.close()
    finally:
        conn.close()

    # Send Telegram summary on completion
    if status == "completed":
        try:
            conn2 = get_db_connection()
            try:
                cur2 = conn2.cursor()
                cur2.execute(
                    "SELECT run_type, EXTRACT(EPOCH FROM (finished_at - started_at))/60 as dur "
                    "FROM scrape_runs WHERE id = %s", (run_id,))
                row = cur2.fetchone()
                run_type = (row[0] or "full") if row else "full"
                dur = int(row[1] or 0) if row else 0
                cur2.close()
            finally:
                conn2.close()

            notify_scrape_summary(run_id, run_type, pages_scraped, total_pages,
                                  listings_new, listings_updated, price_changes, dur)
        except Exception as e:
            logger.warning(f"Telegram summary failed: {e}")


# ─── SCRAPER ────────────────────────────────────────────────────────────────────

class Yad2Scraper:
    def __init__(self):
        self.session = None
        self.current_platform = "Windows"
        self.requests_this_session = 0

    def _new_session(self) -> bool:
        """Create fresh session with browser impersonation."""
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass

        profile, platform = random.choice(BROWSER_PROFILES)
        self.session = curl_requests.Session(impersonate=profile)
        self.current_platform = platform
        self.requests_this_session = 0

        headers = self._headers(is_api=False, platform=platform)
        try:
            resp = self.session.get("https://www.yad2.co.il/", headers=headers, timeout=20)
            self.requests_this_session += 1
            cookies = list(self.session.cookies.keys())
            logger.info(f"New session: {profile}/{platform}, cookies={cookies}")
            return not self._is_blocked(resp)
        except Exception as e:
            logger.error(f"Session creation failed: {e}")
            return False

    def _headers(self, is_api: bool = False, platform: str = "Windows") -> dict:
        platform_map = {"Windows": '"Windows"', "Mac": '"macOS"', "Linux": '"Linux"'}
        plat = platform_map.get(platform, '"Windows"')

        if is_api:
            return {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.yad2.co.il/realestate/rent",
                "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": plat,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": plat,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

    def _is_blocked(self, resp) -> bool:
        if resp.status_code in (403, 429, 503):
            return True
        text_lower = resp.text[:5000].lower()
        if "shieldsquare captcha" in text_lower:
            return True
        if len(resp.text) < 30000 and "access denied" in text_lower:
            return True
        return False

    def _fetch_page(self, page: int) -> Optional[dict]:
        """Fetch a single page via API."""
        url = "https://www.yad2.co.il/api/pre-load/getFeedIndex/realestate/rent"
        params = {"page": str(page)} if page > 1 else {}
        headers = self._headers(is_api=True, platform=self.current_platform)

        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=20)
            self.requests_this_session += 1
        except Exception as e:
            logger.error(f"Page {page} request error: {e}")
            return None

        if self._is_blocked(resp):
            logger.warning(f"Page {page} BLOCKED (status={resp.status_code})")
            return None

        if resp.status_code != 200:
            logger.warning(f"Page {page} HTTP {resp.status_code}")
            return None

        try:
            return resp.json()
        except json.JSONDecodeError:
            logger.error(f"Page {page} invalid JSON")
            return None

    def _extract_listings(self, data: dict) -> list[dict]:
        """Extract listings from API response."""
        listings = []
        for item in data.get("feed", {}).get("feed_items", []):
            if not isinstance(item, dict) or item.get("type") != "ad":
                continue

            images = item.get("images_urls", []) or []
            row4 = {}
            for e in (item.get("row_4") or []):
                if isinstance(e, dict):
                    row4[e.get("key", "")] = e.get("value", "")

            listing = {
                "id": item.get("id", item.get("link_token", "")),
                "ad_number": item.get("ad_number", ""),
                "link_token": item.get("link_token", ""),
                "street": item.get("title_1", item.get("street", "")),
                "property_type": item.get("title_2", item.get("HomeTypeID_text", "")),
                "description_line": item.get("row_2", ""),
                "city": item.get("city", ""),
                "neighborhood": item.get("neighborhood", ""),
                "price": item.get("price", ""),
                "currency": item.get("currency", ""),
                "rooms": item.get("Rooms_text", str(row4.get("rooms", ""))),
                "floor": str(row4.get("floor", item.get("line_2", ""))),
                "size_sqm": item.get("square_meters", str(row4.get("SquareMeter", ""))),
                "date_added": item.get("date_added", ""),
                "updated_at": item.get("updated_at", ""),
                "contact_name": item.get("contact_name", ""),
                "is_merchant": item.get("merchant", False),
                "merchant_name": item.get("merchant_name", ""),
                "coordinates": item.get("coordinates", {}),
                "image_url": images[0] if images else "",
                "images_count": len(images),
                "amenities": {
                    "parking": item.get("Parking_text", ""),
                    "elevator": item.get("Elevator_text", ""),
                    "ac": item.get("AirConditioner_text", ""),
                    "mamad": item.get("mamad_text", ""),
                    "storage": item.get("storeroom_text", ""),
                },
            }
            if listing["id"]:
                listings.append(listing)

        return listings

    def _get_total_pages(self, data: dict) -> int:
        feed = data.get("feed", {})
        total = feed.get("total_pages", 0)
        if total:
            return int(total)
        pag = data.get("pagination", {})
        return int(pag.get("last_page", 0))

    def run_full_scrape(self, resume: dict = None) -> dict:
        """Scrape entire site with batch session rotation. Supports resume."""
        if resume:
            run_id = resume["run_id"]
            pages_scraped = resume["pages_scraped"]
            pages_failed = resume["pages_failed"]
            total_new = resume["listings_new"]
            total_updated = resume["listings_updated"]
            total_price_changes = resume["price_changes"]
            scraped_pages = resume["scraped_pages"]
            total_pages = resume["total_pages"]
            run_started_at = resume["started_at"]
            logger.info(f"RESUMING full scrape run #{run_id} — {pages_scraped}/{total_pages} pages done, "
                       f"resuming from {len(scraped_pages)} completed pages")
        else:
            run_id = start_scrape_run(run_type="full")
            pages_scraped = 0
            pages_failed = 0
            total_new = 0
            total_updated = 0
            total_price_changes = 0
            scraped_pages = set()
            total_pages = 0
            run_started_at = datetime.now(timezone.utc)
            logger.info(f"Starting FULL scrape run #{run_id}")

        try:
            # Need a session
            if not self._new_session():
                raise Exception("Cannot create initial session")
            time.sleep(random.uniform(2, 4))

            # If not resuming or total_pages unknown, fetch page 1 to discover total
            if total_pages == 0:
                data = self._fetch_page(1)
                if not data:
                    raise Exception("Cannot fetch page 1")

                listings = self._extract_listings(data)
                pages_scraped += 1
                scraped_pages.add(1)

                total_pages = self._get_total_pages(data)
                logger.info(f"Page 1: {len(listings)} listings, total pages: {total_pages}")

                new, updated, price_changes = save_listings(listings, run_id)
                total_new += new
                total_updated += updated
                total_price_changes += price_changes

                update_scrape_progress(run_id, pages_scraped, pages_failed,
                                       pages_scraped * 36, total_new, total_updated,
                                       total_price_changes, 1, scraped_pages, total_pages)

            # Build remaining pages (exclude already scraped)
            remaining = [p for p in range(1, total_pages + 1) if p not in scraped_pages]
            random.shuffle(remaining)
            logger.info(f"Pages remaining: {len(remaining)}/{total_pages}")
            attempt = 0

            while remaining and attempt < MAX_RETRIES:
                attempt += 1
                if attempt > 1:
                    logger.info(f"Retry round {attempt}, {len(remaining)} pages remaining")
                    random.shuffle(remaining)

                batches = [remaining[i:i+PAGES_PER_SESSION]
                          for i in range(0, len(remaining), PAGES_PER_SESSION)]
                next_remaining = []

                for batch_idx, batch in enumerate(batches):
                    if batch_idx > 0 or attempt > 1:
                        cooldown = random.uniform(*DELAY_BETWEEN_BATCHES)
                        logger.info(f"Cooldown {cooldown:.0f}s before batch {batch_idx+1}/{len(batches)}")
                        time.sleep(cooldown)

                    if not self._new_session():
                        logger.warning("Session creation failed, retry later")
                        next_remaining.extend(batch)
                        continue

                    time.sleep(random.uniform(2, 4))
                    batch_blocked = False

                    for page in batch:
                        if batch_blocked:
                            next_remaining.append(page)
                            continue

                        delay = random.uniform(*DELAY_WITHIN_BATCH)
                        time.sleep(delay)

                        page_data = self._fetch_page(page)

                        if page_data is None:
                            next_remaining.append(page)
                            batch_blocked = True
                            pages_failed += 1
                            continue

                        page_listings = self._extract_listings(page_data)
                        pages_scraped += 1
                        scraped_pages.add(page)

                        new, updated, price_changes = save_listings(page_listings, run_id)
                        total_new += new
                        total_updated += updated
                        total_price_changes += price_changes

                        # Persist progress after each page
                        update_scrape_progress(run_id, pages_scraped, pages_failed,
                                               pages_scraped * 36, total_new, total_updated,
                                               total_price_changes, page, scraped_pages)

                        logger.info(f"Page {page}: {len(page_listings)} listings "
                                   f"(scraped: {pages_scraped}/{total_pages}, new: {new}, updated: {updated}, price_changes: {price_changes})")

                remaining = next_remaining

            final_failed = len(remaining)

            # Only mark listings inactive if we scraped most pages successfully
            # (>= 80% of total pages), otherwise we'd incorrectly deactivate valid listings
            success_rate = pages_scraped / total_pages if total_pages > 0 else 0
            if success_rate >= 0.8:
                mark_inactive_listings_by_run(run_started_at)
            else:
                logger.warning(f"Skipping mark_inactive: only scraped {pages_scraped}/{total_pages} "
                              f"pages ({success_rate:.0%}), need >= 80%")
            finish_scrape_run(
                run_id, total_pages, pages_scraped, final_failed,
                pages_scraped * 36, total_new, total_updated, total_price_changes, "completed"
            )

            logger.info(f"Full scrape #{run_id} completed: "
                       f"{pages_scraped}/{total_pages} pages, {total_new} new, {total_updated} updated, {total_price_changes} price changes")

            return {
                "run_id": run_id,
                "total_pages": total_pages,
                "pages_scraped": pages_scraped,
                "pages_failed": final_failed,
                "listings_new": total_new,
                "listings_updated": total_updated,
                "price_changes": total_price_changes,
                "status": "completed",
                "run_type": "full"
            }

        except Exception as e:
            logger.error(f"Full scrape failed: {e}")
            finish_scrape_run(run_id, total_pages, pages_scraped, pages_failed,
                            pages_scraped * 36, total_new, total_updated,
                            total_price_changes, "failed", str(e))
            raise

    def run_smart_scrape(self, resume: dict = None) -> dict:
        """Smart scrape: scrape pages sequentially, stop when no new listings found. Supports resume."""
        if resume:
            run_id = resume["run_id"]
            pages_scraped = resume["pages_scraped"]
            pages_failed = resume["pages_failed"]
            total_new = resume["listings_new"]
            total_updated = resume["listings_updated"]
            total_price_changes = resume["price_changes"]
            total_listings_found = resume["listings_found"]
            start_page = resume["last_page_scraped"] + 1
            total_pages = resume["total_pages"]
            consecutive_no_new = 0  # Reset on resume — safe, just means we re-check
            scraped_pages = resume["scraped_pages"]
            logger.info(f"RESUMING smart scrape run #{run_id} — continuing from page {start_page}")
        else:
            run_id = start_scrape_run(run_type="smart")
            pages_scraped = 0
            pages_failed = 0
            total_new = 0
            total_updated = 0
            total_price_changes = 0
            total_listings_found = 0
            consecutive_no_new = 0
            start_page = 1
            total_pages = 0
            scraped_pages = set()
            logger.info(f"Starting SMART scrape run #{run_id} (stop after {SMART_STOP_THRESHOLD} pages with no new listings)")

        try:
            if not self._new_session():
                raise Exception("Cannot create initial session")
            time.sleep(random.uniform(2, 4))

            # If starting fresh, fetch page 1 to discover total_pages
            if start_page == 1:
                data = self._fetch_page(1)
                if not data:
                    raise Exception("Cannot fetch page 1")

                listings = self._extract_listings(data)
                pages_scraped += 1
                total_listings_found += len(listings)
                scraped_pages.add(1)

                total_pages = self._get_total_pages(data)
                logger.info(f"Smart scrape page 1: {len(listings)} listings, total pages available: {total_pages}")

                new, updated, price_changes = save_listings(listings, run_id)
                total_new += new
                total_updated += updated
                total_price_changes += price_changes

                if new == 0:
                    consecutive_no_new += 1
                else:
                    consecutive_no_new = 0

                update_scrape_progress(run_id, pages_scraped, pages_failed,
                                       total_listings_found, total_new, total_updated,
                                       total_price_changes, 1, scraped_pages, total_pages)
                start_page = 2

            # If resuming and we don't know total_pages, fetch page 1 just for that
            if total_pages == 0:
                data = self._fetch_page(1)
                if data:
                    total_pages = self._get_total_pages(data)
                else:
                    raise Exception("Cannot determine total pages")

            page = start_page
            while page <= total_pages:
                if consecutive_no_new >= SMART_STOP_THRESHOLD:
                    logger.info(f"Smart scrape stopping: {SMART_STOP_THRESHOLD} consecutive pages with no new listings (at page {page})")
                    break

                # Rotate session
                if self.requests_this_session >= PAGES_PER_SESSION:
                    cooldown = random.uniform(*DELAY_BETWEEN_BATCHES)
                    logger.info(f"Session rotation cooldown {cooldown:.0f}s")
                    time.sleep(cooldown)

                    if not self._new_session():
                        logger.warning("Session creation failed, waiting and retrying...")
                        time.sleep(random.uniform(30, 60))
                        if not self._new_session():
                            pages_failed += 1
                            page += 1
                            continue
                    time.sleep(random.uniform(2, 4))

                delay = random.uniform(*DELAY_WITHIN_BATCH)
                time.sleep(delay)

                page_data = self._fetch_page(page)

                if page_data is None:
                    pages_failed += 1
                    logger.warning(f"Smart scrape page {page} failed, rotating session")
                    time.sleep(random.uniform(*DELAY_BETWEEN_BATCHES))
                    if self._new_session():
                        time.sleep(random.uniform(2, 4))
                        page_data = self._fetch_page(page)

                    if page_data is None:
                        page += 1
                        continue

                page_listings = self._extract_listings(page_data)
                pages_scraped += 1
                total_listings_found += len(page_listings)
                scraped_pages.add(page)

                new, updated, price_changes = save_listings(page_listings, run_id)
                total_new += new
                total_updated += updated
                total_price_changes += price_changes

                if new == 0:
                    consecutive_no_new += 1
                else:
                    consecutive_no_new = 0

                # Persist progress after each page
                update_scrape_progress(run_id, pages_scraped, pages_failed,
                                       total_listings_found, total_new, total_updated,
                                       total_price_changes, page, scraped_pages)

                logger.info(f"Smart page {page}: {len(page_listings)} listings "
                           f"(new: {new}, updated: {updated}, consecutive_no_new: {consecutive_no_new}/{SMART_STOP_THRESHOLD})")

                page += 1

            # Do NOT mark inactive listings on smart scrape

            finish_scrape_run(
                run_id, total_pages, pages_scraped, pages_failed,
                total_listings_found, total_new, total_updated,
                total_price_changes, "completed"
            )

            logger.info(f"Smart scrape #{run_id} completed: {pages_scraped}/{total_pages} pages, "
                       f"{total_listings_found} listings seen, {total_new} new, "
                       f"{total_updated} updated, {total_price_changes} price changes")

            return {
                "run_id": run_id,
                "total_pages": total_pages,
                "pages_scraped": pages_scraped,
                "pages_failed": pages_failed,
                "listings_found": total_listings_found,
                "listings_new": total_new,
                "listings_updated": total_updated,
                "price_changes": total_price_changes,
                "status": "completed",
                "run_type": "smart"
            }

        except Exception as e:
            logger.error(f"Smart scrape failed: {e}")
            finish_scrape_run(run_id, total_pages, pages_scraped, pages_failed,
                            total_listings_found, total_new, total_updated,
                            total_price_changes, "failed", str(e))
            raise


# ─── MAIN LOOP ──────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Yad2 Scraper Service Starting")
    logger.info(f"Scrape interval: {SCRAPE_INTERVAL_SECONDS}s")
    logger.info(f"Pages per session: {PAGES_PER_SESSION}")
    logger.info(f"Smart stop threshold: {SMART_STOP_THRESHOLD} pages")
    logger.info(f"Full scrape every: {FULL_SCRAPE_EVERY} runs")
    logger.info("=" * 60)

    # Initialize database
    init_db()

    scraper = Yad2Scraper()
    force_full = os.environ.get("FORCE_FULL_SCRAPE", "").lower() in ("1", "true", "yes")

    # Restore run_counter from DB
    run_counter = int(get_state("run_counter", "0"))
    logger.info(f"Restored run_counter={run_counter} from DB")

    # Clean up stale runs with no progress, then check for resumable ones
    cleanup_stale_runs()

    # One-time fix: re-activate listings wrongly marked inactive by incomplete full scrape
    # Run #14 only scraped 163/833 pages but still called mark_inactive, deactivating ~17k listings
    if get_state("fix_reactivate_v1", "0") == "0":
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE listings SET is_active = TRUE WHERE is_active = FALSE")
            reactivated = cur.rowcount
            conn.commit()
            cur.close()
        finally:
            conn.close()
        set_state("fix_reactivate_v1", "1")
        if reactivated > 0:
            logger.info(f"One-time fix: re-activated {reactivated} listings wrongly marked inactive")

    interrupted = get_interrupted_run()
    if interrupted:
        logger.info(f"Found interrupted {interrupted['run_type']} scrape run #{interrupted['run_id']} "
                    f"({interrupted['pages_scraped']}/{interrupted['total_pages']} pages done)")
        try:
            if interrupted["run_type"] == "smart":
                result = scraper.run_smart_scrape(resume=interrupted)
            else:
                result = scraper.run_full_scrape(resume=interrupted)
            logger.info(f"Resumed scrape completed: {result}")
        except Exception as e:
            logger.error(f"Resume scrape error: {e}")

        run_counter += 1
        set_state("run_counter", str(run_counter))

    while True:
        try:
            completed_runs = get_completed_run_count()
            is_first_run = completed_runs == 0
            is_full_cycle = run_counter % FULL_SCRAPE_EVERY == 0

            if force_full or is_first_run or is_full_cycle:
                reason = "FORCE_FULL_SCRAPE env" if force_full else ("first run ever" if is_first_run else f"every {FULL_SCRAPE_EVERY} runs")
                logger.info(f"Starting FULL scrape (reason: {reason})...")
                result = scraper.run_full_scrape()
                force_full = False
            else:
                logger.info(f"Starting SMART scrape (run {run_counter}, next full at {((run_counter // FULL_SCRAPE_EVERY) + 1) * FULL_SCRAPE_EVERY})...")
                result = scraper.run_smart_scrape()

            logger.info(f"Scrape completed: {result}")
        except Exception as e:
            logger.error(f"Scrape error: {e}")

        run_counter += 1
        set_state("run_counter", str(run_counter))
        logger.info(f"Sleeping {SCRAPE_INTERVAL_SECONDS}s until next scrape...")
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
