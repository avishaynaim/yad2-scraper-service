#!/usr/bin/env python3
"""
Yad2 Full Site Scraper Service
==============================
Scrapes ALL rental listings from yad2.co.il/realestate/rent
Stores in PostgreSQL, runs on Railway with scheduled intervals.

Based on investigation findings:
- Radware Bot Manager protection
- 5 requests per session limit
- Session rotation with homepage warmup
"""

import json
import os
import random
import time
import logging
from datetime import datetime, timezone
from typing import Optional

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
            conn = get_db_connection()
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
    cur = conn.cursor()

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_runs (
            id SERIAL PRIMARY KEY,
            started_at TIMESTAMP DEFAULT NOW(),
            finished_at TIMESTAMP,
            total_pages INTEGER,
            pages_scraped INTEGER,
            pages_failed INTEGER,
            listings_found INTEGER,
            listings_new INTEGER,
            listings_updated INTEGER,
            status VARCHAR(50) DEFAULT 'running',
            error_message TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database initialized")


def save_listings(listings: list[dict], run_id: int) -> tuple[int, int]:
    """Save listings to database. Returns (new_count, updated_count)."""
    if not listings:
        return 0, 0

    conn = get_db_connection()
    cur = conn.cursor()

    now = datetime.now(timezone.utc)
    new_count = 0
    updated_count = 0

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

        # Check if exists
        cur.execute("SELECT id FROM listings WHERE id = %s", (listing["id"],))
        exists = cur.fetchone()

        if exists:
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
            # Insert new
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

    conn.commit()
    cur.close()
    conn.close()

    return new_count, updated_count


def mark_inactive_listings(seen_ids: set[str]):
    """Mark listings not seen in this run as inactive."""
    if not seen_ids:
        return

    conn = get_db_connection()
    cur = conn.cursor()

    # Mark as inactive if not seen and was previously active
    placeholders = ",".join(["%s"] * len(seen_ids))
    cur.execute(f"""
        UPDATE listings
        SET is_active = FALSE
        WHERE is_active = TRUE AND id NOT IN ({placeholders})
    """, tuple(seen_ids))

    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    if affected > 0:
        logger.info(f"Marked {affected} listings as inactive")


def start_scrape_run() -> int:
    """Create a new scrape run record."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scrape_runs (started_at, status) VALUES (NOW(), 'running') RETURNING id"
    )
    run_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return run_id


def finish_scrape_run(run_id: int, total_pages: int, pages_scraped: int,
                       pages_failed: int, listings_found: int,
                       listings_new: int, listings_updated: int,
                       status: str = "completed", error: str = None):
    """Update scrape run with final stats."""
    conn = get_db_connection()
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
            status = %s,
            error_message = %s
        WHERE id = %s
    """, (total_pages, pages_scraped, pages_failed, listings_found,
          listings_new, listings_updated, status, error, run_id))
    conn.commit()
    cur.close()
    conn.close()


# ─── SCRAPER ────────────────────────────────────────────────────────────────────

class Yad2Scraper:
    def __init__(self):
        self.session = None
        self.requests_this_session = 0

    def _new_session(self) -> bool:
        """Create fresh session with browser impersonation."""
        profile, platform = random.choice(BROWSER_PROFILES)
        self.session = curl_requests.Session(impersonate=profile)
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
        headers = self._headers(is_api=True)

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

    def run_full_scrape(self) -> dict:
        """Scrape entire site with batch session rotation."""
        run_id = start_scrape_run()
        logger.info(f"Starting scrape run #{run_id}")

        all_listings: dict[str, dict] = {}
        pages_scraped = 0
        pages_failed = 0
        total_new = 0
        total_updated = 0

        try:
            # Get first page to discover total
            if not self._new_session():
                raise Exception("Cannot create initial session")

            time.sleep(random.uniform(2, 4))
            data = self._fetch_page(1)

            if not data:
                raise Exception("Cannot fetch page 1")

            listings = self._extract_listings(data)
            for l in listings:
                all_listings[l["id"]] = l
            pages_scraped += 1

            total_pages = self._get_total_pages(data)
            logger.info(f"Page 1: {len(listings)} listings, total pages: {total_pages}")

            # Save page 1 immediately
            new, updated = save_listings(listings, run_id)
            total_new += new
            total_updated += updated

            # Build page queue
            remaining = list(range(2, total_pages + 1))
            random.shuffle(remaining)
            attempt = 0

            while remaining and attempt < MAX_RETRIES:
                attempt += 1
                if attempt > 1:
                    logger.info(f"Retry round {attempt}, {len(remaining)} pages remaining")
                    random.shuffle(remaining)

                # Split into batches
                batches = [remaining[i:i+PAGES_PER_SESSION]
                          for i in range(0, len(remaining), PAGES_PER_SESSION)]
                next_remaining = []

                for batch_idx, batch in enumerate(batches):
                    # New session for each batch
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
                        for l in page_listings:
                            all_listings[l["id"]] = l
                        pages_scraped += 1

                        # Save incrementally
                        new, updated = save_listings(page_listings, run_id)
                        total_new += new
                        total_updated += updated

                        logger.info(f"Page {page}: {len(page_listings)} listings "
                                   f"(total: {len(all_listings)}, new: {new}, updated: {updated})")

                remaining = next_remaining

            # Mark listings not seen as inactive
            mark_inactive_listings(set(all_listings.keys()))

            # Final stats
            final_failed = len(remaining)
            finish_scrape_run(
                run_id, total_pages, pages_scraped, final_failed,
                len(all_listings), total_new, total_updated, "completed"
            )

            logger.info(f"Scrape #{run_id} completed: {len(all_listings)} listings, "
                       f"{pages_scraped}/{total_pages} pages, {total_new} new, {total_updated} updated")

            return {
                "run_id": run_id,
                "total_pages": total_pages,
                "pages_scraped": pages_scraped,
                "pages_failed": final_failed,
                "listings_found": len(all_listings),
                "listings_new": total_new,
                "listings_updated": total_updated,
                "status": "completed"
            }

        except Exception as e:
            logger.error(f"Scrape failed: {e}")
            finish_scrape_run(run_id, 0, pages_scraped, pages_failed,
                            len(all_listings), total_new, total_updated,
                            "failed", str(e))
            raise


# ─── MAIN LOOP ──────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Yad2 Scraper Service Starting")
    logger.info(f"Scrape interval: {SCRAPE_INTERVAL_SECONDS}s")
    logger.info(f"Pages per session: {PAGES_PER_SESSION}")
    logger.info("=" * 60)

    # Initialize database
    init_db()

    scraper = Yad2Scraper()

    while True:
        try:
            logger.info("Starting scheduled scrape...")
            result = scraper.run_full_scrape()
            logger.info(f"Scrape completed: {result}")
        except Exception as e:
            logger.error(f"Scrape error: {e}")

        logger.info(f"Sleeping {SCRAPE_INTERVAL_SECONDS}s until next scrape...")
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
