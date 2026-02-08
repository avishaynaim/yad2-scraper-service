#!/usr/bin/env python3
"""
REST API for accessing scraped Yad2 data.
Run alongside the scraper for data access.
"""

import os
import logging

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder=None)
CORS(app)


@app.route("/dashboard")
def dashboard():
    return send_from_directory('.', 'dashboard.html')

DATABASE_URL = os.environ.get("DATABASE_URL")

# Connection pool (min 2, max 10 connections)
_db_pool = None


def _get_pool():
    global _db_pool
    if _db_pool is None or _db_pool.closed:
        _db_pool = pool.ThreadedConnectionPool(2, 10, DATABASE_URL, cursor_factory=RealDictCursor)
    return _db_pool


def get_db():
    try:
        return _get_pool().getconn()
    except Exception:
        # Fallback to direct connection if pool fails
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def put_db(conn):
    """Return connection to pool."""
    try:
        conn.rollback()  # Reset any aborted transaction state before returning to pool
        _get_pool().putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


_price_history_initialized = False


def ensure_price_history_table():
    """Create price_history table if it doesn't exist."""
    global _price_history_initialized
    if _price_history_initialized:
        return
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id SERIAL PRIMARY KEY,
                listing_id VARCHAR(50) NOT NULL,
                price VARCHAR(50),
                price_numeric INTEGER,
                recorded_at TIMESTAMP DEFAULT NOW(),
                scrape_run_id INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_price_history_listing ON price_history(listing_id);
            CREATE INDEX IF NOT EXISTS idx_price_history_recorded ON price_history(recorded_at);
        """)
        conn.commit()
        cur.close()
        _price_history_initialized = True
    except Exception as e:
        logging.warning(f"Error creating price_history table: {e}")
    finally:
        if conn:
            put_db(conn)


@app.route("/")
def index():
    return jsonify({
        "service": "Yad2 Scraper API",
        "dashboard": "/dashboard",
        "endpoints": {
            "/dashboard": "GET - Interactive dashboard UI",
            "/listings": "GET - List all active listings with filters",
            "/listings/<id>": "GET - Get single listing by ID",
            "/listings/<id>/price-history": "GET - Get price history for a listing",
            "/price-changes": "GET - Get recent price changes across all listings",
            "/stats": "GET - Get database statistics",
            "/runs": "GET - Get scrape run history",
            "/cities": "GET - List all cities",
            "/neighborhoods": "GET - List neighborhoods (optional city filter)",
            "/analytics/neighborhoods": "GET - Price analytics per neighborhood",
            "/analytics/price-map": "GET - Listings with coordinates for map",
            "/analytics/trends": "GET - Daily price trends over time",
            "/analytics/deals": "GET - Find listings priced below neighborhood average",
            "/analytics/market-summary": "GET - High-level market overview with price distribution",
            "/analytics/city/<name>": "GET - Detailed stats for a specific city",
            "/analytics/compare": "GET - Compare two neighborhoods side by side",
            "/analytics/stale": "GET - Find longest-on-market listings",
            "/analytics/price-drops": "GET - Neighborhoods with most price drops",
            "/health": "GET - Health check"
        }
    })


@app.route("/health")
def health():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": "database connection failed"}), 500
    finally:
        if conn:
            put_db(conn)


@app.route("/stats")
def stats():
    ensure_price_history_table()

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*) as total_listings,
                COUNT(*) FILTER (WHERE is_active) as active_listings,
                COUNT(DISTINCT city) as cities,
                COUNT(DISTINCT neighborhood) as neighborhoods,
                MIN(price_numeric) FILTER (WHERE price_numeric > 0) as min_price,
                MAX(price_numeric) as max_price,
                ROUND(AVG(price_numeric) FILTER (WHERE price_numeric > 0)) as avg_price,
                MIN(first_seen_at) as oldest_listing,
                MAX(last_seen_at) as newest_update
            FROM listings
        """)
        result = dict(cur.fetchone())

        cur.execute("""
            SELECT city, COUNT(*) as count
            FROM listings
            WHERE is_active = TRUE
            GROUP BY city
            ORDER BY count DESC
            LIMIT 20
        """)
        result["top_cities"] = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT rooms, COUNT(*) as count
            FROM listings
            WHERE is_active = TRUE
            GROUP BY rooms
            ORDER BY rooms
        """)
        result["rooms_distribution"] = [dict(r) for r in cur.fetchall()]

        try:
            cur.execute("""
                SELECT
                    COUNT(*) as total_price_records,
                    COUNT(DISTINCT listing_id) as listings_with_history,
                    COUNT(*) FILTER (WHERE recorded_at > NOW() - INTERVAL '24 hours') as changes_last_24h,
                    COUNT(*) FILTER (WHERE recorded_at > NOW() - INTERVAL '7 days') as changes_last_7d
                FROM price_history
            """)
            result["price_history"] = dict(cur.fetchone())
        except Exception:
            result["price_history"] = {
                "total_price_records": 0,
                "listings_with_history": 0,
                "changes_last_24h": 0,
                "changes_last_7d": 0
            }

        cur.close()
        return jsonify(result)

    except Exception as e:
        return jsonify({
            "error": "Failed to load stats",
            "total_listings": 0,
            "active_listings": 0,
            "cities": 0,
            "neighborhoods": 0,
            "top_cities": [],
            "rooms_distribution": [],
            "price_history": {
                "total_price_records": 0,
                "listings_with_history": 0,
                "changes_last_24h": 0,
                "changes_last_7d": 0
            }
        }), 500
    finally:
        if conn:
            put_db(conn)


@app.route("/listings")
def list_listings():
    # Query params
    city = request.args.get("city")
    neighborhood = request.args.get("neighborhood")
    min_price = request.args.get("min_price", type=int)
    max_price = request.args.get("max_price", type=int)
    min_rooms = request.args.get("min_rooms", type=int)
    max_rooms = request.args.get("max_rooms", type=int)
    is_merchant = request.args.get("is_merchant")
    active_only = request.args.get("active_only", "true").lower() == "true"
    limit = min(request.args.get("limit", 100, type=int), 1000)
    offset = max(request.args.get("offset", 0, type=int), 0)
    sort_by = request.args.get("sort_by", "last_seen_at")
    sort_order = request.args.get("sort_order", "desc")

    # Build query
    conditions = []
    params = []

    if active_only:
        conditions.append("is_active = TRUE")

    if city:
        conditions.append("city ILIKE %s")
        params.append(f"%{city}%")

    if neighborhood:
        conditions.append("neighborhood ILIKE %s")
        params.append(f"%{neighborhood}%")

    if min_price:
        conditions.append("price_numeric >= %s")
        params.append(min_price)

    if max_price:
        conditions.append("price_numeric <= %s")
        params.append(max_price)

    if min_rooms:
        conditions.append("CASE WHEN rooms ~ '^[0-9.]+$' THEN rooms::numeric ELSE 0 END >= %s")
        params.append(min_rooms)

    if max_rooms:
        conditions.append("CASE WHEN rooms ~ '^[0-9.]+$' THEN rooms::numeric ELSE 0 END <= %s")
        params.append(max_rooms)

    if is_merchant is not None:
        conditions.append("is_merchant = %s")
        params.append(is_merchant.lower() == "true")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # Validate sort
    allowed_sorts = ["last_seen_at", "first_seen_at", "price_numeric", "date_added", "rooms"]
    if sort_by not in allowed_sorts:
        sort_by = "last_seen_at"
    sort_order = "DESC" if sort_order.lower() == "desc" else "ASC"

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        # Get total count
        cur.execute(f"SELECT COUNT(*) FROM listings WHERE {where_clause}", params)
        total = cur.fetchone()["count"]

        # Get listings
        cur.execute(f"""
            SELECT id, ad_number, link_token, street, property_type,
                   description_line, city, neighborhood, price, price_numeric,
                   rooms, floor, size_sqm, date_added, updated_at,
                   contact_name, is_merchant, merchant_name,
                   latitude, longitude, image_url, images_count,
                   amenities, first_seen_at, last_seen_at, is_active
            FROM listings
            WHERE {where_clause}
            ORDER BY {sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        listings = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "count": len(listings),
        "listings": listings
    })


@app.route("/listings/<listing_id>")
def get_listing(listing_id):
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT * FROM listings WHERE id = %s", (listing_id,))
        listing = cur.fetchone()

        if not listing:
            cur.close()
            return jsonify({"error": "Listing not found"}), 404

        result = dict(listing)

        # Get price history count
        try:
            cur.execute(
                "SELECT COUNT(*) as count FROM price_history WHERE listing_id = %s",
                (listing_id,)
            )
            result["price_history_count"] = cur.fetchone()["count"]
        except Exception:
            result["price_history_count"] = 0

        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify(result)


@app.route("/listings/<listing_id>/price-history")
def get_listing_price_history(listing_id):
    """Get price history for a specific listing."""
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT id, city, street, neighborhood FROM listings WHERE id = %s", (listing_id,))
        listing = cur.fetchone()

        if not listing:
            cur.close()
            return jsonify({"error": "Listing not found"}), 404

        try:
            cur.execute("""
                SELECT price, price_numeric, recorded_at, scrape_run_id
                FROM price_history
                WHERE listing_id = %s
                ORDER BY recorded_at ASC
            """, (listing_id,))
            history = [dict(r) for r in cur.fetchall()]
        except Exception:
            history = []

        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "listing_id": listing_id,
        "city": listing["city"],
        "street": listing["street"],
        "neighborhood": listing["neighborhood"],
        "price_changes": len(history) - 1 if history else 0,
        "history": history
    })


@app.route("/price-changes")
def list_price_changes():
    """Get recent price changes across all listings."""
    limit = min(request.args.get("limit", 100, type=int), 500)
    offset = request.args.get("offset", 0, type=int)
    city = request.args.get("city")
    days = request.args.get("days", 7, type=int)  # Default last 7 days

    days = max(1, min(days, 365))

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        # Build query with optional city filter
        conditions = [
            "ph.recorded_at > NOW() - make_interval(days => %s)"
        ]
        params = [days]

        if city:
            conditions.append("l.city ILIKE %s")
            params.append(f"%{city}%")

        where_clause = " AND ".join(conditions)

        # Get price changes using CTE to properly filter and paginate
        cur.execute(f"""
            WITH changes AS (
                SELECT
                    ph.listing_id,
                    l.link_token,
                    l.city,
                    l.street,
                    l.neighborhood,
                    l.rooms,
                    ph.price,
                    ph.price_numeric,
                    ph.recorded_at,
                    LAG(ph.price_numeric) OVER (
                        PARTITION BY ph.listing_id ORDER BY ph.recorded_at
                    ) as previous_price
                FROM price_history ph
                JOIN listings l ON l.id = ph.listing_id
                WHERE {where_clause}
            )
            SELECT * FROM changes
            WHERE previous_price IS NOT NULL
            ORDER BY recorded_at DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        changes = []
        for row in cur.fetchall():
            r = dict(row)
            r["price_diff"] = r["price_numeric"] - r["previous_price"]
            r["price_diff_percent"] = round(
                (r["price_diff"] / r["previous_price"]) * 100, 1
            ) if r["previous_price"] else 0
            changes.append(r)

        # Get total count of actual changes (not initial prices)
        cur.execute(f"""
            WITH changes AS (
                SELECT ph.listing_id,
                    LAG(ph.price_numeric) OVER (
                        PARTITION BY ph.listing_id ORDER BY ph.recorded_at
                    ) as previous_price
                FROM price_history ph
                JOIN listings l ON l.id = ph.listing_id
                WHERE {where_clause}
            )
            SELECT COUNT(*) as count FROM changes WHERE previous_price IS NOT NULL
        """, params)
        total = cur.fetchone()["count"]

        # Get summary stats
        cur.execute(f"""
            SELECT
                COUNT(DISTINCT ph.listing_id) as listings_with_changes,
                COUNT(*) as total_price_records
            FROM price_history ph
            JOIN listings l ON l.id = ph.listing_id
            WHERE {where_clause}
        """, params)
        summary = dict(cur.fetchone())

        cur.close()
    except Exception:
        changes = []
        total = 0
        summary = {"listings_with_changes": 0, "total_price_records": 0}
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "days": days,
        "count": len(changes),
        "summary": summary,
        "changes": changes
    })


@app.route("/runs")
def list_runs():
    limit = min(request.args.get("limit", 20, type=int), 100)

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, started_at, finished_at, total_pages, pages_scraped,
                   pages_failed, listings_found, listings_new, listings_updated,
                   price_changes, status, error_message, run_type, last_page_scraped
            FROM scrape_runs
            ORDER BY started_at DESC
            LIMIT %s
        """, (limit,))
        runs = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({"runs": runs})


@app.route("/cities")
def list_cities():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT city, COUNT(*) as listings_count
            FROM listings
            WHERE is_active = TRUE AND city IS NOT NULL
            GROUP BY city
            ORDER BY listings_count DESC
        """)
        cities = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({"cities": cities})


@app.route("/neighborhoods")
def list_neighborhoods():
    city = request.args.get("city")

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        if city:
            cur.execute("""
                SELECT neighborhood, COUNT(*) as listings_count
                FROM listings
                WHERE is_active = TRUE AND neighborhood IS NOT NULL AND city ILIKE %s
                GROUP BY neighborhood
                ORDER BY listings_count DESC
            """, (f"%{city}%",))
        else:
            cur.execute("""
                SELECT neighborhood, city, COUNT(*) as listings_count
                FROM listings
                WHERE is_active = TRUE AND neighborhood IS NOT NULL
                GROUP BY neighborhood, city
                ORDER BY listings_count DESC
                LIMIT 100
            """)
        neighborhoods = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({"neighborhoods": neighborhoods})


@app.route("/analytics/neighborhoods")
def neighborhood_analytics():
    """Get price analytics per neighborhood, optionally filtered by city."""
    city = request.args.get("city")
    min_listings = request.args.get("min_listings", 3, type=int)

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        conditions = ["is_active = TRUE", "price_numeric > 0", "neighborhood IS NOT NULL"]
        params = []

        if city:
            conditions.append("city ILIKE %s")
            params.append(f"%{city}%")

        where = " AND ".join(conditions)

        cur.execute(f"""
            SELECT
                city,
                neighborhood,
                COUNT(*) as listings_count,
                ROUND(AVG(price_numeric))::int as avg_price,
                MIN(price_numeric) as min_price,
                MAX(price_numeric) as max_price,
                (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_numeric))::int as median_price,
                ROUND(AVG(CASE WHEN size_sqm ~ '^[0-9]+$' AND size_sqm::int > 0
                    THEN price_numeric::numeric / size_sqm::int END))::int as avg_price_per_sqm,
                ROUND(AVG(CASE WHEN rooms ~ '^[0-9.]+$' THEN rooms::numeric END), 1) as avg_rooms
            FROM listings
            WHERE {where}
            GROUP BY city, neighborhood
            HAVING COUNT(*) >= %s
            ORDER BY avg_price ASC
        """, params + [min_listings])

        neighborhoods = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "count": len(neighborhoods),
        "min_listings": min_listings,
        "neighborhoods": neighborhoods
    })


@app.route("/analytics/price-map")
def price_map():
    """Get listings with coordinates for map visualization."""
    city = request.args.get("city")
    limit = min(request.args.get("limit", 500, type=int), 2000)

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        conditions = [
            "is_active = TRUE",
            "latitude IS NOT NULL",
            "longitude IS NOT NULL",
            "price_numeric > 0"
        ]
        params = []

        if city:
            conditions.append("city ILIKE %s")
            params.append(f"%{city}%")

        where = " AND ".join(conditions)

        cur.execute(f"""
            SELECT id, city, neighborhood, street, rooms, price_numeric,
                   size_sqm, floor, latitude, longitude, link_token,
                   is_merchant, first_seen_at
            FROM listings
            WHERE {where}
            ORDER BY price_numeric ASC
            LIMIT %s
        """, params + [limit])

        listings = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "count": len(listings),
        "listings": listings
    })


@app.route("/analytics/trends")
def price_trends():
    """Get daily average price trends over time."""
    city = request.args.get("city")
    days = max(1, min(request.args.get("days", 30, type=int), 365))

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        conditions = ["ph.recorded_at > NOW() - make_interval(days => %s)"]
        params = [days]

        if city:
            conditions.append("l.city ILIKE %s")
            params.append(f"%{city}%")

        where = " AND ".join(conditions)

        cur.execute(f"""
            WITH ph_with_lag AS (
                SELECT
                    ph.listing_id,
                    ph.price_numeric,
                    ph.recorded_at,
                    LAG(ph.price_numeric) OVER (
                        PARTITION BY ph.listing_id ORDER BY ph.recorded_at
                    ) as prev_price
                FROM price_history ph
                JOIN listings l ON l.id = ph.listing_id
                WHERE {where}
            )
            SELECT
                DATE(recorded_at) as date,
                COUNT(*) as price_changes,
                COUNT(DISTINCT listing_id) as listings_affected,
                ROUND(AVG(price_numeric))::int as avg_price,
                SUM(CASE WHEN prev_price IS NOT NULL AND price_numeric < prev_price THEN 1 ELSE 0 END) as drops,
                SUM(CASE WHEN prev_price IS NOT NULL AND price_numeric > prev_price THEN 1 ELSE 0 END) as raises
            FROM ph_with_lag
            GROUP BY DATE(recorded_at)
            ORDER BY date ASC
        """, params)

        trends = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "days": days,
        "count": len(trends),
        "trends": trends
    })


# ─── TELEGRAM CONFIG ─────────────────────────────────────────────────────────

@app.route("/telegram/status")
def telegram_status():
    """Check if Telegram notifications are configured."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    try:
        min_drop = float(os.environ.get("TELEGRAM_MIN_DROP_PERCENT", "5"))
    except (ValueError, TypeError):
        min_drop = 5.0
    return jsonify({
        "configured": bool(bot_token and chat_id),
        "min_drop_percent": min_drop
    })


@app.route("/telegram/test", methods=["POST"])
def telegram_test():
    """Send a test Telegram message."""
    import urllib.request
    import urllib.parse

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        return jsonify({"error": "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars not set"}), 400

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id,
            "text": "Yad2 Scraper: Test notification - Telegram is configured correctly!",
            "parse_mode": "HTML"
        }).encode()
        req = urllib.request.Request(url, data=data)
        resp = urllib.request.urlopen(req, timeout=10)
        return jsonify({"success": True, "message": "Test message sent"})
    except Exception:
        return jsonify({"error": "Failed to send Telegram message. Check bot token and chat ID."}), 500


@app.route("/analytics/deals")
def deals_finder():
    """Find listings priced significantly below their neighborhood average."""
    city = request.args.get("city")
    min_discount = request.args.get("min_discount", 15, type=int)  # minimum % below avg
    min_listings_in_area = request.args.get("min_listings", 5, type=int)
    limit = min(request.args.get("limit", 50, type=int), 200)

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        conditions = [
            "l.is_active = TRUE",
            "l.price_numeric > 0",
            "l.neighborhood IS NOT NULL"
        ]
        params = []

        if city:
            conditions.append("l.city ILIKE %s")
            params.append(f"%{city}%")

        where = " AND ".join(conditions)

        # Build params list carefully for the CTE + main query
        query_params = []
        if city:
            query_params.append(f"%{city}%")  # CTE city filter
        query_params.append(min_listings_in_area)  # CTE HAVING
        query_params.extend(params)  # main WHERE conditions
        query_params.append(min_discount)  # discount threshold
        query_params.append(limit)  # LIMIT

        cur.execute(f"""
            WITH neighborhood_stats AS (
                SELECT city, neighborhood,
                    AVG(price_numeric) as avg_price,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_numeric) as median_price,
                    COUNT(*) as area_count
                FROM listings
                WHERE is_active = TRUE AND price_numeric > 0 AND neighborhood IS NOT NULL
                    {('AND city ILIKE %s' if city else '')}
                GROUP BY city, neighborhood
                HAVING COUNT(*) >= %s
            )
            SELECT
                l.id, l.link_token, l.city, l.neighborhood, l.street,
                l.rooms, l.price_numeric, l.size_sqm, l.floor,
                l.image_url, l.is_merchant, l.first_seen_at,
                ns.avg_price::int as area_avg,
                ns.median_price::int as area_median,
                ns.area_count,
                ROUND(((1 - l.price_numeric::numeric / ns.avg_price) * 100)::numeric, 1) as discount_pct
            FROM listings l
            JOIN neighborhood_stats ns ON l.city = ns.city AND l.neighborhood = ns.neighborhood
            WHERE {where}
                AND l.price_numeric < ns.avg_price * (1 - %s / 100.0)
            ORDER BY discount_pct DESC
            LIMIT %s
        """, query_params)

        deals = [dict(r) for r in cur.fetchall()]
        cur.close()
    except Exception as e:
        logging.error(f"Deals query error: {e}")
        return jsonify({"count": 0, "min_discount": min_discount, "deals": [], "error": "Query failed"}), 500
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "count": len(deals),
        "min_discount": min_discount,
        "deals": deals
    })


@app.route("/analytics/market-summary")
def market_summary():
    """Get a high-level market summary with key metrics and recent activity."""
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        # New listings in last 24h / 7d
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE first_seen_at > NOW() - INTERVAL '24 hours') as new_24h,
                COUNT(*) FILTER (WHERE first_seen_at > NOW() - INTERVAL '7 days') as new_7d,
                COUNT(*) FILTER (WHERE is_active = FALSE AND last_seen_at > NOW() - INTERVAL '7 days') as removed_7d,
                COUNT(*) FILTER (WHERE is_active = TRUE) as active_total,
                ROUND(AVG(price_numeric) FILTER (WHERE is_active = TRUE AND price_numeric > 0))::int as avg_price_all,
                ROUND(AVG(price_numeric) FILTER (WHERE is_active = TRUE AND price_numeric > 0 AND first_seen_at > NOW() - INTERVAL '7 days'))::int as avg_price_new
            FROM listings
        """)
        overview = dict(cur.fetchone())

        # Price distribution buckets
        cur.execute("""
            SELECT
                CASE
                    WHEN price_numeric < 2000 THEN '0-2K'
                    WHEN price_numeric < 3000 THEN '2-3K'
                    WHEN price_numeric < 4000 THEN '3-4K'
                    WHEN price_numeric < 5000 THEN '4-5K'
                    WHEN price_numeric < 6000 THEN '5-6K'
                    WHEN price_numeric < 8000 THEN '6-8K'
                    WHEN price_numeric < 10000 THEN '8-10K'
                    ELSE '10K+'
                END as bucket,
                COUNT(*) as count
            FROM listings
            WHERE is_active = TRUE AND price_numeric > 0
            GROUP BY bucket
            ORDER BY MIN(price_numeric)
        """)
        price_dist = [dict(r) for r in cur.fetchall()]

        # Top 5 cheapest neighborhoods (min 5 listings)
        cur.execute("""
            SELECT city, neighborhood,
                ROUND(AVG(price_numeric))::int as avg_price,
                COUNT(*) as count
            FROM listings
            WHERE is_active = TRUE AND price_numeric > 0 AND neighborhood IS NOT NULL
            GROUP BY city, neighborhood
            HAVING COUNT(*) >= 5
            ORDER BY avg_price ASC
            LIMIT 5
        """)
        cheapest = [dict(r) for r in cur.fetchall()]

        # Top 5 most expensive neighborhoods
        cur.execute("""
            SELECT city, neighborhood,
                ROUND(AVG(price_numeric))::int as avg_price,
                COUNT(*) as count
            FROM listings
            WHERE is_active = TRUE AND price_numeric > 0 AND neighborhood IS NOT NULL
            GROUP BY city, neighborhood
            HAVING COUNT(*) >= 5
            ORDER BY avg_price DESC
            LIMIT 5
        """)
        expensive = [dict(r) for r in cur.fetchall()]

        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "overview": overview,
        "price_distribution": price_dist,
        "cheapest_neighborhoods": cheapest,
        "most_expensive_neighborhoods": expensive
    })


@app.route("/analytics/compare")
def compare_neighborhoods():
    """Compare two neighborhoods side by side."""
    n1 = request.args.get("n1", "")
    n2 = request.args.get("n2", "")
    c1 = request.args.get("c1", "")  # optional city for n1
    c2 = request.args.get("c2", "")  # optional city for n2

    if not n1 or not n2:
        return jsonify({"error": "Both n1 and n2 (neighborhood names) are required"}), 400

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        results = {}
        for label, neigh, city in [("n1", n1, c1), ("n2", n2, c2)]:
            conditions = ["is_active = TRUE", "price_numeric > 0", "neighborhood ILIKE %s"]
            params = [f"%{neigh}%"]
            if city:
                conditions.append("city ILIKE %s")
                params.append(f"%{city}%")

            where = " AND ".join(conditions)

            cur.execute(f"""
                SELECT
                    city,
                    neighborhood,
                    COUNT(*) as listings_count,
                    ROUND(AVG(price_numeric))::int as avg_price,
                    MIN(price_numeric) as min_price,
                    MAX(price_numeric) as max_price,
                    (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_numeric))::int as median_price,
                    ROUND(AVG(CASE WHEN size_sqm ~ '^[0-9]+$' AND size_sqm::int > 0
                        THEN price_numeric::numeric / size_sqm::int END))::int as avg_price_per_sqm,
                    ROUND(AVG(CASE WHEN rooms ~ '^[0-9.]+$' THEN rooms::numeric END), 1) as avg_rooms,
                    ROUND(AVG(CASE WHEN size_sqm ~ '^[0-9]+$' THEN size_sqm::int END))::int as avg_sqm,
                    COUNT(*) FILTER (WHERE is_merchant) as agents,
                    COUNT(*) FILTER (WHERE NOT is_merchant) as private
                FROM listings
                WHERE {where}
                GROUP BY city, neighborhood
                ORDER BY COUNT(*) DESC
                LIMIT 1
            """, params)

            row = cur.fetchone()
            if row:
                results[label] = dict(row)
            else:
                results[label] = {"neighborhood": neigh, "city": city or "Not found", "listings_count": 0}

            # Room distribution for this neighborhood
            cur.execute(f"""
                SELECT rooms, COUNT(*) as count
                FROM listings
                WHERE {where} AND rooms IS NOT NULL AND rooms != ''
                GROUP BY rooms
                ORDER BY rooms
            """, params)
            results[f"{label}_rooms"] = [dict(r) for r in cur.fetchall()]

        cur.close()
    finally:
        if conn:
            put_db(conn)

    return jsonify(results)


@app.route("/analytics/stale")
def stale_listings():
    """Find listings that have been on the market the longest (potentially overpriced)."""
    city = request.args.get("city")
    min_days = request.args.get("min_days", 14, type=int)
    limit = min(request.args.get("limit", 50, type=int), 200)

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        conditions = [
            "is_active = TRUE",
            "price_numeric > 0",
            "first_seen_at < NOW() - make_interval(days => %s)"
        ]
        params = [min_days]

        if city:
            conditions.append("city ILIKE %s")
            params.append(f"%{city}%")

        where = " AND ".join(conditions)

        cur.execute(f"""
            SELECT id, link_token, city, neighborhood, street, rooms,
                   price_numeric, size_sqm, floor, image_url,
                   is_merchant, first_seen_at, last_seen_at,
                   EXTRACT(DAY FROM NOW() - first_seen_at)::int as days_on_market
            FROM listings
            WHERE {where}
            ORDER BY first_seen_at ASC
            LIMIT %s
        """, params + [limit])

        listings = [dict(r) for r in cur.fetchall()]

        # Get summary stats
        cur.execute(f"""
            SELECT
                COUNT(*) as total_stale,
                ROUND(AVG(price_numeric))::int as avg_price,
                ROUND(AVG(EXTRACT(DAY FROM NOW() - first_seen_at)))::int as avg_days
            FROM listings
            WHERE {where}
        """, params)
        summary = dict(cur.fetchone())

        cur.close()
    except Exception as e:
        logging.error(f"Stale listings error: {e}")
        return jsonify({"count": 0, "listings": [], "error": "Query failed"}), 500
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "count": len(listings),
        "min_days": min_days,
        "summary": summary,
        "listings": listings
    })


@app.route("/analytics/city/<city_name>")
def city_stats(city_name):
    """Get detailed statistics for a specific city."""
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                city,
                COUNT(*) as total_listings,
                COUNT(*) FILTER (WHERE is_active) as active_listings,
                ROUND(AVG(price_numeric) FILTER (WHERE is_active AND price_numeric > 0))::int as avg_price,
                MIN(price_numeric) FILTER (WHERE is_active AND price_numeric > 0) as min_price,
                MAX(price_numeric) FILTER (WHERE is_active AND price_numeric > 0) as max_price,
                (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_active AND price_numeric > 0 THEN price_numeric END))::int as median_price,
                ROUND(AVG(CASE WHEN is_active AND rooms ~ '^[0-9.]+$' THEN rooms::numeric END), 1) as avg_rooms,
                ROUND(AVG(CASE WHEN is_active AND size_sqm ~ '^[0-9]+$' AND size_sqm::int > 0
                    THEN price_numeric::numeric / size_sqm::int END))::int as avg_price_per_sqm,
                COUNT(*) FILTER (WHERE is_active AND is_merchant) as agent_listings,
                COUNT(*) FILTER (WHERE is_active AND NOT is_merchant) as private_listings,
                COUNT(*) FILTER (WHERE first_seen_at > NOW() - INTERVAL '24 hours') as new_24h,
                COUNT(*) FILTER (WHERE first_seen_at > NOW() - INTERVAL '7 days') as new_7d,
                COUNT(DISTINCT neighborhood) FILTER (WHERE is_active) as neighborhoods
            FROM listings
            WHERE city ILIKE %s
            GROUP BY city
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """, (f"%{city_name}%",))

        overview = cur.fetchone()
        if not overview or overview["total_listings"] == 0:
            cur.close()
            return jsonify({"error": "City not found"}), 404

        result = {"overview": dict(overview)}

        # Top neighborhoods in this city
        cur.execute("""
            SELECT neighborhood, COUNT(*) as count,
                ROUND(AVG(price_numeric) FILTER (WHERE price_numeric > 0))::int as avg_price,
                MIN(price_numeric) FILTER (WHERE price_numeric > 0) as min_price,
                MAX(price_numeric) FILTER (WHERE price_numeric > 0) as max_price
            FROM listings
            WHERE is_active AND city ILIKE %s AND neighborhood IS NOT NULL
            GROUP BY neighborhood
            ORDER BY count DESC
            LIMIT 20
        """, (f"%{city_name}%",))
        result["neighborhoods"] = [dict(r) for r in cur.fetchall()]

        # Room distribution
        cur.execute("""
            SELECT rooms, COUNT(*) as count
            FROM listings
            WHERE is_active AND city ILIKE %s AND rooms IS NOT NULL AND rooms != ''
            GROUP BY rooms
            ORDER BY rooms
        """, (f"%{city_name}%",))
        result["rooms_distribution"] = [dict(r) for r in cur.fetchall()]

        # Price buckets
        cur.execute("""
            SELECT
                CASE
                    WHEN price_numeric < 2000 THEN '0-2K'
                    WHEN price_numeric < 3000 THEN '2-3K'
                    WHEN price_numeric < 4000 THEN '3-4K'
                    WHEN price_numeric < 5000 THEN '4-5K'
                    WHEN price_numeric < 6000 THEN '5-6K'
                    WHEN price_numeric < 8000 THEN '6-8K'
                    WHEN price_numeric < 10000 THEN '8-10K'
                    ELSE '10K+'
                END as bucket,
                COUNT(*) as count
            FROM listings
            WHERE is_active AND city ILIKE %s AND price_numeric > 0
            GROUP BY bucket
            ORDER BY MIN(price_numeric)
        """, (f"%{city_name}%",))
        result["price_distribution"] = [dict(r) for r in cur.fetchall()]

        cur.close()
    except Exception as e:
        logging.error(f"City stats error: {e}")
        return jsonify({"error": "Failed to load city stats"}), 500
    finally:
        if conn:
            put_db(conn)

    return jsonify(result)


@app.route("/analytics/price-drops")
def price_drop_leaderboard():
    """Neighborhoods with the most price drops — signals softening markets."""
    days = max(1, min(request.args.get("days", 30, type=int), 365))
    min_drops = request.args.get("min_drops", 2, type=int)

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            WITH drops AS (
                SELECT
                    ph.listing_id,
                    l.city,
                    l.neighborhood,
                    ph.price_numeric as new_price,
                    LAG(ph.price_numeric) OVER (
                        PARTITION BY ph.listing_id ORDER BY ph.recorded_at
                    ) as old_price,
                    ph.recorded_at
                FROM price_history ph
                JOIN listings l ON l.id = ph.listing_id
                WHERE ph.recorded_at > NOW() - make_interval(days => %s)
            ),
            actual_drops AS (
                SELECT * FROM drops
                WHERE old_price IS NOT NULL AND new_price < old_price
            )
            SELECT
                city,
                neighborhood,
                COUNT(*) as drop_count,
                COUNT(DISTINCT listing_id) as listings_affected,
                ROUND(AVG(old_price - new_price))::int as avg_drop_amount,
                ROUND(AVG((old_price - new_price)::numeric / NULLIF(old_price, 0) * 100), 1) as avg_drop_pct,
                MAX(old_price - new_price) as biggest_drop
            FROM actual_drops
            WHERE neighborhood IS NOT NULL
            GROUP BY city, neighborhood
            HAVING COUNT(*) >= %s
            ORDER BY drop_count DESC
            LIMIT 30
        """, (days, min_drops))

        neighborhoods = [dict(r) for r in cur.fetchall()]

        # Overall summary
        cur.execute("""
            WITH drops AS (
                SELECT
                    ph.price_numeric as new_price,
                    LAG(ph.price_numeric) OVER (
                        PARTITION BY ph.listing_id ORDER BY ph.recorded_at
                    ) as old_price
                FROM price_history ph
                WHERE ph.recorded_at > NOW() - make_interval(days => %s)
            )
            SELECT
                COUNT(*) FILTER (WHERE old_price IS NOT NULL AND new_price < old_price) as total_drops,
                COUNT(*) FILTER (WHERE old_price IS NOT NULL AND new_price > old_price) as total_raises
            FROM drops
        """, (days,))
        summary = dict(cur.fetchone())

        cur.close()
    except Exception as e:
        logging.error(f"Price drop leaderboard error: {e}")
        return jsonify({"error": "Query failed"}), 500
    finally:
        if conn:
            put_db(conn)

    return jsonify({
        "days": days,
        "count": len(neighborhoods),
        "summary": summary,
        "neighborhoods": neighborhoods
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
