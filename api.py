#!/usr/bin/env python3
"""
REST API for accessing scraped Yad2 data.
Run alongside the scraper for data access.
"""

import os
import json
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder='.')
CORS(app)


@app.route("/dashboard")
def dashboard():
    return send_from_directory('.', 'dashboard.html')

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def ensure_price_history_table():
    """Create price_history table if it doesn't exist."""
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
        conn.close()
    except Exception as e:
        print(f"Error creating price_history table: {e}")


# Initialize tables on startup
ensure_price_history_table()


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
            "/health": "GET - Health check"
        }
    })


@app.route("/health")
def health():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


@app.route("/stats")
def stats():
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
    stats = dict(cur.fetchone())

    cur.execute("""
        SELECT city, COUNT(*) as count
        FROM listings
        WHERE is_active = TRUE
        GROUP BY city
        ORDER BY count DESC
        LIMIT 20
    """)
    stats["top_cities"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT rooms, COUNT(*) as count
        FROM listings
        WHERE is_active = TRUE
        GROUP BY rooms
        ORDER BY rooms
    """)
    stats["rooms_distribution"] = [dict(r) for r in cur.fetchall()]

    # Price history stats (handle if table is empty or has issues)
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total_price_records,
                COUNT(DISTINCT listing_id) as listings_with_history,
                COUNT(*) FILTER (WHERE recorded_at > NOW() - INTERVAL '24 hours') as changes_last_24h,
                COUNT(*) FILTER (WHERE recorded_at > NOW() - INTERVAL '7 days') as changes_last_7d
            FROM price_history
        """)
        price_stats = dict(cur.fetchone())
        stats["price_history"] = price_stats
    except Exception:
        stats["price_history"] = {
            "total_price_records": 0,
            "listings_with_history": 0,
            "changes_last_24h": 0,
            "changes_last_7d": 0
        }

    cur.close()
    conn.close()

    return jsonify(stats)


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
    offset = request.args.get("offset", 0, type=int)
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
        conditions.append("rooms::int >= %s")
        params.append(min_rooms)

    if max_rooms:
        conditions.append("rooms::int <= %s")
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
    conn.close()

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "count": len(listings),
        "listings": listings
    })


@app.route("/listings/<listing_id>")
def get_listing(listing_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM listings WHERE id = %s", (listing_id,))
    listing = cur.fetchone()

    if not listing:
        cur.close()
        conn.close()
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
    conn.close()

    return jsonify(result)


@app.route("/listings/<listing_id>/price-history")
def get_listing_price_history(listing_id):
    """Get price history for a specific listing."""
    conn = get_db()
    cur = conn.cursor()

    # Check if listing exists
    cur.execute("SELECT id, city, street, neighborhood FROM listings WHERE id = %s", (listing_id,))
    listing = cur.fetchone()

    if not listing:
        cur.close()
        conn.close()
        return jsonify({"error": "Listing not found"}), 404

    # Get price history
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
    conn.close()

    return jsonify({
        "listing_id": listing_id,
        "city": listing["city"],
        "street": listing["street"],
        "neighborhood": listing["neighborhood"],
        "price_changes": len(history) - 1 if history else 0,  # First entry is initial price
        "history": history
    })


@app.route("/price-changes")
def list_price_changes():
    """Get recent price changes across all listings."""
    limit = min(request.args.get("limit", 100, type=int), 500)
    offset = request.args.get("offset", 0, type=int)
    city = request.args.get("city")
    days = request.args.get("days", 7, type=int)  # Default last 7 days

    conn = get_db()
    cur = conn.cursor()

    try:
        # Build query with optional city filter
        conditions = [
            "ph.recorded_at > NOW() - INTERVAL '%s days'" % days
        ]
        params = []

        if city:
            conditions.append("l.city ILIKE %s")
            params.append(f"%{city}%")

        where_clause = " AND ".join(conditions)

        # Get price changes with listing info
        cur.execute(f"""
            SELECT
                ph.listing_id,
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
            ORDER BY ph.recorded_at DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        changes = []
        for row in cur.fetchall():
            r = dict(row)
            if r["previous_price"] is not None:  # Skip initial prices
                r["price_diff"] = r["price_numeric"] - r["previous_price"]
                r["price_diff_percent"] = round(
                    (r["price_diff"] / r["previous_price"]) * 100, 1
                ) if r["previous_price"] else 0
                changes.append(r)

        # Get total count of changes
        cur.execute(f"""
            SELECT COUNT(*) as count
            FROM price_history ph
            JOIN listings l ON l.id = ph.listing_id
            WHERE {where_clause}
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

    except Exception:
        changes = []
        total = 0
        summary = {"listings_with_changes": 0, "total_price_records": 0}

    cur.close()
    conn.close()

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

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM scrape_runs
        ORDER BY started_at DESC
        LIMIT %s
    """, (limit,))

    runs = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({"runs": runs})


@app.route("/cities")
def list_cities():
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
    conn.close()

    return jsonify({"cities": cities})


@app.route("/neighborhoods")
def list_neighborhoods():
    city = request.args.get("city")

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
    conn.close()

    return jsonify({"neighborhoods": neighborhoods})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
