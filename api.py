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
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


@app.route("/")
def index():
    return jsonify({
        "service": "Yad2 Scraper API",
        "endpoints": {
            "/listings": "GET - List all active listings with filters",
            "/listings/<id>": "GET - Get single listing by ID",
            "/stats": "GET - Get database statistics",
            "/runs": "GET - Get scrape run history",
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

    cur.close()
    conn.close()

    if not listing:
        return jsonify({"error": "Listing not found"}), 404

    return jsonify(dict(listing))


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
