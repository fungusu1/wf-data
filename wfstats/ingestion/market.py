from __future__ import annotations

import logging
import re
import sqlite3
import time
from typing import Any

import requests

from wfstats.database import DB_PATH, create_db

ORDERS_URL = "https://api.warframe.market/v2/orders/item/{slug}"
STATISTICS_URL = "https://api.warframe.market/v1/items/{slug}/statistics"
SOURCE_NAME = "wfm_orders"

log = logging.getLogger(__name__)


def _slug_variants(slug: str) -> list[str]:
    variants = [slug]
    normalized = slug.lower().replace("’", "'")
    cleaned = normalized.replace("'", "")
    cleaned = re.sub(r"_+", "_", cleaned)
    alt_dash = cleaned.replace("_", "-")
    alt_underscore = cleaned.replace("-", "_")
    for candidate in (cleaned, alt_dash, alt_underscore):
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def _open_db() -> sqlite3.Connection:
    conn = create_db(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _fetch_orders(slug: str) -> list[dict[str, Any]]:
    for candidate in _slug_variants(slug):
        response = requests.get(ORDERS_URL.format(slug=candidate), timeout=30)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        payload = response.json()
        return payload.get("data", [])
    log.warning("No warframe.market v2 orders found for slug %s", slug)
    return []


def _fetch_statistics(slug: str) -> dict[str, Any]:
    for candidate in _slug_variants(slug):
        response = requests.get(STATISTICS_URL.format(slug=candidate), timeout=30)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        payload = response.json()
        return payload.get("payload", {}).get("statistics_closed", {})
    log.warning("No warframe.market v1 statistics found for slug %s", slug)
    return {}


def _percentile(values: list[int], p: float) -> float:
    if not values:
        raise ValueError("Cannot compute percentile of empty values")
    if len(values) == 1:
        return float(values[0])

    position = (len(values) - 1) * p
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(values) - 1)
    weight = position - lower_index
    lower_value = values[lower_index]
    upper_value = values[upper_index]
    return lower_value + (upper_value - lower_value) * weight


def _filter_sell_outliers(sell_prices: list[int]) -> list[int]:
    if len(sell_prices) < 8:
        return sell_prices

    q1 = _percentile(sell_prices, 0.25)
    q3 = _percentile(sell_prices, 0.75)
    iqr = q3 - q1
    if iqr <= 0:
        return sell_prices

    upper_fence = q3 + (1.5 * iqr)
    filtered = [price for price in sell_prices if price <= upper_fence]
    return filtered or sell_prices


def _summarize_orders(orders: list[dict[str, Any]]) -> dict[str, float | int | None]:
    sell_prices = sorted(
        int(order["platinum"])
        for order in orders
        if order.get("type") == "sell" and order.get("platinum") is not None
    )
    if not sell_prices:
        return {
            "order_count": 0,
            "min_sell_price": None,
            "avg_sell_price": None,
            "current_median_sell_price": None,
        }

    filtered_prices = _filter_sell_outliers(sell_prices)
    count = len(filtered_prices)
    middle = count // 2
    if count % 2:
        median = float(filtered_prices[middle])
    else:
        median = (filtered_prices[middle - 1] + filtered_prices[middle]) / 2

    return {
        "order_count": count,
        "min_sell_price": float(filtered_prices[0]),
        "avg_sell_price": round(sum(filtered_prices) / count, 2),
        "current_median_sell_price": float(median),
    }


def _summarize_statistics(statistics: dict[str, Any]) -> dict[str, float | None]:
    rows = statistics.get("90days") or []
    if not rows:
        return {
            "historical_median_90d": None,
            "historical_wa_price_90d": None,
        }

    latest = rows[-1]
    return {
        "historical_median_90d": latest.get("median"),
        "historical_wa_price_90d": latest.get("wa_price"),
    }


def _candidate_items(conn: sqlite3.Connection, limit: int | None) -> list[sqlite3.Row]:
    query = """
        SELECT
            i.id,
            i.item_name,
            i.wfm_slug,
            MAX(
                CASE
                    WHEN r.is_vaulted = 0 THEN 2
                    WHEN m.id IS NOT NULL THEN 1
                    ELSE 0
                END
            ) AS source_priority,
            MAX(mo.fetched_at) AS last_fetched_at
        FROM items i
        LEFT JOIN relic_rewards rr ON rr.item_id = i.id
        LEFT JOIN relics r ON r.id = rr.relic_id
        LEFT JOIN mods m ON m.item_id = i.id
        LEFT JOIN market_orders mo ON mo.item_id = i.id
        WHERE i.is_tradable = 1 AND i.wfm_slug IS NOT NULL
          AND (rr.id IS NOT NULL OR m.id IS NOT NULL)
        GROUP BY i.id, i.item_name, i.wfm_slug
        ORDER BY source_priority DESC, last_fetched_at IS NOT NULL, last_fetched_at, i.item_name
    """
    params: list[object] = []
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return conn.execute(query, params).fetchall()


def sync_market_orders(conn: sqlite3.Connection | None = None, limit: int = 5) -> dict[str, Any]:
    own_connection = conn is None
    db = conn or _open_db()
    item_limit = None if limit <= 0 else max(1, min(limit, 1000))

    try:
        items = _candidate_items(db, item_limit)
        synced = 0
        order_count = 0
        fetched_at = int(time.time())

        for item in items:
            slug = item["wfm_slug"]
            log.info("Fetching market orders for %s", slug)
            orders = _fetch_orders(slug)
            statistics = _fetch_statistics(slug)
            order_summary = _summarize_orders(orders)
            statistics_summary = _summarize_statistics(statistics)

            with db:
                db.execute("DELETE FROM market_orders WHERE item_id = ?", [item["id"]])

                for order in orders:
                    user = order.get("user") or {}
                    order_type = order.get("type")
                    platinum = order.get("platinum")
                    quantity = order.get("quantity") or 1

                    if order_type not in {"sell", "buy"} or platinum is None:
                        continue

                    db.execute(
                        """
                        INSERT INTO market_orders (
                            item_id,
                            order_type,
                            platinum,
                            quantity,
                            user_name,
                            user_status,
                            fetched_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            item["id"],
                            order_type,
                            int(platinum),
                            int(quantity),
                            user.get("ingameName") or user.get("slug") or "unknown",
                            user.get("status"),
                            fetched_at,
                        ],
                    )
                    order_count += 1

                db.execute(
                    """
                    INSERT INTO sync_log (source, fetched_at, content_hash, item_count, notes)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [SOURCE_NAME, fetched_at, slug, 1, f"orders={len(orders)} item={item['item_name']}"],
                )
                db.execute(
                    """
                    INSERT INTO item_price_cache (
                        item_id,
                        order_count,
                        min_sell_price,
                        avg_sell_price,
                        current_median_sell_price,
                        historical_median_90d,
                        historical_wa_price_90d,
                        fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(item_id) DO UPDATE SET
                        order_count = excluded.order_count,
                        min_sell_price = excluded.min_sell_price,
                        avg_sell_price = excluded.avg_sell_price,
                        current_median_sell_price = excluded.current_median_sell_price,
                        historical_median_90d = excluded.historical_median_90d,
                        historical_wa_price_90d = excluded.historical_wa_price_90d,
                        fetched_at = excluded.fetched_at
                    """,
                    [
                        item["id"],
                        order_summary["order_count"],
                        order_summary["min_sell_price"],
                        order_summary["avg_sell_price"],
                        order_summary["current_median_sell_price"],
                        statistics_summary["historical_median_90d"],
                        statistics_summary["historical_wa_price_90d"],
                        fetched_at,
                    ],
                )
            synced += 1

        return {
            "source": SOURCE_NAME,
            "item_count": synced,
            "order_count": order_count,
        }
    finally:
        if own_connection:
            db.close()
