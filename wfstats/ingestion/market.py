from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

import requests

from wfstats.database import DB_PATH, create_db

ORDERS_URL = "https://api.warframe.market/v2/orders/item/{slug}"
SOURCE_NAME = "wfm_orders"

log = logging.getLogger(__name__)


def _open_db() -> sqlite3.Connection:
    conn = create_db(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _fetch_orders(slug: str) -> list[dict[str, Any]]:
    response = requests.get(ORDERS_URL.format(slug=slug), timeout=30)
    response.raise_for_status()
    payload = response.json()
    return payload.get("data", [])


def _candidate_items(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            i.id,
            i.item_name,
            i.wfm_slug,
            MAX(CASE WHEN r.is_vaulted = 0 THEN 1 ELSE 0 END) AS has_unvaulted_source,
            MAX(mo.fetched_at) AS last_fetched_at
        FROM items i
        JOIN relic_rewards rr ON rr.item_id = i.id
        JOIN relics r ON r.id = rr.relic_id
        LEFT JOIN market_orders mo ON mo.item_id = i.id
        WHERE i.is_tradable = 1 AND i.wfm_slug IS NOT NULL
        GROUP BY i.id, i.item_name, i.wfm_slug
        ORDER BY has_unvaulted_source DESC, last_fetched_at IS NOT NULL, last_fetched_at, i.item_name
        LIMIT ?
        """,
        [limit],
    ).fetchall()


def sync_market_orders(conn: sqlite3.Connection | None = None, limit: int = 5) -> dict[str, Any]:
    own_connection = conn is None
    db = conn or _open_db()
    limit = max(1, min(limit, 50))

    try:
        items = _candidate_items(db, limit)
        synced = 0
        order_count = 0
        fetched_at = int(time.time())

        for item in items:
            slug = item["wfm_slug"]
            log.info("Fetching market orders for %s", slug)
            orders = _fetch_orders(slug)

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
            synced += 1

        return {
            "source": SOURCE_NAME,
            "item_count": synced,
            "order_count": order_count,
        }
    finally:
        if own_connection:
            db.close()
