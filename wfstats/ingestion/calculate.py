from __future__ import annotations

import sqlite3
import time
from collections import defaultdict

from wfstats.database import DB_PATH, create_db


def _open_db() -> sqlite3.Connection:
    conn = create_db(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def calculate_relic_ev(conn: sqlite3.Connection | None = None) -> dict[str, int]:
    own_connection = conn is None
    db = conn or _open_db()

    try:
        rows = db.execute(
            """
            SELECT
                r.id AS relic_id,
                rc.state,
                i.id AS item_id,
                rc.drop_chance,
                p.avg_sell_price
            FROM relics r
            JOIN relic_rewards rr ON rr.relic_id = r.id
            JOIN relic_reward_chances rc ON rc.relic_reward_id = rr.id
            JOIN items i ON i.id = rr.item_id
            LEFT JOIN v_item_prices p ON p.item_id = i.id
            """
        ).fetchall()

        now = int(time.time())
        grouped: dict[tuple[int, str], dict[str, object]] = defaultdict(
            lambda: {
                "expected_value_plat": 0.0,
                "best_item_id": None,
                "best_item_chance": None,
                "best_item_price": None,
                "best_weighted_value": -1.0,
            }
        )

        for row in rows:
            avg_price = row["avg_sell_price"]
            if avg_price is None:
                continue

            key = (int(row["relic_id"]), str(row["state"]))
            chance = float(row["drop_chance"])
            weighted_value = chance * float(avg_price)
            bucket = grouped[key]
            bucket["expected_value_plat"] = float(bucket["expected_value_plat"]) + weighted_value

            if weighted_value > float(bucket["best_weighted_value"]):
                bucket["best_weighted_value"] = weighted_value
                bucket["best_item_id"] = int(row["item_id"])
                bucket["best_item_chance"] = chance
                bucket["best_item_price"] = float(avg_price)

        with db:
            db.execute("DELETE FROM relic_ev")
            for (relic_id, state), value in grouped.items():
                db.execute(
                    """
                    INSERT INTO relic_ev (
                        relic_id,
                        state,
                        expected_value_plat,
                        best_item_id,
                        best_item_chance,
                        best_item_price,
                        calculated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        relic_id,
                        state,
                        round(float(value["expected_value_plat"]), 2),
                        value["best_item_id"],
                        value["best_item_chance"],
                        value["best_item_price"],
                        now,
                    ],
                )

        return {
            "relic_state_count": len(grouped),
        }
    finally:
        if own_connection:
            db.close()
