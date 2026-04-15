"""
Ingest relic and reward item data from WFCD warframe-items.

This populates:
    - items
    - relics
    - relic_rewards
    - relic_reward_chances
    - sync_log
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import time
from typing import Any

import requests

from wfstats.database import DB_PATH, create_db

RELICS_URL = "https://raw.githubusercontent.com/WFCD/warframe-items/master/data/json/Relics.json"
SOURCE_NAME = "warframe_items"
LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CHANCE_TABLE: dict[str, dict[str, float]] = {
    "Common": {"Intact": 0.2533, "Exceptional": 0.2333, "Flawless": 0.2000, "Radiant": 0.1667},
    "Uncommon": {"Intact": 0.1100, "Exceptional": 0.1333, "Flawless": 0.1667, "Radiant": 0.2000},
    "Rare": {"Intact": 0.0200, "Exceptional": 0.0400, "Flawless": 0.0600, "Radiant": 0.1000},
}

REFINEMENT_STATES = ["Intact", "Exceptional", "Flawless", "Radiant"]


def get_relics() -> tuple[list[dict[str, Any]], str]:
    """Fetch relic data and return the parsed payload with a content hash."""
    log.info("Fetching %s", RELICS_URL)
    response = requests.get(RELICS_URL, timeout=30)
    response.raise_for_status()
    raw = response.content
    digest = hashlib.sha256(raw).hexdigest()
    return json.loads(raw), digest


def check_hash(conn: sqlite3.Connection, digest: str) -> bool:
    """Return True when the latest sync already used the same payload hash."""
    row = conn.execute(
        """
        SELECT content_hash
        FROM sync_log
        WHERE source = ?
        ORDER BY fetched_at DESC, id DESC
        LIMIT 1
        """,
        [SOURCE_NAME],
    ).fetchone()
    return bool(row and row[0] == digest)


def _get_nested(data: dict[str, Any], *path: str) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _as_bool(value: Any) -> int:
    return int(bool(value))


def _market_slug(data: dict[str, Any]) -> str | None:
    return (
        _get_nested(data, "warframeMarket", "urlName")
        or _get_nested(data, "marketInfo", "urlName")
        or data.get("urlName")
    )


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _parse_relic_identity(relic: dict[str, Any]) -> tuple[str, str]:
    name = str(relic.get("name") or "").replace(" Relic", "").strip()
    parts = name.split()
    if parts and parts[-1] in REFINEMENT_STATES:
        parts = parts[:-1]
    if len(parts) >= 2:
        return parts[0], parts[1]

    tier = relic.get("tier")
    relic_name = relic.get("relicName")
    if tier and relic_name:
        return str(tier), str(relic_name)

    raise ValueError(f"Unable to determine relic identity from payload: {relic!r}")


def _parse_relic_state(relic: dict[str, Any]) -> str | None:
    name = str(relic.get("name") or "").replace(" Relic", "").strip()
    parts = name.split()
    if parts and parts[-1] in REFINEMENT_STATES:
        return parts[-1]
    return None


def _item_payload_from_reward(reward: dict[str, Any]) -> dict[str, Any]:
    item_name = reward.get("itemName") or reward.get("name") or _get_nested(reward, "item", "name")
    market_slug = _market_slug(reward) or _market_slug(reward.get("item") or {})
    nested_unique_name = _get_nested(reward, "item", "uniqueName")
    reward_unique_name = reward.get("uniqueName")

    if not item_name:
        raise ValueError(f"Reward is missing item identity fields: {reward!r}")

    wfcd_id = None
    for candidate in (reward_unique_name, nested_unique_name):
        if candidate and "/Projections/" not in str(candidate):
            wfcd_id = str(candidate)
            break

    if wfcd_id is None and market_slug:
        wfcd_id = f"wfm:{market_slug}"
    if wfcd_id is None:
        wfcd_id = f"name:{_slugify(str(item_name))}"

    return {
        "wfcd_id": wfcd_id,
        "item_name": str(item_name),
        "wfm_slug": market_slug,
        "image_name": reward.get("imageName") or reward.get("thumbnail"),
        "is_tradable": _as_bool(
            reward.get("tradable")
            if reward.get("tradable") is not None
            else reward.get("isTradable") if reward.get("isTradable") is not None else market_slug
        ),
    }


def insert_item(conn: sqlite3.Connection, item: dict[str, Any]) -> int:
    conn.execute(
        """
        INSERT INTO items (wfcd_id, item_name, wfm_slug, image_name, is_tradable)
        VALUES (:wfcd_id, :item_name, :wfm_slug, :image_name, :is_tradable)
        ON CONFLICT(wfcd_id) DO UPDATE SET
            item_name = excluded.item_name,
            wfm_slug = excluded.wfm_slug,
            image_name = excluded.image_name,
            is_tradable = excluded.is_tradable
        """,
        item,
    )
    row = conn.execute("SELECT id FROM items WHERE wfcd_id = ?", [item["wfcd_id"]]).fetchone()
    return int(row[0])


def insert_relic(conn: sqlite3.Connection, relic: dict[str, Any]) -> int:
    tier, relic_name = _parse_relic_identity(relic)
    payload = {
        "wfcd_id": f"relic:{tier}:{relic_name}",
        "wfm_slug": _market_slug(relic),
        "tier": tier,
        "relic_name": relic_name,
        "image_name": relic.get("imageName") or relic.get("thumbnail"),
        "is_vaulted": _as_bool(relic.get("vaulted")),
    }
    conn.execute(
        """
        INSERT INTO relics (wfcd_id, wfm_slug, tier, relic_name, image_name, is_vaulted)
        VALUES (:wfcd_id, :wfm_slug, :tier, :relic_name, :image_name, :is_vaulted)
        ON CONFLICT(wfcd_id) DO UPDATE SET
            wfm_slug = excluded.wfm_slug,
            tier = excluded.tier,
            relic_name = excluded.relic_name,
            image_name = excluded.image_name,
            is_vaulted = excluded.is_vaulted
        """,
        payload,
    )
    row = conn.execute("SELECT id FROM relics WHERE wfcd_id = ?", [payload["wfcd_id"]]).fetchone()
    return int(row[0])


def insert_chance(conn: sqlite3.Connection, relic_reward_id: int, chances: dict[str, float]) -> None:
    for state in REFINEMENT_STATES:
        drop_chance = chances.get(state)
        if drop_chance is None:
            continue
        conn.execute(
            """
            INSERT INTO relic_reward_chances (relic_reward_id, state, drop_chance)
            VALUES (?, ?, ?)
            ON CONFLICT(relic_reward_id, state) DO UPDATE SET
                drop_chance = excluded.drop_chance
            """,
            [relic_reward_id, state, drop_chance],
        )


def _infer_rarity(chances: dict[str, float]) -> str:
    best_rarity = "Common"
    best_score: float | None = None
    for rarity, pattern in CHANCE_TABLE.items():
        score = 0.0
        matched = 0
        for state in REFINEMENT_STATES:
            if state in chances:
                score += abs(chances[state] - pattern[state])
                matched += 1
        if matched == 0:
            continue
        if best_score is None or score < best_score:
            best_rarity = rarity
            best_score = score
    return best_rarity


def _replace_relic_rewards(conn: sqlite3.Connection, relic_id: int, relic_entries: dict[str, dict[str, Any]]) -> int:
    conn.execute(
        """
        DELETE FROM relic_reward_chances
        WHERE relic_reward_id IN (
            SELECT id FROM relic_rewards WHERE relic_id = ?
        )
        """,
        [relic_id],
    )
    conn.execute("DELETE FROM relic_rewards WHERE relic_id = ?", [relic_id])

    grouped_rewards: dict[str, dict[str, Any]] = {}
    for state, relic in relic_entries.items():
        rewards = relic.get("rewards") or []
        for reward in rewards:
            item = _item_payload_from_reward(reward)
            bucket = grouped_rewards.setdefault(
                item["wfcd_id"],
                {
                    "item": item,
                    "chances": {},
                },
            )
            bucket["chances"][state] = round(float(reward["chance"]) / 100, 4)

    reward_count = 0
    for reward in grouped_rewards.values():
        item_id = insert_item(conn, reward["item"])
        rarity = _infer_rarity(reward["chances"])
        conn.execute(
            """
            INSERT INTO relic_rewards (relic_id, item_id, rarity)
            VALUES (?, ?, ?)
            """,
            [relic_id, item_id, rarity],
        )
        reward_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        insert_chance(conn, reward_id, reward["chances"])
        reward_count += 1

    return reward_count


def sync_relics(conn: sqlite3.Connection | None = None, force: bool = False) -> dict[str, Any]:
    """Fetch and upsert relic data into the local database."""
    own_connection = conn is None
    db = conn or create_db(DB_PATH)

    try:
        payload, digest = get_relics()
        if not force and check_hash(db, digest):
            log.info("Skipping relic sync; source hash unchanged")
            return {"source": SOURCE_NAME, "skipped": True, "content_hash": digest}

        synced_at = int(time.time())
        relic_count = 0
        reward_count = 0
        touched_items: set[int] = set()

        grouped_relics: dict[tuple[str, str], dict[str, Any]] = {}
        for relic in payload:
            rewards = relic.get("rewards") or []
            if not rewards:
                continue

            tier, relic_name = _parse_relic_identity(relic)
            state = _parse_relic_state(relic)
            if state is None:
                continue
            key = (tier, relic_name)
            bucket = grouped_relics.setdefault(key, {"base_relic": relic, "states": {}})
            if state == "Intact":
                bucket["base_relic"] = relic
            bucket["states"][state] = relic

        with db:
            for grouped in grouped_relics.values():
                base_relic = grouped["base_relic"]
                state_entries = grouped["states"]
                if not state_entries:
                    continue

                relic_id = insert_relic(db, base_relic)
                relic_count += 1
                reward_count += _replace_relic_rewards(db, relic_id, state_entries)

                item_rows = db.execute(
                    "SELECT item_id FROM relic_rewards WHERE relic_id = ?",
                    [relic_id],
                ).fetchall()
                touched_items.update(int(row[0]) for row in item_rows)

            db.execute(
                """
                INSERT INTO sync_log (source, fetched_at, content_hash, item_count, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                [SOURCE_NAME, synced_at, digest, relic_count, f"rewards={reward_count} items={len(touched_items)}"],
            )

        log.info(
            "Synced %s relics, %s rewards, %s items",
            relic_count,
            reward_count,
            len(touched_items),
        )
        return {
            "source": SOURCE_NAME,
            "skipped": False,
            "content_hash": digest,
            "relic_count": relic_count,
            "reward_count": reward_count,
            "item_count": len(touched_items),
        }
    finally:
        if own_connection:
            db.close()


def test() -> None:
    result = sync_relics(force=True)
    print(result)


if __name__ == "__main__":
    test()
