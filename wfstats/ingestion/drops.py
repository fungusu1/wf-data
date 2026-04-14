from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from typing import Any

import requests

from wfstats.database import DB_PATH, create_db

MISSION_REWARDS_URL = "https://raw.githubusercontent.com/WFCD/warframe-drop-data/main/data/missionRewards.json"
TRANSIENT_REWARDS_URL = "https://raw.githubusercontent.com/WFCD/warframe-drop-data/main/data/transientRewards.json"

MISSION_SOURCE_NAME = "mission_rewards"
TRANSIENT_SOURCE_NAME = "transient_rewards"

log = logging.getLogger(__name__)


def _open_db() -> sqlite3.Connection:
    conn = create_db(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _fetch_json(url: str) -> tuple[dict[str, Any], str]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    raw = response.content
    return response.json(), hashlib.sha256(raw).hexdigest()


def _check_hash(conn: sqlite3.Connection, source: str, digest: str) -> bool:
    row = conn.execute(
        """
        SELECT content_hash
        FROM sync_log
        WHERE source = ?
        ORDER BY fetched_at DESC, id DESC
        LIMIT 1
        """,
        [source],
    ).fetchone()
    return bool(row and row[0] == digest)


def _parse_relic_name(item_name: str) -> tuple[str, str] | None:
    if not item_name.endswith(" Relic"):
        return None
    base = item_name[: -len(" Relic")].strip()
    parts = base.split()
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def _iter_mission_reward_groups(rewards: Any) -> list[tuple[str | None, list[dict[str, Any]]]]:
    if isinstance(rewards, list):
        return [(None, rewards)]
    if isinstance(rewards, dict):
        return [(rotation, entries) for rotation, entries in rewards.items()]
    return []


def sync_mission_rewards(conn: sqlite3.Connection | None = None, force: bool = False) -> dict[str, Any]:
    own_connection = conn is None
    db = conn or _open_db()

    try:
        payload, digest = _fetch_json(MISSION_REWARDS_URL)
        if not force and _check_hash(db, MISSION_SOURCE_NAME, digest):
            return {"source": MISSION_SOURCE_NAME, "skipped": True, "content_hash": digest}

        missions = payload.get("missionRewards", {})
        inserted = 0
        fetched_at = int(time.time())

        with db:
            db.execute("DELETE FROM relic_mission_sources")
            for planet, nodes in missions.items():
                for node, mission in nodes.items():
                    game_mode = mission.get("gameMode")
                    rewards = mission.get("rewards") or {}
                    for rotation, entries in _iter_mission_reward_groups(rewards):
                        for entry in entries:
                            relic_name = _parse_relic_name(str(entry.get("itemName") or ""))
                            if relic_name is None:
                                continue

                            relic = db.execute(
                                "SELECT id FROM relics WHERE tier = ? AND relic_name = ?",
                                [relic_name[0], relic_name[1]],
                            ).fetchone()
                            if relic is None:
                                continue

                            db.execute(
                                """
                                INSERT INTO relic_mission_sources (
                                    relic_id,
                                    planet,
                                    node,
                                    game_mode,
                                    rotation,
                                    drop_chance,
                                    rarity
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(relic_id, planet, node, rotation) DO UPDATE SET
                                    game_mode = excluded.game_mode,
                                    drop_chance = excluded.drop_chance,
                                    rarity = excluded.rarity
                                """,
                                [
                                    relic["id"],
                                    planet,
                                    node,
                                    game_mode,
                                    rotation,
                                    float(entry["chance"]),
                                    entry["rarity"],
                                ],
                            )
                            inserted += 1

            db.execute(
                """
                INSERT INTO sync_log (source, fetched_at, content_hash, item_count, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                [MISSION_SOURCE_NAME, fetched_at, digest, inserted, None],
            )

        return {
            "source": MISSION_SOURCE_NAME,
            "skipped": False,
            "content_hash": digest,
            "row_count": inserted,
        }
    finally:
        if own_connection:
            db.close()


def sync_transient_rewards(conn: sqlite3.Connection | None = None, force: bool = False) -> dict[str, Any]:
    own_connection = conn is None
    db = conn or _open_db()

    try:
        payload, digest = _fetch_json(TRANSIENT_REWARDS_URL)
        if not force and _check_hash(db, TRANSIENT_SOURCE_NAME, digest):
            return {"source": TRANSIENT_SOURCE_NAME, "skipped": True, "content_hash": digest}

        rewards = payload.get("transientRewards", [])
        inserted = 0
        fetched_at = int(time.time())

        with db:
            db.execute("DELETE FROM relic_transient_sources")
            for objective in rewards:
                objective_name = objective.get("objectiveName")
                for entry in objective.get("rewards") or []:
                    relic_name = _parse_relic_name(str(entry.get("itemName") or ""))
                    if relic_name is None:
                        continue

                    relic = db.execute(
                        "SELECT id FROM relics WHERE tier = ? AND relic_name = ?",
                        [relic_name[0], relic_name[1]],
                    ).fetchone()
                    if relic is None:
                        continue

                    rotation = entry.get("rotation")
                    full_name = objective_name if not rotation else f"{objective_name} ({rotation})"
                    db.execute(
                        """
                        INSERT INTO relic_transient_sources (
                            relic_id,
                            objective_name,
                            drop_chance,
                            rarity
                        ) VALUES (?, ?, ?, ?)
                        ON CONFLICT(relic_id, objective_name) DO UPDATE SET
                            drop_chance = excluded.drop_chance,
                            rarity = excluded.rarity
                        """,
                        [
                            relic["id"],
                            full_name,
                            float(entry["chance"]),
                            entry["rarity"],
                        ],
                    )
                    inserted += 1

            db.execute(
                """
                INSERT INTO sync_log (source, fetched_at, content_hash, item_count, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                [TRANSIENT_SOURCE_NAME, fetched_at, digest, inserted, None],
            )

        return {
            "source": TRANSIENT_SOURCE_NAME,
            "skipped": False,
            "content_hash": digest,
            "row_count": inserted,
        }
    finally:
        if own_connection:
            db.close()


def sync_drop_sources(force: bool = False) -> dict[str, Any]:
    db = _open_db()
    try:
        mission = sync_mission_rewards(conn=db, force=force)
        transient = sync_transient_rewards(conn=db, force=force)
        return {
            "mission_rewards": mission,
            "transient_rewards": transient,
        }
    finally:
        db.close()
