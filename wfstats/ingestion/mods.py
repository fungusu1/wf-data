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

MODS_URL = "https://raw.githubusercontent.com/WFCD/warframe-items/master/data/json/Mods.json"
MOD_LOCATIONS_URL = "https://raw.githubusercontent.com/WFCD/warframe-drop-data/main/data/modLocations.json"
SYNDICATES_URL = "https://raw.githubusercontent.com/WFCD/warframe-drop-data/main/data/syndicates.json"

SOURCE_NAME = "mods"

log = logging.getLogger(__name__)


def _open_db() -> sqlite3.Connection:
    conn = create_db(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _fetch_json(url: str) -> tuple[Any, str]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    raw = response.content
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


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


def _item_payload_from_mod(mod: dict[str, Any]) -> dict[str, Any]:
    name = str(mod["name"])
    return {
        "wfcd_id": str(mod["uniqueName"]),
        "item_name": name,
        "wfm_slug": _slugify(name),
        "image_name": mod.get("imageName") or mod.get("wikiaThumbnail"),
        "is_tradable": int(bool(mod.get("tradable"))),
    }


def _mod_payload_from_mod(mod: dict[str, Any], item_id: int, syndicate_augment: bool) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "mod_name": str(mod["name"]),
        "mod_type": mod.get("type"),
        "compat_name": mod.get("compatName"),
        "polarity": mod.get("polarity"),
        "rarity": mod.get("rarity"),
        "base_drain": mod.get("baseDrain"),
        "fusion_limit": mod.get("fusionLimit"),
        "is_augment": int(bool(mod.get("isAugment"))),
        "is_syndicate_augment": int(bool(syndicate_augment)),
    }


def _upsert_item(conn: sqlite3.Connection, item: dict[str, Any]) -> int:
    row = conn.execute("SELECT id FROM items WHERE wfcd_id = ?", [item["wfcd_id"]]).fetchone()
    if row is None and item.get("wfm_slug"):
        row = conn.execute("SELECT id FROM items WHERE wfm_slug = ?", [item["wfm_slug"]]).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT id FROM items WHERE LOWER(item_name) = LOWER(?)",
            [item["item_name"]],
        ).fetchone()

    if row is not None:
        # Mods like Requiem mods can already exist as relic rewards. Reuse the
        # shared item row instead of inserting a duplicate slug/name.
        conn.execute(
            """
            UPDATE items
            SET
                item_name = ?,
                wfm_slug = COALESCE(?, wfm_slug),
                image_name = COALESCE(?, image_name),
                is_tradable = CASE
                    WHEN is_tradable = 1 OR ? = 1 THEN 1
                    ELSE 0
                END
            WHERE id = ?
            """,
            [
                item["item_name"],
                item.get("wfm_slug"),
                item.get("image_name"),
                item["is_tradable"],
                int(row[0]),
            ],
        )
        return int(row[0])

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


def _upsert_mod(conn: sqlite3.Connection, payload: dict[str, Any]) -> int:
    conn.execute(
        """
        INSERT INTO mods (
            item_id,
            mod_name,
            mod_type,
            compat_name,
            polarity,
            rarity,
            base_drain,
            fusion_limit,
            is_augment,
            is_syndicate_augment
        ) VALUES (
            :item_id,
            :mod_name,
            :mod_type,
            :compat_name,
            :polarity,
            :rarity,
            :base_drain,
            :fusion_limit,
            :is_augment,
            :is_syndicate_augment
        )
        ON CONFLICT(item_id) DO UPDATE SET
            mod_name = excluded.mod_name,
            mod_type = excluded.mod_type,
            compat_name = excluded.compat_name,
            polarity = excluded.polarity,
            rarity = excluded.rarity,
            base_drain = excluded.base_drain,
            fusion_limit = excluded.fusion_limit,
            is_augment = excluded.is_augment,
            is_syndicate_augment = excluded.is_syndicate_augment
        """,
        payload,
    )
    row = conn.execute("SELECT id FROM mods WHERE item_id = ?", [payload["item_id"]]).fetchone()
    return int(row[0])


def _clear_mod_sources(conn: sqlite3.Connection, mod_id: int) -> None:
    conn.execute("DELETE FROM mod_sources WHERE mod_id = ?", [mod_id])


def _mod_drop_sources(mod: dict[str, Any]) -> list[dict[str, Any]]:
    return list(mod.get("drops") or [])


def _upsert_mod_source(conn: sqlite3.Connection, payload: list[Any]) -> None:
    conn.execute(
        """
        INSERT INTO mod_sources (
            mod_id,
            source_category,
            source_name,
            source_detail,
            chance,
            rarity,
            enemy_mod_drop_chance,
            standing
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mod_id, source_category, source_name, source_detail) DO UPDATE SET
            chance = excluded.chance,
            rarity = excluded.rarity,
            enemy_mod_drop_chance = excluded.enemy_mod_drop_chance,
            standing = excluded.standing
        """,
        payload,
    )


def _syndicate_vendor_sources(syndicates: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    vendor_sources: dict[str, list[dict[str, Any]]] = {}
    for syndicate_name, rewards in syndicates.items():
        for reward in rewards or []:
            item_name = reward.get("item")
            if not item_name:
                continue
            vendor_sources.setdefault(str(item_name), []).append(
                {
                    "source_name": syndicate_name,
                    "source_detail": reward.get("place"),
                    "standing": reward.get("standing"),
                    "chance": reward.get("chance"),
                    "rarity": reward.get("rarity"),
                }
            )
    return vendor_sources


def sync_mods(conn: sqlite3.Connection | None = None, force: bool = False) -> dict[str, Any]:
    own_connection = conn is None
    db = conn or _open_db()

    try:
        mods_payload, mods_hash = _fetch_json(MODS_URL)
        locations_payload, locations_hash = _fetch_json(MOD_LOCATIONS_URL)
        syndicates_payload, syndicates_hash = _fetch_json(SYNDICATES_URL)

        if (
            not force
            and _check_hash(db, "mods_items", mods_hash)
            and _check_hash(db, "mods_locations", locations_hash)
            and _check_hash(db, "mods_syndicates", syndicates_hash)
        ):
            return {
                "source": SOURCE_NAME,
                "skipped": True,
                "content_hash": f"{mods_hash}:{locations_hash}:{syndicates_hash}",
            }

        mod_locations = {entry["modName"]: entry.get("enemies") or [] for entry in locations_payload.get("modLocations", [])}
        syndicate_vendor_sources = _syndicate_vendor_sources(syndicates_payload.get("syndicates", {}))

        mod_count = 0
        source_count = 0
        vendor_mod_count = 0
        fetched_at = int(time.time())

        with db:
            db.execute("DELETE FROM mod_sources")
            for mod in mods_payload:
                if mod.get("category") != "Mods":
                    continue

                item_id = _upsert_item(db, _item_payload_from_mod(mod))
                syndicate_augment = bool(mod.get("isAugment")) and mod["name"] in syndicate_vendor_sources
                mod_id = _upsert_mod(db, _mod_payload_from_mod(mod, item_id, syndicate_augment))
                _clear_mod_sources(db, mod_id)

                for source in _mod_drop_sources(mod):
                    _upsert_mod_source(
                        db,
                        [
                            mod_id,
                            "drop",
                            source.get("location") or "unknown",
                            source.get("type"),
                            float(source.get("chance")) if source.get("chance") is not None else None,
                            source.get("rarity"),
                            None,
                            None,
                        ],
                    )
                    source_count += 1

                for enemy in mod_locations.get(mod["name"], []):
                    _upsert_mod_source(
                        db,
                        [
                            mod_id,
                            "enemy",
                            enemy.get("enemyName") or "unknown",
                            mod["name"],
                            float(enemy.get("chance")) if enemy.get("chance") is not None else None,
                            enemy.get("rarity"),
                            float(enemy.get("enemyModDropChance")) if enemy.get("enemyModDropChance") is not None else None,
                            None,
                        ],
                    )
                    source_count += 1

                vendor_sources = syndicate_vendor_sources.get(mod["name"], [])
                for vendor in vendor_sources:
                    _upsert_mod_source(
                        db,
                        [
                            mod_id,
                            "vendor",
                            vendor["source_name"],
                            vendor["source_detail"],
                            float(vendor["chance"]) if vendor["chance"] is not None else None,
                            vendor["rarity"],
                            None,
                            vendor["standing"],
                        ],
                    )
                    source_count += 1

                if vendor_sources:
                    vendor_mod_count += 1

                mod_count += 1

            db.execute(
                """
                INSERT INTO sync_log (source, fetched_at, content_hash, item_count, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                ["mods_items", fetched_at, mods_hash, mod_count, None],
            )
            db.execute(
                """
                INSERT INTO sync_log (source, fetched_at, content_hash, item_count, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                ["mods_locations", fetched_at, locations_hash, source_count, None],
            )
            db.execute(
                """
                INSERT INTO sync_log (source, fetched_at, content_hash, item_count, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                ["mods_syndicates", fetched_at, syndicates_hash, vendor_mod_count, None],
            )

        return {
            "source": SOURCE_NAME,
            "skipped": False,
            "content_hash": f"{mods_hash}:{locations_hash}:{syndicates_hash}",
            "mod_count": mod_count,
            "source_count": source_count,
            "vendor_mod_count": vendor_mod_count,
        }
    finally:
        if own_connection:
            db.close()
