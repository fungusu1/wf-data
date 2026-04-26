from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from wfstats.database import create_db
from wfstats.helpers import get_con
from wfstats.ingestion.calculate import calculate_relic_ev
from wfstats.ingestion.drops import sync_drop_sources
from wfstats.ingestion.items import sync_relics
from wfstats.ingestion.mods import sync_mods
from wfstats.ingestion.market import sync_market_orders
from wfstats.scheduler import (
    get_scheduler_status,
    should_force_startup_refresh,
    start_background_market_refresh,
    start_scheduler,
    stop_scheduler,
)

STATIC_DIR = Path(__file__).parent / "static"
REFINEMENT_STATES = ("Intact", "Exceptional", "Flawless", "Radiant")
SYNDICATE_VENDOR_NAMES = (
    "Arbiters of Hexis",
    "Cephalon Suda",
    "New Loka",
    "Perrin Sequence",
    "Red Veil",
    "Steel Meridian",
)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    conn = create_db()
    conn.close()
    try:
        sync_mods()
    except Exception:
        log.exception("Initial mod sync failed")
    if should_force_startup_refresh():
        started = start_background_market_refresh(force=True)
        if started:
            log.info("Started background market refresh during app startup")
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(title="wfstats", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _scheduler_payload() -> dict[str, object]:
    return get_scheduler_status().__dict__


def _validate_state(state: str) -> str:
    if state not in REFINEMENT_STATES:
        raise HTTPException(status_code=422, detail="Invalid relic state")
    return state


def _clamp_limit(limit: int) -> int:
    return max(1, min(limit, 200))


def _normalize_optional_limit(limit: int) -> int | None:
    if limit <= 0:
        return None
    return min(limit, 5000)


def _group_ev_by_state(rows) -> dict[str, float | None]:
    ev_by_state = {state: None for state in REFINEMENT_STATES}
    for row in rows:
        ev_by_state[row["state"]] = row["expected_value_plat"]
    return ev_by_state


def _price_payload(row) -> dict[str, object]:
    return {
        "order_count": row["order_count"],
        "min_sell_price": row["min_sell_price"],
        "avg_sell_price": row["avg_sell_price"],
        "current_median_sell_price": row["median_sell_price"],
        "historical_median_90d": row["historical_median_90d"],
        "historical_wa_price_90d": row["historical_wa_price_90d"],
    }


def _split_names(value: str | None) -> list[str]:
    return sorted(filter(None, str(value or "").split(",")))


def _normalize_name_lookup(value: str) -> str:
    lowered = value.lower().replace("’", "'")
    return "".join(ch for ch in lowered if ch.isalnum())


def _syndicate_name_case_expression(column: str) -> str:
    parts = []
    for name in SYNDICATE_VENDOR_NAMES:
        escaped = name.replace("'", "''")
        parts.append(f"WHEN {column} = '{escaped}' OR {column} LIKE '{escaped},%' THEN '{escaped}'")
    return "CASE " + " ".join(parts) + " ELSE NULL END"


def _top_relic_cards(limit: int, include_vaulted: bool, sort_state: str) -> list[dict[str, object]]:
    sort_state = _validate_state(sort_state)
    limit_value = _normalize_optional_limit(limit)

    conn = get_con()
    try:
        query = """
            SELECT
                r.id,
                r.tier,
                r.relic_name,
                r.image_name,
                r.is_vaulted,
                MAX(CASE WHEN ev.state = 'Intact' THEN ev.expected_value_plat END) AS intact_ev,
                MAX(CASE WHEN ev.state = 'Exceptional' THEN ev.expected_value_plat END) AS exceptional_ev,
                MAX(CASE WHEN ev.state = 'Flawless' THEN ev.expected_value_plat END) AS flawless_ev,
                MAX(CASE WHEN ev.state = 'Radiant' THEN ev.expected_value_plat END) AS radiant_ev
            FROM relics r
            LEFT JOIN relic_ev ev ON ev.relic_id = r.id
            WHERE (? = 1 OR r.is_vaulted = 0)
            GROUP BY r.id, r.tier, r.relic_name, r.image_name, r.is_vaulted
            ORDER BY
                CASE ?
                    WHEN 'Intact' THEN COALESCE(intact_ev, -1)
                    WHEN 'Exceptional' THEN COALESCE(exceptional_ev, -1)
                    WHEN 'Flawless' THEN COALESCE(flawless_ev, -1)
                    ELSE COALESCE(radiant_ev, -1)
                END DESC,
                r.tier,
                r.relic_name
        """
        params: list[object] = [int(include_vaulted), sort_state]
        if limit_value is not None:
            query += " LIMIT ?"
            params.append(limit_value)
        relic_rows = conn.execute(query, params).fetchall()

        cards = []
        for row in relic_rows:
            ev_by_state = {
                "Intact": row["intact_ev"],
                "Exceptional": row["exceptional_ev"],
                "Flawless": row["flawless_ev"],
                "Radiant": row["radiant_ev"],
            }
            cards.append(
                {
                    "tier": row["tier"],
                    "relic_name": row["relic_name"],
                    "name": f"{row['tier']} {row['relic_name']}",
                    "image_name": row["image_name"],
                    "is_vaulted": row["is_vaulted"],
                    "ev": ev_by_state.get(sort_state),
                    "ev_by_state": ev_by_state,
                }
            )
        return cards
    finally:
        conn.close()


def _top_mission_cards(limit: int, include_vaulted: bool, state: str) -> list[dict[str, object]]:
    state = _validate_state(state)
    limit_value = _normalize_optional_limit(limit)

    conn = get_con()
    try:
        query = """
            SELECT
                ms.planet,
                ms.node,
                ms.game_mode,
                ms.rotation,
                ms.drop_chance,
                r.tier,
                r.relic_name,
                r.image_name,
                r.is_vaulted,
                ev.expected_value_plat,
                ROUND(ev.expected_value_plat * (ms.drop_chance / 100.0), 2) AS expected_plat_per_reward
            FROM relic_mission_sources ms
            JOIN relics r ON r.id = ms.relic_id
            JOIN relic_ev ev ON ev.relic_id = r.id AND ev.state = ?
            WHERE (? = 1 OR r.is_vaulted = 0)
            ORDER BY expected_plat_per_reward DESC, ms.drop_chance DESC
        """
        params: list[object] = [state, int(include_vaulted)]
        if limit_value is not None:
            query += " LIMIT ?"
            params.append(limit_value)
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _mods_query(
    limit: int | None = None,
    search: str | None = None,
) -> tuple[str, list[object]]:
    syndicate_case = _syndicate_name_case_expression("ms.source_name")
    query = """
        SELECT
            m.id,
            m.mod_name,
            m.mod_type,
            m.compat_name,
            m.polarity,
            m.rarity,
            m.base_drain,
            m.fusion_limit,
            m.is_augment,
            m.is_syndicate_augment,
            i.wfcd_id,
            i.wfm_slug,
            i.image_name,
            i.is_tradable,
            p.order_count,
            p.min_sell_price,
            p.avg_sell_price,
            p.median_sell_price,
            p.historical_median_90d,
            p.historical_wa_price_90d,
            COUNT(ms.id) AS source_count,
            MAX(CASE WHEN ms.source_category = 'enemy' OR ms.enemy_mod_drop_chance IS NOT NULL THEN 1 ELSE 0 END) AS has_enemy_source,
            MAX(CASE WHEN ms.source_name LIKE '%%, Rotation %%' THEN 1 ELSE 0 END) AS has_mission_source,
            MAX(CASE
                WHEN ms.id IS NOT NULL
                 AND NOT (ms.source_category = 'enemy' OR ms.enemy_mod_drop_chance IS NOT NULL)
                 AND ms.source_name NOT LIKE '%%, Rotation %%'
                THEN 1
                ELSE 0
            END) AS has_other_source,
            GROUP_CONCAT(
                DISTINCT CASE
                    WHEN ms.source_category = 'vendor' THEN ms.source_name
                    ELSE NULL
                END
            ) AS vendor_names,
            GROUP_CONCAT(
                DISTINCT """ + syndicate_case + """
            ) AS syndicate_names
        FROM mods m
        JOIN items i ON i.id = m.item_id
        LEFT JOIN v_item_prices p ON p.item_id = i.id
        LEFT JOIN mod_sources ms ON ms.mod_id = m.id
        WHERE 1 = 1
    """
    params: list[object] = []
    if search:
        query += " AND LOWER(m.mod_name) LIKE ?"
        params.append(f"%{search.lower()}%")
    query += """
        GROUP BY
            m.id,
            m.mod_name,
            m.mod_type,
            m.compat_name,
            m.polarity,
            m.rarity,
            m.base_drain,
            m.fusion_limit,
            m.is_augment,
            m.is_syndicate_augment,
            i.wfcd_id,
            i.wfm_slug,
            i.image_name,
            i.is_tradable,
            p.order_count,
            p.min_sell_price,
            p.avg_sell_price,
            p.median_sell_price,
            p.historical_median_90d,
            p.historical_wa_price_90d
        ORDER BY
            COALESCE(p.historical_median_90d, p.median_sell_price, p.avg_sell_price, 0) DESC,
            m.mod_name
    """
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return query, params


@app.post("/sync/mods")
@app.post("/api/sync/mods")
def sync_mod_data(force: bool = False):
    try:
        return sync_mods(force=force)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Mod sync failed: {exc}") from exc


@app.get("/")
def frontend():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/status")
def status():
    return {
        "status": "ok",
        "scheduler": _scheduler_payload(),
    }


@app.post("/sync/items")
def sync_items(force: bool = False):
    try:
        return sync_relics(force=force)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Item sync failed: {exc}") from exc


@app.post("/sync/market")
def sync_market(limit: int = 0):
    try:
        result = sync_market_orders(limit=limit)
        result["relic_ev"] = calculate_relic_ev()
        return result
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Market sync failed: {exc}") from exc


@app.get("/scheduler")
def scheduler_status():
    return _scheduler_payload()


@app.post("/sync/drops")
def sync_drops(force: bool = False):
    try:
        return sync_drop_sources(force=force)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Drop sync failed: {exc}") from exc


@app.get("/relics")
def list_relics(
    tier: str | None = None,
    vaulted: bool | None = None,
    limit: int = 50,
):
    limit = _clamp_limit(limit)
    conn = get_con()
    try:
        query = """
            SELECT
                id,
                wfcd_id,
                wfm_slug,
                tier,
                relic_name,
                image_name,
                is_vaulted
            FROM relics
            WHERE 1 = 1
        """
        params: list[object] = []

        if tier:
            query += " AND tier = ?"
            params.append(tier)
        if vaulted is not None:
            query += " AND is_vaulted = ?"
            params.append(int(vaulted))

        query += " ORDER BY tier, relic_name LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@app.get("/relics/top")
def top_relics(
    state: str = "Intact",
    limit: int = 20,
):
    return _top_relic_cards(limit=limit, include_vaulted=False, sort_state=state)


@app.get("/relics/top/vaulted")
def top_relics_including_vaulted(
    state: str = "Intact",
    limit: int = 20,
):
    return _top_relic_cards(limit=limit, include_vaulted=True, sort_state=state)


@app.get("/missions/top")
def top_missions(
    state: str = "Intact",
    limit: int = 20,
):
    return _top_mission_cards(limit=limit, include_vaulted=False, state=state)


@app.get("/missions/top/vaulted")
def top_missions_including_vaulted(
    state: str = "Intact",
    limit: int = 20,
):
    return _top_mission_cards(limit=limit, include_vaulted=True, state=state)


@app.get("/relics/{tier}/{relic_name}")
def get_relic(tier: str, relic_name: str):
    conn = get_con()
    try:
        relic = conn.execute(
            """
            SELECT
                id,
                wfcd_id,
                wfm_slug,
                tier,
                relic_name,
                image_name,
                is_vaulted
            FROM relics
            WHERE tier = ? AND relic_name = ?
            """,
            [tier, relic_name],
        ).fetchone()

        if relic is None:
            raise HTTPException(status_code=404, detail="Relic not found")

        ev_rows = conn.execute(
            """
            SELECT state, expected_value_plat
            FROM relic_ev
            WHERE relic_id = ?
            """,
            [relic["id"]],
        ).fetchall()

        reward_rows = conn.execute(
            """
            SELECT
                i.item_name,
                i.wfm_slug,
                i.image_name,
                i.is_tradable,
                rr.rarity,
                rc.state,
                rc.drop_chance,
                p.order_count,
                p.min_sell_price,
                ROUND(p.avg_sell_price, 2) AS avg_sell_price,
                p.median_sell_price,
                p.historical_median_90d,
                p.historical_wa_price_90d
            FROM relic_rewards rr
            JOIN items i ON i.id = rr.item_id
            JOIN relic_reward_chances rc ON rc.relic_reward_id = rr.id
            LEFT JOIN v_item_prices p ON p.item_id = i.id
            WHERE rr.relic_id = ?
            ORDER BY
                CASE rr.rarity
                    WHEN 'Rare' THEN 1
                    WHEN 'Uncommon' THEN 2
                    ELSE 3
                END,
                i.item_name,
                rc.state
            """,
            [relic["id"]],
        ).fetchall()

        mission_rows = conn.execute(
            """
            SELECT
                planet,
                node,
                game_mode,
                rotation,
                drop_chance,
                rarity
            FROM relic_mission_sources
            WHERE relic_id = ?
            ORDER BY drop_chance DESC, planet, node, rotation
            """,
            [relic["id"]],
        ).fetchall()

        transient_rows = conn.execute(
            """
            SELECT
                objective_name,
                drop_chance,
                rarity
            FROM relic_transient_sources
            WHERE relic_id = ?
            ORDER BY drop_chance DESC, objective_name
            """,
            [relic["id"]],
        ).fetchall()

        rewards: list[dict[str, object]] = []
        reward_index: dict[tuple[str, str], dict[str, object]] = {}
        for row in reward_rows:
            key = (row["item_name"], row["rarity"])
            reward = reward_index.get(key)
            if reward is None:
                reward = {
                    "item_name": row["item_name"],
                    "wfm_slug": row["wfm_slug"],
                    "image_name": row["image_name"],
                    "is_tradable": row["is_tradable"],
                    "rarity": row["rarity"],
                    "order_count": row["order_count"],
                    "min_sell_price": row["min_sell_price"],
                    "avg_sell_price": row["avg_sell_price"],
                    "median_sell_price": row["median_sell_price"],
                    "historical_median_90d": row["historical_median_90d"],
                    "historical_wa_price_90d": row["historical_wa_price_90d"],
                    "chances": {},
                }
                reward_index[key] = reward
                rewards.append(reward)
            reward["chances"][row["state"]] = row["drop_chance"]

        return {
            **dict(relic),
            "name": f"{relic['tier']} {relic['relic_name']}",
            "ev_by_state": _group_ev_by_state(ev_rows),
            "rewards": rewards,
            "missions": [dict(row) for row in mission_rows],
            "transient_sources": [dict(row) for row in transient_rows],
        }
    finally:
        conn.close()


@app.get("/mods")
@app.get("/api/mods")
def list_mods(
    search: str | None = None,
    limit: int = 50,
):
    requested_limit = _normalize_optional_limit(limit)
    conn = get_con()
    try:
        query, params = _mods_query(limit=None, search=search)
        rows = conn.execute(query, params).fetchall()
        payload = []
        for row in rows:
            syndicate_names = _split_names(row["syndicate_names"])
            if syndicate_names:
                continue
            payload.append(
                {
                "id": row["id"],
                "name": row["mod_name"],
                "mod_name": row["mod_name"],
                "mod_type": row["mod_type"],
                "compat_name": row["compat_name"],
                "polarity": row["polarity"],
                "rarity": row["rarity"],
                "base_drain": row["base_drain"],
                "fusion_limit": row["fusion_limit"],
                "is_augment": row["is_augment"],
                "is_syndicate_augment": row["is_syndicate_augment"],
                "wfcd_id": row["wfcd_id"],
                "wfm_slug": row["wfm_slug"],
                "image_name": row["image_name"],
                "is_tradable": row["is_tradable"],
                "price": _price_payload(row),
                "source_count": row["source_count"],
                "vendor_names": _split_names(row["vendor_names"]),
                "source_flags": {
                    "enemy": bool(row["has_enemy_source"]),
                    "mission": bool(row["has_mission_source"]),
                    "other": bool(row["has_other_source"]),
                },
            }
            )
        if requested_limit is not None:
            return payload[:requested_limit]
        return payload
    finally:
        conn.close()


@app.get("/syndicate-mods")
@app.get("/api/syndicate-mods")
def list_syndicate_mods(
    search: str | None = None,
    vendor: str | None = None,
    limit: int = 50,
):
    requested_limit = _normalize_optional_limit(limit)
    conn = get_con()
    try:
        query, params = _mods_query(limit=None, search=search)
        rows = conn.execute(query, params).fetchall()
        payload = []
        for row in rows:
            vendor_names = _split_names(row["syndicate_names"])
            if not vendor_names:
                continue
            if vendor and vendor not in vendor_names:
                continue
            payload.append(
                {
                    "id": row["id"],
                    "name": row["mod_name"],
                    "mod_name": row["mod_name"],
                    "mod_type": row["mod_type"],
                    "compat_name": row["compat_name"],
                    "polarity": row["polarity"],
                    "rarity": row["rarity"],
                    "base_drain": row["base_drain"],
                    "fusion_limit": row["fusion_limit"],
                    "is_augment": row["is_augment"],
                    "is_syndicate_augment": row["is_syndicate_augment"],
                    "wfcd_id": row["wfcd_id"],
                    "wfm_slug": row["wfm_slug"],
                    "image_name": row["image_name"],
                    "is_tradable": row["is_tradable"],
                    "price": _price_payload(row),
                    "source_count": row["source_count"],
                    "vendor_names": vendor_names,
                    "source_flags": {
                        "enemy": bool(row["has_enemy_source"]),
                        "mission": bool(row["has_mission_source"]),
                        "other": bool(row["has_other_source"]),
                    },
                }
            )
        if requested_limit is not None:
            return payload[:requested_limit]
        return payload
    finally:
        conn.close()


@app.get("/syndicate-vendors")
@app.get("/api/syndicate-vendors")
def list_syndicate_vendors():
    return list(SYNDICATE_VENDOR_NAMES)


@app.get("/mods/{mod_name:path}")
@app.get("/api/mods/{mod_name:path}")
def get_mod(mod_name: str):
    conn = get_con()
    try:
        mod = conn.execute(
            """
            SELECT
                m.id,
                m.mod_name,
                m.mod_type,
                m.compat_name,
                m.polarity,
                m.rarity,
                m.base_drain,
                m.fusion_limit,
                m.is_augment,
                m.is_syndicate_augment,
                i.wfcd_id,
                i.wfm_slug,
                i.image_name,
                i.is_tradable,
                p.order_count,
                p.min_sell_price,
                p.avg_sell_price,
                p.median_sell_price,
                p.historical_median_90d,
                p.historical_wa_price_90d
            FROM mods m
            JOIN items i ON i.id = m.item_id
            LEFT JOIN v_item_prices p ON p.item_id = i.id
            WHERE LOWER(m.mod_name) = LOWER(?)
               OR LOWER(REPLACE(REPLACE(m.mod_name, '''', ''), '’', '')) = LOWER(REPLACE(REPLACE(?, '''', ''), '’', ''))
            """,
            [mod_name, mod_name],
        ).fetchone()

        if mod is None:
            normalized_lookup = _normalize_name_lookup(mod_name)
            rows = conn.execute(
                """
                SELECT
                    m.id,
                    m.mod_name,
                    m.mod_type,
                    m.compat_name,
                    m.polarity,
                    m.rarity,
                    m.base_drain,
                    m.fusion_limit,
                    m.is_augment,
                    m.is_syndicate_augment,
                    i.wfcd_id,
                    i.wfm_slug,
                    i.image_name,
                    i.is_tradable,
                    p.order_count,
                    p.min_sell_price,
                    p.avg_sell_price,
                    p.median_sell_price,
                    p.historical_median_90d,
                    p.historical_wa_price_90d
                FROM mods m
                JOIN items i ON i.id = m.item_id
                LEFT JOIN v_item_prices p ON p.item_id = i.id
                """
            ).fetchall()
            mod = next((row for row in rows if _normalize_name_lookup(row["mod_name"]) == normalized_lookup), None)
        if mod is None:
            raise HTTPException(status_code=404, detail="Mod not found")

        sources = conn.execute(
            """
            SELECT
                source_category,
                source_name,
                source_detail,
                chance,
                rarity,
                enemy_mod_drop_chance,
                standing
            FROM mod_sources
            WHERE mod_id = ?
            ORDER BY
                CASE
                    WHEN source_category = 'enemy' OR enemy_mod_drop_chance IS NOT NULL THEN 1
                    WHEN source_name LIKE '%, Rotation %' THEN 2
                    ELSE 3
                END,
                COALESCE(chance, enemy_mod_drop_chance, -1) DESC,
                source_name,
                source_detail
            """,
            [mod["id"]],
        ).fetchall()

        payload = dict(mod)
        vendor_names = sorted(
            {
                str(row["source_name"])
                for row in sources
                if row["source_category"] == "vendor" and row["source_name"]
            }
        )
        payload.update(
            {
                "name": mod["mod_name"],
                "price": _price_payload(mod),
                "sources": [dict(row) for row in sources],
                "source_count": len(sources),
                "vendor_names": vendor_names,
            }
        )
        return payload
    finally:
        conn.close()
