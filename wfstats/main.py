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
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    conn = create_db()
    conn.close()
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
def sync_market(limit: int = 5):
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
