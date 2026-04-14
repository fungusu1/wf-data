from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from wfstats.database import create_db
from wfstats.helpers import get_con
from wfstats.ingestion.calculate import calculate_relic_ev
from wfstats.ingestion.drops import sync_drop_sources
from wfstats.ingestion.items import sync_relics
from wfstats.ingestion.market import sync_market_orders
from wfstats.scheduler import get_scheduler_status, start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(_: FastAPI):
    conn = create_db()
    conn.close()
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(title="wfstats", lifespan=lifespan)


@app.get("/")
def root():
    scheduler = get_scheduler_status()
    return {
        "status": "ok",
        "scheduler": scheduler.__dict__,
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
    return get_scheduler_status().__dict__


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
    limit = max(1, min(limit, 200))
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


def _top_relics_response(state: str, limit: int, include_vaulted: bool):
    if state not in {"Intact", "Exceptional", "Flawless", "Radiant"}:
        raise HTTPException(status_code=422, detail="Invalid relic state")
    limit = max(1, min(limit, 200))

    conn = get_con()
    try:
        query = """
            SELECT
                r.tier,
                r.relic_name,
                r.is_vaulted,
                ev.state,
                ev.expected_value_plat,
                i.item_name AS best_item,
                ev.best_item_chance,
                ev.best_item_price
            FROM relic_ev ev
            JOIN relics r ON r.id = ev.relic_id
            LEFT JOIN items i ON i.id = ev.best_item_id
            WHERE ev.state = ?
        """
        params: list[object] = [state]
        if not include_vaulted:
            query += " AND r.is_vaulted = 0"
        query += " ORDER BY ev.expected_value_plat DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _top_missions_response(state: str, limit: int, include_vaulted: bool):
    if state not in {"Intact", "Exceptional", "Flawless", "Radiant"}:
        raise HTTPException(status_code=422, detail="Invalid relic state")
    limit = max(1, min(limit, 200))

    conn = get_con()
    try:
        query = """
            SELECT
                ms.planet,
                ms.node,
                ms.game_mode,
                ms.rotation,
                ms.drop_chance,
                ms.rarity,
                r.tier,
                r.relic_name,
                r.is_vaulted,
                ev.state,
                ev.expected_value_plat,
                ROUND(ev.expected_value_plat * (ms.drop_chance / 100.0), 2) AS expected_plat_per_reward,
                i.item_name AS best_item,
                ev.best_item_chance,
                ev.best_item_price
            FROM relic_mission_sources ms
            JOIN relics r ON r.id = ms.relic_id
            JOIN relic_ev ev ON ev.relic_id = r.id
            LEFT JOIN items i ON i.id = ev.best_item_id
            WHERE ev.state = ?
        """
        params: list[object] = [state]
        if not include_vaulted:
            query += " AND r.is_vaulted = 0"
        query += " ORDER BY expected_plat_per_reward DESC, ms.drop_chance DESC LIMIT ?"
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
    return _top_relics_response(state=state, limit=limit, include_vaulted=False)


@app.get("/relics/top/vaulted")
def top_relics_including_vaulted(
    state: str = "Intact",
    limit: int = 20,
):
    return _top_relics_response(state=state, limit=limit, include_vaulted=True)


@app.get("/missions/top")
def top_missions(
    state: str = "Intact",
    limit: int = 20,
):
    return _top_missions_response(state=state, limit=limit, include_vaulted=False)


@app.get("/missions/top/vaulted")
def top_missions_including_vaulted(
    state: str = "Intact",
    limit: int = 20,
):
    return _top_missions_response(state=state, limit=limit, include_vaulted=True)


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

        reward_rows = conn.execute(
            """
            SELECT
                i.item_name,
                i.wfm_slug,
                i.image_name,
                i.is_tradable,
                rr.rarity,
                rc.state,
                rc.drop_chance
            FROM relic_rewards rr
            JOIN items i ON i.id = rr.item_id
            JOIN relic_reward_chances rc ON rc.relic_reward_id = rr.id
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
                    "chances": {},
                }
                reward_index[key] = reward
                rewards.append(reward)
            reward["chances"][row["state"]] = row["drop_chance"]

        return {
            **dict(relic),
            "rewards": rewards,
        }
    finally:
        conn.close()
