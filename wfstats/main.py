from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from wfstats.database import create_db
from wfstats.helpers import get_con
from wfstats.ingestion.items import sync_relics


@asynccontextmanager
async def lifespan(_: FastAPI):
    conn = create_db()
    conn.close()
    yield


app = FastAPI(title="wfstats", lifespan=lifespan)


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/sync/items")
def sync_items(force: bool = False):
    try:
        return sync_relics(force=force)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Item sync failed: {exc}") from exc


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


@app.get("/relics/top")
def top_relics(
    state: str = "Intact",
    limit: int = 20,
):
    if state not in {"Intact", "Exceptional", "Flawless", "Radiant"}:
        raise HTTPException(status_code=422, detail="Invalid relic state")
    limit = max(1, min(limit, 200))
    conn = get_con()
    try:
        rows = conn.execute(
            """
            SELECT
                r.tier,
                r.relic_name,
                ev.state,
                ev.expected_value_plat,
                i.item_name AS best_item,
                ev.best_item_chance,
                ev.best_item_price
            FROM relic_ev ev
            JOIN relics r ON r.id = ev.relic_id
            LEFT JOIN items i ON i.id = ev.best_item_id
            WHERE ev.state = ?
            ORDER BY ev.expected_value_plat DESC
            LIMIT ?
            """,
            [state, limit],
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


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
