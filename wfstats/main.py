from fastapi import FastAPI, HTTPException
from helpers import get_con

app = FastAPI()

@app.get("/")   
def root():
    return {"status": "working"}

@app.get("/relics")
def list_relics(tier: str = None, state: str = "Intact"):
    conn = get_con()
    query = "SELECT * FROM relics WHERE state = ?"
    params = [state]
    if tier:
        query += " AND tier = ?"
        params.append(tier)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]
    
@app.get("/relics/top")
def top_relics(state: str = "Intact", limit: int = 20):
    """Relics ranked by expected platinum value, best first."""
    conn = get_con()
    rows = conn.execute("""
        SELECT
            r.tier,
            r.relic_name,
            r.state,
            ev.expected_value_plat,
            i.item_name   AS best_item,
            ev.best_item_chance,
            ev.best_item_price
        FROM relic_ev ev
        JOIN relics r ON r.id = ev.relic_id
        LEFT JOIN items i ON i.id = ev.best_item_id
        WHERE ev.state = ?
        ORDER BY ev.expected_value_plat DESC
        LIMIT ?
    """, [state, limit]).fetchall()
    conn.close()
    return [dict(r) for r in rows]
