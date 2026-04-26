import sqlite3 as s

DB_PATH = "warframe.db"

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ─────────────────────────────────────────────
-- Sync / audit log
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sync_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT    NOT NULL,  -- 'warframe_items' | 'mission_rewards' | 'transient_rewards' | 'wfm_orders'
    fetched_at      INTEGER NOT NULL,  -- unix timestamp
    content_hash    TEXT,              -- hash of raw payload for change detection
    item_count      INTEGER,           -- how many records were upserted
    notes           TEXT
);

-- ─────────────────────────────────────────────
-- Items  (shared marketable entities: relic rewards, mods, etc.)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    wfcd_id         TEXT    UNIQUE NOT NULL,  -- uniqueName  e.g. /Lotus/Weapons/Tenno/Akstiletto/AkstilettoBarrelPrime
    item_name       TEXT    NOT NULL,         -- "Akstiletto Prime Barrel"
    wfm_slug        TEXT    UNIQUE,           -- warframeMarket.urlName, NULL if not tradeable on WFM
    image_name      TEXT,                     -- imageName from warframe-items → https://cdn.warframestat.us/img/{image_name}
    is_tradable     INTEGER NOT NULL DEFAULT 0  -- item.tradable from warframe-items
    -- is_prime_part is intentionally omitted: derive it as (wfcd_id LIKE '%Prime%')
);

-- ─────────────────────────────────────────────
-- Relics  (source: warframe-items Relics.json)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS relics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    wfcd_id         TEXT    UNIQUE NOT NULL,  -- uniqueName from warframe-items
    wfm_slug        TEXT    UNIQUE,           -- marketInfo.urlName (relics can be traded)
    tier            TEXT    NOT NULL,         -- "Axi" | "Neo" | "Meso" | "Lith"
    relic_name      TEXT    NOT NULL,         -- "A1"
    image_name      TEXT,                     -- imageName → CDN
    is_vaulted      INTEGER NOT NULL DEFAULT 0,
    UNIQUE (tier, relic_name)
    -- Refinement state (Intact/Exceptional/Flawless/Radiant) is NOT stored here.
    -- It does not change which items drop — only the per-reward chances, which
    -- live in relic_reward_chances below.
);

-- ─────────────────────────────────────────────
-- Relic rewards  (source: warframe-items Relics.json → rewards[])
-- One row per (relic, item) pair — rarity is fixed regardless of state.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS relic_rewards (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    relic_id        INTEGER NOT NULL REFERENCES relics(id) ON DELETE CASCADE,
    item_id         INTEGER NOT NULL REFERENCES items(id)  ON DELETE CASCADE,
    rarity          TEXT    NOT NULL,   -- "Common" | "Uncommon" | "Rare"
    UNIQUE (relic_id, item_id)
);

-- ─────────────────────────────────────────────
-- Mods  (source: warframe-items Mods.json)
-- One row per mod, with tradeability and augment metadata stored on the item.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mods (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id             INTEGER NOT NULL UNIQUE REFERENCES items(id) ON DELETE CASCADE,
    mod_name            TEXT    NOT NULL,
    mod_type            TEXT,
    compat_name         TEXT,
    polarity            TEXT,
    rarity              TEXT,
    base_drain          INTEGER,
    fusion_limit        INTEGER,
    is_augment          INTEGER NOT NULL DEFAULT 0,
    is_syndicate_augment INTEGER NOT NULL DEFAULT 0
);

-- ─────────────────────────────────────────────
-- Mod sources
-- Includes enemy drops, vendor offerings, and other WFCD-listed acquisition paths.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mod_sources (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    mod_id                  INTEGER NOT NULL REFERENCES mods(id) ON DELETE CASCADE,
    source_category         TEXT    NOT NULL, -- 'enemy' | 'vendor' | 'drop'
    source_name             TEXT    NOT NULL,
    source_detail           TEXT,
    chance                  REAL,
    rarity                  TEXT,
    enemy_mod_drop_chance   REAL,
    standing                INTEGER,
    UNIQUE (mod_id, source_category, source_name, source_detail)
);

-- ─────────────────────────────────────────────
-- Per-state drop chances  (derived from rarity + refinement rules)
-- Populated once at ingest from the known WFCD chance table.
-- Kept as explicit rows so queries don't need to embed the math.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS relic_reward_chances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    relic_reward_id INTEGER NOT NULL REFERENCES relic_rewards(id) ON DELETE CASCADE,
    state           TEXT    NOT NULL,   -- "Intact" | "Exceptional" | "Flawless" | "Radiant"
    drop_chance     REAL    NOT NULL,   -- e.g. 0.1333
    UNIQUE (relic_reward_id, state)
);

-- ─────────────────────────────────────────────
-- Mission relic sources  (source: drop-data /data/missionRewards.json)
-- Which missions drop which relics as a reward.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS relic_mission_sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    relic_id        INTEGER NOT NULL REFERENCES relics(id) ON DELETE CASCADE,
    planet          TEXT    NOT NULL,
    node            TEXT    NOT NULL,
    game_mode       TEXT,
    rotation        TEXT,               -- "A" | "B" | "C" | NULL for non-rotating
    drop_chance     REAL    NOT NULL,
    rarity          TEXT    NOT NULL,
    UNIQUE (relic_id, planet, node, rotation)
);

-- ─────────────────────────────────────────────
-- Transient relic sources  (source: drop-data /data/transientRewards.json)
-- Non-permanent missions (Sorties, Nightmare, etc.)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS relic_transient_sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    relic_id        INTEGER NOT NULL REFERENCES relics(id) ON DELETE CASCADE,
    objective_name  TEXT    NOT NULL,   -- e.g. "Sortie Stage 3 - Exterminate"
    drop_chance     REAL    NOT NULL,
    rarity          TEXT    NOT NULL,
    UNIQUE (relic_id, objective_name)
);

-- ─────────────────────────────────────────────
-- Market orders cache  (source: WFM /v2/orders/item/{slug})
-- Treated as a rolling cache — rows older than your TTL should be deleted
-- before each refresh, not accumulated indefinitely.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id         INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    order_type      TEXT    NOT NULL,   -- "sell" | "buy"
    platinum        INTEGER NOT NULL,
    quantity        INTEGER NOT NULL DEFAULT 1,
    user_name       TEXT    NOT NULL,
    user_status     TEXT,               -- "ingame" | "online" | "offline"
    fetched_at      INTEGER NOT NULL    -- unix timestamp
);

-- ─────────────────────────────────────────────
-- Item price cache
-- Combines current order-book pricing with historical 90-day market stats.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS item_price_cache (
    item_id                  INTEGER PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,
    order_count              INTEGER,
    min_sell_price           REAL,
    avg_sell_price           REAL,
    current_median_sell_price REAL,
    historical_median_90d    REAL,
    historical_wa_price_90d  REAL,
    fetched_at               INTEGER NOT NULL
);

-- ─────────────────────────────────────────────
-- Relic expected value cache
-- Computed from relic_reward_chances × current market_orders.
-- Invalidated and recomputed whenever market_orders is refreshed.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS relic_ev (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    relic_id            INTEGER NOT NULL REFERENCES relics(id) ON DELETE CASCADE,
    state               TEXT    NOT NULL,   -- "Intact" | "Exceptional" | "Flawless" | "Radiant"
    expected_value_plat REAL,
    best_item_id        INTEGER REFERENCES items(id),
    best_item_chance    REAL,
    best_item_price     REAL,
    calculated_at       INTEGER NOT NULL,
    UNIQUE (relic_id, state)
);

-- ─────────────────────────────────────────────
-- Indexes
-- ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_relics_tier_name          ON relics(tier, relic_name);
CREATE INDEX IF NOT EXISTS idx_relic_rewards_relic       ON relic_rewards(relic_id);
CREATE INDEX IF NOT EXISTS idx_relic_rewards_item        ON relic_rewards(item_id);
CREATE INDEX IF NOT EXISTS idx_relic_reward_chances      ON relic_reward_chances(relic_reward_id, state);
CREATE INDEX IF NOT EXISTS idx_mods_item                 ON mods(item_id);
CREATE INDEX IF NOT EXISTS idx_mods_name                 ON mods(mod_name);
CREATE INDEX IF NOT EXISTS idx_mods_syndicate_augment    ON mods(is_syndicate_augment);
CREATE INDEX IF NOT EXISTS idx_mod_sources_mod           ON mod_sources(mod_id);
CREATE INDEX IF NOT EXISTS idx_mod_sources_category      ON mod_sources(source_category, source_name);
CREATE INDEX IF NOT EXISTS idx_mission_sources_relic     ON relic_mission_sources(relic_id);
CREATE INDEX IF NOT EXISTS idx_transient_sources_relic   ON relic_transient_sources(relic_id);
CREATE INDEX IF NOT EXISTS idx_market_orders_item        ON market_orders(item_id, fetched_at);
CREATE INDEX IF NOT EXISTS idx_market_orders_type_price  ON market_orders(item_id, order_type, platinum);
CREATE INDEX IF NOT EXISTS idx_item_price_cache_fetched  ON item_price_cache(fetched_at);
CREATE INDEX IF NOT EXISTS idx_relic_ev_value            ON relic_ev(expected_value_plat DESC);
CREATE INDEX IF NOT EXISTS idx_items_wfm_slug            ON items(wfm_slug);

-- ─────────────────────────────────────────────
-- Views  (convenience — no stored data)
-- ─────────────────────────────────────────────

DROP VIEW IF EXISTS v_item_prices;
CREATE VIEW v_item_prices AS
SELECT
    item_id,
    order_count,
    min_sell_price,
    avg_sell_price,
    current_median_sell_price AS median_sell_price,
    historical_median_90d,
    historical_wa_price_90d
FROM item_price_cache;

DROP VIEW IF EXISTS v_relic_rewards_priced;
CREATE VIEW v_relic_rewards_priced AS
SELECT
    r.tier || ' ' || r.relic_name              AS relic,
    r.is_vaulted,
    i.item_name,
    i.wfm_slug,
    rr.rarity,
    rc.state,
    rc.drop_chance,
    p.min_sell_price,
    ROUND(rc.drop_chance * COALESCE(p.min_sell_price, 0), 2) AS weighted_value
FROM relics r
JOIN relic_rewards       rr ON rr.relic_id        = r.id
JOIN relic_reward_chances rc ON rc.relic_reward_id = rr.id
JOIN items               i  ON i.id               = rr.item_id
LEFT JOIN v_item_prices  p  ON p.item_id           = i.id;
""";

def create_db(path: str = DB_PATH) -> s.Connection:
    conn = s.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

if __name__ == "__main__":
    conn = create_db()
    conn.close()
    print(f"Database created at '{DB_PATH}'")
