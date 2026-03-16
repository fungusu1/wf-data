import sqlite3 as s
import time
from database import create_db, DB_PATH

def seed(conn: s.Connection):
    cur = conn.cursor()
    now = int(time.time())

    # sync_log
    cur.executemany("INSERT OR IGNORE INTO sync_log (source, fetched_at, drop_data_hash, notes) VALUES (?,?,?,?)", [
        ("drop_data",     now, "abc123def456", "Initial seed"),
        ("market_prices", now, None,           "Initial seed"),
    ])

    # relics
    cur.executemany("INSERT OR IGNORE INTO relics (wfcd_id, tier, relic_name, state) VALUES (?,?,?,?)", [
        ("relic-axi-a1-intact",    "Axi",  "A1", "Intact"),
        ("relic-axi-a1-rad",       "Axi",  "A1", "Radiant"),
        ("relic-meso-b1-intact",   "Meso", "B1", "Intact"),
        ("relic-neo-c1-intact",    "Neo",  "C1", "Intact"),
        ("relic-lith-d1-intact",   "Lith", "D1", "Intact"),
    ])

    # relic_mission_sources
    cur.executemany("""
        INSERT OR IGNORE INTO relic_mission_sources
            (relic_id, planet, node, game_mode, rotation, drop_chance, rarity)
        VALUES (?,?,?,?,?,?,?)""", [
        (1, "Sedna",   "Hydron",    "Defense",    "A", 11.11, "Uncommon"),
        (1, "Sedna",   "Hydron",    "Defense",    "B",  7.69, "Uncommon"),
        (3, "Jupiter", "Ganymede",  "Interception","A", 14.29, "Common"),
        (4, "Void",    "Marduk",    "Capture",    None, 20.0,  "Common"),
        (5, "Mars",    "Ara",       "Defense",    "A",  9.09, "Uncommon"),
    ])

    # relic_transient_sources
    cur.executemany("""
        INSERT OR IGNORE INTO relic_transient_sources
            (relic_id, objective_name, drop_chance, rarity)
        VALUES (?,?,?,?)""", [
        (1, "Void Fissure - Capture",   10.0, "Uncommon"),
        (2, "Void Fissure - Survival",   5.0, "Rare"),
        (3, "Void Fissure - Defense",   12.5, "Common"),
    ])

    # items
    cur.executemany("""
        INSERT OR IGNORE INTO items
            (wfcd_id, item_name, wfm_slug, is_prime_part)
        VALUES (?,?,?,?)""", [
        ("item-forma",          "Forma Blueprint",              "forma_blueprint",              0),
        ("item-akstiletto-bar", "Akstiletto Prime Barrel",      "akstiletto_prime_barrel",      1),
        ("item-akstiletto-rec", "Akstiletto Prime Receiver",    "akstiletto_prime_receiver",    1),
        ("item-ash-neuroptics", "Ash Prime Neuroptics",         "ash_prime_neuroptics",         1),
        ("item-orthos-blade",   "Orthos Prime Blade",           "orthos_prime_blade",           1),
        ("item-ash-set",        "Ash Prime Set",                "ash_prime_set",                0),
    ])

    # relic_rewards  (relic_id, item_id, state, drop_chance, rarity)
    cur.executemany("""
        INSERT OR IGNORE INTO relic_rewards
            (relic_id, item_id, state, drop_chance, rarity)
        VALUES (?,?,?,?,?)""", [
        # Axi A1 Intact
        (1, 1, "Intact", 25.33, "Common"),    # Forma
        (1, 2, "Intact", 25.33, "Common"),    # Akstiletto Barrel
        (1, 3, "Intact", 25.33, "Common"),    # Akstiletto Receiver
        (1, 4, "Intact", 11.00, "Uncommon"),  # Ash Neuroptics
        (1, 5, "Intact", 11.00, "Uncommon"),  # Orthos Blade
        (1, 6, "Intact",  2.00, "Rare"),      # Ash Set
        # Axi A1 Radiant (rare item bumped up)
        (2, 1, "Radiant", 16.67, "Common"),
        (2, 2, "Radiant", 16.67, "Common"),
        (2, 3, "Radiant", 16.67, "Common"),
        (2, 4, "Radiant", 16.67, "Uncommon"),
        (2, 5, "Radiant", 16.67, "Uncommon"),
        (2, 6, "Radiant", 16.67, "Rare"),
        # Meso B1 Intact
        (3, 1, "Intact", 25.33, "Common"),
        (3, 4, "Intact", 25.33, "Common"),
        (3, 5, "Intact", 25.33, "Common"),
        (3, 2, "Intact", 11.00, "Uncommon"),
        (3, 3, "Intact", 11.00, "Uncommon"),
        (3, 6, "Intact",  2.00, "Rare"),
    ])

    # item_prices
    cur.executemany("""
        INSERT OR IGNORE INTO item_prices
            (item_id, avg_sell_price, median_sell_price, min_sell_price, order_count, fetched_at)
        VALUES (?,?,?,?,?,?)""", [
        (1,  15.0,  14.0,  10.0, 42, now),   # Forma
        (2,  80.0,  75.0,  60.0, 18, now),   # Akstiletto Barrel
        (3,  55.0,  50.0,  45.0, 22, now),   # Akstiletto Receiver
        (4, 120.0, 115.0,  90.0,  9, now),   # Ash Neuroptics
        (5,  30.0,  28.0,  20.0, 31, now),   # Orthos Blade
        (6, 350.0, 330.0, 280.0,  5, now),   # Ash Set
    ])

    # market_orders_raw
    cur.executemany("""
        INSERT INTO market_orders_raw
            (item_id, order_type, platinum, quantity, user_status, fetched_at)
        VALUES (?,?,?,?,?,?)""", [
        (4, "sell",  90, 1, "ingame",  now),
        (4, "sell", 115, 1, "online",  now),
        (4, "sell", 120, 1, "ingame",  now),
        (4, "sell", 160, 2, "offline", now),
        (6, "sell", 280, 1, "ingame",  now),
        (6, "sell", 340, 1, "online",  now),
        (6, "sell", 420, 1, "offline", now),
    ])

    # relic_ev  —  EV = Σ (drop_chance/100 * avg_price)
    # Axi A1 Intact:  (25.33*.01*15) + (25.33*.01*80) + (25.33*.01*55) + (11*.01*120) + (11*.01*30) + (2*.01*350)
    axi_a1_intact_ev = (
        (25.33/100 * 15) + (25.33/100 * 80) + (25.33/100 * 55) +
        (11/100 * 120)   + (11/100 * 30)    + (2/100 * 350)
    )
    # Axi A1 Radiant: all slots equal at 16.67%
    axi_a1_rad_ev = sum(p * 16.67/100 for p in [15, 80, 55, 120, 30, 350])

    cur.executemany("""
        INSERT OR REPLACE INTO relic_ev
            (relic_id, state, expected_value_plat, best_item_id, best_item_chance, best_item_price, calculated_at)
        VALUES (?,?,?,?,?,?,?)""", [
        (1, "Intact",  round(axi_a1_intact_ev, 2), 6, 2.00,  350.0, now),
        (2, "Radiant", round(axi_a1_rad_ev,    2), 6, 16.67, 350.0, now),
    ])

    conn.commit()
    print("Seed data inserted.")


if __name__ == "__main__":
    conn = create_db(DB_PATH)
    seed(conn)
    conn.close()