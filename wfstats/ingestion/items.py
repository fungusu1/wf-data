"""
ingests from warframe-items Relics.json, updates:
    - items
    - relics
    - relic rewards
    - relic reward chances
run when warframe is updated
""" 

# get raw data
# method for upserting item
# upserting relic, chance
# method to iterate over raw and use the upsert methods to db
# driver to run and error handle
# hosting on RENDER?

import requests
import sqlite3
import logging
import json
import hashlib

from database import DB_PATH, create_db

RELICS_URL = "https://raw.githubusercontent.com/WFCD/warframe-items/master/data/json/Relics.json"
SOURCE_NAME = "warframe_items"
LOG_LEVEL   = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---
CHANCE_TABLE: dict[str, dict[str, float]] = {
    "Common":   {"Intact": 0.2533, "Exceptional": 0.2333, "Flawless": 0.2000, "Radiant": 0.1667},
    "Uncommon": {"Intact": 0.1100, "Exceptional": 0.1333, "Flawless": 0.1667, "Radiant": 0.2000},
    "Rare":     {"Intact": 0.0200, "Exceptional": 0.0400, "Flawless": 0.0600, "Radiant": 0.1000},
}

REFINEMENT_STATES = ["Intact", "Exceptional", "Flawless", "Radiant"]

# ----------------------------------------------------------------------------------
# request #
def get_relics() -> tuple[list[dict], str]:
    """returns parsed json and hash from relic data"""
    log.info(f"Getting {RELICS_URL}")
    r = requests.get(RELICS_URL, timeout=30)
    r.raise_for_status()
    raw = r.content
    digest = hashlib.sha256(raw).hexdigest()
    return json.loads(raw), digest

def check_hash():
    pass


if __name__ == "__main__":
    raw, digest = get_relics()
    print(raw)
    print(digest)



