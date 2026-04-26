import sqlite3 as s

from wfstats.database import DB_PATH, create_db

def get_con() -> s.Connection:
    create_db(DB_PATH)
    conn = s.connect(DB_PATH)
    conn.row_factory = s.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn
