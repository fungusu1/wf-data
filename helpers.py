import sqlite3 as s

DB_PATH = 'warframe.db'

def get_con() -> s.Connection:
    conn = s.connect(DB_PATH)
    conn.row_factory = s.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn