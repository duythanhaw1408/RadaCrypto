from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "state" / "state.db"
SQL_DIR = ROOT / "sql" / "sqlite"

def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        for name in ["001_state.sql", "002_indexes.sql"]:
            sql = (SQL_DIR / name).read_text(encoding="utf-8")
            conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    print(f"Initialized SQLite DB at: {DB_PATH}")
