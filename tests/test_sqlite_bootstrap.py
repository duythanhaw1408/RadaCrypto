import sqlite3
from pathlib import Path

def test_sqlite_sql_files_exist():
    root = Path(__file__).resolve().parents[1]
    assert (root / "sql/sqlite/001_state.sql").exists()
    assert (root / "sql/sqlite/002_indexes.sql").exists()

def test_sqlite_bootstrap_executes(tmp_path):
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(db_path)
    root = Path(__file__).resolve().parents[1]
    sql1 = (root / "sql/sqlite/001_state.sql").read_text(encoding="utf-8")
    sql2 = (root / "sql/sqlite/002_indexes.sql").read_text(encoding="utf-8")
    conn.executescript(sql1)
    conn.executescript(sql2)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {r[0] for r in rows}
    assert "instrument_dim" in names
    assert "thesis" in names
    conn.close()
