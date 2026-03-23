#!/usr/bin/env python3
"""
CFTE Pro Dashboard — Giao diện web cao cấp theo dõi Crypto Flow Thesis Engine.
Chạy: PYTHONPATH=src python3 scripts/dashboard.py
Truy cập: http://localhost:8686
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "data"
STATE_DB = DATA / "state" / "state.db"
PORT = 8686


def _read_json(path: Path) -> dict | list | None:
    try:
        if not path.exists(): return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_jsonl_tail(path: Path, n: int = 50) -> list[dict]:
    try:
        if not path.exists(): return []
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        return [json.loads(l) for l in lines[-n:]]
    except Exception:
        return []


def _read_log_tail(path: Path, n: int = 100) -> str:
    try:
        if not path.exists(): return "(Chưa có log)"
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        return "\n".join(lines[-n:])
    except Exception:
        return "(Lỗi đọc log)"


def _query_db():
    if not STATE_DB.exists():
        return {"thesis_count": 0, "outcome_count": 0, "event_count": 0, "theses": [], "tpfm_m5": [], "tpfm_m30": [], "tpfm_4h": []}
    try:
        conn = sqlite3.connect(STATE_DB)
        conn.row_factory = sqlite3.Row
        
        tc = conn.execute("SELECT COUNT(*) FROM thesis").fetchone()[0]
        oc = conn.execute("SELECT COUNT(*) FROM thesis_outcome").fetchone()[0]
        ec = conn.execute("SELECT COUNT(*) FROM thesis_event").fetchone()[0]
        
        theses = [dict(r) for r in conn.execute(
            "SELECT * FROM thesis ORDER BY opened_ts DESC LIMIT 50"
        ).fetchall()]
        
        tpfm_m5 = [dict(r) for r in conn.execute(
            "SELECT * FROM tpfm_m5_snapshot ORDER BY window_end_ts DESC LIMIT 100"
        ).fetchall()]
        
        tpfm_m30 = [dict(r) for r in conn.execute(
            "SELECT * FROM tpfm_m30_regime ORDER BY window_end_ts DESC LIMIT 50"
        ).fetchall()]
        
        tpfm_4h = [dict(r) for r in conn.execute(
            "SELECT * FROM tpfm_4h_structural ORDER BY window_end_ts DESC LIMIT 10"
        ).fetchall()]
        
        conn.close()
        return {
            "thesis_count": tc, "outcome_count": oc, "event_count": ec, 
            "theses": theses, "tpfm_m5": tpfm_m5, "tpfm_m30": tpfm_m30, "tpfm_4h": tpfm_4h
        }
    except Exception as e:
        return {"error": str(e)}


def _build_api_data():
    health = _read_json(DATA / "review" / "health_status.json")
    daily = _read_json(DATA / "review" / "daily_summary.json")
    weekly = _read_json(DATA / "review" / "weekly_review.json")
    thesis_log = _read_jsonl_tail(DATA / "thesis" / "thesis_log.jsonl", 50)
    live_log = _read_log_tail(Path("/tmp/cfte_live_stdout.log"), 100)
    db = _query_db()
    
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "health": health,
        "daily": daily,
        "weekly": weekly,
        "thesis_log": thesis_log,
        "live_log": live_log,
        "db": db,
    }


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # Serve from the 'docs' directory relative to the project root
        directory = BASE / "docs"
        super().__init__(*args, directory=str(directory), **kwargs)

    def do_GET(self):
        # We can still keep /api/data for legacy or custom needs
        path = urlparse(self.path).path
        if path == "/api/data":
            data = _build_api_data()
            body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        else:
            # Fallback to standard static file serving (from docs/ as set in __init__)
            return super().do_GET()

    def log_message(self, format, *args):
        pass


def main():
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"🚀 CFTE Pro Dashboard đang chạy tại: http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()


if __name__ == "__main__":
    main()
