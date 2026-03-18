from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import aiosqlite

from cfte.models.events import ThesisSignal
from cfte.thesis.state import ThesisEventRecord


class ThesisSQLiteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    async def migrate_schema(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            # Check if entry_px exists in thesis table
            async with db.execute("PRAGMA table_info(thesis)") as cursor:
                columns = await cursor.fetchall()
                column_names = [c[1] for c in columns]
                
                if "entry_px" not in column_names:
                    print("Đang nâng cấp cơ sở dữ liệu (thêm entry_px)...")
                    await db.execute("ALTER TABLE thesis ADD COLUMN entry_px REAL")
            
            # Ensure thesis_outcome table exists
            await db.execute("""
                CREATE TABLE IF NOT EXISTS thesis_outcome (
                    thesis_id TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    target_ts INTEGER NOT NULL,
                    realized_px REAL,
                    realized_high REAL,
                    realized_low REAL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (thesis_id, horizon),
                    FOREIGN KEY (thesis_id) REFERENCES thesis(thesis_id)
                )
            """)
            await db.commit()

    async def save_thesis(self, signal: ThesisSignal, opened_ts: int, entry_px: float | None = None, closed_ts: int | None = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO thesis (
                    thesis_id, instrument_key, setup, direction, timeframe,
                    regime_bucket, stage, score, confidence, coverage,
                    opened_ts, closed_ts, entry_px
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.thesis_id,
                    signal.instrument_key,
                    signal.setup,
                    signal.direction,
                    signal.timeframe,
                    signal.regime_bucket,
                    signal.stage,
                    signal.score,
                    signal.confidence,
                    signal.coverage,
                    opened_ts,
                    closed_ts,
                    entry_px,
                ),
            )
            await db.commit()

    async def init_outcomes(self, thesis_id: str, horizons: list[str], opened_ts: int) -> None:
        horizon_seconds = {
            "1h": 3600,
            "4h": 14400,
            "24h": 86400
        }
        async with aiosqlite.connect(self.db_path) as db:
            now = int(time.time() * 1000)
            for h in horizons:
                seconds = horizon_seconds.get(h)
                if not seconds:
                    continue
                target_ts = opened_ts + (seconds * 1000)
                await db.execute(
                    """
                    INSERT OR IGNORE INTO thesis_outcome (
                        thesis_id, horizon, target_ts, status, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (thesis_id, h, target_ts, "PENDING", now),
                )
            await db.commit()

    async def save_outcome(self, thesis_id: str, horizon: str, realized_px: float, realized_high: float, realized_low: float) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            now = int(time.time() * 1000)
            await db.execute(
                """
                UPDATE thesis_outcome 
                SET realized_px = ?, realized_high = ?, realized_low = ?, status = ?, updated_at = ?
                WHERE thesis_id = ? AND horizon = ?
                """,
                (realized_px, realized_high, realized_low, "COMPLETED", now, thesis_id, horizon),
            )
            await db.commit()

    async def get_pending_outcomes(self) -> list[dict[str, Any]]:
        now = int(time.time() * 1000)
        query = """
            SELECT o.*, t.instrument_key, t.entry_px 
            FROM thesis_outcome o
            JOIN thesis t ON o.thesis_id = t.thesis_id
            WHERE o.status = 'PENDING' AND o.target_ts <= ?
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, (now,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_thesis_outcomes(self, thesis_id: str) -> list[dict[str, Any]]:
        query = "SELECT * FROM thesis_outcome WHERE thesis_id = ?"
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, (thesis_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def append_event(self, event: ThesisEventRecord) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO thesis_event (
                    thesis_id, event_type, delta_score, reason_json, event_ts
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.thesis_id,
                    event.event_type,
                    0.0,  # delta_score not fully used yet in Record, but kept for schema
                    json.dumps({"summary_vi": event.summary_vi, "score": event.score}, ensure_ascii=False),
                    event.event_ts,
                ),
            )
            await db.commit()

    async def get_active_thesis(self, instrument_key: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM thesis WHERE closed_ts IS NULL"
        params = []
        if instrument_key:
            query += " AND instrument_key = ?"
            params.append(instrument_key)

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_recent_thesis(self, limit: int = 5) -> list[dict[str, Any]]:
        query = "SELECT * FROM thesis ORDER BY opened_ts DESC LIMIT ?"
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_daily_summary_stats(self, date_str: str | None = None) -> dict[str, Any]:
        """
        Returns stats for the given date (default today).
        date_str format: 'YYYY-MM-DD'
        """
        if not date_str:
            date_str = time.strftime("%Y-%m-%d")
        
        start_ts = int(time.mktime(time.strptime(date_str, "%Y-%m-%d")) * 1000)
        end_ts = start_ts + 86400000 # 24h in ms
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Total signals opened today
            async with db.execute(
                "SELECT COUNT(*) as cnt, AVG(score) as avg_score FROM thesis WHERE opened_ts >= ? AND opened_ts < ?",
                (start_ts, end_ts)
            ) as cursor:
                row = await cursor.fetchone()
                total_new = row["cnt"]
                avg_score = row["avg_score"] or 0.0
            
            # Stage distribution for those signals
            async with db.execute(
                "SELECT stage, COUNT(*) as cnt FROM thesis WHERE opened_ts >= ? AND opened_ts < ? GROUP BY stage",
                (start_ts, end_ts)
            ) as cursor:
                stage_rows = await cursor.fetchall()
                stage_dist = {row["stage"]: row["cnt"] for row in stage_rows}
            
            # Outcomes completed today
            async with db.execute(
                "SELECT COUNT(*) as cnt FROM thesis_outcome WHERE updated_at >= ? AND updated_at < ? AND status = 'COMPLETED'",
                (start_ts, end_ts)
            ) as cursor:
                row = await cursor.fetchone()
                outcomes_count = row["cnt"]
            
            return {
                "date": date_str,
                "total_new": total_new,
                "avg_score": avg_score,
                "stage_dist": stage_dist,
                "outcomes_count": outcomes_count
            }

    async def get_setup_scorecard(self) -> list[dict[str, Any]]:
        """
        Returns performance stats grouped by setup.
        """
        query = """
            SELECT 
                t.setup,
                COUNT(DISTINCT t.thesis_id) as total_signals,
                AVG(t.score) as avg_score,
                o.horizon,
                COUNT(o.thesis_id) as outcome_count,
                AVG((o.realized_px - t.entry_px) / t.entry_px * 100) as avg_return
            FROM thesis t
            LEFT JOIN thesis_outcome o ON t.thesis_id = o.thesis_id AND o.status = 'COMPLETED'
            WHERE t.entry_px IS NOT NULL AND t.entry_px > 0
            GROUP BY t.setup, o.horizon
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                
                # Pivot by setup
                scorecard = {}
                for row in rows:
                    setup = row["setup"]
                    if setup not in scorecard:
                        scorecard[setup] = {
                            "setup": setup,
                            "total_signals": row["total_signals"],
                            "avg_score": row["avg_score"],
                            "horizons": {}
                        }
                    if row["horizon"]:
                        scorecard[setup]["horizons"][row["horizon"]] = {
                            "count": row["outcome_count"],
                            "avg_return": row["avg_return"]
                        }
                
                return list(scorecard.values())
