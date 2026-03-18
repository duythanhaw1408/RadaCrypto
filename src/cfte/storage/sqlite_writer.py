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

    async def save_thesis(self, signal: ThesisSignal, opened_ts: int, closed_ts: int | None = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO thesis (
                    thesis_id, instrument_key, setup, direction, timeframe,
                    regime_bucket, stage, score, confidence, coverage,
                    opened_ts, closed_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            await db.commit()

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
