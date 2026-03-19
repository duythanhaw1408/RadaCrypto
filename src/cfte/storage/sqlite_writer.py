from __future__ import annotations

import aiosqlite
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

from cfte.models.events import Stage, ThesisSignal
from cfte.thesis.state import ThesisEventRecord

_HORIZON_SECONDS: Final[dict[str, int]] = {
    "1h": 3600,
    "4h": 14400,
    "24h": 86400,
}

_FINAL_REVIEW_HORIZON: Final[str] = "24h"


class ThesisSQLiteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    async def migrate_schema(self) -> None:
        with sqlite3.connect(self.db_path) as db:
            columns = db.execute("PRAGMA table_info(thesis)").fetchall()
            column_names = [c[1] for c in columns]
            if "entry_px" not in column_names:
                print("Đang nâng cấp cơ sở dữ liệu (thêm entry_px)...")
                db.execute("ALTER TABLE thesis ADD COLUMN entry_px REAL")
            if "invalidation_px" not in column_names:
                print("Đang nâng cấp cơ sở dữ liệu (thêm invalidation_px)...")
                db.execute("ALTER TABLE thesis ADD COLUMN invalidation_px REAL")

            db.execute(
                """
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
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS tpfm_m5_snapshot (
                    snapshot_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    window_start_ts INTEGER NOT NULL,
                    window_end_ts INTEGER NOT NULL,
                    initiative_score REAL,
                    initiative_polarity TEXT,
                    inventory_score REAL,
                    inventory_polarity TEXT,
                    energy_score REAL,
                    energy_state TEXT,
                    response_efficiency_score REAL,
                    response_efficiency_state TEXT,
                    matrix_cell TEXT,
                    micro_conclusion TEXT,
                    tradability_score REAL,
                    delta_quote REAL,
                    cvd_slope REAL,
                    trade_burst REAL,
                    active_thesis_count INTEGER,
                    actionable_count INTEGER,
                    health_state TEXT
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS tpfm_m30_regime (
                    regime_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    window_start_ts INTEGER NOT NULL,
                    window_end_ts INTEGER NOT NULL,
                    m5_count INTEGER,
                    dominant_cell TEXT,
                    dominant_regime TEXT,
                    transition_path TEXT,
                    regime_persistence_score REAL,
                    net_delta_quote REAL,
                    avg_trade_burst REAL,
                    macro_conclusion_code TEXT,
                    macro_posture TEXT,
                    health_state TEXT
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS tpfm_4h_structural (
                    structural_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    window_start_ts INTEGER NOT NULL,
                    window_end_ts INTEGER NOT NULL,
                    m30_count INTEGER,
                    dominant_regime_share TEXT,
                    dominant_cell_share TEXT,
                    structural_bias TEXT,
                    transition_map TEXT,
                    net_delta_quote REAL,
                    avg_persistence REAL,
                    ai_analysis_vi TEXT,
                    health_state TEXT
                )
                """
            )
            db.commit()

    async def save_thesis(
        self,
        signal: ThesisSignal,
        opened_ts: int,
        entry_px: float | None = None,
        closed_ts: int | None = None,
    ) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT OR REPLACE INTO thesis (
                    thesis_id, instrument_key, setup, direction, timeframe,
                    regime_bucket, stage, score, confidence, coverage,
                    invalidation_px, opened_ts, closed_ts, entry_px
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    None,
                    opened_ts,
                    closed_ts,
                    entry_px,
                ),
            )
            db.commit()

    async def init_outcomes(self, thesis_id: str, horizons: list[str], opened_ts: int) -> None:
        with sqlite3.connect(self.db_path) as db:
            now = int(time.time() * 1000)
            for horizon in horizons:
                seconds = _HORIZON_SECONDS.get(horizon)
                if seconds is None:
                    continue
                target_ts = opened_ts + (seconds * 1000)
                db.execute(
                    """
                    INSERT OR IGNORE INTO thesis_outcome (
                        thesis_id, horizon, target_ts, status, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (thesis_id, horizon, target_ts, "PENDING", now),
                )
            db.commit()

    async def save_outcome(
        self,
        thesis_id: str,
        horizon: str,
        realized_px: float,
        realized_high: float,
        realized_low: float,
    ) -> None:
        with sqlite3.connect(self.db_path) as db:
            now = int(time.time() * 1000)
            db.execute(
                """
                UPDATE thesis_outcome
                SET realized_px = ?, realized_high = ?, realized_low = ?, status = ?, updated_at = ?
                WHERE thesis_id = ? AND horizon = ?
                """,
                (realized_px, realized_high, realized_low, "COMPLETED", now, thesis_id, horizon),
            )
            db.commit()

    async def get_pending_outcomes(self) -> list[dict[str, Any]]:
        now = int(time.time() * 1000)
        query = """
            SELECT o.*, t.instrument_key, t.entry_px, t.direction, t.stage
            FROM thesis_outcome o
            JOIN thesis t ON o.thesis_id = t.thesis_id
            WHERE o.status = 'PENDING' AND o.target_ts <= ?
        """
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, (now,)).fetchall()
            return [dict(row) for row in rows]

    async def get_thesis_outcomes(self, thesis_id: str) -> list[dict[str, Any]]:
        query = "SELECT * FROM thesis_outcome WHERE thesis_id = ? ORDER BY target_ts ASC"
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, (thesis_id,)).fetchall()
            return [dict(row) for row in rows]

    async def append_event(self, event: ThesisEventRecord) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT INTO thesis_event (
                    thesis_id, event_type, delta_score, reason_json, event_ts
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.thesis_id,
                    event.event_type,
                    0.0,
                    json.dumps(
                        {
                            "summary_vi": event.summary_vi,
                            "score": event.score,
                            "confidence": event.confidence,
                            "from_stage": event.from_stage,
                            "to_stage": event.to_stage,
                        },
                        ensure_ascii=False,
                    ),
                    event.event_ts,
                ),
            )
            db.commit()

    async def update_thesis_stage(self, thesis_id: str, next_stage: Stage, closed_ts: int | None = None) -> None:
        with sqlite3.connect(self.db_path) as db:
            if closed_ts is None:
                db.execute("UPDATE thesis SET stage = ? WHERE thesis_id = ?", (next_stage, thesis_id))
            else:
                db.execute(
                    "UPDATE thesis SET stage = ?, closed_ts = ? WHERE thesis_id = ?",
                    (next_stage, closed_ts, thesis_id),
                )
            db.commit()

    async def get_thesis_by_id(self, thesis_id: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM thesis WHERE thesis_id = ?", (thesis_id,)).fetchone()
            return dict(row) if row else None

    async def get_active_thesis(self, instrument_key: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM thesis WHERE closed_ts IS NULL"
        params: list[Any] = []
        if instrument_key:
            query += " AND instrument_key = ?"
            params.append(instrument_key)

        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    async def get_recent_thesis(self, limit: int = 5) -> list[dict[str, Any]]:
        query = "SELECT * FROM thesis ORDER BY opened_ts DESC LIMIT ?"
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, (limit,)).fetchall()
            return [dict(row) for row in rows]

    async def get_db_diagnostics(self) -> dict[str, Any]:
        """
        Returns stats about the database for health checks.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Thesis count
            async with db.execute("SELECT COUNT(*) as cnt FROM thesis") as cursor:
                row = await cursor.fetchone()
                thesis_count = row["cnt"]
            
            # Outcome count
            async with db.execute("SELECT COUNT(*) as cnt FROM thesis_outcome") as cursor:
                row = await cursor.fetchone()
                outcome_count = row["cnt"]
                
            # Event count
            async with db.execute("SELECT COUNT(*) as cnt FROM thesis_event") as cursor:
                row = await cursor.fetchone()
                event_count = row["cnt"]
            
            return {
                "file_path": str(self.db_path),
                "file_size_kb": self.db_path.stat().st_size / 1024 if self.db_path.exists() else 0,
                "thesis_count": thesis_count,
                "outcome_count": outcome_count,
                "event_count": event_count
            }

    async def get_daily_summary_stats(self, date_str: str | None = None) -> dict[str, Any]:
        if not date_str:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Parse date_str as UTC midnight
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_ts = int(dt.timestamp() * 1000)
        end_ts = start_ts + 86400000
        return await self.get_period_summary(start_ts=start_ts, end_ts=end_ts, label=date_str)

    async def get_period_summary(self, start_ts: int, end_ts: int, label: str) -> dict[str, Any]:
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            opened = db.execute(
                "SELECT COUNT(*) as cnt, AVG(score) as avg_score, AVG(confidence) as avg_confidence FROM thesis WHERE opened_ts >= ? AND opened_ts < ?",
                (start_ts, end_ts),
            ).fetchone()
            stage_rows = db.execute(
                "SELECT stage, COUNT(*) as cnt FROM thesis WHERE opened_ts >= ? AND opened_ts < ? GROUP BY stage ORDER BY cnt DESC",
                (start_ts, end_ts),
            ).fetchall()
            setup_rows = db.execute(
                "SELECT setup, COUNT(*) as cnt FROM thesis WHERE opened_ts >= ? AND opened_ts < ? GROUP BY setup ORDER BY cnt DESC",
                (start_ts, end_ts),
            ).fetchall()
            closed_rows = db.execute(
                "SELECT stage, COUNT(*) as cnt FROM thesis WHERE closed_ts >= ? AND closed_ts < ? GROUP BY stage ORDER BY cnt DESC",
                (start_ts, end_ts),
            ).fetchall()
            outcome_row = db.execute(
                """
                SELECT COUNT(*) as completed_count,
                       AVG(CASE WHEN t.entry_px > 0 THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0 ELSE NULL END) as avg_return,
                       AVG(CASE
                            WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0
                            WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' THEN ((t.entry_px - o.realized_px) / t.entry_px) * 100.0
                            ELSE NULL END) as avg_edge,
                       SUM(CASE
                            WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' AND o.realized_px >= t.entry_px THEN 1
                            WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' AND o.realized_px <= t.entry_px THEN 1
                            ELSE 0 END) as positive_outcomes
                FROM thesis_outcome o
                JOIN thesis t ON t.thesis_id = o.thesis_id
                WHERE o.status = 'COMPLETED' AND o.updated_at >= ? AND o.updated_at < ?
                """,
                (start_ts, end_ts),
            ).fetchone()

            return {
                "label": label,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "opened_count": int(opened["cnt"] or 0),
                "avg_score": float(opened["avg_score"] or 0.0),
                "avg_confidence": float(opened["avg_confidence"] or 0.0),
                "stage_dist": {row["stage"]: row["cnt"] for row in stage_rows},
                "setup_dist": {row["setup"]: row["cnt"] for row in setup_rows},
                "closed_stage_dist": {row["stage"]: row["cnt"] for row in closed_rows},
                "outcomes_count": int(outcome_row["completed_count"] or 0),
                "avg_return": float(outcome_row["avg_return"] or 0.0),
                "avg_edge": float(outcome_row["avg_edge"] or 0.0),
                "positive_outcomes": int(outcome_row["positive_outcomes"] or 0),
            }

    async def get_setup_scorecard(self) -> list[dict[str, Any]]:
        query = """
            SELECT
                t.setup,
                COUNT(DISTINCT t.thesis_id) as total_signals,
                AVG(t.score) as avg_score,
                AVG(t.confidence) as avg_confidence,
                SUM(CASE WHEN t.stage = 'RESOLVED' THEN 1 ELSE 0 END) as resolved_count,
                SUM(CASE WHEN t.stage = 'INVALIDATED' THEN 1 ELSE 0 END) as invalidated_count,
                o.horizon,
                COUNT(o.thesis_id) as outcome_count,
                AVG(CASE WHEN t.entry_px > 0 THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0 ELSE NULL END) as avg_return,
                AVG(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' THEN ((t.entry_px - o.realized_px) / t.entry_px) * 100.0
                    ELSE NULL END) as avg_edge,
                SUM(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' AND o.realized_px >= t.entry_px THEN 1
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' AND o.realized_px <= t.entry_px THEN 1
                    ELSE 0 END) as wins
            FROM thesis t
            LEFT JOIN thesis_outcome o ON t.thesis_id = o.thesis_id AND o.status = 'COMPLETED'
            GROUP BY t.setup, o.horizon
            ORDER BY total_signals DESC, t.setup ASC
        """
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query).fetchall()
            scorecard: dict[str, dict[str, Any]] = {}
            for row in rows:
                setup = row["setup"]
                if setup not in scorecard:
                    scorecard[setup] = {
                        "setup": setup,
                        "total_signals": int(row["total_signals"] or 0),
                        "avg_score": float(row["avg_score"] or 0.0),
                        "avg_confidence": float(row["avg_confidence"] or 0.0),
                        "resolved_count": int(row["resolved_count"] or 0),
                        "invalidated_count": int(row["invalidated_count"] or 0),
                        "horizons": {},
                    }
                horizon = row["horizon"]
                if horizon:
                    count = int(row["outcome_count"] or 0)
                    wins = int(row["wins"] or 0)
                    scorecard[setup]["horizons"][horizon] = {
                        "count": count,
                        "wins": wins,
                        "win_rate": (wins / count) if count else 0.0,
                        "avg_return": float(row["avg_return"] or 0.0),
                        "avg_edge": float(row["avg_edge"] or 0.0),
                    }
            return list(scorecard.values())

    async def finalize_thesis_from_outcome(self, thesis_id: str, horizon: str, updated_at: int) -> Stage | None:
        if horizon != _FINAL_REVIEW_HORIZON:
            return None
        thesis = await self.get_thesis_by_id(thesis_id)
        if thesis is None or thesis.get("closed_ts") is not None or not thesis.get("entry_px"):
            return None
        outcomes = await self.get_thesis_outcomes(thesis_id)
        final_outcome = next((row for row in outcomes if row["horizon"] == horizon and row["status"] == "COMPLETED"), None)
        if final_outcome is None:
            return None

        entry_px = float(thesis["entry_px"])
        realized_px = float(final_outcome["realized_px"])
        direction = str(thesis["direction"])
        is_positive = (direction == "LONG_BIAS" and realized_px >= entry_px) or (
            direction == "SHORT_BIAS" and realized_px <= entry_px
        )
        next_stage: Stage = "RESOLVED" if is_positive else "INVALIDATED"
        await self.update_thesis_stage(thesis_id=thesis_id, next_stage=next_stage, closed_ts=updated_at)
        return next_stage
    async def save_tpfm_snapshot(self, snapshot: Any) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT INTO tpfm_m5_snapshot (
                    snapshot_id, symbol, window_start_ts, window_end_ts,
                    initiative_score, initiative_polarity, inventory_score, inventory_polarity,
                    energy_score, energy_state, response_efficiency_score, response_efficiency_state,
                    matrix_cell, micro_conclusion, tradability_score, delta_quote, cvd_slope,
                    trade_burst, active_thesis_count, actionable_count, health_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.symbol,
                    snapshot.window_start_ts,
                    snapshot.window_end_ts,
                    snapshot.initiative_score,
                    snapshot.initiative_polarity,
                    snapshot.inventory_score,
                    snapshot.inventory_polarity,
                    snapshot.energy_score,
                    snapshot.energy_state,
                    snapshot.response_efficiency_score,
                    snapshot.response_efficiency_state,
                    snapshot.matrix_cell,
                    snapshot.micro_conclusion,
                    snapshot.tradability_score,
                    snapshot.delta_quote,
                    snapshot.cvd_slope,
                    snapshot.trade_burst,
                    snapshot.active_thesis_count,
                    snapshot.actionable_count,
                    snapshot.health_state,
                ),
            )
            db.commit()

    async def save_tpfm_m30_regime(self, regime: Any) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT INTO tpfm_m30_regime (
                    regime_id, symbol, window_start_ts, window_end_ts,
                    m5_count, dominant_cell, dominant_regime, transition_path,
                    regime_persistence_score, net_delta_quote, avg_trade_burst,
                    macro_conclusion_code, macro_posture, health_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    regime.regime_id,
                    regime.symbol,
                    regime.window_start_ts,
                    regime.window_end_ts,
                    regime.m5_count,
                    regime.dominant_cell,
                    regime.dominant_regime,
                    json.dumps(regime.transition_path),
                    regime.regime_persistence_score,
                    regime.net_delta_quote,
                    regime.avg_trade_burst,
                    regime.macro_conclusion_code,
                    regime.macro_posture,
                    regime.health_state,
                ),
            )
            db.commit()

    async def save_tpfm_4h_report(self, report: Any) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT INTO tpfm_4h_structural (
                    structural_id, symbol, window_start_ts, window_end_ts,
                    m30_count, dominant_regime_share, dominant_cell_share,
                    structural_bias, transition_map, net_delta_quote,
                    avg_persistence, ai_analysis_vi, health_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report.structural_id,
                    report.symbol,
                    report.window_start_ts,
                    report.window_end_ts,
                    report.m30_count,
                    json.dumps(report.dominant_regime_share),
                    json.dumps(report.dominant_cell_share),
                    report.structural_bias,
                    json.dumps(report.transition_map),
                    report.net_delta_quote,
                    report.avg_persistence,
                    report.ai_analysis_vi,
                    report.health_state,
                ),
            )
            db.commit()
