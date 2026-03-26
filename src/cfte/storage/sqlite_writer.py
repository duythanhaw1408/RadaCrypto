from __future__ import annotations

import aiosqlite
import json
import sqlite3
import time
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
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
        self._lock_id = "primary_writer"
        self.schema_synced = False

    def _pid_is_alive(self, pid: int) -> bool:
        if pid <= 0: return False
        import os
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            return False
        return True

    async def acquire_writer_lock(self, run_id: str, pid: int, host: str) -> None:
        """Enforces single-writer principle at the database level."""
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            # Ensure the lock table exists before we even check it
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS system_writer_lock (
                    lock_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    pid INTEGER,
                    host TEXT,
                    acquired_at TEXT
                )
                """
            )
            row = db.execute("SELECT * FROM system_writer_lock WHERE lock_id = ?", (self._lock_id,)).fetchone()
            if row:
                existing_pid = row["pid"]
                existing_run_id = row["run_id"]
                if existing_pid == pid and existing_run_id == run_id:
                    return # Already have it
                if self._pid_is_alive(existing_pid):
                    raise RuntimeError(
                        f"Database đang được phiên khác sử dụng (pid={existing_pid}, run_id={existing_run_id}, host={row['host']})."
                    )
            
            db.execute(
                "INSERT OR REPLACE INTO system_writer_lock (lock_id, run_id, pid, host, acquired_at) VALUES (?, ?, ?, ?, ?)",
                (self._lock_id, run_id, pid, host, now)
            )
            db.commit()

    async def release_writer_lock(self, run_id: str, pid: int) -> None:
        with sqlite3.connect(self.db_path) as db:
            try:
                db.execute(
                    "DELETE FROM system_writer_lock WHERE lock_id = ? AND run_id = ? AND pid = ?",
                    (self._lock_id, run_id, pid)
                )
                db.commit()
            except sqlite3.OperationalError:
                # If the table doesn't exist, we don't have a lock to release anyway
                pass

    async def migrate_schema(self) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS thesis (
                    thesis_id TEXT PRIMARY KEY,
                    instrument_key TEXT,
                    setup TEXT,
                    direction TEXT,
                    timeframe TEXT,
                    regime_bucket TEXT,
                    stage TEXT,
                    score REAL,
                    confidence REAL,
                    coverage REAL,
                    invalidation_px REAL,
                    opened_ts INTEGER,
                    closed_ts INTEGER,
                    entry_px REAL,
                    matrix_cell_at_entry TEXT,
                    flow_state_at_entry TEXT,
                    transition_code_at_entry TEXT,
                    flow_grade_at_entry TEXT
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS thesis_event (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thesis_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    delta_score REAL,
                    reason_json TEXT,
                    event_ts INTEGER NOT NULL,
                    FOREIGN KEY (thesis_id) REFERENCES thesis(thesis_id)
                )
                """
            )
            columns = db.execute("PRAGMA table_info(thesis)").fetchall()
            column_names = [c[1] for c in columns]
            if "entry_px" not in column_names:
                print("Đang nâng cấp cơ sở dữ liệu (thêm entry_px)...")
                db.execute("ALTER TABLE thesis ADD COLUMN entry_px REAL")
            if "invalidation_px" not in column_names:
                print("Đang nâng cấp cơ sở dữ liệu (thêm invalidation_px)...")
                db.execute("ALTER TABLE thesis ADD COLUMN invalidation_px REAL")
            for col in ["matrix_cell_at_entry", "flow_state_at_entry", "transition_code_at_entry", "flow_grade_at_entry"]:
                if col not in column_names:
                    print(f"Đang nâng cấp cơ sở dữ liệu (thêm {col})...")
                    db.execute(f"ALTER TABLE thesis ADD COLUMN {col} TEXT")

            db.execute(
                """
                CREATE TABLE IF NOT EXISTS thesis_outcome (
                    thesis_id TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    target_ts INTEGER NOT NULL,
                    realized_px REAL,
                    realized_high REAL,
                    realized_low REAL,
                    fill_px REAL,
                    mae_bps REAL,
                    mfe_bps REAL,
                    exit_ts INTEGER,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (thesis_id, horizon),
                    FOREIGN KEY (thesis_id) REFERENCES thesis(thesis_id)
                )
                """
            )
            out_cols = [c[1] for c in db.execute("PRAGMA table_info(thesis_outcome)").fetchall()]
            for col in ["fill_px", "mae_bps", "mfe_bps", "exit_ts"]:
                if col not in out_cols:
                    db.execute(f"ALTER TABLE thesis_outcome ADD COLUMN {col} REAL")

            # TPFM M5 Migrations
            db.execute("CREATE TABLE IF NOT EXISTS tpfm_m5_snapshot (snapshot_id TEXT PRIMARY KEY)")
            m5_cols = [c[1] for c in db.execute("PRAGMA table_info(tpfm_m5_snapshot)").fetchall()]
            m5_definitions = {
                "symbol": "TEXT NOT NULL DEFAULT 'BTCUSDT'",
                "window_start_ts": "INTEGER NOT NULL DEFAULT 0",
                "window_end_ts": "INTEGER NOT NULL DEFAULT 0",
                "run_id": "TEXT DEFAULT ''",
                "microprice": "REAL DEFAULT 0.0",
                "open_px": "REAL DEFAULT 0.0",
                "high_px": "REAL DEFAULT 0.0",
                "low_px": "REAL DEFAULT 0.0",
                "close_px": "REAL DEFAULT 0.0",
                "initiative_score": "REAL", "initiative_polarity": "TEXT", "initiative_strength": "REAL",
                "inventory_score": "REAL", "inventory_polarity": "TEXT", "inventory_strength": "REAL",
                "axis_confidence": "REAL",
                "energy_score": "REAL", "energy_state": "TEXT",
                "response_efficiency_score": "REAL", "response_efficiency_state": "TEXT",
                "matrix_cell": "TEXT", "micro_conclusion": "TEXT", "matrix_alias_vi": "TEXT",
                "continuation_bias": "TEXT", "preferred_posture": "TEXT", "tradability_grade": "TEXT",
                "tradability_score": "REAL", "delta_quote": "REAL",
                "cvd_slope": "REAL", "trade_burst": "REAL",
                "absorption_score": "REAL", "imbalance_l1": "REAL",
                "centered_imbalance_l1": "REAL", "signed_absorption_score": "REAL",
                "microprice_gap_bps": "REAL", "spread_bps": "REAL",
                "active_thesis_count": "INTEGER", "new_thesis_count": "INTEGER",
                "actionable_count": "INTEGER", "invalidated_count": "INTEGER", "resolved_count": "INTEGER",
                "dominant_setups": "TEXT", "setup_score_map": "TEXT",
                "futures_delta_available": "INTEGER", "futures_delta": "REAL",
                "oi_delta": "REAL", "oi_state": "TEXT", "basis_bps": "REAL", "funding_rate": "REAL",
                "spot_futures_relation": "TEXT", "context_quality_score": "REAL",
                "venue_confirmation_state": "TEXT", "leader_venue": "TEXT", "lagger_venue": "TEXT",
                "venue_vwap_spread_bps": "REAL", "liquidation_context_available": "INTEGER",
                "liquidation_bias": "TEXT", "liquidation_count": "INTEGER", "liquidation_quote": "REAL",
                "flow_state_code": "TEXT DEFAULT 'NEUTRAL'",
                "forced_flow_state": "TEXT DEFAULT 'NONE'", "forced_flow_intensity": "REAL DEFAULT 0.0",
                "inventory_defense_state": "TEXT DEFAULT 'NONE'", "liquidation_intensity": "REAL DEFAULT 0.0",
                "transition_ready": "INTEGER DEFAULT 0", "trap_risk": "REAL DEFAULT 0.0",
                "decision_posture": "TEXT DEFAULT 'WAIT'",
                "decision_summary_vi": "TEXT",
                "entry_condition_vi": "TEXT",
                "confirm_needed_vi": "TEXT",
                "avoid_if_vi": "TEXT",
                "review_tags_json": "TEXT",
                "blind_spot_flags": "TEXT", "observed_facts": "TEXT", "inferred_facts": "TEXT",
                "missing_context": "TEXT", "risk_flags": "TEXT", "action_plan_vi": "TEXT",
                "invalid_if": "TEXT", "health_state": "TEXT",
                "delta_zscore": "REAL DEFAULT 0.0",
                "aggression_ratio": "REAL DEFAULT 0.5",
                "sweep_quote": "REAL DEFAULT 0.0",
                "sweep_buy_quote": "REAL DEFAULT 0.0",
                "sweep_sell_quote": "REAL DEFAULT 0.0",
                "burst_persistence": "REAL DEFAULT 0.0",
                "microprice_drift_bps": "REAL DEFAULT 0.0",
                "replenishment_bid_score": "REAL DEFAULT 0.0",
                "replenishment_ask_score": "REAL DEFAULT 0.0",
                "oi_expansion_ratio": "REAL DEFAULT 0.0",
                "futures_aggression_ratio": "REAL DEFAULT 0.5",
                "basis_state": "TEXT DEFAULT 'BALANCED'",
                "leader_confidence": "REAL DEFAULT 0.0",
                "aligned_window_ms": "INTEGER DEFAULT 0",
                "initiative_delta_1": "REAL DEFAULT 0.0",
                "initiative_delta_3": "REAL DEFAULT 0.0",
                "initiative_delta_5": "REAL DEFAULT 0.0",
                "inventory_delta_1": "REAL DEFAULT 0.0",
                "inventory_delta_3": "REAL DEFAULT 0.0",
                "inventory_delta_5": "REAL DEFAULT 0.0",
                "agreement_delta_3": "REAL DEFAULT 0.0",
                "tradability_delta_3": "REAL DEFAULT 0.0",
                "forced_flow_delta_3": "REAL DEFAULT 0.0",
                "tempo_state": "TEXT DEFAULT 'UNKNOWN'",
                "persistence_state": "TEXT DEFAULT 'UNKNOWN'",
                "exhaustion_risk": "REAL DEFAULT 0.0",
                "history_depth": "INTEGER DEFAULT 0",
                "sequence_id": "TEXT DEFAULT ''",
                "sequence_signature": "TEXT DEFAULT 'UNKNOWN'",
                "sequence_length": "INTEGER DEFAULT 0",
                "sequence_family": "TEXT DEFAULT 'UNKNOWN'",
                "sequence_quality": "REAL DEFAULT 0.0",
                "edge_score": "REAL DEFAULT 0.0",
                "edge_confidence": "TEXT DEFAULT 'LOW'",
                "historical_win_rate": "REAL DEFAULT 0.0",
                "expected_rr": "REAL DEFAULT 0.0",
                "pattern_code": "TEXT DEFAULT 'UNCLASSIFIED'",
                "pattern_alias_vi": "TEXT DEFAULT 'Chưa phân loại pattern'",
                "pattern_family": "TEXT DEFAULT 'NONE'",
                "pattern_phase": "TEXT DEFAULT 'FORMING'",
                "pattern_strength": "REAL DEFAULT 0.0",
                "pattern_quality": "REAL DEFAULT 0.0",
                "pattern_failure_risk": "REAL DEFAULT 0.0",
                "sequence_start_ts": "REAL DEFAULT 0.0",
                "sequence_duration_sec": "REAL DEFAULT 0.0",
                "is_sequence_pivot": "INTEGER DEFAULT 0",
                "parent_context_json": "TEXT DEFAULT '{}'",
                "t_plus_1_price": "REAL DEFAULT 0.0",
                "t_plus_5_price": "REAL DEFAULT 0.0",
                "t_plus_12_price": "REAL DEFAULT 0.0",
            }
            for col, dft in m5_definitions.items():
                if col not in m5_cols:
                    db.execute(f"ALTER TABLE tpfm_m5_snapshot ADD COLUMN {col} {dft}")

            # Mark as synced
            self.schema_synced = True

            # TPFM M30 Migrations
            db.execute("CREATE TABLE IF NOT EXISTS tpfm_m30_regime (regime_id TEXT PRIMARY KEY)")
            m30_cols = [c[1] for c in db.execute("PRAGMA table_info(tpfm_m30_regime)").fetchall()]
            m30_definitions = {
                "symbol": "TEXT NOT NULL DEFAULT 'BTCUSDT'",
                "window_start_ts": "INTEGER NOT NULL DEFAULT 0",
                "window_end_ts": "INTEGER NOT NULL DEFAULT 0",
                "m5_count": "INTEGER", "dominant_cell": "TEXT",
                "dominant_regime": "TEXT", "transition_path": "TEXT",
                "regime_persistence_score": "REAL", "net_delta_quote": "REAL",
                "avg_trade_burst": "REAL", "macro_conclusion_code": "TEXT",
                "macro_posture": "TEXT", "health_state": "TEXT"
            }
            for col, dft in m30_definitions.items():
                if col not in m30_cols:
                    db.execute(f"ALTER TABLE tpfm_m30_regime ADD COLUMN {col} {dft}")

            # TPFM 4H Migrations
            db.execute("CREATE TABLE IF NOT EXISTS tpfm_4h_structural (structural_id TEXT PRIMARY KEY)")
            h4_cols = [c[1] for c in db.execute("PRAGMA table_info(tpfm_4h_structural)").fetchall()]
            h4_definitions = {
                "symbol": "TEXT NOT NULL DEFAULT 'BTCUSDT'",
                "window_start_ts": "INTEGER NOT NULL DEFAULT 0",
                "window_end_ts": "INTEGER NOT NULL DEFAULT 0",
                "m30_count": "INTEGER", "dominant_regime_share": "TEXT",
                "dominant_cell_share": "TEXT", "structural_bias": "TEXT",
                "transition_map": "TEXT", "net_delta_quote": "REAL",
                "avg_persistence": "REAL", "structural_score": "REAL",
                "ai_analysis_vi": "TEXT", "health_state": "TEXT"
            }
            for col, dft in h4_definitions.items():
                if col not in h4_cols:
                    db.execute(f"ALTER TABLE tpfm_4h_structural ADD COLUMN {col} {dft}")

            # Flow Transition Events Table
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS tpfm_transition_event (
                    transition_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    from_cell TEXT,
                    to_cell TEXT,
                    transition_code TEXT,
                    transition_family TEXT,
                    transition_alias_vi TEXT,
                    from_flow_state_code TEXT,
                    to_flow_state_code TEXT,
                    transition_speed REAL,
                    transition_quality REAL,
                    persistence_score REAL,
                    forced_flow_involved INTEGER,
                    trap_risk REAL,
                    from_decision_posture TEXT,
                    to_decision_posture TEXT,
                    decision_shift TEXT,
                    metadata TEXT
                )
                """
            )
            transition_cols = [c[1] for c in db.execute("PRAGMA table_info(tpfm_transition_event)").fetchall()]
            transition_definitions = {
                "transition_family": "TEXT DEFAULT 'STRUCTURE_SHIFT'",
                "transition_alias_vi": "TEXT DEFAULT 'Chuyển pha cấu trúc'",
                "from_flow_state_code": "TEXT DEFAULT 'NEUTRAL_BALANCE'",
                "to_flow_state_code": "TEXT DEFAULT 'NEUTRAL_BALANCE'",
                "from_decision_posture": "TEXT DEFAULT 'WAIT'",
                "to_decision_posture": "TEXT DEFAULT 'WAIT'",
            }
            for col, dft in transition_definitions.items():
                if col not in transition_cols:
                    db.execute(f"ALTER TABLE tpfm_transition_event ADD COLUMN {col} {dft}")
            
            # Sequence Events Table
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS tpfm_sequence_event (
                    sequence_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL,
                    sequence_signature TEXT,
                    sequence_family TEXT,
                    sequence_length INTEGER,
                    cumulative_initiative REAL,
                    cumulative_inventory REAL,
                    max_energy REAL,
                    sequence_quality REAL,
                    is_active INTEGER
                )
                """
            )

            # Phase 1: Matrix-Native Pattern Event
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS flow_pattern_event (
                    pattern_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    pattern_code TEXT NOT NULL,
                    pattern_alias_vi TEXT NOT NULL,
                    pattern_family TEXT NOT NULL,
                    pattern_phase TEXT NOT NULL,
                    sequence_id TEXT NOT NULL,
                    sequence_signature TEXT NOT NULL,
                    sequence_length INTEGER NOT NULL,
                    tempo_state TEXT NOT NULL,
                    persistence_state TEXT NOT NULL,
                    pattern_strength REAL NOT NULL,
                    pattern_quality REAL NOT NULL,
                    pattern_failure_risk REAL NOT NULL,
                    matrix_cell TEXT NOT NULL,
                    flow_state_code TEXT NOT NULL,
                    metadata_json TEXT
                )
                """
            )
            # Migration: Ensure metadata_json exists (Patch 4)
            pattern_cols = [c[1] for c in db.execute("PRAGMA table_info(flow_pattern_event)").fetchall()]
            if "metadata_json" not in pattern_cols:
                try:
                    db.execute("ALTER TABLE flow_pattern_event ADD COLUMN metadata_json TEXT")
                except Exception:
                    # In case it existed as 'metadata'
                    if "metadata" in pattern_cols:
                        try:
                            db.execute("ALTER TABLE flow_pattern_event RENAME COLUMN metadata TO metadata_json")
                        except Exception:
                            pass
            
            try:
                db.execute("CREATE INDEX IF NOT EXISTS idx_flow_pattern_symbol_ts ON flow_pattern_event (symbol, timestamp)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_flow_pattern_signature_ts ON flow_pattern_event (sequence_signature, timestamp)")
            except:
                pass
            
            # Phase 2: Pattern Performance Outcome
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS flow_pattern_outcome (
                    outcome_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    pattern_code TEXT,
                    sequence_signature TEXT,
                    start_px REAL,
                    t1_px REAL, t5_px REAL, t12_px REAL,
                    r1_bps REAL, r5_bps REAL, r12_bps REAL,
                    max_favorable_bps REAL,
                    max_adverse_bps REAL,
                    metadata_json TEXT
                )
                """
            )
            try:
                db.execute("CREATE INDEX IF NOT EXISTS idx_outcome_pattern ON flow_pattern_outcome (pattern_code)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_outcome_sym ON flow_pattern_outcome (symbol)")
            except:
                pass

            
            # System Writer Lock Table
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS system_writer_lock (
                    lock_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    pid INTEGER,
                    host TEXT,
                    acquired_at TEXT
                )
                """
            )

            # ═══ Phase 20-A: Market Timeline Architecture ═══
            # Table 1: Normalized frame state (M5/M30/H1/H4/H12/D1)
            db.execute("""
                CREATE TABLE IF NOT EXISTS flow_frame_history (
                    frame_state_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'scan',
                    symbol TEXT NOT NULL DEFAULT 'BTCUSDT',
                    venue TEXT NOT NULL DEFAULT 'binance',
                    frame TEXT NOT NULL DEFAULT 'M5',
                    market_ts INTEGER NOT NULL DEFAULT 0,
                    window_start_ts INTEGER NOT NULL DEFAULT 0,
                    window_end_ts INTEGER NOT NULL DEFAULT 0,
                    emitted_at_ts INTEGER NOT NULL DEFAULT 0,
                    ingested_at_ts INTEGER NOT NULL DEFAULT 0,
                    record_seq INTEGER NOT NULL DEFAULT 0,
                    is_final INTEGER NOT NULL DEFAULT 1,
                    source_kind TEXT NOT NULL DEFAULT 'tpfm',
                    source_ref_id TEXT NOT NULL DEFAULT '',
                    snapshot_id TEXT DEFAULT '',
                    pattern_id TEXT DEFAULT '',
                    stack_id TEXT DEFAULT '',
                    open_px REAL DEFAULT 0.0,
                    high_px REAL DEFAULT 0.0,
                    low_px REAL DEFAULT 0.0,
                    close_px REAL DEFAULT 0.0,
                    volume_quote REAL DEFAULT 0.0,
                    matrix_cell TEXT NOT NULL DEFAULT 'NEUTRAL_INIT__NEUTRAL_INV',
                    matrix_alias_vi TEXT NOT NULL DEFAULT 'Trung tính',
                    flow_state_code TEXT NOT NULL DEFAULT 'NEUTRAL',
                    pattern_code TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
                    pattern_phase TEXT NOT NULL DEFAULT 'FORMING',
                    sequence_id TEXT DEFAULT '',
                    sequence_signature TEXT DEFAULT 'UNKNOWN',
                    sequence_length INTEGER NOT NULL DEFAULT 0,
                    flow_bias TEXT NOT NULL DEFAULT 'NEUTRAL',
                    tempo_state TEXT NOT NULL DEFAULT 'UNKNOWN',
                    persistence_state TEXT NOT NULL DEFAULT 'UNKNOWN',
                    decision_posture TEXT NOT NULL DEFAULT 'WAIT',
                    tradability_grade TEXT NOT NULL DEFAULT 'D',
                    agreement_score REAL DEFAULT 0.0,
                    tradability_score REAL DEFAULT 0.0,
                    context_quality_score REAL DEFAULT 0.0,
                    market_quality_score REAL DEFAULT 0.0,
                    stack_signature TEXT DEFAULT '',
                    stack_alignment TEXT DEFAULT 'UNKNOWN',
                    stack_quality REAL DEFAULT 0.0,
                    parent_m30_end_ts INTEGER,
                    parent_h1_end_ts INTEGER,
                    parent_h4_end_ts INTEGER,
                    parent_h12_end_ts INTEGER,
                    parent_d1_end_ts INTEGER,
                    health_state TEXT NOT NULL DEFAULT 'HEALTHY',
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                )
            """)
            try:
                db.execute("CREATE INDEX IF NOT EXISTS idx_ffh_sym_frame_end ON flow_frame_history(symbol, frame, window_end_ts DESC)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_ffh_run_frame_end ON flow_frame_history(run_id, frame, window_end_ts DESC)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_ffh_market_ts ON flow_frame_history(market_ts DESC)")
                db.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_ffh_run_frame_end_seq ON flow_frame_history(run_id, frame, window_end_ts, record_seq)")
            except Exception:
                pass

            # Table 2: Unified timeline events (market_ts-sorted, UI-facing)
            db.execute("""
                CREATE TABLE IF NOT EXISTS flow_timeline_event (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'scan',
                    symbol TEXT NOT NULL DEFAULT 'BTCUSDT',
                    venue TEXT NOT NULL DEFAULT 'binance',
                    frame TEXT NOT NULL DEFAULT 'M5',
                    market_ts INTEGER NOT NULL DEFAULT 0,
                    window_start_ts INTEGER,
                    window_end_ts INTEGER,
                    anchor_frame TEXT NOT NULL DEFAULT 'M5',
                    anchor_window_end_ts INTEGER,
                    emitted_at_ts INTEGER NOT NULL DEFAULT 0,
                    ingested_at_ts INTEGER NOT NULL DEFAULT 0,
                    record_seq INTEGER NOT NULL DEFAULT 0,
                    is_final INTEGER NOT NULL DEFAULT 1,
                    event_type TEXT NOT NULL DEFAULT 'STATE',
                    signal_kind TEXT NOT NULL DEFAULT 'PATTERN',
                    severity TEXT NOT NULL DEFAULT 'INFO',
                    priority INTEGER NOT NULL DEFAULT 0,
                    snapshot_id TEXT DEFAULT '',
                    transition_id TEXT DEFAULT '',
                    pattern_id TEXT DEFAULT '',
                    stack_id TEXT DEFAULT '',
                    thesis_id TEXT DEFAULT '',
                    matrix_cell TEXT DEFAULT '',
                    matrix_alias_vi TEXT DEFAULT '',
                    flow_state_code TEXT DEFAULT '',
                    pattern_code TEXT DEFAULT '',
                    pattern_phase TEXT DEFAULT '',
                    sequence_signature TEXT DEFAULT '',
                    decision_posture TEXT DEFAULT 'WAIT',
                    tradability_grade TEXT DEFAULT 'D',
                    action_label_vi TEXT DEFAULT '',
                    why_now_vi TEXT DEFAULT '',
                    invalid_if_vi TEXT DEFAULT '',
                    summary_vi TEXT DEFAULT '',
                    parent_m30_end_ts INTEGER,
                    parent_h1_end_ts INTEGER,
                    parent_h4_end_ts INTEGER,
                    parent_h12_end_ts INTEGER,
                    parent_d1_end_ts INTEGER,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                )
            """)
            try:
                db.execute("CREATE INDEX IF NOT EXISTS idx_fte_run_market_seq ON flow_timeline_event(run_id, market_ts DESC, record_seq DESC)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_fte_sym_frame ON flow_timeline_event(symbol, frame, market_ts DESC)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_fte_thesis ON flow_timeline_event(thesis_id, market_ts DESC)")
            except Exception:
                pass

            # Table 3: MTF stack snapshot per M5 window
            db.execute("""
                CREATE TABLE IF NOT EXISTS flow_stack_history (
                    stack_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'scan',
                    symbol TEXT NOT NULL DEFAULT 'BTCUSDT',
                    venue TEXT NOT NULL DEFAULT 'binance',
                    market_ts INTEGER NOT NULL DEFAULT 0,
                    anchor_frame TEXT NOT NULL DEFAULT 'M5',
                    anchor_window_start_ts INTEGER NOT NULL DEFAULT 0,
                    anchor_window_end_ts INTEGER NOT NULL DEFAULT 0,
                    emitted_at_ts INTEGER NOT NULL DEFAULT 0,
                    ingested_at_ts INTEGER NOT NULL DEFAULT 0,
                    record_seq INTEGER NOT NULL DEFAULT 0,
                    is_final INTEGER NOT NULL DEFAULT 1,
                    m5_state_id TEXT,
                    m30_state_id TEXT,
                    h1_state_id TEXT,
                    h4_state_id TEXT,
                    h12_state_id TEXT,
                    d1_state_id TEXT,
                    m5_end_ts INTEGER,
                    m30_end_ts INTEGER,
                    h1_end_ts INTEGER,
                    h4_end_ts INTEGER,
                    h12_end_ts INTEGER,
                    d1_end_ts INTEGER,
                    stack_signature TEXT NOT NULL DEFAULT '',
                    stack_alignment TEXT NOT NULL DEFAULT 'UNKNOWN',
                    stack_conflict TEXT NOT NULL DEFAULT 'UNKNOWN',
                    micro_vs_macro TEXT NOT NULL DEFAULT 'UNKNOWN',
                    stack_pressure REAL NOT NULL DEFAULT 0.0,
                    stack_quality REAL NOT NULL DEFAULT 0.0,
                    macro_bias TEXT NOT NULL DEFAULT 'NEUTRAL',
                    trigger_bias TEXT NOT NULL DEFAULT 'NEUTRAL',
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                )
            """)
            try:
                db.execute("CREATE INDEX IF NOT EXISTS idx_fsh_run_anchor ON flow_stack_history(run_id, anchor_window_end_ts DESC)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_fsh_symbol ON flow_stack_history(symbol, market_ts DESC)")
                db.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_fsh_run_anchor_seq ON flow_stack_history(run_id, anchor_frame, anchor_window_end_ts, record_seq)")
            except Exception:
                pass

            db.commit()

    # ─── Phase 20-A: Market Timeline Architecture ─────────────────────────────

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def save_flow_frame_state(self, row: dict) -> None:
        """Save a normalized frame state record (M5/M30/H4 etc.) into flow_frame_history."""
        now = self._now_ms()
        row.setdefault("ingested_at_ts", now)
        row.setdefault("emitted_at_ts", now)
        row.setdefault("metadata_json", "{}")
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT OR REPLACE INTO flow_frame_history (
                    frame_state_id, run_id, mode, symbol, venue, frame,
                    market_ts, window_start_ts, window_end_ts, emitted_at_ts, ingested_at_ts,
                    record_seq, is_final, source_kind, source_ref_id,
                    snapshot_id, pattern_id, stack_id,
                    open_px, high_px, low_px, close_px, volume_quote,
                    matrix_cell, matrix_alias_vi, flow_state_code, pattern_code, pattern_phase,
                    sequence_id, sequence_signature, sequence_length,
                    flow_bias, tempo_state, persistence_state,
                    decision_posture, tradability_grade,
                    agreement_score, tradability_score, context_quality_score, market_quality_score,
                    stack_signature, stack_alignment, stack_quality,
                    parent_m30_end_ts, parent_h1_end_ts, parent_h4_end_ts, parent_h12_end_ts, parent_d1_end_ts,
                    health_state, metadata_json
                ) VALUES (
                    :frame_state_id, :run_id, :mode, :symbol, :venue, :frame,
                    :market_ts, :window_start_ts, :window_end_ts, :emitted_at_ts, :ingested_at_ts,
                    :record_seq, :is_final, :source_kind, :source_ref_id,
                    :snapshot_id, :pattern_id, :stack_id,
                    :open_px, :high_px, :low_px, :close_px, :volume_quote,
                    :matrix_cell, :matrix_alias_vi, :flow_state_code, :pattern_code, :pattern_phase,
                    :sequence_id, :sequence_signature, :sequence_length,
                    :flow_bias, :tempo_state, :persistence_state,
                    :decision_posture, :tradability_grade,
                    :agreement_score, :tradability_score, :context_quality_score, :market_quality_score,
                    :stack_signature, :stack_alignment, :stack_quality,
                    :parent_m30_end_ts, :parent_h1_end_ts, :parent_h4_end_ts, :parent_h12_end_ts, :parent_d1_end_ts,
                    :health_state, :metadata_json
                )
                """,
                row,
            )
            db.commit()

    def save_flow_timeline_event(self, row: dict) -> None:
        """Save a timeline event into flow_timeline_event (STATE, DECISION, THESIS etc.)."""
        now = self._now_ms()
        row.setdefault("ingested_at_ts", now)
        row.setdefault("emitted_at_ts", now)
        row.setdefault("metadata_json", "{}")
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT OR REPLACE INTO flow_timeline_event (
                    event_id, run_id, mode, symbol, venue, frame,
                    market_ts, window_start_ts, window_end_ts,
                    anchor_frame, anchor_window_end_ts,
                    emitted_at_ts, ingested_at_ts, record_seq, is_final,
                    event_type, signal_kind, severity, priority,
                    snapshot_id, transition_id, pattern_id, stack_id, thesis_id,
                    matrix_cell, matrix_alias_vi, flow_state_code, pattern_code, pattern_phase,
                    sequence_signature, decision_posture, tradability_grade,
                    action_label_vi, why_now_vi, invalid_if_vi, summary_vi,
                    parent_m30_end_ts, parent_h1_end_ts, parent_h4_end_ts, parent_h12_end_ts, parent_d1_end_ts,
                    metadata_json
                ) VALUES (
                    :event_id, :run_id, :mode, :symbol, :venue, :frame,
                    :market_ts, :window_start_ts, :window_end_ts,
                    :anchor_frame, :anchor_window_end_ts,
                    :emitted_at_ts, :ingested_at_ts, :record_seq, :is_final,
                    :event_type, :signal_kind, :severity, :priority,
                    :snapshot_id, :transition_id, :pattern_id, :stack_id, :thesis_id,
                    :matrix_cell, :matrix_alias_vi, :flow_state_code, :pattern_code, :pattern_phase,
                    :sequence_signature, :decision_posture, :tradability_grade,
                    :action_label_vi, :why_now_vi, :invalid_if_vi, :summary_vi,
                    :parent_m30_end_ts, :parent_h1_end_ts, :parent_h4_end_ts, :parent_h12_end_ts, :parent_d1_end_ts,
                    :metadata_json
                )
                """,
                row,
            )
            db.commit()

    def save_flow_stack_history(self, row: dict) -> None:
        """Save a MTF stack snapshot anchored at one M5 window."""
        now = self._now_ms()
        row.setdefault("ingested_at_ts", now)
        row.setdefault("emitted_at_ts", now)
        row.setdefault("metadata_json", "{}")
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT OR REPLACE INTO flow_stack_history (
                    stack_id, run_id, mode, symbol, venue,
                    market_ts, anchor_frame, anchor_window_start_ts, anchor_window_end_ts,
                    emitted_at_ts, ingested_at_ts, record_seq, is_final,
                    m5_state_id, m30_state_id, h1_state_id, h4_state_id, h12_state_id, d1_state_id,
                    m5_end_ts, m30_end_ts, h1_end_ts, h4_end_ts, h12_end_ts, d1_end_ts,
                    stack_signature, stack_alignment, stack_conflict, micro_vs_macro,
                    stack_pressure, stack_quality, macro_bias, trigger_bias,
                    metadata_json
                ) VALUES (
                    :stack_id, :run_id, :mode, :symbol, :venue,
                    :market_ts, :anchor_frame, :anchor_window_start_ts, :anchor_window_end_ts,
                    :emitted_at_ts, :ingested_at_ts, :record_seq, :is_final,
                    :m5_state_id, :m30_state_id, :h1_state_id, :h4_state_id, :h12_state_id, :d1_state_id,
                    :m5_end_ts, :m30_end_ts, :h1_end_ts, :h4_end_ts, :h12_end_ts, :d1_end_ts,
                    :stack_signature, :stack_alignment, :stack_conflict, :micro_vs_macro,
                    :stack_pressure, :stack_quality, :macro_bias, :trigger_bias,
                    :metadata_json
                )
                """,
                row,
            )
            db.commit()

    def load_flow_frames(
        self, run_id: str | None, mode: str, symbol: str, limit: int = 100, frame: str | None = None
    ) -> list[dict]:
        """Load flow_frame_history rows sorted by market_ts DESC."""
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            query_parts = ["SELECT * FROM flow_frame_history WHERE mode=? AND symbol=? AND is_final=1"]
            params = [mode, symbol]
            
            if run_id:
                query_parts.append("AND run_id=?")
                params.append(run_id)
            if frame:
                query_parts.append("AND frame=?")
                params.append(frame)
                
            query = " ".join(query_parts) + " ORDER BY market_ts DESC LIMIT ?"
            params.append(limit)
            
            rows = db.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]

    def load_flow_timeline(
        self, run_id: str | None, mode: str, symbol: str, limit: int = 50, min_priority: int = 0
    ) -> list[dict]:
        """Load flow_timeline_event rows sorted by market_ts ASC, record_seq ASC."""
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            query_parts = ["SELECT * FROM flow_timeline_event WHERE mode=? AND symbol=? AND priority>=?"]
            params = [mode, symbol, min_priority]
            
            if run_id:
                query_parts.append("AND run_id=?")
                params.append(run_id)
                
            query = " ".join(query_parts) + " ORDER BY market_ts ASC, record_seq ASC LIMIT ?"
            params.append(limit)
            
            rows = db.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]

    def load_flow_stack(
        self, run_id: str | None, mode: str, symbol: str, limit: int = 20
    ) -> list[dict]:
        """Load flow_stack_history rows sorted by anchor_window_end_ts DESC."""
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            query_parts = ["SELECT * FROM flow_stack_history WHERE mode=? AND symbol=? AND is_final=1"]
            params = [mode, symbol]
            
            if run_id:
                query_parts.append("AND run_id=?")
                params.append(run_id)
                
            query = " ".join(query_parts) + " ORDER BY anchor_window_end_ts DESC LIMIT ?"
            params.append(limit)
            
            rows = db.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]

    # ──────────────────────────────────────────────────────────────────────────

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
                    invalidation_px, opened_ts, closed_ts, entry_px,
                    matrix_cell_at_entry, flow_state_at_entry, transition_code_at_entry, flow_grade_at_entry
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    signal.metadata.get("matrix_cell_at_entry") if signal.metadata else None,
                    signal.metadata.get("flow_state_at_entry") if signal.metadata else None,
                    signal.metadata.get("transition_code_at_entry") if signal.metadata else None,
                    signal.metadata.get("flow_grade_at_entry") if signal.metadata else None,
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
        fill_px: float | None = None,
        mae_bps: float | None = None,
        mfe_bps: float | None = None,
        exit_ts: int | None = None,
    ) -> None:
        with sqlite3.connect(self.db_path) as db:
            now = int(time.time() * 1000)
            db.execute(
                """
                UPDATE thesis_outcome
                SET realized_px = ?, realized_high = ?, realized_low = ?, 
                    fill_px = ?, mae_bps = ?, mfe_bps = ?, exit_ts = ?,
                    status = ?, updated_at = ?
                WHERE thesis_id = ? AND horizon = ?
                """,
                (
                    realized_px, realized_high, realized_low, 
                    fill_px, mae_bps, mfe_bps, exit_ts,
                    "COMPLETED", now, thesis_id, horizon
                ),
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

    async def get_daily_summary_stats(self, date_str: str | None = None, timezone_str: str = "UTC") -> dict[str, Any]:
        tz = ZoneInfo(timezone_str)
        if not date_str:
            date_str = datetime.now(tz).strftime("%Y-%m-%d")
        
        # Parse date_str in the target timezone to get the correct day boundaries
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
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
                       COUNT(o.fill_px) as fill_count,
                       AVG(o.mae_bps) as avg_mae,
                       AVG(o.mfe_bps) as avg_mfe,
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
                "fill_count": int(outcome_row["fill_count"] or 0),
                "avg_mae": float(outcome_row["avg_mae"] or 0.0),
                "avg_mfe": float(outcome_row["avg_mfe"] or 0.0),
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
                    ELSE 0 END) as wins,
                AVG(o.mae_bps) as avg_mae,
                AVG(o.mfe_bps) as avg_mfe
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
                        "avg_mae": float(row["avg_mae"] or 0.0),
                        "avg_mfe": float(row["avg_mfe"] or 0.0),
                    }
            return list(scorecard.values())

    async def get_matrix_scorecard(self, *, start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]:
        query = """
            WITH thesis_matrix_ranked AS (
                SELECT
                    t.thesis_id,
                    t.setup,
                    t.direction,
                    t.score,
                    t.confidence,
                    COALESCE(s.matrix_cell, t.matrix_cell_at_entry, 'UNKNOWN') as matrix_cell,
                    COALESCE(s.matrix_alias_vi, 'Chưa có matrix') as matrix_alias_vi,
                    COALESCE(s.spot_futures_relation, 'NO_TPFM_CONTEXT') as spot_futures_relation,
                    COALESCE(s.venue_confirmation_state, 'UNCONFIRMED') as venue_confirmation_state,
                    COALESCE(s.liquidation_bias, 'UNKNOWN') as liquidation_bias,
                    COALESCE(s.tradability_grade, t.flow_grade_at_entry, 'D') as tradability_grade,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.thesis_id
                        ORDER BY
                            CASE
                                WHEN s.window_end_ts IS NULL THEN 2
                                WHEN s.window_end_ts >= t.opened_ts THEN 0
                                ELSE 1
                            END,
                            ABS(COALESCE(s.window_end_ts, t.opened_ts) - t.opened_ts)
                    ) as rn
                FROM thesis t
                LEFT JOIN tpfm_m5_snapshot s
                    ON s.window_start_ts <= t.opened_ts
                   AND ABS(s.window_end_ts - t.opened_ts) <= 300000
                WHERE (? IS NULL OR t.opened_ts >= ?)
                  AND (? IS NULL OR t.opened_ts < ?)
            ),
            thesis_matrix AS (
                SELECT
                    thesis_id,
                    setup,
                    direction,
                    score,
                    confidence,
                    matrix_cell,
                    matrix_alias_vi,
                    spot_futures_relation,
                    venue_confirmation_state,
                    liquidation_bias,
                    tradability_grade
                FROM thesis_matrix_ranked
                WHERE rn = 1
            )
            SELECT
                tm.matrix_cell,
                tm.matrix_alias_vi,
                tm.spot_futures_relation,
                tm.venue_confirmation_state,
                tm.liquidation_bias,
                tm.tradability_grade,
                COUNT(DISTINCT tm.thesis_id) as total_signals,
                AVG(tm.score) as avg_score,
                AVG(tm.confidence) as avg_confidence,
                o.horizon,
                COUNT(o.thesis_id) as outcome_count,
                AVG(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' THEN ((t.entry_px - o.realized_px) / t.entry_px) * 100.0
                    ELSE NULL END) as avg_edge,
                SUM(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' AND o.realized_px >= t.entry_px THEN 1
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' AND o.realized_px <= t.entry_px THEN 1
                    ELSE 0 END) as wins,
                AVG(o.mae_bps) as avg_mae,
                AVG(o.mfe_bps) as avg_mfe
            FROM thesis_matrix tm
            JOIN thesis t ON t.thesis_id = tm.thesis_id
            LEFT JOIN thesis_outcome o ON tm.thesis_id = o.thesis_id AND o.status = 'COMPLETED'
            GROUP BY
                tm.matrix_cell,
                tm.matrix_alias_vi,
                tm.spot_futures_relation,
                tm.venue_confirmation_state,
                tm.liquidation_bias,
                tm.tradability_grade,
                o.horizon
            ORDER BY total_signals DESC, tm.matrix_cell ASC
        """
        params = (start_ts, start_ts, end_ts, end_ts)
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, params).fetchall()
            scorecard: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
            for row in rows:
                bucket_key = (
                    row["matrix_cell"],
                    row["spot_futures_relation"],
                    row["venue_confirmation_state"],
                    row["liquidation_bias"],
                    row["tradability_grade"],
                )
                if bucket_key not in scorecard:
                    scorecard[bucket_key] = {
                        "matrix_cell": row["matrix_cell"],
                        "matrix_alias_vi": row["matrix_alias_vi"],
                        "spot_futures_relation": row["spot_futures_relation"],
                        "venue_confirmation_state": row["venue_confirmation_state"],
                        "liquidation_bias": row["liquidation_bias"],
                        "tradability_grade": row["tradability_grade"],
                        "total_signals": int(row["total_signals"] or 0),
                        "avg_score": float(row["avg_score"] or 0.0),
                        "avg_confidence": float(row["avg_confidence"] or 0.0),
                        "horizons": {},
                    }
                horizon = row["horizon"]
                if horizon:
                    count = int(row["outcome_count"] or 0)
                    wins = int(row["wins"] or 0)
                    scorecard[bucket_key]["horizons"][horizon] = {
                        "count": count,
                        "wins": wins,
                        "win_rate": (wins / count) if count else 0.0,
                        "avg_edge": float(row["avg_edge"] or 0.0),
                        "avg_mae": float(row["avg_mae"] or 0.0),
                        "avg_mfe": float(row["avg_mfe"] or 0.0),
                    }
            return list(scorecard.values())

    async def get_flow_state_scorecard(self, *, start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]:
        query = """
            WITH thesis_flow_ranked AS (
                SELECT
                    t.thesis_id,
                    t.direction,
                    t.score,
                    t.confidence,
                    COALESCE(s.flow_state_code, t.flow_state_at_entry, 'NO_FLOW_CONTEXT') as flow_state_code,
                    COALESCE(s.forced_flow_state, 'NONE') as forced_flow_state,
                    COALESCE(s.inventory_defense_state, 'NONE') as inventory_defense_state,
                    COALESCE(s.decision_posture, 'WAIT') as decision_posture,
                    COALESCE(s.tradability_grade, t.flow_grade_at_entry, 'D') as tradability_grade,
                    COALESCE(s.trap_risk, 0.0) as trap_risk,
                    COALESCE(s.forced_flow_intensity, 0.0) as forced_flow_intensity,
                    COALESCE(s.context_quality_score, 0.0) as context_quality_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.thesis_id
                        ORDER BY
                            CASE
                                WHEN s.window_end_ts IS NULL THEN 2
                                WHEN s.window_end_ts >= t.opened_ts THEN 0
                                ELSE 1
                            END,
                            ABS(COALESCE(s.window_end_ts, t.opened_ts) - t.opened_ts)
                    ) as rn
                FROM thesis t
                LEFT JOIN tpfm_m5_snapshot s
                    ON s.window_start_ts <= t.opened_ts
                   AND ABS(s.window_end_ts - t.opened_ts) <= 300000
                WHERE (? IS NULL OR t.opened_ts >= ?)
                  AND (? IS NULL OR t.opened_ts < ?)
            ),
            thesis_flow AS (
                SELECT
                    thesis_id,
                    direction,
                    score,
                    confidence,
                    flow_state_code,
                    forced_flow_state,
                    inventory_defense_state,
                    decision_posture,
                    tradability_grade,
                    trap_risk,
                    forced_flow_intensity,
                    context_quality_score
                FROM thesis_flow_ranked
                WHERE rn = 1
            )
            SELECT
                tf.flow_state_code,
                tf.forced_flow_state,
                tf.inventory_defense_state,
                tf.decision_posture,
                tf.tradability_grade,
                COUNT(DISTINCT tf.thesis_id) as total_signals,
                AVG(tf.score) as avg_score,
                AVG(tf.confidence) as avg_confidence,
                AVG(tf.trap_risk) as avg_trap_risk,
                AVG(tf.forced_flow_intensity) as avg_forced_flow_intensity,
                AVG(tf.context_quality_score) as avg_context_quality_score,
                o.horizon,
                COUNT(o.thesis_id) as outcome_count,
                AVG(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' THEN ((t.entry_px - o.realized_px) / t.entry_px) * 100.0
                    ELSE NULL END) as avg_edge,
                SUM(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' AND o.realized_px >= t.entry_px THEN 1
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' AND o.realized_px <= t.entry_px THEN 1
                    ELSE 0 END) as wins,
                AVG(o.mae_bps) as avg_mae,
                AVG(o.mfe_bps) as avg_mfe
            FROM thesis_flow tf
            JOIN thesis t ON t.thesis_id = tf.thesis_id
            LEFT JOIN thesis_outcome o ON tf.thesis_id = o.thesis_id AND o.status = 'COMPLETED'
            GROUP BY
                tf.flow_state_code,
                tf.forced_flow_state,
                tf.inventory_defense_state,
                tf.decision_posture,
                tf.tradability_grade,
                o.horizon
            ORDER BY total_signals DESC, tf.flow_state_code ASC
        """
        params = (start_ts, start_ts, end_ts, end_ts)
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, params).fetchall()
            scorecard: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
            for row in rows:
                bucket_key = (
                    row["flow_state_code"],
                    row["forced_flow_state"],
                    row["inventory_defense_state"],
                    row["decision_posture"],
                    row["tradability_grade"],
                )
                if bucket_key not in scorecard:
                    scorecard[bucket_key] = {
                        "flow_state_code": row["flow_state_code"],
                        "forced_flow_state": row["forced_flow_state"],
                        "inventory_defense_state": row["inventory_defense_state"],
                        "decision_posture": row["decision_posture"],
                        "tradability_grade": row["tradability_grade"],
                        "total_signals": int(row["total_signals"] or 0),
                        "avg_score": float(row["avg_score"] or 0.0),
                        "avg_confidence": float(row["avg_confidence"] or 0.0),
                        "avg_trap_risk": float(row["avg_trap_risk"] or 0.0),
                        "avg_forced_flow_intensity": float(row["avg_forced_flow_intensity"] or 0.0),
                        "avg_context_quality_score": float(row["avg_context_quality_score"] or 0.0),
                        "horizons": {},
                    }
                horizon = row["horizon"]
                if horizon:
                    count = int(row["outcome_count"] or 0)
                    wins = int(row["wins"] or 0)
                    scorecard[bucket_key]["horizons"][horizon] = {
                        "count": count,
                        "wins": wins,
                        "win_rate": (wins / count) if count else 0.0,
                        "avg_edge": float(row["avg_edge"] or 0.0),
                        "avg_mae": float(row["avg_mae"] or 0.0),
                        "avg_mfe": float(row["avg_mfe"] or 0.0),
                    }
            return list(scorecard.values())

    async def get_forced_flow_scorecard(self, *, start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]:
        query = """
            WITH thesis_forced_ranked AS (
                SELECT
                    t.thesis_id,
                    t.direction,
                    t.score,
                    t.confidence,
                    COALESCE(s.forced_flow_state, 'NONE') as forced_flow_state,
                    COALESCE(s.liquidation_bias, 'UNKNOWN') as liquidation_bias,
                    COALESCE(s.basis_state, 'BALANCED') as basis_state,
                    COALESCE(s.tradability_grade, 'D') as tradability_grade,
                    COALESCE(s.forced_flow_intensity, 0.0) as forced_flow_intensity,
                    COALESCE(s.liquidation_intensity, 0.0) as liquidation_intensity,
                    COALESCE(s.trap_risk, 0.0) as trap_risk,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.thesis_id
                        ORDER BY
                            CASE
                                WHEN s.window_end_ts IS NULL THEN 2
                                WHEN s.window_end_ts >= t.opened_ts THEN 0
                                ELSE 1
                            END,
                            ABS(COALESCE(s.window_end_ts, t.opened_ts) - t.opened_ts)
                    ) as rn
                FROM thesis t
                LEFT JOIN tpfm_m5_snapshot s
                    ON s.window_start_ts <= t.opened_ts
                   AND ABS(s.window_end_ts - t.opened_ts) <= 300000
                WHERE (? IS NULL OR t.opened_ts >= ?)
                  AND (? IS NULL OR t.opened_ts < ?)
            ),
            thesis_forced AS (
                SELECT
                    thesis_id,
                    direction,
                    score,
                    confidence,
                    forced_flow_state,
                    liquidation_bias,
                    basis_state,
                    tradability_grade,
                    forced_flow_intensity,
                    liquidation_intensity,
                    trap_risk
                FROM thesis_forced_ranked
                WHERE rn = 1
            )
            SELECT
                tf.forced_flow_state,
                tf.liquidation_bias,
                tf.basis_state,
                tf.tradability_grade,
                COUNT(DISTINCT tf.thesis_id) as total_signals,
                AVG(tf.score) as avg_score,
                AVG(tf.confidence) as avg_confidence,
                AVG(tf.forced_flow_intensity) as avg_forced_flow_intensity,
                AVG(tf.liquidation_intensity) as avg_liquidation_intensity,
                AVG(tf.trap_risk) as avg_trap_risk,
                o.horizon,
                COUNT(o.thesis_id) as outcome_count,
                AVG(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' THEN ((t.entry_px - o.realized_px) / t.entry_px) * 100.0
                    ELSE NULL END) as avg_edge,
                SUM(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' AND o.realized_px >= t.entry_px THEN 1
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' AND o.realized_px <= t.entry_px THEN 1
                    ELSE 0 END) as wins,
                AVG(o.mae_bps) as avg_mae,
                AVG(o.mfe_bps) as avg_mfe
            FROM thesis_forced tf
            JOIN thesis t ON t.thesis_id = tf.thesis_id
            LEFT JOIN thesis_outcome o ON tf.thesis_id = o.thesis_id AND o.status = 'COMPLETED'
            GROUP BY
                tf.forced_flow_state,
                tf.liquidation_bias,
                tf.basis_state,
                tf.tradability_grade,
                o.horizon
            ORDER BY total_signals DESC, tf.forced_flow_state ASC
        """
        params = (start_ts, start_ts, end_ts, end_ts)
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, params).fetchall()
            scorecard: dict[tuple[str, str, str, str], dict[str, Any]] = {}
            for row in rows:
                bucket_key = (
                    row["forced_flow_state"],
                    row["liquidation_bias"],
                    row["basis_state"],
                    row["tradability_grade"],
                )
                if bucket_key not in scorecard:
                    scorecard[bucket_key] = {
                        "forced_flow_state": row["forced_flow_state"],
                        "liquidation_bias": row["liquidation_bias"],
                        "basis_state": row["basis_state"],
                        "tradability_grade": row["tradability_grade"],
                        "total_signals": int(row["total_signals"] or 0),
                        "avg_score": float(row["avg_score"] or 0.0),
                        "avg_confidence": float(row["avg_confidence"] or 0.0),
                        "avg_forced_flow_intensity": float(row["avg_forced_flow_intensity"] or 0.0),
                        "avg_liquidation_intensity": float(row["avg_liquidation_intensity"] or 0.0),
                        "avg_trap_risk": float(row["avg_trap_risk"] or 0.0),
                        "horizons": {},
                    }
                horizon = row["horizon"]
                if horizon:
                    count = int(row["outcome_count"] or 0)
                    wins = int(row["wins"] or 0)
                    scorecard[bucket_key]["horizons"][horizon] = {
                        "count": count,
                        "wins": wins,
                        "win_rate": (wins / count) if count else 0.0,
                        "avg_edge": float(row["avg_edge"] or 0.0),
                        "avg_mae": float(row["avg_mae"] or 0.0),
                        "avg_mfe": float(row["avg_mfe"] or 0.0),
                    }
            return list(scorecard.values())

    async def get_transition_scorecard(self, *, start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]:
        query = """
            WITH thesis_transition_ranked AS (
                SELECT
                    t.thesis_id,
                    COALESCE(NULLIF(t.transition_code_at_entry, ''), e.transition_code, 'NO_TRANSITION_CONTEXT') as transition_code,
                    COALESCE(e.transition_family, 'UNKNOWN') as transition_family,
                    COALESCE(e.transition_alias_vi, 'Chưa có transition') as transition_alias_vi,
                    COALESCE(e.transition_speed, 0.0) as transition_speed,
                    COALESCE(e.transition_quality, 0.0) as transition_quality,
                    COALESCE(e.persistence_score, 0.0) as persistence_score,
                    COALESCE(e.trap_risk, 0.0) as trap_risk,
                    COALESCE(e.forced_flow_involved, 0) as forced_flow_involved,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.thesis_id
                        ORDER BY
                            CASE
                                WHEN e.timestamp IS NULL THEN 2
                                WHEN e.timestamp >= t.opened_ts THEN 0
                                ELSE 1
                            END,
                            ABS(COALESCE(e.timestamp, t.opened_ts) - t.opened_ts)
                    ) as rn
                FROM thesis t
                LEFT JOIN tpfm_transition_event e
                    ON ABS(e.timestamp - t.opened_ts) <= 300000
                WHERE (? IS NULL OR t.opened_ts >= ?)
                  AND (? IS NULL OR t.opened_ts < ?)
            ),
            thesis_transition AS (
                SELECT
                    thesis_id,
                    transition_code,
                    transition_family,
                    transition_alias_vi,
                    transition_speed,
                    transition_quality,
                    persistence_score,
                    trap_risk,
                    forced_flow_involved
                FROM thesis_transition_ranked
                WHERE rn = 1
            )
            SELECT
                tt.transition_code,
                tt.transition_family,
                tt.transition_alias_vi,
                COUNT(DISTINCT tt.thesis_id) as total_signals,
                AVG(tt.transition_speed) as avg_transition_speed,
                AVG(tt.transition_quality) as avg_transition_quality,
                AVG(tt.persistence_score) as avg_persistence_score,
                AVG(tt.trap_risk) as avg_trap_risk,
                AVG(CASE WHEN tt.forced_flow_involved = 1 THEN 1.0 ELSE 0.0 END) as forced_ratio,
                o.horizon,
                COUNT(o.thesis_id) as outcome_count,
                AVG(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' THEN ((o.realized_px - t.entry_px) / t.entry_px) * 100.0
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' THEN ((t.entry_px - o.realized_px) / t.entry_px) * 100.0
                    ELSE NULL END) as avg_edge,
                SUM(CASE
                    WHEN t.entry_px > 0 AND t.direction = 'LONG_BIAS' AND o.realized_px >= t.entry_px THEN 1
                    WHEN t.entry_px > 0 AND t.direction = 'SHORT_BIAS' AND o.realized_px <= t.entry_px THEN 1
                    ELSE 0 END) as wins,
                AVG(o.mae_bps) as avg_mae,
                AVG(o.mfe_bps) as avg_mfe
            FROM thesis_transition tt
            JOIN thesis t ON t.thesis_id = tt.thesis_id
            LEFT JOIN thesis_outcome o ON tt.thesis_id = o.thesis_id AND o.status = 'COMPLETED'
            GROUP BY tt.transition_code, tt.transition_family, tt.transition_alias_vi, o.horizon
            ORDER BY total_signals DESC, tt.transition_code ASC
        """
        params = (start_ts, start_ts, end_ts, end_ts)
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(query, params).fetchall()
            scorecard: dict[str, dict[str, Any]] = {}
            for row in rows:
                transition_code = row["transition_code"]
                if transition_code not in scorecard:
                    scorecard[transition_code] = {
                        "transition_code": transition_code,
                        "transition_family": row["transition_family"],
                        "transition_alias_vi": row["transition_alias_vi"],
                        "total_signals": int(row["total_signals"] or 0),
                        "avg_transition_speed": float(row["avg_transition_speed"] or 0.0),
                        "avg_transition_quality": float(row["avg_transition_quality"] or 0.0),
                        "avg_persistence_score": float(row["avg_persistence_score"] or 0.0),
                        "avg_trap_risk": float(row["avg_trap_risk"] or 0.0),
                        "forced_ratio": float(row["forced_ratio"] or 0.0),
                        "horizons": {},
                    }
                horizon = row["horizon"]
                if horizon:
                    count = int(row["outcome_count"] or 0)
                    wins = int(row["wins"] or 0)
                    scorecard[transition_code]["horizons"][horizon] = {
                        "count": count,
                        "wins": wins,
                        "win_rate": (wins / count) if count else 0.0,
                        "avg_edge": float(row["avg_edge"] or 0.0),
                        "avg_mae": float(row["avg_mae"] or 0.0),
                        "avg_mfe": float(row["avg_mfe"] or 0.0),
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
            columns = [
                "snapshot_id", "symbol", "window_start_ts", "window_end_ts", "run_id",
                "microprice", "open_px", "high_px", "low_px", "close_px",
                "initiative_score", "initiative_polarity", "initiative_strength",
                "inventory_score", "inventory_polarity", "inventory_strength", "axis_confidence",
                "energy_score", "energy_state", "response_efficiency_score", "response_efficiency_state",
                "matrix_cell", "micro_conclusion", "matrix_alias_vi", "continuation_bias", "preferred_posture", "tradability_grade",
                "tradability_score", "delta_quote", "cvd_slope", "trade_burst", "absorption_score", "imbalance_l1",
                "centered_imbalance_l1", "signed_absorption_score", "microprice_gap_bps", "spread_bps",
                "active_thesis_count", "new_thesis_count", "actionable_count", "invalidated_count", "resolved_count",
                "dominant_setups", "setup_score_map", "futures_delta_available", "futures_delta", "oi_delta", "oi_state",
                "basis_bps", "funding_rate", "spot_futures_relation", "context_quality_score",
                "venue_confirmation_state", "leader_venue", "lagger_venue", "venue_vwap_spread_bps",
                "liquidation_context_available", "liquidation_bias", "liquidation_count", "liquidation_quote",
                "flow_state_code", "forced_flow_state", "forced_flow_intensity", "inventory_defense_state",
                "liquidation_intensity", "transition_ready", "trap_risk", "decision_posture",
                "decision_summary_vi", "entry_condition_vi", "confirm_needed_vi", "avoid_if_vi", "review_tags_json",
                "blind_spot_flags", "observed_facts", "inferred_facts", "missing_context", "risk_flags",
                "action_plan_vi", "invalid_if", "health_state",
                "delta_zscore", "aggression_ratio", "sweep_quote",
                "sweep_buy_quote", "sweep_sell_quote", "burst_persistence", "microprice_drift_bps",
                "replenishment_bid_score", "replenishment_ask_score",
                "oi_expansion_ratio", "futures_aggression_ratio", "basis_state",
                "leader_confidence", "aligned_window_ms",
                "edge_score", "edge_confidence", "historical_win_rate", "expected_rr",
                "pattern_code", "pattern_alias_vi", "pattern_family", "pattern_phase",
                "pattern_strength", "pattern_quality", "pattern_failure_risk",
                "sequence_start_ts", "sequence_duration_sec", "is_sequence_pivot",
                "parent_context_json", "t_plus_1_price", "t_plus_5_price", "t_plus_12_price"
            ]
            values = (
                snapshot.snapshot_id,
                snapshot.symbol,
                snapshot.window_start_ts,
                snapshot.window_end_ts,
                snapshot.run_id,
                snapshot.microprice,
                snapshot.open_px,
                snapshot.high_px,
                snapshot.low_px,
                snapshot.close_px,
                snapshot.initiative_score,
                snapshot.initiative_polarity,
                snapshot.initiative_strength,
                snapshot.inventory_score,
                snapshot.inventory_polarity,
                snapshot.inventory_strength,
                snapshot.axis_confidence,
                snapshot.energy_score,
                snapshot.energy_state,
                snapshot.response_efficiency_score,
                snapshot.response_efficiency_state,
                snapshot.matrix_cell,
                snapshot.micro_conclusion,
                snapshot.matrix_alias_vi,
                snapshot.continuation_bias,
                snapshot.preferred_posture,
                snapshot.tradability_grade,
                snapshot.tradability_score,
                snapshot.delta_quote,
                snapshot.cvd_slope,
                snapshot.trade_burst,
                snapshot.absorption_score,
                snapshot.imbalance_l1,
                snapshot.centered_imbalance_l1,
                snapshot.signed_absorption_score,
                snapshot.microprice_gap_bps,
                snapshot.spread_bps,
                snapshot.active_thesis_count,
                snapshot.new_thesis_count,
                snapshot.actionable_count,
                snapshot.invalidated_count,
                snapshot.resolved_count,
                json.dumps(snapshot.dominant_setups, ensure_ascii=False),
                json.dumps(snapshot.setup_score_map, ensure_ascii=False),
                int(snapshot.futures_delta_available),
                snapshot.futures_delta,
                snapshot.oi_delta,
                snapshot.oi_state,
                snapshot.basis_bps,
                snapshot.funding_rate,
                snapshot.spot_futures_relation,
                snapshot.context_quality_score,
                snapshot.venue_confirmation_state,
                snapshot.leader_venue,
                snapshot.lagger_venue,
                snapshot.venue_vwap_spread_bps,
                int(snapshot.liquidation_context_available),
                snapshot.liquidation_bias,
                snapshot.liquidation_count,
                snapshot.liquidation_quote,
                snapshot.flow_state_code,
                snapshot.forced_flow_state,
                snapshot.forced_flow_intensity,
                snapshot.inventory_defense_state,
                snapshot.liquidation_intensity,
                int(snapshot.transition_ready),
                snapshot.trap_risk,
                snapshot.decision_posture,
                snapshot.decision_summary_vi,
                snapshot.entry_condition_vi,
                snapshot.confirm_needed_vi,
                snapshot.avoid_if_vi,
                snapshot.review_tags_json,
                json.dumps(snapshot.blind_spot_flags, ensure_ascii=False),
                json.dumps(snapshot.observed_facts, ensure_ascii=False),
                json.dumps(snapshot.inferred_facts, ensure_ascii=False),
                json.dumps(snapshot.missing_context, ensure_ascii=False),
                json.dumps(snapshot.risk_flags, ensure_ascii=False),
                snapshot.action_plan_vi,
                snapshot.invalid_if,
                snapshot.health_state,
                snapshot.delta_zscore,
                snapshot.aggression_ratio,
                snapshot.sweep_quote,
                snapshot.sweep_buy_quote,
                snapshot.sweep_sell_quote,
                snapshot.burst_persistence,
                snapshot.microprice_drift_bps,
                snapshot.replenishment_bid_score,
                snapshot.replenishment_ask_score,
                snapshot.oi_expansion_ratio,
                snapshot.futures_aggression_ratio,
                snapshot.basis_state,
                snapshot.leader_confidence,
                snapshot.aligned_window_ms,
                snapshot.edge_profile.edge_score if snapshot.edge_profile else 0.0,
                snapshot.edge_profile.confidence if snapshot.edge_profile else "UNKNOWN",
                snapshot.edge_profile.historical_win_rate if snapshot.edge_profile else 0.0,
                snapshot.edge_profile.expected_rr if snapshot.edge_profile else 0.0,
                snapshot.pattern_code,
                snapshot.pattern_alias_vi,
                snapshot.pattern_family,
                snapshot.pattern_phase,
                snapshot.pattern_strength,
                snapshot.pattern_quality,
                snapshot.pattern_failure_risk,
                snapshot.sequence_start_ts,
                snapshot.sequence_duration_sec,
                int(snapshot.is_sequence_pivot),
                json.dumps(snapshot.parent_context, ensure_ascii=False),
                snapshot.t_plus_1_price,
                snapshot.t_plus_5_price,
                snapshot.t_plus_12_price,
            )
            placeholders = ", ".join("?" for _ in values)
            db.execute(
                f"INSERT INTO tpfm_m5_snapshot ({', '.join(columns)}) VALUES ({placeholders})",
                values,
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
                    avg_persistence, structural_score, ai_analysis_vi, health_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    report.structural_score,
                    report.ai_analysis_vi,
                    report.health_state,
                ),
            )
            db.commit()

    async def save_flow_transition(self, event: Any) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT INTO tpfm_transition_event (
                    transition_id, symbol, venue, timestamp,
                    from_cell, to_cell, transition_code, transition_family, transition_alias_vi,
                    from_flow_state_code, to_flow_state_code,
                    transition_speed, transition_quality, persistence_score,
                    forced_flow_involved, trap_risk, from_decision_posture, to_decision_posture,
                    decision_shift, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.transition_id,
                    event.symbol,
                    event.venue,
                    event.timestamp,
                    event.from_cell,
                    event.to_cell,
                    event.transition_code,
                    event.transition_family,
                    event.transition_alias_vi,
                    event.from_flow_state_code,
                    event.to_flow_state_code,
                    event.transition_speed,
                    event.transition_quality,
                    event.persistence_score,
                    int(event.forced_flow_involved),
                    event.trap_risk,
                    event.from_decision_posture,
                    event.to_decision_posture,
                    event.decision_shift,
                    json.dumps(event.metadata, ensure_ascii=False),
                ),
            )
            db.commit()

    async def save_sequence_event(self, event: Any) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT OR REPLACE INTO tpfm_sequence_event (
                    sequence_id, symbol, venue, start_ts, end_ts,
                    sequence_signature, sequence_family, sequence_length,
                    cumulative_initiative, cumulative_inventory,
                    max_energy, sequence_quality, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.sequence_id,
                    event.symbol,
                    event.venue,
                    event.start_ts,
                    event.end_ts,
                    event.sequence_signature,
                    event.sequence_family,
                    event.sequence_length,
                    event.cumulative_initiative,
                    event.cumulative_inventory,
                    event.max_energy,
                    event.sequence_quality,
                    int(event.is_active),
                ),
            )
            db.commit()

    async def save_flow_pattern_event(self, event: Any) -> None:
        if not event:
            return
        # Handle both dict (from JSON-serializable metadata) and object
        def _get(attr):
            if isinstance(event, dict):
                return event.get(attr)
            return getattr(event, attr, None)

        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT OR REPLACE INTO flow_pattern_event (
                    pattern_id, snapshot_id, symbol, venue, timestamp, 
                    pattern_code, pattern_alias_vi, pattern_family, 
                    pattern_phase, sequence_id, sequence_signature, 
                    sequence_length, tempo_state, persistence_state, 
                    pattern_strength, pattern_quality, pattern_failure_risk, 
                    matrix_cell, flow_state_code, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _get("pattern_id"),
                    _get("snapshot_id"),
                    _get("symbol"),
                    _get("venue"),
                    _get("timestamp"),
                    _get("pattern_code"),
                    _get("pattern_alias_vi"),
                    _get("pattern_family"),
                    _get("pattern_phase"),
                    _get("sequence_id"),
                    _get("sequence_signature"),
                    _get("sequence_length"),
                    _get("tempo_state"),
                    _get("persistence_state"),
                    _get("pattern_strength"),
                    _get("pattern_quality"),
                    _get("pattern_failure_risk"),
                    _get("matrix_cell"),
                    _get("flow_state_code"),
                    json.dumps(_get("metadata"), ensure_ascii=False) if _get("metadata") else "{}"
                ),
            )
            db.commit()

    async def save_pattern_outcome(self, outcome: Any) -> None:
        if not outcome:
            return
        # Handle both dict and object
        def _get(attr):
            if isinstance(outcome, dict):
                return outcome.get(attr)
            return getattr(outcome, attr, None)

        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT OR REPLACE INTO flow_pattern_outcome (
                    outcome_id, snapshot_id, symbol, timestamp, pattern_code, sequence_signature,
                    start_px, t1_px, t5_px, t12_px, r1_bps, r5_bps, r12_bps,
                    max_favorable_bps, max_adverse_bps, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _get("outcome_id"),
                    _get("snapshot_id"),
                    _get("symbol"),
                    _get("timestamp"),
                    _get("pattern_code"),
                    _get("sequence_signature"),
                    _get("start_px"),
                    _get("t1_px"),
                    _get("t5_px"),
                    _get("t12_px"),
                    _get("r1_bps"),
                    _get("r5_bps"),
                    _get("r12_bps"),
                    _get("max_favorable_bps"),
                    _get("max_adverse_bps"),
                    json.dumps(_get("metadata"), ensure_ascii=False) if _get("metadata") else "{}"
                ),
            )
            db.commit()
    async def get_pattern_scorecard(self) -> list[dict]:
        """Queries historical statistics for flow patterns and sequences."""
        with sqlite3.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            cursor = db.execute(
                """
                SELECT 
                    pattern_code,
                    sequence_signature,
                    COUNT(*) as count,
                    IFNULL(AVG(CASE WHEN r5_bps > 0 THEN 1 ELSE 0 END), 0.0) as win_rate_5m,
                    IFNULL(AVG(max_favorable_bps / NULLIF(ABS(max_adverse_bps), 0)), 0.0) as avg_rr
                FROM flow_pattern_outcome
                GROUP BY pattern_code, sequence_signature
                HAVING COUNT(*) >= 1
                """
            )
            return [dict(row) for row in cursor.fetchall()]
