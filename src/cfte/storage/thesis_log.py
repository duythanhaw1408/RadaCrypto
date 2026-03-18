from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cfte.models.events import ThesisSignal
from cfte.thesis.cards import signal_to_dict


class ThesisLogWriter:
    def __init__(self, output_path: str | Path) -> None:
        self.output_path = Path(output_path)

    def append_record(self, record: dict[str, Any]) -> Path:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
        return self.output_path

    def append_scan_result(
        self,
        *,
        profile_name: str,
        events_path: str,
        instrument_key: str,
        actionable_threshold: float,
        feature_windows: int,
        selected_signals: list[ThesisSignal],
        total_signals: int,
    ) -> Path:
        return self.append_record(
            {
                "flow": "scan",
                "profile": profile_name,
                "events_path": events_path,
                "instrument_key": instrument_key,
                "actionable_threshold": actionable_threshold,
                "feature_windows": feature_windows,
                "total_signals": total_signals,
                "selected_count": len(selected_signals),
                "signals": [signal_to_dict(signal) for signal in selected_signals],
            }
        )

    def append_live_snapshot(
        self,
        *,
        profile_name: str,
        symbol: str,
        instrument_key: str,
        event_type: str,
        venue_ts: int,
        trade_window_size: int,
        signals: list[ThesisSignal],
        health: dict[str, Any],
    ) -> Path:
        return self.append_record(
            {
                "flow": "live",
                "profile": profile_name,
                "symbol": symbol,
                "instrument_key": instrument_key,
                "event_type": event_type,
                "venue_ts": venue_ts,
                "trade_window_size": trade_window_size,
                "signal_count": len(signals),
                "signals": [signal_to_dict(signal) for signal in signals],
                "health": health,
            }
        )
