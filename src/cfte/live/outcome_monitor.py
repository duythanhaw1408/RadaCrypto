from __future__ import annotations

import asyncio
import logging
from typing import Any

from cfte.collectors.binance_public import fetch_historical_kline
from cfte.storage.sqlite_writer import ThesisSQLiteStore
from cfte.thesis.state import ThesisEventRecord

logger = logging.getLogger(__name__)

class OutcomeMonitor:
    def __init__(self, store: ThesisSQLiteStore) -> None:
        self.store = store
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        print("Bắt đầu trình theo dõi kết quả luận điểm (Outcome Monitor)...")
        while not self._stop_event.is_set():
            try:
                pending = await self.store.get_pending_outcomes()
                if pending:
                    for outcome in pending:
                        await self._process_outcome(outcome)
            except Exception as exc:
                print(f"Lỗi Outcome Monitor: {exc}")
            
            # Check every minute
            for _ in range(60):
                if self._stop_event.is_set():
                    break
                await asyncio.sleep(1)

    async def _process_outcome(self, outcome: dict[str, Any]) -> None:
        thesis_id = outcome["thesis_id"]
        horizon = outcome["horizon"]
        symbol = instrument_to_symbol(outcome["instrument_key"])
        target_ts = outcome["target_ts"]

        print(f"Đang kiểm tra kết quả {horizon} cho {thesis_id} ({symbol})...")
        kline = await asyncio.to_thread(fetch_historical_kline, symbol, target_ts)
        
        if kline:
            # Kline format: [Open time, Open, High, Low, Close, Volume, ...]
            close_px = float(kline[4])
            high_px = float(kline[2])
            low_px = float(kline[3])
            
            await self.store.save_outcome(
                thesis_id=thesis_id,
                horizon=horizon,
                realized_px=close_px,
                realized_high=high_px,
                realized_low=low_px
            )
            terminal_stage = await self.store.finalize_thesis_from_outcome(
                thesis_id=thesis_id,
                horizon=horizon,
                updated_at=target_ts,
            )
            if terminal_stage is not None:
                await self.store.append_event(
                    ThesisEventRecord(
                        thesis_id=thesis_id,
                        event_type="outcome_terminal",
                        from_stage=outcome["stage"],
                        to_stage=terminal_stage,
                        event_ts=target_ts,
                        summary_vi=f"Luận điểm được chốt {terminal_stage.lower()} theo outcome {horizon}.",
                        score=0.0,
                        confidence=0.0,
                    )
                )
                print(f"Đã cập nhật kết quả {horizon} cho {thesis_id}: Price={close_px} | terminal={terminal_stage}")
            else:
                print(f"Đã cập nhật kết quả {horizon} cho {thesis_id}: Price={close_px}")
        else:
            print(f"Chưa lấy được dữ liệu giá cho {horizon} của {thesis_id}. Sẽ thử lại sau.")

    def stop(self) -> None:
        self._stop_event.set()

def instrument_to_symbol(instrument_key: str) -> str:
    # BINANCE:BTCUSDT:SPOT -> BTCUSDT
    parts = instrument_key.split(":")
    if len(parts) >= 2:
        return parts[1]
    return instrument_key
