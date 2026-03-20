from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time

from cfte.models.events import TapeSnapshot, ThesisSignal, ThesisOutcome
from cfte.storage.sqlite_writer import ThesisSQLiteStore

@dataclass(slots=True)
class ActiveVirtualTrade:
    thesis_id: str
    instrument_key: str
    direction: str
    entry_px: float
    entry_ts: int
    horizon_ts: int
    mae_bps: float = 0.0
    mfe_bps: float = 0.0
    last_px: float = 0.0

class OutcomeRealismEngine:
    def __init__(self, store: ThesisSQLiteStore, slippage_bps: float = 1.5, fee_bps: float = 2.0):
        self.store = store
        self.slippage_bps = slippage_bps
        self.fee_bps = fee_bps
        self.active_trades: Dict[str, ActiveVirtualTrade] = {}

    def on_signal(self, signal: ThesisSignal, snapshot: TapeSnapshot) -> None:
        """Called when a signal stage changes to ACTIONABLE."""
        if signal.thesis_id in self.active_trades:
            return

        # Virtual Entry Simulation
        # Slippage usually pushes the entry price further away
        direction_sign = 1.0 if signal.direction == "LONG_BIAS" else -1.0
        slippage_mult = 1.0 + (direction_sign * (self.slippage_bps / 10000.0))
        
        # We assume entry is at mid_price + half_spread (market order realism)
        entry_base = snapshot.mid_px + (direction_sign * (0.5 * snapshot.spread_bps / 10000.0) * snapshot.mid_px)
        fill_px = entry_base * slippage_mult
        
        # Fix 1h horizon for simplicity in Phase 3
        horizon_ms = 3600 * 1000
        
        self.active_trades[signal.thesis_id] = ActiveVirtualTrade(
            thesis_id=signal.thesis_id,
            instrument_key=signal.instrument_key,
            direction=signal.direction,
            entry_px=fill_px,
            entry_ts=snapshot.window_end_ts,
            horizon_ts=snapshot.window_end_ts + horizon_ms,
            last_px=fill_px
        )
        print(f"🚀 [REALISM] Virtual Entry: {signal.thesis_id[:8]} at {fill_px:,.2f} (Slippage: {self.slippage_bps}bps)")

    async def update(self, snapshot: TapeSnapshot) -> None:
        """Update MAE/MFE for all active virtual trades based on current price."""
        now = snapshot.window_end_ts
        current_px = snapshot.last_trade_px
        
        to_finalize = []
        
        for tid, trade in self.active_trades.items():
            # Calculate current excursion
            diff_bps = ((current_px - trade.entry_px) / trade.entry_px) * 10000.0
            
            if trade.direction == "LONG_BIAS":
                # MAE is the maximum drop (negative diff)
                trade.mae_bps = min(trade.mae_bps, diff_bps)
                # MFE is the maximum rise (positive diff)
                trade.mfe_bps = max(trade.mfe_bps, diff_bps)
            else: # SHORT_BIAS
                # MAE is the maximum rise (positive diff for price, but adverse for short)
                trade.mae_bps = min(trade.mae_bps, -diff_bps)
                # MFE is the maximum drop (negative diff for price, but favorable for short)
                trade.mfe_bps = max(trade.mfe_bps, -diff_bps)
            
            trade.last_px = current_px
            
            if now >= trade.horizon_ts:
                to_finalize.append(tid)

        for tid in to_finalize:
            await self._finalize_trade(tid)

    async def _finalize_trade(self, thesis_id: str) -> None:
        trade = self.active_trades.pop(thesis_id)
        print(f"🏁 [REALISM] Finalizing Virtual Trade: {thesis_id[:8]} | MAE: {trade.mae_bps:.1f}bps | MFE: {trade.mfe_bps:.1f}bps")
        
        # Save to DB
        await self.store.save_outcome(
            thesis_id=thesis_id,
            horizon="1h",
            realized_px=trade.last_px,
            realized_high=0.0, # Not used here, we use MAE/MFE
            realized_low=0.0,
            fill_px=trade.entry_px,
            mae_bps=trade.mae_bps,
            mfe_bps=trade.mfe_bps,
            exit_ts=trade.horizon_ts
        )
