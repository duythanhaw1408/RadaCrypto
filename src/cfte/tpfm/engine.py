import math
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone
import uuid
from collections import Counter

from cfte.models.events import TapeSnapshot, NormalizedTrade
from cfte.tpfm.models import TPFMSnapshot, TPFM30mRegime, TPFM4hStructural

class TPFMStateEngine:
    def __init__(self, symbol: str = "BTCUSDT", venue: str = "binance"):
        self.symbol = symbol
        self.venue = venue
        self._prev_snapshot: Optional[TPFMSnapshot] = None
        
    def calculate_m5_snapshot(
        self, 
        window_start_ts: int, 
        window_end_ts: int, 
        trades: List[NormalizedTrade],
        snapshots: List[TapeSnapshot], 
        active_theses: List = None,
        futures_context: Dict[str, Any] = None
    ) -> TPFMSnapshot:
        if not trades or not snapshots:
            return self._empty_snapshot(window_start_ts, window_end_ts)

        # 1. Basic Metrics
        total_buy_quote = sum(t.quote_qty for t in trades if t.taker_side == "BUY")
        total_sell_quote = sum(t.quote_qty for t in trades if t.taker_side == "SELL")
        total_quote = total_buy_quote + total_sell_quote
        delta_quote = total_buy_quote - total_sell_quote
        
        window_sec = (window_end_ts - window_start_ts) / 1000.0
        trade_burst = len(trades) / max(window_sec, 1.0)
        cvd_slope = delta_quote / max(window_sec, 1.0)
        taker_imbalance = delta_quote / max(total_quote, 1.0)

        # 2. Initiative Score
        z_delta = self._soft_clamp(delta_quote / 50000.0)
        z_cvd = self._soft_clamp(cvd_slope / 100.0)
        z_burst = self._soft_clamp((trade_burst - 2.0) / 5.0)
        z_taker = taker_imbalance
        initiative_score = (0.4 * z_delta + 0.3 * z_cvd + 0.2 * z_burst + 0.1 * z_taker)
        
        if initiative_score >= 0.35: initiative_polarity = "POS_INIT"
        elif initiative_score <= -0.35: initiative_polarity = "NEG_INIT"
        else: initiative_polarity = "NEUTRAL_INIT"

        # 3. Inventory Score
        avg_imb_l1 = sum(s.imbalance_l1 for s in snapshots) / len(snapshots)
        avg_absorption = sum(s.absorption_proxy for s in snapshots) / len(snapshots)
        last_s = snapshots[-1]
        spread = max(last_s.mid_px * last_s.spread_bps / 10000.0, 0.0001)
        micro_pos = (last_s.mid_px - last_s.microprice) / spread
        
        z_imb = self._soft_clamp(avg_imb_l1)
        z_abs = self._soft_clamp((avg_absorption - 5000.0) / 10000.0)
        z_micro = self._soft_clamp(micro_pos)
        inventory_score = (0.35 * z_imb + 0.25 * 0.0 + 0.25 * z_abs + 0.15 * z_micro)
        
        if inventory_score >= 0.30: inventory_polarity = "POS_INV"
        elif inventory_score <= -0.30: inventory_polarity = "NEG_INV"
        else: inventory_polarity = "NEUTRAL_INV"

        # 4. Energy Score
        prices = [t.price for t in trades]
        high, low = max(prices), min(prices)
        range_bps = ((high - low) / low) * 10000.0
        z_range = self._soft_clamp(range_bps / 50.0)
        energy_score = (0.45 * z_burst + 0.30 * z_range + 0.25 * z_delta)
        
        if energy_score < 0.20: energy_state = "COMPRESSION"
        elif energy_score > 0.75: energy_state = "EXHAUSTING"
        else: energy_state = "EXPANDING"

        # 5. Response Efficiency
        start_px = snapshots[0].mid_px
        end_px = snapshots[-1].mid_px
        ret_bps = ((end_px - start_px) / start_px) * 10000.0
        response_efficiency = ret_bps / max(abs(z_delta * 10.0), 0.25)
        if response_efficiency > 0.40: response_efficiency_state = "FOLLOW_THROUGH"
        elif response_efficiency < -0.20: response_efficiency_state = "ABSORBED_OR_TRAP"
        else: response_efficiency_state = "MIXED"

        # 6. Matrix Cell
        matrix_cell = f"{initiative_polarity}__{inventory_polarity}"
        
        # 7. Tradability & Conflict (Phase T1 Upgrade)
        tradability_score = (0.5 * abs(initiative_score) + 0.5 * abs(inventory_score))
        
        # Agreement: sign(init) == sign(inv) AND both are non-neutral
        agreement_score = 0.0
        conflict_score = 0.0
        if initiative_polarity != "NEUTRAL_INIT" and inventory_polarity != "NEUTRAL_INV":
            if (initiative_score > 0 and inventory_score > 0) or (initiative_score < 0 and inventory_score < 0):
                agreement_score = 1.0
            else:
                conflict_score = 1.0
        
        # Micro Conclusion Mapping
        micro_conclusion = "UNCERTAIN"
        if agreement_score == 1.0:
            micro_conclusion = "BULLISH_CONFLUENCE" if initiative_score > 0 else "BEARISH_CONFLUENCE"
        elif conflict_score == 1.0:
            micro_conclusion = "ABSORPTION_IN_PROGRESS"
        elif initiative_polarity != "NEUTRAL_INIT" and inventory_polarity == "NEUTRAL_INV":
            micro_conclusion = "INITIATIVE_DRIVEN"
        elif initiative_polarity == "NEUTRAL_INIT" and inventory_polarity != "NEUTRAL_INV":
            micro_conclusion = "INVENTORY_DRIVEN"
            
        # 8. Market Quality
        market_quality_score = (agreement_score * 0.4 + (1.0 - abs(energy_score - 0.5)) * 0.6)
        
        # --- PHASE 2: Novelty & Escalation ---
        novelty_score = 0.0
        escalation_reason = []
        
        if self._prev_snapshot:
            if self._prev_snapshot.matrix_cell != matrix_cell:
                novelty_score += 0.7
                escalation_reason.append(f"Shift: {self._prev_snapshot.matrix_cell} -> {matrix_cell}")
            if self._prev_snapshot.inventory_polarity != inventory_polarity:
                novelty_score += 0.2
                escalation_reason.append(f"Inventory Flip: {inventory_polarity}")

        if active_theses:
            actionables = [t for t in active_theses if t.signal.stage == "ACTIONABLE"]
            if actionables:
                novelty_score += 0.3
                escalation_reason.append(f"Active Signals: {len(actionables)} actionable")

        should_escalate = novelty_score >= 0.65 or energy_state == "EXHAUSTING"
        if energy_state == "EXHAUSTING":
            escalation_reason.append("Energy Exhaustion detected")

        snapshot = TPFMSnapshot(
            snapshot_id=str(uuid.uuid4()),
            symbol=self.symbol,
            venue=self.venue,
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            initiative_score=initiative_score,
            initiative_polarity=initiative_polarity,
            inventory_score=inventory_score,
            inventory_polarity=inventory_polarity,
            energy_score=energy_score,
            energy_state=energy_state,
            response_efficiency_score=response_efficiency,
            response_efficiency_state=response_efficiency_state,
            matrix_cell=matrix_cell,
            tradability_score=tradability_score,
            conflict_score=conflict_score,
            agreement_score=agreement_score,
            market_quality_score=market_quality_score,
            micro_conclusion=micro_conclusion,
            delta_quote=delta_quote,
            cvd_slope=cvd_slope,
            trade_burst=trade_burst,
            absorption_score=avg_absorption,
            imbalance_l1=avg_imb_l1,
            novelty_score=novelty_score,
            should_escalate=should_escalate,
            escalation_reason=escalation_reason
        )
        
        self._prev_snapshot = snapshot
        
        # --- PHASE 5: Context Overlay (High-Fidelity) ---
        self._apply_context_overlay(snapshot, futures_context)
        
        return snapshot

    def _apply_context_overlay(
        self, 
        snapshot: TPFMSnapshot, 
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Refines TPFM with real-world Futures Context (OI, Basis, Funding)"""
        if not context or not context.get("available", False):
            snapshot.futures_context_available = False
            snapshot.context_warning_flags.append("THIẾU CONTEXT FUTURES")
            return

        snapshot.futures_context_available = True
        snapshot.futures_context_fresh = context.get("fresh", False)
        
        spot_delta = snapshot.delta_quote
        futures_delta = context.get("futures_delta", 0.0)
        oi_delta = context.get("oi_delta", 0.0)
        basis_bps = context.get("basis_bps", 0.0)
        
        # 1. Divergence/Confluence Logic
        if (spot_delta > 0 and futures_delta > 0) or (spot_delta < 0 and futures_delta < 0):
            snapshot.context_score = 1.0 # Confluence
            snapshot.agreement_score += 0.2
            snapshot.tradability_score *= 1.2
            snapshot.escalation_reason.append("Futures Xác Nhận (Confluence)")
        elif (spot_delta > 0 and futures_delta < 0) or (spot_delta < 0 and futures_delta > 0):
            snapshot.context_score = -1.0 # Divergence
            snapshot.conflict_score += 0.3
            snapshot.tradability_score *= 0.8
            snapshot.context_warning_flags.append("FUTURES LỆCH NHỊP (Divergence)")
            
        # 2. Leadership & OI Extension
        if abs(spot_delta) > abs(futures_delta) * 1.5:
            snapshot.futures_bias_proxy = "SPOT_LED"
        elif abs(futures_delta) > abs(spot_delta) * 1.5:
            snapshot.futures_bias_proxy = "FUTURES_LED"
            if oi_delta > 0:
                snapshot.micro_conclusion = "OI_DRIVEN_MOVE"
                snapshot.escalation_reason.append("OI Mở Rộng Ủng Hộ")
        
        # 3. Basis & Funding Context
        snapshot.basis_bps = basis_bps
        snapshot.funding_rate = context.get("funding_rate", 0.0)
        
        if abs(basis_bps) > 10.0: # Threshold for basis divergence
            snapshot.basis_divergence_state = "DIVERGING_POS" if basis_bps > 0 else "DIVERGING_NEG"
            snapshot.context_warning_flags.append("BASIS PHÂN KỲ")
            
        if abs(snapshot.funding_rate) > 0.0005: # High funding
            snapshot.funding_bias = "POSITIVE" if snapshot.funding_rate > 0 else "NEGATIVE"
            snapshot.context_warning_flags.append("FUNDING QUÁ NÓNG")

        # 4. Context Quality
        snapshot.context_quality_score = 0.8 if snapshot.futures_context_fresh else 0.4
        
        # 5. Final Absorption Check
        if snapshot.context_score == -1.0 and abs(futures_delta) >= abs(spot_delta) * 1.5:
            snapshot.micro_conclusion = "ABSORBED_BY_FUTURES"
    def calculate_4h_structural(self, regimes: List[TPFM30mRegime]) -> TPFM4hStructural:
        """Synthesizes 8 30m regimes into a 4-hour structural report"""
        if not regimes:
            return self._empty_4h_structural(0, 0)
        
        m30_count = len(regimes)
        regime_names = [r.dominant_regime for r in regimes]
        cell_names = [r.dominant_cell for r in regimes]
        
        regime_counter = Counter(regime_names)
        cell_counter = Counter(cell_names)
        
        # Calculate shares
        regime_share = {k: v / m30_count for k, v in regime_counter.items()}
        cell_share = {k: v / m30_count for k, v in cell_counter.items()}
        
        # Determine bias
        total_delta = sum(r.net_delta_quote for r in regimes)
        avg_persistence = sum(r.regime_persistence_score for r in regimes) / m30_count
        avg_traps = sum(r.invalidation_pressure_score for r in regimes) / m30_count # Using inv_pressure as trap proxy here for now or update model
        
        if total_delta > 500000: bias = "BULLISH_CONTROL"
        elif total_delta < -500000: bias = "BEARISH_CONTROL"
        else: bias = "NEUTRAL_RANGE"
        
        # Structural conclusion
        dominant_regime = regime_counter.most_common(1)[0][0]
        
        return TPFM4hStructural(
            structural_id=str(uuid.uuid4()),
            symbol=self.symbol,
            venue=self.venue,
            window_start_ts=regimes[0].window_start_ts,
            window_end_ts=regimes[-1].window_end_ts,
            m30_count=m30_count,
            dominant_regime_share=regime_share,
            dominant_cell_share=cell_share,
            structural_bias=bias,
            transition_map=regime_names,
            net_delta_quote=total_delta,
            avg_persistence=avg_persistence,
            structural_score=avg_persistence * (1.0 - avg_traps),
            structural_quality="HIGH" if avg_persistence > 0.7 else "MEDIUM" if avg_persistence > 0.4 else "LOW",
            should_send_ai_report=True
        )

    def _empty_4h_structural(self, start: int, end: int) -> TPFM4hStructural:
        return TPFM4hStructural(
            structural_id=str(uuid.uuid4()),
            symbol=self.symbol,
            window_start_ts=start,
            window_end_ts=end,
            health_state="EMPTY_DATA"
        )

    def calculate_30m_regime(self, snapshots: List[TPFMSnapshot]) -> TPFM30mRegime:
        """Aggregates 6 M5 snapshots into a 30-minute regime synthesis"""
        if not snapshots:
            return self._empty_regime(0, 0)
        
        m5_count = len(snapshots)
        cells = [s.matrix_cell for s in snapshots]
        counter = Counter(cells)
        dominant_cell = counter.most_common(1)[0][0]
        persistence = counter[dominant_cell] / m5_count
        
        # Actionability & Invalidation (Phase T3 Upgrade)
        actionable_snaps = [s for s in snapshots if s.actionable_count > 0]
        invalidated_snaps = [s for s in snapshots if s.invalidated_count > 0]
        trap_snaps = [s for s in snapshots if s.response_efficiency_state == "ABSORBED_OR_TRAP"]
        
        act_density = len(actionable_snaps) / m5_count
        inv_pressure = len(invalidated_snaps) / m5_count
        trap_rate = len(trap_snaps) / m5_count
        
        # Aggregate scores
        avg_conflict = sum(s.conflict_score for s in snapshots) / m5_count
        avg_agreement = sum(s.agreement_score for s in snapshots) / m5_count
        avg_tradability = sum(s.tradability_score for s in snapshots) / m5_count
        total_novelty = sum(s.novelty_score for s in snapshots)
        
        # Determine dominant regime from dominant_cell
        regime_map = {
            "POS_INIT__POS_INV": "STRONG_ACCUMULATION",
            "POS_INIT__NEUTRAL_INV": "INITIATIVE_BUYING",
            "NEG_INIT__NEG_INV": "STRONG_DISTRIBUTION",
            "NEG_INIT__NEUTRAL_INV": "INITIATIVE_SELLING",
            "NEUTRAL_INIT__POS_INV": "PASSIVE_SUPPORT",
            "NEUTRAL_INIT__NEG_INV": "PASSIVE_RESISTANCE",
        }
        dominant_regime = regime_map.get(dominant_cell, "NEUTRAL_CONSOLIDATION")
        
        return TPFM30mRegime(
            regime_id=str(uuid.uuid4()),
            symbol=self.symbol,
            venue=self.venue,
            window_start_ts=snapshots[0].window_start_ts,
            window_end_ts=snapshots[-1].window_end_ts,
            m5_count=m5_count,
            dominant_cell=dominant_cell,
            dominant_regime=dominant_regime,
            transition_path=cells,
            regime_persistence_score=persistence,
            actionability_density=act_density,
            invalidation_pressure_score=inv_pressure,
            conflict_score=avg_conflict,
            agreement_score=avg_agreement,
            tradability_score=avg_tradability,
            novelty_score=total_novelty,
            net_delta_quote=sum(s.delta_quote for s in snapshots),
            avg_trade_burst=sum(s.trade_burst for s in snapshots) / m5_count,
            macro_conclusion_code=f"{dominant_regime} (Persist: {persistence:.2f})",
            macro_posture="WAIT_FOR_REGIME" if persistence < 0.5 or dominant_regime == "NEUTRAL_CONSOLIDATION" else "FOLLOW_REGIME"
        )

    def _soft_clamp(self, x: float) -> float:
        return max(-1.0, min(1.0, x))

    def _empty_snapshot(self, start: int, end: int) -> TPFMSnapshot:
        return TPFMSnapshot(
            snapshot_id=str(uuid.uuid4()),
            symbol=self.symbol,
            window_start_ts=start,
            window_end_ts=end,
            health_state="EMPTY_DATA"
        )

    def _empty_regime(self, start: int, end: int) -> TPFM30mRegime:
        return TPFM30mRegime(
            regime_id=str(uuid.uuid4()),
            symbol=self.symbol,
            window_start_ts=start,
            window_end_ts=end,
            health_state="EMPTY_DATA"
        )
