import json
import math
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import uuid
from collections import Counter, deque

from cfte.models.events import NormalizedTrade, TapeSnapshot
from cfte.tpfm.models import TPFMSnapshot, FlowTransitionEvent, FlowDecisionView, TPFM30mRegime, TPFM4hStructural
from cfte.tpfm.probability import ProbabilityEngine
from cfte.tpfm.ai_explainer import TPFMAIExplainer

_MATRIX_ALIASES_VI = {
    "POS_INIT__POS_INV": {
        "alias": "Thuận pha mua",
        "bias": "LONG_CONTINUATION",
        "posture": "AGGRESSIVE",
        "decision": "Ưu tiên continuation long khi microprice giữ vững và bid tiếp tục dâng cao.",
        "confirm_needed": "Bid dâng kèm delta futures dương",
        "invalidation": "Initiative mất dương hoặc inventory không còn đỡ.",
        "entry_condition": "Microprice retest + Bid replenishment",
        "avoid_if": "Spread giãn > 5bps hoặc Delta futures đảo âm",
    },
    "POS_INIT__NEG_INV": {
        "alias": "Mua gặp hấp thụ bán",
        "bias": "BREAKOUT_TRAP_RISK",
        "posture": "CONSERVATIVE",
        "decision": "Chỉ theo long khi vượt hấp thụ và có xác nhận thêm từ futures/venue lead.",
        "confirm_needed": "Giá phá vỡ microprice + CVD spot dốc lên",
        "invalidation": "Inventory tiếp tục âm hoặc giá không giữ được trên microprice.",
        "entry_condition": "Breakout vùng hấp thụ + Venue confirmation",
        "avoid_if": "OI giảm mạnh trong khi giá tăng",
    },
    "NEG_INIT__POS_INV": {
        "alias": "Bán gặp hấp thụ mua",
        "bias": "SHORT_TRAP_RISK",
        "posture": "CONSERVATIVE",
        "decision": "Ưu tiên chờ failed breakdown hoặc hồi bán lại thay vì đuổi short.",
        "confirm_needed": "Giá thủng sâu bid zone kèm initiative NEG mạnh",
        "invalidation": "Initiative chuyển dương hoặc bid tiếp tục hấp thụ bán.",
        "entry_condition": "Failed breakdown + Passive absorption",
        "avoid_if": "Funding rate quá âm hoặc Basis thu hẹp",
    },
    "NEG_INIT__NEG_INV": {
        "alias": "Thuận pha bán",
        "bias": "SHORT_CONTINUATION",
        "posture": "AGGRESSIVE",
        "decision": "Ưu tiên continuation short khi hồi yếu và ask vẫn ép xuống.",
        "confirm_needed": "Ask đè mạnh kèm futures delta âm",
        "invalidation": "Initiative mất âm hoặc inventory không còn đè.",
        "entry_condition": "Microprice resistance + Ask pressure",
        "avoid_if": "Volume profile cạn kiệt ở đáy hoặc Liquidation short nổ lớn",
    },
    "POS_INIT__NEUTRAL_INV": {
        "alias": "Mua chủ động chưa có đỡ",
        "bias": "INITIATIVE_BUY_ONLY",
        "posture": "WAIT",
        "decision": "Theo dõi breakout nhưng chưa đuổi, cần inventory xác nhận.",
        "confirm_needed": "Inventory flip POS",
        "invalidation": "Dòng tiền mua nguội đi hoặc thất bại giữ breakout.",
        "entry_condition": "Inventory shift sang POS_INV",
        "avoid_if": "CVD spot đi ngang",
    },
    "NEG_INIT__NEUTRAL_INV": {
        "alias": "Bán chủ động chưa có đè",
        "bias": "INITIATIVE_SELL_ONLY",
        "posture": "WAIT",
        "decision": "Theo dõi breakdown nhưng chưa đuổi, cần inventory xác nhận.",
        "confirm_needed": "Inventory flip NEG",
        "invalidation": "Dòng tiền bán nguội đi hoặc giá reclaim lại microprice.",
        "entry_condition": "Inventory shift sang NEG_INV",
        "avoid_if": "CVD futures phân kỳ dương",
    },
    "NEUTRAL_INIT__POS_INV": {
        "alias": "Đỡ thụ động",
        "bias": "PASSIVE_LONG",
        "posture": "CONSERVATIVE",
        "decision": "Canh phản ứng long ở vùng bid mạnh, tránh đuổi khi initiative còn trung tính.",
        "confirm_needed": "Initiative flip POS",
        "invalidation": "Bid không còn hấp thụ hoặc initiative chuyển âm.",
        "entry_condition": "Rejection tại Bid zone + Initiative flip POS",
        "avoid_if": "Market depth phía Ask dày đặc",
    },
    "NEUTRAL_INIT__NEG_INV": {
        "alias": "Đè thụ động",
        "bias": "PASSIVE_SHORT",
        "posture": "CONSERVATIVE",
        "decision": "Canh phản ứng short ở vùng ask mạnh, tránh đuổi khi initiative còn trung tính.",
        "confirm_needed": "Initiative flip NEG",
        "invalidation": "Ask không còn đè hoặc initiative chuyển dương.",
        "entry_condition": "Rejection tại Ask zone + Initiative flip NEG",
        "avoid_if": "OI tăng mạnh kèm giá giữ vững",
    },
}

class TPFMStateEngine:
    def __init__(self, symbol: str = "BTCUSDT", venue: str = "binance"):
        self.symbol = symbol
        self.venue = venue
        self._prev_snapshot: Optional[TPFMSnapshot] = None
        self._rolling_delta = deque(maxlen=200) # For z-score
        self._recent_snapshots = deque(maxlen=12)
        self._current_sequence: Optional['FlowSequenceEvent'] = None
        self._pending_patterns: List[Dict[str, Any]] = []
        
        # Load local module safely inside module or import at top
        from cfte.tpfm.probability import ProbabilityEngine
        from cfte.tpfm.ai_explainer import TPFMAIExplainer
        
        self._probability_engine = ProbabilityEngine()
        self._ai_explainer = TPFMAIExplainer()
        
    def calculate_m5_snapshot(
        self, 
        window_start_ts: int, 
        window_end_ts: int, 
        trades: List[NormalizedTrade],
        snapshots: List[TapeSnapshot], 
        active_theses: List = None,
        futures_context: Dict[str, Any] = None,
        use_ai_brief: bool = False
    ) -> TPFMSnapshot:
        if not trades or not snapshots:
            return self._empty_snapshot(window_start_ts, window_end_ts)

        total_buy_quote = sum(t.quote_qty for t in trades if t.taker_side == "BUY")
        total_sell_quote = sum(t.quote_qty for t in trades if t.taker_side == "SELL")
        total_quote = total_buy_quote + total_sell_quote
        delta_quote = total_buy_quote - total_sell_quote

        window_sec = max((window_end_ts - window_start_ts) / 1000.0, 1.0)
        trade_burst = len(trades) / max(window_sec, 1.0)
        cvd_slope = delta_quote / max(window_sec, 1.0)
        taker_imbalance = delta_quote / max(total_quote, 1.0)
        avg_freshness = self._mean([self._snapshot_meta_float(s, "recent_quote_share", 1.0) for s in snapshots], default=1.0)
        avg_gap = self._mean([self._snapshot_meta_float(s, "gap_seconds", 0.0) for s in snapshots], default=0.0)
        avg_spread_bps = self._mean([float(s.spread_bps) for s in snapshots], default=0.0)
        avg_burst_persistence = self._mean(
            [self._snapshot_meta_float(s, "burst_persistence", 0.0) for s in snapshots],
            default=0.0,
        )
        avg_microprice_drift_bps = self._mean(
            [self._snapshot_meta_float(s, "microprice_drift_bps", 0.0) for s in snapshots],
            default=0.0,
        )

        z_delta = self._soft_clamp(delta_quote / 50000.0)
        
        # vNext: Rolling Z-Score
        self._rolling_delta.append(delta_quote)
        d_mean = sum(self._rolling_delta) / len(self._rolling_delta)
        d_var = sum((x - d_mean)**2 for x in self._rolling_delta) / len(self._rolling_delta)
        d_std = math.sqrt(d_var) if d_var > 0 else 1.0
        delta_zscore = (delta_quote - d_mean) / d_std
        
        # vNext: Aggression Ratio
        aggression_ratio = total_buy_quote / max(total_quote, 1.0)
        
        z_cvd = self._soft_clamp(cvd_slope / 100.0)
        z_burst = self._soft_clamp((trade_burst - 2.0) / 5.0)
        z_persistence = self._soft_clamp((avg_burst_persistence - 0.45) * 2.0)
        z_taker = taker_imbalance
        initiative_score = (0.35 * z_delta + 0.25 * z_cvd + 0.15 * z_burst + 0.10 * z_taker + 0.15 * z_persistence)
        initiative_score = self._soft_clamp(initiative_score)
        
        sweep_quote = sum(self._snapshot_meta_float(s, "sweep_quote", 0.0) for s in snapshots)
        sweep_buy_quote = sum(self._snapshot_meta_float(s, "sweep_buy_quote", 0.0) for s in snapshots)
        sweep_sell_quote = sum(self._snapshot_meta_float(s, "sweep_sell_quote", 0.0) for s in snapshots)
        initiative_polarity = self._polarity_label(
            initiative_score,
            positive_threshold=0.35,
            negative_threshold=-0.35,
            positive_label="POS_INIT",
            negative_label="NEG_INIT",
            neutral_label="NEUTRAL_INIT",
        )

        avg_imb_l1 = sum(s.imbalance_l1 for s in snapshots) / len(snapshots)
        avg_absorption = sum(s.absorption_proxy for s in snapshots) / len(snapshots)
        last_s = snapshots[-1]
        spread = max(last_s.mid_px * last_s.spread_bps / 10000.0, 0.0001)
        centered_imbalance = self._center_imbalance(avg_imb_l1)
        last_trade_px = self._snapshot_attr_float(last_s, "last_trade_px", float(last_s.mid_px))
        micro_gap_bps = ((last_trade_px - last_s.microprice) / max(last_s.mid_px, 0.0001)) * 10000.0
        micro_pos = self._soft_clamp(micro_gap_bps / max(last_s.spread_bps, 1.0))
        z_abs = self._soft_clamp((avg_absorption - 5000.0) / 10000.0)
        
        # vNext: Replenishment integration
        bid_replen = sum(self._snapshot_meta_float(s, "replenishment_bid_score", 0.0) for s in snapshots)
        ask_replen = sum(self._snapshot_meta_float(s, "replenishment_ask_score", 0.0) for s in snapshots)
        z_replen = self._soft_clamp((bid_replen - ask_replen) / 20000.0)
        
        inventory_side = self._resolve_inventory_side(centered_imbalance, micro_gap_bps)
        signed_absorption = z_abs * inventory_side
        z_micro_drift = self._soft_clamp(avg_microprice_drift_bps / 8.0)
        inventory_score = (0.30 * centered_imbalance + 0.20 * signed_absorption + 0.20 * micro_pos + 0.20 * z_replen + 0.10 * z_micro_drift)
        inventory_score = self._soft_clamp(inventory_score)
        inventory_polarity = self._polarity_label(
            inventory_score,
            positive_threshold=0.30,
            negative_threshold=-0.30,
            positive_label="POS_INV",
            negative_label="NEG_INV",
            neutral_label="NEUTRAL_INV",
        )

        prices = [t.price for t in trades]
        high, low = max(prices), min(prices)
        range_bps = ((high - low) / low) * 10000.0
        z_range = self._soft_clamp(range_bps / 50.0)
        energy_score = (0.35 * z_burst + 0.25 * z_range + 0.20 * z_delta + 0.20 * z_persistence)
        
        if energy_score < 0.20: energy_state = "COMPRESSION"
        elif energy_score > 0.75: energy_state = "EXHAUSTING"
        else: energy_state = "EXPANDING"

        start_px = snapshots[0].mid_px
        end_px = snapshots[-1].mid_px
        ret_bps = ((end_px - start_px) / start_px) * 10000.0
        response_efficiency = ret_bps / max(abs(z_delta * 10.0), 0.25)
        if response_efficiency > 0.40: response_efficiency_state = "FOLLOW_THROUGH"
        elif response_efficiency < -0.20: response_efficiency_state = "ABSORBED_OR_TRAP"
        else: response_efficiency_state = "MIXED"

        matrix_cell = f"{initiative_polarity}__{inventory_polarity}"
        agreement_score = 0.0
        conflict_score = 0.0
        if initiative_polarity != "NEUTRAL_INIT" and inventory_polarity != "NEUTRAL_INV":
            if (initiative_score > 0 and inventory_score > 0) or (initiative_score < 0 and inventory_score < 0):
                agreement_score = 1.0
            else:
                conflict_score = 1.0

        micro_conclusion = "UNCERTAIN"
        if agreement_score == 1.0:
            micro_conclusion = "BULLISH_CONFLUENCE" if initiative_score > 0 else "BEARISH_CONFLUENCE"
        elif conflict_score == 1.0:
            micro_conclusion = "ABSORPTION_IN_PROGRESS"
        elif initiative_polarity != "NEUTRAL_INIT" and inventory_polarity == "NEUTRAL_INV":
            micro_conclusion = "INITIATIVE_DRIVEN"
        elif initiative_polarity == "NEUTRAL_INIT" and inventory_polarity != "NEUTRAL_INV":
            micro_conclusion = "INVENTORY_DRIVEN"

        axis_confidence = self._build_axis_confidence(
            avg_freshness=avg_freshness,
            avg_gap=avg_gap,
            trade_count=len(trades),
            avg_spread_bps=avg_spread_bps,
        )
        tradability_score = self._soft_clamp(
            0.35 * abs(initiative_score)
            + 0.35 * abs(inventory_score)
            + 0.15 * axis_confidence
            + 0.10 * max(response_efficiency, 0.0)
            + 0.05 * avg_burst_persistence
        )
        market_quality_score = self._soft_clamp(
            0.30 * agreement_score
            + 0.25 * axis_confidence
            + 0.20 * (1.0 - min(1.0, abs(energy_score - 0.5)))
            + 0.15 * max(0.0, 1.0 - avg_spread_bps / 12.0)
            + 0.05 * max(0.0, 1.0 - avg_gap / 2.0)
            + 0.05 * avg_burst_persistence
        )

        novelty_score = 0.0
        escalation_reason = []
        if self._prev_snapshot:
            if self._prev_snapshot.matrix_cell != matrix_cell:
                novelty_score += 0.7
                escalation_reason.append(f"Shift: {self._prev_snapshot.matrix_cell} -> {matrix_cell}")
            if self._prev_snapshot.inventory_polarity != inventory_polarity:
                novelty_score += 0.2
                escalation_reason.append(f"Inventory Flip: {inventory_polarity}")

        active_records = active_theses or []
        active_signals = [item.signal for item in active_records if hasattr(item, "signal")]
        actionable_signals = [signal for signal in active_signals if signal.stage == "ACTIONABLE"]
        if actionable_signals:
                novelty_score += 0.3
                escalation_reason.append(f"Active Signals: {len(actionable_signals)} actionable")

        should_escalate = novelty_score >= 0.65 or energy_state == "EXHAUSTING"
        if energy_state == "EXHAUSTING":
            escalation_reason.append("Energy Exhaustion detected")

        m_contract = self._matrix_contract(matrix_cell)
        matrix_alias_vi = m_contract["alias"]
        continuation_bias = m_contract["bias"]
        preferred_posture = m_contract["decision"]
        invalid_if = m_contract["invalidation"]
        
        setup_score_map = self._build_setup_score_map(active_records)
        dominant_setups = [setup for setup, _ in sorted(setup_score_map.items(), key=lambda item: item[1], reverse=True)[:3]]
        
        snapshot = TPFMSnapshot(
            snapshot_id=str(uuid.uuid4()),
            symbol=self.symbol,
            venue=self.venue,
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            microprice=snapshots[-1].microprice,
            initiative_score=initiative_score,
            initiative_polarity=initiative_polarity,
            initiative_strength=abs(initiative_score),
            inventory_score=inventory_score,
            inventory_polarity=inventory_polarity,
            inventory_strength=abs(inventory_score),
            axis_confidence=axis_confidence,
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
            matrix_alias_vi=matrix_alias_vi,
            continuation_bias=continuation_bias,
            preferred_posture=preferred_posture,
            delta_quote=delta_quote,
            cvd_slope=cvd_slope,
            trade_burst=trade_burst,
            absorption_score=avg_absorption,
            imbalance_l1=avg_imb_l1,
            centered_imbalance_l1=centered_imbalance,
            signed_absorption_score=signed_absorption,
            microprice_gap_bps=micro_gap_bps,
            spread_bps=avg_spread_bps,
            delta_zscore=delta_zscore,
            aggression_ratio=aggression_ratio,
            sweep_quote=sweep_quote,
            sweep_buy_quote=sweep_buy_quote,
            sweep_sell_quote=sweep_sell_quote,
            burst_persistence=avg_burst_persistence,
            microprice_drift_bps=avg_microprice_drift_bps,
            replenishment_bid_score=bid_replen,
            replenishment_ask_score=ask_replen,
            novelty_score=novelty_score,
            dominant_setups=dominant_setups,
            setup_score_map=setup_score_map,
            active_thesis_count=len([signal for signal in active_signals if signal.stage in {"DETECTED", "WATCHLIST", "CONFIRMED", "ACTIONABLE"}]),
            new_thesis_count=len([item for item in active_records if hasattr(item, "opened_ts") and item.opened_ts >= window_start_ts]),
            actionable_count=len(actionable_signals),
            invalidated_count=len([signal for signal in active_signals if signal.stage == "INVALIDATED"]),
            resolved_count=len([signal for signal in active_signals if signal.stage == "RESOLVED"]),
            should_escalate=should_escalate,
            escalation_reason=escalation_reason,
            invalid_if=invalid_if,
            action_plan_vi=m_contract["decision"],
        )
        
        # vNext: Populate Decision View components
        snapshot.decision_summary_vi = m_contract["decision"]
        snapshot.entry_condition_vi = m_contract["entry_condition"]
        snapshot.confirm_needed_vi = m_contract.get("confirm_needed", "N/A")
        snapshot.avoid_if_vi = m_contract["avoid_if"]
        snapshot.entry_condition = m_contract["entry_condition"]
        snapshot.avoid_if = m_contract["avoid_if"]
        snapshot.decision_posture = m_contract["posture"]

        self._apply_input_quality_flags(snapshot, avg_freshness=avg_freshness, avg_gap=avg_gap, trade_count=len(trades))
        # Phase 2: Sequence & MTF Intelligence
        self._apply_context_overlay(snapshot, futures_context)
        self._calc_forced_flow(snapshot, futures_context)
        
        # 1. Sequence Tracking (Pivot, Duration)
        closed_sequence = self._derive_sequence_tracking(snapshot)
        
        # 2. MTF Integration (M30/H4)
        self._derive_multi_timeframe_context(snapshot)
        
        # 3. Outcome Prediction (Skeleton)
        self._estimate_outcome_targets(snapshot)
        
        # 4. Phase 3: Probability & Expectancy Engine
        self._derive_probability_edge(snapshot)
        
        # 5. Phase 11: Agent Output Contract
        if use_ai_brief:
            self._derive_ai_brief(snapshot)
        
        # Phase 1: Matrix-Native Pattern Engine
        self._derive_temporal_memory(snapshot)
        snapshot.tempo_state = self._classify_tempo_state(snapshot)
        snapshot.persistence_state = self._classify_persistence_state(snapshot)
        
        self._derive_decision_view(snapshot, context=futures_context)
        snapshot.trap_risk = self._estimate_trap_risk(snapshot, previous=self._prev_snapshot)
        
        snapshot.exhaustion_risk = self._estimate_exhaustion_risk(snapshot, self._recent_snapshots)
        self._derive_matrix_native_pattern(snapshot)
        snapshot.sequence_signature = self._build_sequence_signature(snapshot)
        self._derive_pattern_phase(snapshot)
        snapshot.pattern_strength = self._estimate_pattern_strength(snapshot)
        snapshot.pattern_quality = self._estimate_pattern_quality(snapshot)
        snapshot.pattern_failure_risk = self._estimate_pattern_failure_risk(snapshot)
        
        pattern_event = self._build_flow_pattern_event(snapshot)
        snapshot.metadata["pattern_event"] = pattern_event
        
        self._detect_transitions(snapshot)
        self._finalize_output_contract(snapshot)
        self._recent_snapshots.append(snapshot)
        self._prev_snapshot = snapshot
        
        # If we need to return the sequence, we either return it as a third element or attach it to metadata
        if closed_sequence:
            snapshot.metadata["closed_sequence"] = closed_sequence

        # Phase 14: Pattern Outcome Tracking
        snapshot.metadata["pattern_outcomes"] = self._update_pattern_outcomes(snapshot, trades)

        # Phase D: Probability / Expectancy Engine (Finding 3 Uplift)
        snapshot.edge_profile = self._probability_engine.evaluate_edge(
            matrix_cell=snapshot.matrix_cell,
            sequence_length=snapshot.sequence_length,
            pattern_code=snapshot.metadata.get("pattern_event").pattern_code if snapshot.metadata.get("pattern_event") else None,
            sequence_signature=snapshot.sequence_signature
        )

        # snapshot.transition_event is already populated by _detect_transitions if a shift occurred
        return snapshot

    def _apply_context_overlay(
        self, 
        snapshot: TPFMSnapshot, 
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Refines TPFM with futures, liquidation, and cross-venue context."""
        ctx = self._normalize_context(context)
        if not ctx.get("available", False):
            snapshot.futures_context_available = False
            snapshot.context_warning_flags.append("THIẾU CONTEXT FUTURES")
            self._append_unique(snapshot.blind_spot_flags, "NO_FUTURES_CONTEXT")
            return

        snapshot.futures_context_available = True
        snapshot.futures_context_fresh = ctx.get("fresh", False) and not ctx.get("is_stale", False)
        snapshot.futures_delta_available = bool(ctx.get("futures_delta_available", "futures_delta" in ctx))
        snapshot.liquidation_context_available = bool(ctx.get("liquidation_context_available", False))
        snapshot.venue_confirmation_state = str(ctx.get("venue_confirmation_state", snapshot.venue_confirmation_state))
        
        # Latency tracking in snapshot metadata if available
        if "ws_latency_ms" in ctx:
            snapshot.metadata["futures_ws_latency_ms"] = ctx["ws_latency_ms"]
        if ctx.get("is_stale"):
            snapshot.context_warning_flags.append("FUTURES FEED BỊ TẮC (STALE)")
            self._append_unique(snapshot.blind_spot_flags, "STALE_WS_FEED")

        snapshot.leader_venue = str(ctx.get("leader_venue", snapshot.leader_venue))
        snapshot.lagger_venue = str(ctx.get("lagger_venue", snapshot.lagger_venue))
        snapshot.venue_vwap_spread_bps = float(ctx.get("venue_vwap_spread_bps", snapshot.venue_vwap_spread_bps))
        snapshot.leader_confidence = float(ctx.get("leader_confidence", ctx.get("lead_score", 0.0)))
        snapshot.aligned_window_ms = int(ctx.get("aligned_window_ms", 0) or 0)

        spot_delta = snapshot.delta_quote
        snapshot.futures_delta = float(ctx.get("futures_delta", 0.0))
        snapshot.futures_aggression_ratio = float(ctx.get("futures_aggression_ratio", snapshot.futures_aggression_ratio))
        snapshot.liquidation_quote = float(ctx.get("liquidation_quote", ctx.get("liquidation_vol", 0.0)))
        snapshot.liquidation_count = int(ctx.get("liquidation_count", 0))
        snapshot.liquidation_bias = str(ctx.get("liquidation_bias", "UNKNOWN"))
        snapshot.liquidation_intensity = float(ctx.get("liquidation_intensity", snapshot.liquidation_intensity))
        snapshot.oi_delta = float(ctx.get("oi_delta", 0.0))
        snapshot.oi_expansion_ratio = float(ctx.get("oi_expansion_ratio", 0.0))
        snapshot.basis_state = str(ctx.get("basis_state", snapshot.basis_state))
        
        # vNext: Calc context score (confluence vs divergence)
        c_score = 0.0
        if snapshot.futures_delta * snapshot.delta_quote > 0:
            c_score = 1.0
        elif snapshot.futures_delta * snapshot.delta_quote < 0:
            c_score = -1.0
        snapshot.context_score = c_score

        # Test compatibility: futures_bias_proxy
        if abs(snapshot.futures_delta) > abs(snapshot.delta_quote) * 1.5:
            snapshot.futures_bias_proxy = "FUTURES_LED"
        else:
            snapshot.futures_bias_proxy = "SPOT_LED"

        # Liquidation Intensity
        if snapshot.liquidation_context_available and snapshot.liquidation_quote > 0 and snapshot.liquidation_intensity <= 0:
            snapshot.liquidation_intensity = snapshot.liquidation_quote / max(1.0, abs(snapshot.delta_quote))

    def _calc_forced_flow(self, snapshot: TPFMSnapshot, context: Optional[Dict[str, Any]]) -> None:
        """Identifies if the current flow is 'forced' (liqs, squeezes, etc)"""
        if not snapshot.liquidation_context_available:
            snapshot.forced_flow_state = "NONE"
            snapshot.forced_flow_intensity = 0.0
            return
            
        if snapshot.liquidation_intensity >= 0.75 or snapshot.liquidation_quote > 50_000:
            snapshot.forced_flow_state = "LIQUIDATION_LED"
            if snapshot.liquidation_bias == "SHORTS_FLUSHED" and snapshot.delta_quote > 0:
                snapshot.forced_flow_state = "SQUEEZE_LED"
        elif snapshot.basis_state in {"OVERHEATED_PREMIUM", "DEEP_DISCOUNT"} or abs(snapshot.basis_bps) > 10.0:
            snapshot.forced_flow_state = "GAP_LED"
        else:
            snapshot.forced_flow_state = "NONE"
        snapshot.forced_flow_intensity = round(
            max(
                snapshot.liquidation_intensity,
                abs(snapshot.basis_bps) / 10.0 if snapshot.forced_flow_state == "GAP_LED" else 0.0,
                abs(snapshot.oi_expansion_ratio) * 8.0 if snapshot.forced_flow_state != "NONE" else 0.0,
            ),
            2,
        )

    def _detect_transitions(self, snapshot: TPFMSnapshot) -> None:
        """Analyzes changes between the current and previous snapshot"""
        previous = self._prev_snapshot
        if previous is None:
            return

        if not self._has_meaningful_transition(previous, snapshot):
            return

        transition_family = self._classify_transition_family(previous, snapshot)
        transition_code = f"{transition_family}_TO_{self._transition_target_code(snapshot)}"
        transition_alias_vi = self._build_transition_alias_vi(transition_family, snapshot)
        transition_speed = self._estimate_transition_speed(previous, snapshot)
        persistence_score = self._estimate_transition_persistence(snapshot)
        transition_quality = self._estimate_transition_quality(
            snapshot,
            transition_family=transition_family,
            transition_speed=transition_speed,
            persistence_score=persistence_score,
        )
        decision_shift = self._decision_shift_label(previous, snapshot)

        snapshot.transition_ready = True
        event = FlowTransitionEvent(
            transition_id=str(uuid.uuid4()),
            symbol=snapshot.symbol,
            venue=snapshot.venue,
            timestamp=snapshot.window_end_ts,
            from_cell=previous.matrix_cell,
            to_cell=snapshot.matrix_cell,
            transition_code=transition_code,
            transition_speed=transition_speed,
            transition_quality=transition_quality,
            persistence_score=persistence_score,
            transition_family=transition_family,
            transition_alias_vi=transition_alias_vi,
            from_flow_state_code=previous.flow_state_code,
            to_flow_state_code=snapshot.flow_state_code,
            forced_flow_involved=snapshot.forced_flow_state != "NONE",
            trap_risk=snapshot.trap_risk,
            from_decision_posture=previous.decision_posture,
            to_decision_posture=snapshot.decision_posture,
            decision_shift=decision_shift,
            metadata={
                "vNext": True,
                "from_flow_state_code": previous.flow_state_code,
                "to_flow_state_code": snapshot.flow_state_code,
                "from_grade": previous.tradability_grade,
                "to_grade": snapshot.tradability_grade,
                "from_spot_futures_relation": previous.spot_futures_relation,
                "to_spot_futures_relation": snapshot.spot_futures_relation,
            }
        )
        snapshot.transition_event = event
        snapshot.metadata["last_transition"] = asdict(event)

    def _has_meaningful_transition(self, previous: TPFMSnapshot, current: TPFMSnapshot) -> bool:
        return any(
            (
                previous.matrix_cell != current.matrix_cell,
                previous.flow_state_code != current.flow_state_code,
                previous.decision_posture != current.decision_posture,
                previous.forced_flow_state != current.forced_flow_state,
                abs(previous.trap_risk - current.trap_risk) >= 0.25,
            )
        )

    def _flow_bias_label(self, snapshot: TPFMSnapshot) -> str:
        if snapshot.initiative_score >= 0.15 or "LONG" in snapshot.flow_state_code:
            return "LONG"
        if snapshot.initiative_score <= -0.15 or "SHORT" in snapshot.flow_state_code:
            return "SHORT"
        return "NEUTRAL"

    def _transition_target_code(self, snapshot: TPFMSnapshot) -> str:
        flow_state = snapshot.flow_state_code.split("__", 1)[0]
        if "LONG" in flow_state or self._flow_bias_label(snapshot) == "LONG":
            return "LONG"
        if "SHORT" in flow_state or self._flow_bias_label(snapshot) == "SHORT":
            return "SHORT"
        return "BALANCE"

    def _classify_transition_family(self, previous: TPFMSnapshot, current: TPFMSnapshot) -> str:
        prev_bias = self._flow_bias_label(previous)
        curr_bias = self._flow_bias_label(current)

        if current.forced_flow_state != "NONE" and previous.forced_flow_state != current.forced_flow_state:
            return "FORCED"
        if current.trap_risk >= 0.65 or current.response_efficiency_state == "ABSORBED_OR_TRAP":
            if prev_bias != curr_bias and curr_bias != "NEUTRAL":
                return "TRAP_FLIP"
            return "TRAP"
        if prev_bias not in {"NEUTRAL", curr_bias} and curr_bias != "NEUTRAL":
            return "FLIP"
        if previous.inventory_polarity != current.inventory_polarity and previous.initiative_polarity == current.initiative_polarity:
            return "INVENTORY_CONFIRM"
        if previous.matrix_cell == current.matrix_cell and previous.flow_state_code != current.flow_state_code:
            return "REPRICE"
        if prev_bias == curr_bias and curr_bias != "NEUTRAL":
            return "CONTINUATION"
        if curr_bias == "NEUTRAL":
            return "REBALANCE"
        return "STRUCTURE_SHIFT"

    def _build_transition_alias_vi(self, transition_family: str, snapshot: TPFMSnapshot) -> str:
        target = self._transition_target_code(snapshot)
        alias_map = {
            ("FORCED", "LONG"): "Dòng tiền cưỡng bức đẩy sang mua",
            ("FORCED", "SHORT"): "Dòng tiền cưỡng bức đẩy sang bán",
            ("TRAP_FLIP", "LONG"): "Lật sang mua nhưng mang tính bẫy",
            ("TRAP_FLIP", "SHORT"): "Lật sang bán nhưng mang tính bẫy",
            ("TRAP", "LONG"): "Pha mua có dấu hiệu bẫy",
            ("TRAP", "SHORT"): "Pha bán có dấu hiệu bẫy",
            ("FLIP", "LONG"): "Đảo chiều sang mua",
            ("FLIP", "SHORT"): "Đảo chiều sang bán",
            ("CONTINUATION", "LONG"): "Tiếp diễn mua",
            ("CONTINUATION", "SHORT"): "Tiếp diễn bán",
            ("INVENTORY_CONFIRM", "LONG"): "Inventory xác nhận phe mua",
            ("INVENTORY_CONFIRM", "SHORT"): "Inventory xác nhận phe bán",
            ("REPRICE", "LONG"): "Tái định giá theo hướng mua",
            ("REPRICE", "SHORT"): "Tái định giá theo hướng bán",
            ("REBALANCE", "BALANCE"): "Quay về cân bằng",
        }
        return alias_map.get((transition_family, target), "Chuyển pha cấu trúc")

    def _estimate_transition_speed(self, previous: TPFMSnapshot, current: TPFMSnapshot) -> float:
        dt_ms = max(1, current.window_end_ts - previous.window_end_ts)
        normalized_dt = max(dt_ms / 300000.0, 0.25)
        raw = (
            0.40 * abs(current.initiative_score - previous.initiative_score)
            + 0.30 * abs(current.inventory_score - previous.inventory_score)
            + 0.15 * abs(current.energy_score - previous.energy_score)
            + 0.15 * abs(current.context_score - previous.context_score)
        ) / normalized_dt
        return round(max(0.0, min(1.0, raw)), 2)

    def _estimate_transition_persistence(self, snapshot: TPFMSnapshot) -> float:
        history = list(self._recent_snapshots) + [snapshot]
        if not history:
            return 0.0
        target = self._transition_target_code(snapshot)
        trailing = 0
        for item in reversed(history):
            if self._transition_target_code(item) != target:
                break
            trailing += 1
        persistence = min(1.0, trailing / 4.0)
        if snapshot.response_efficiency_state == "FOLLOW_THROUGH":
            persistence = min(1.0, persistence + 0.1)
        return round(persistence, 2)

    def _estimate_transition_quality(
        self,
        snapshot: TPFMSnapshot,
        *,
        transition_family: str,
        transition_speed: float,
        persistence_score: float,
    ) -> float:
        family_bonus = {
            "CONTINUATION": 0.08,
            "INVENTORY_CONFIRM": 0.06,
            "FORCED": 0.04,
            "REPRICE": 0.02,
            "FLIP": 0.0,
            "REBALANCE": -0.03,
            "TRAP": -0.10,
            "TRAP_FLIP": -0.12,
        }.get(transition_family, 0.0)
        quality = (
            0.30 * snapshot.market_quality_score
            + 0.25 * snapshot.tradability_score
            + 0.15 * snapshot.axis_confidence
            + 0.15 * transition_speed
            + 0.15 * persistence_score
            + family_bonus
            - 0.15 * snapshot.trap_risk
        )
        return round(max(0.0, min(1.0, quality)), 2)

    def _decision_shift_label(self, previous: TPFMSnapshot, current: TPFMSnapshot) -> str:
        if previous.decision_posture == current.decision_posture:
            return f"HOLD_{current.decision_posture}"
        return f"{previous.decision_posture}_TO_{current.decision_posture}"

    def _estimate_trap_risk(self, snapshot: TPFMSnapshot, previous: Optional[TPFMSnapshot]) -> float:
        risk = 0.0
        if snapshot.response_efficiency_state == "ABSORBED_OR_TRAP":
            risk += 0.30
        if snapshot.conflict_score > snapshot.agreement_score:
            risk += min(0.20, 0.10 * snapshot.conflict_score)
        if snapshot.spot_futures_relation == "DIVERGENT":
            risk += 0.15
        if snapshot.venue_confirmation_state == "DIVERGENT":
            risk += 0.10
        if snapshot.sweep_quote > 0 and snapshot.burst_persistence < 0.30:
            risk += 0.15
        if snapshot.forced_flow_state != "NONE" and snapshot.response_efficiency_state != "FOLLOW_THROUGH":
            risk += 0.10
        if previous is not None and previous.matrix_cell != snapshot.matrix_cell and snapshot.agreement_score < previous.agreement_score:
            risk += 0.10
        return round(max(0.0, min(1.0, risk)), 2)

    def _derive_decision_view(self, snapshot: TPFMSnapshot, context: Optional[Dict[str, Any]] = None) -> None:
        """Maps flow state to a clear trader decision posture"""
        ctx = self._normalize_context(context)
        snapshot.decision_summary_vi = snapshot.preferred_posture or snapshot.decision_summary_vi
            
        snapshot.oi_value = float(ctx.get("oi_value", 0.0))
        basis_bps = float(ctx.get("basis_bps", 0.0))
        snapshot.basis_bps = basis_bps
        snapshot.funding_rate = float(ctx.get("funding_rate", 0.0))

        if not snapshot.futures_context_fresh:
            snapshot.context_warning_flags.append("CONTEXT FUTURES CHƯA TƯƠI")
            self._append_unique(snapshot.blind_spot_flags, "STALE_FUTURES_CONTEXT")
            snapshot.tradability_score *= 0.9

        if snapshot.futures_delta_available:
            f_delta = snapshot.futures_delta
            s_delta = snapshot.delta_quote
            oi_d = snapshot.oi_delta
            futures_aggr_skew = (snapshot.futures_aggression_ratio - 0.5) * 2.0

            if f_delta > 0:
                snapshot.futures_pressure_bias = "BULLISH"
            elif f_delta < 0:
                snapshot.futures_pressure_bias = "BEARISH"
                
            if (s_delta > 0 and f_delta > 0) or (s_delta < 0 and f_delta < 0):
                snapshot.context_score = 1.0
                snapshot.spot_futures_relation = "CONFLUENT"
                snapshot.agreement_score += 0.2
                snapshot.tradability_score *= 1.2
                snapshot.escalation_reason.append("Futures Xác Nhận (Confluence)")
            elif (s_delta > 0 and f_delta < 0) or (s_delta < 0 and f_delta > 0):
                snapshot.context_score = -1.0
                snapshot.spot_futures_relation = "DIVERGENT"
                snapshot.conflict_score += 0.3
                snapshot.tradability_score *= 0.8
                snapshot.context_warning_flags.append("FUTURES LỆCH NHỊP (Divergence)")
            else:
                snapshot.spot_futures_relation = "FLAT_FUTURES"

            if abs(s_delta) > abs(f_delta) * 1.5:
                snapshot.futures_bias_proxy = "SPOT_LED"
                snapshot.spot_futures_relation = "SPOT_LED"
            elif abs(f_delta) > abs(s_delta) * 1.5:
                snapshot.futures_bias_proxy = "FUTURES_LED"
                snapshot.spot_futures_relation = "FUTURES_LED"
                if oi_d > 0:
                    snapshot.micro_conclusion = "OI_DRIVEN_MOVE"
                    snapshot.escalation_reason.append("OI Mở Rộng Ủng Hộ")

            if snapshot.context_score == -1.0 and abs(f_delta) >= abs(s_delta) * 1.5:
                snapshot.micro_conclusion = "ABSORBED_BY_FUTURES"

            if snapshot.futures_pressure_bias == "BULLISH" and futures_aggr_skew >= 0.2:
                snapshot.agreement_score += 0.05
            elif snapshot.futures_pressure_bias == "BEARISH" and futures_aggr_skew <= -0.2:
                snapshot.agreement_score += 0.05
        else:
            snapshot.spot_futures_relation = "NO_FUTURES_DELTA"
            snapshot.context_warning_flags.append("THIẾU FUTURES DELTA")
            self._append_unique(snapshot.blind_spot_flags, "NO_FUTURES_DELTA")

        if snapshot.liquidation_quote >= 50_000 or snapshot.liquidation_intensity >= 0.75:
            snapshot.escalation_reason.append(f"Thanh lý lớn: {self._fmt_quote(snapshot.liquidation_quote)}")
            if snapshot.liquidation_bias == "LONGS_FLUSHED":
                snapshot.micro_conclusion = "LONG_FLUSH_DETECTED"
                if snapshot.delta_quote < 0:
                    snapshot.tradability_score *= 1.05
                else:
                    snapshot.context_warning_flags.append("LONG FLUSH ĐI NGƯỢC SPOT DELTA")
            elif snapshot.liquidation_bias == "SHORTS_FLUSHED":
                snapshot.micro_conclusion = "SHORT_SQUEEZE_DETECTED"
                if snapshot.delta_quote > 0:
                    snapshot.tradability_score *= 1.05
                else:
                    snapshot.context_warning_flags.append("SHORT SQUEEZE ĐI NGƯỢC SPOT DELTA")

        if not snapshot.liquidation_context_available:
            self._append_unique(snapshot.blind_spot_flags, "NO_LIQUIDATION_FEED")
        if snapshot.venue_confirmation_state == "CONFIRMED":
            snapshot.tradability_score *= 1.05 + (0.05 * min(1.0, snapshot.leader_confidence))
            snapshot.agreement_score += 0.1
        elif snapshot.venue_confirmation_state == "ALT_LEAD":
            snapshot.tradability_score *= 0.95
            snapshot.context_warning_flags.append("BINANCE KHÔNG DẪN NHỊP LIÊN SÀN")
        elif snapshot.venue_confirmation_state == "DIVERGENT":
            snapshot.tradability_score *= 0.85
            snapshot.conflict_score += 0.2
            snapshot.context_warning_flags.append("ĐA SÀN PHÂN KỲ VWAP")
        else:
            self._append_unique(snapshot.blind_spot_flags, "NO_VENUE_CONFIRMATION")
        if snapshot.venue_confirmation_state != "UNCONFIRMED" and snapshot.leader_confidence < 0.55:
            snapshot.context_warning_flags.append("LEADER LIÊN SÀN CHƯA ĐỦ CHẮC")
        if snapshot.aligned_window_ms and snapshot.aligned_window_ms < 500:
            snapshot.context_warning_flags.append("WINDOW ĐA SÀN GỐI NHAU QUÁ NGẮN")

        if snapshot.oi_expansion_ratio > 0.003 or snapshot.oi_delta > 0:
            snapshot.oi_state = "EXPANDING"
        elif snapshot.oi_expansion_ratio < -0.003 or snapshot.oi_delta < 0:
            snapshot.oi_state = "CONTRACTING"

        if snapshot.basis_state in {"OVERHEATED_PREMIUM", "DEEP_DISCOUNT"} or abs(basis_bps) > 10.0:
            snapshot.basis_divergence_state = "DIVERGING_POS" if basis_bps > 0 else "DIVERGING_NEG"
            snapshot.context_warning_flags.append("BASIS PHÂN KỲ")
        elif snapshot.basis_state in {"PREMIUM", "DISCOUNT"}:
            snapshot.basis_divergence_state = "DIVERGING_POS" if basis_bps > 0 else "DIVERGING_NEG"
        if abs(snapshot.funding_rate) > 0.0005:
            snapshot.funding_bias = "POSITIVE" if snapshot.funding_rate > 0 else "NEGATIVE"
            snapshot.context_warning_flags.append("FUNDING QUÁ NÓNG")
        snapshot.context_quality_score = min(
            1.0,
            (0.35 if snapshot.futures_context_fresh else 0.05)
            + (0.25 if snapshot.futures_delta_available and not ctx.get("is_stale") else 0.0)
            + (0.10 if snapshot.venue_confirmation_state != "UNCONFIRMED" else 0.0)
            + (0.10 * min(1.0, snapshot.leader_confidence))
            + (0.10 if snapshot.liquidation_context_available else 0.0)
            + (0.10 if snapshot.oi_state != "STABLE" else 0.0),
        )
        snapshot.tradability_score = self._soft_clamp(snapshot.tradability_score)
    
    def _normalize_context(self, context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        ctx = dict(context or {})
        ctx.setdefault("available", False)
        ctx.setdefault("fresh", False)
        ctx.setdefault("is_stale", not bool(ctx.get("fresh", False)))
        ctx.setdefault("futures_delta_available", "futures_delta" in ctx)
        ctx.setdefault("liquidation_context_available", False)
        ctx.setdefault("venue_confirmation_state", "UNCONFIRMED")
        ctx.setdefault("leader_venue", "")
        ctx.setdefault("lagger_venue", "")
        ctx.setdefault("venue_vwap_spread_bps", 0.0)
        ctx.setdefault("leader_confidence", ctx.get("lead_score", 0.0))
        ctx.setdefault("aligned_window_ms", 0)
        ctx.setdefault("futures_aggression_ratio", 0.5)
        ctx.setdefault("oi_expansion_ratio", 0.0)
        ctx.setdefault("basis_state", "BALANCED")
        ctx.setdefault("liquidation_intensity", 0.0)
        return ctx

    # ==========================================
    # Phase A: Temporal Intelligence (History)
    # ==========================================
    


    # ==========================================
    # Phase 1: Matrix-Native Pattern Engine
    # ==========================================
    
    def _derive_temporal_memory(self, snapshot: TPFMSnapshot) -> None:
        """Calculates temporal deltas compared to history."""
        hist = list(self._recent_snapshots)
        depth = len(hist)
        snapshot.history_depth = depth
        if depth == 0:
            return
            
        def _avg(attr: str, n: int) -> float:
            vals = [getattr(s, attr) for s in hist[-n:]]
            return sum(vals) / len(vals)
            
        snapshot.initiative_delta_1 = snapshot.initiative_score - hist[-1].initiative_score
        snapshot.inventory_delta_1 = snapshot.inventory_score - hist[-1].inventory_score
        
        n3 = min(3, depth)
        snapshot.initiative_delta_3 = snapshot.initiative_score - _avg("initiative_score", n3)
        snapshot.inventory_delta_3 = snapshot.inventory_score - _avg("inventory_score", n3)
        snapshot.agreement_delta_3 = snapshot.agreement_score - _avg("agreement_score", n3)
        snapshot.tradability_delta_3 = snapshot.tradability_score - _avg("tradability_score", n3)
        
        n5 = min(5, depth)
        snapshot.initiative_delta_5 = snapshot.initiative_score - _avg("initiative_score", n5)
        snapshot.inventory_delta_5 = snapshot.inventory_score - _avg("inventory_score", n5)

    def _classify_tempo_state(self, snapshot: TPFMSnapshot) -> str:
        if abs(snapshot.initiative_delta_3) >= 0.12 and snapshot.agreement_delta_3 >= 0.08:
            return "ACCELERATING"
        elif abs(snapshot.initiative_delta_3) >= 0.08 and (snapshot.tradability_delta_3 < 0 or snapshot.response_efficiency_state == 'ABSORBED_OR_TRAP'):
            return "DECELERATING"
        return "STABLE"

    def _classify_persistence_state(self, snapshot: TPFMSnapshot) -> str:
        if snapshot.sequence_length <= 1:
            return "EARLY"
        elif 2 <= snapshot.sequence_length <= 3:
            return "BUILDING"
        elif 4 <= snapshot.sequence_length <= 6:
            return "PERSISTENT"
        return "EXTENDED"

    def _estimate_exhaustion_risk(self, snapshot: TPFMSnapshot, history: List[TPFMSnapshot]) -> float:
        risk = 0.0
        if snapshot.persistence_state in ("PERSISTENT", "EXTENDED"):
            risk += 0.2
        if snapshot.tempo_state == "DECELERATING":
            risk += 0.3
        if snapshot.response_efficiency_state == "ABSORBED_OR_TRAP":
            risk += 0.2
        if snapshot.trap_risk > 0.5:
            risk += 0.2
        return round(min(1.0, max(0.0, risk)), 2)

    def _derive_matrix_native_pattern(self, snapshot: TPFMSnapshot) -> None:
        cell = snapshot.matrix_cell
        snapshot.pattern_family = cell
        
        if cell == "POS_INIT__POS_INV":
            if snapshot.sequence_length >= 2:
                snapshot.pattern_code = "CONTI_LONG"
                snapshot.pattern_alias_vi = "Tiếp diễn Mua"
            if snapshot.tempo_state == "DECELERATING" and snapshot.exhaustion_risk > 0.6:
                snapshot.pattern_code = "EXHAUSTION_LONG"
                snapshot.pattern_alias_vi = "Cạn kiệt lực Mua"
        elif cell == "NEG_INIT__NEG_INV":
            if snapshot.sequence_length >= 2:
                snapshot.pattern_code = "CONTI_SHORT"
                snapshot.pattern_alias_vi = "Tiếp diễn Bán"
            if snapshot.tempo_state == "DECELERATING" and snapshot.exhaustion_risk > 0.6:
                snapshot.pattern_code = "EXHAUSTION_SHORT"
                snapshot.pattern_alias_vi = "Cạn kiệt lực Bán"
        elif cell == "POS_INIT__NEG_INV":
            snapshot.pattern_code = "TRAP_LONG"
            snapshot.pattern_alias_vi = "Bẫy Mua (Hấp thụ Bán)"
            if snapshot.trap_risk < 0.4 and snapshot.response_efficiency_state == "FOLLOW_THROUGH":
                snapshot.pattern_code = "ABSORB_SHORT"
                snapshot.pattern_alias_vi = "Hấp thụ ngược (Chống Bán)"
        elif cell == "NEG_INIT__POS_INV":
            snapshot.pattern_code = "TRAP_SHORT"
            snapshot.pattern_alias_vi = "Bẫy Bán (Hấp thụ Mua)"
            if snapshot.trap_risk < 0.4 and snapshot.response_efficiency_state == "FOLLOW_THROUGH":
                snapshot.pattern_code = "ABSORB_LONG"
                snapshot.pattern_alias_vi = "Hấp thụ ngược (Chống Mua)"
        elif cell == "POS_INIT__NEUTRAL_INV":
            snapshot.pattern_code = "BREAKOUT_FORMING_LONG"
            snapshot.pattern_alias_vi = "Đang nén bứt phá Mua"
        elif cell == "NEG_INIT__NEUTRAL_INV":
            snapshot.pattern_code = "BREAKDOWN_FORMING_SHORT"
            snapshot.pattern_alias_vi = "Đang nén phá đáy Bán"
        elif cell == "NEUTRAL_INIT__POS_INV":
            snapshot.pattern_code = "PASSIVE_ACCUMULATION"
            snapshot.pattern_alias_vi = "Tích lũy thụ động"
        elif cell == "NEUTRAL_INIT__NEG_INV":
            snapshot.pattern_code = "PASSIVE_DISTRIBUTION"
            snapshot.pattern_alias_vi = "Phân phối thụ động"
        elif cell == "NEUTRAL_INIT__NEUTRAL_INV":
            snapshot.pattern_code = "BALANCE"
            snapshot.pattern_alias_vi = "Cân bằng Flow"
                
        # Forced flow overrides
        if snapshot.forced_flow_state == "SQUEEZE_LED" and snapshot.delta_quote > 0:
            snapshot.pattern_code = "SQUEEZE_LONG"
            snapshot.pattern_alias_vi = "Squeeze Mua"
        elif snapshot.forced_flow_state == "LIQUIDATION_LED" and snapshot.delta_quote < 0:
            snapshot.pattern_code = "FLUSH_SHORT"
            snapshot.pattern_alias_vi = "Xả Short (Flush)"

    def _build_sequence_signature(self, snapshot: TPFMSnapshot) -> str:
        cell_short = snapshot.matrix_cell.replace("_INIT", "").replace("_INV", "").replace("__", "_")
        return f"{cell_short}x{snapshot.sequence_length}|{snapshot.tempo_state}|{snapshot.forced_flow_state}|{snapshot.response_efficiency_state}"

    def _derive_pattern_phase(self, snapshot: TPFMSnapshot) -> None:
        if snapshot.trap_risk > 0.75:
            snapshot.pattern_phase = "FAILED"
            return
            
        if snapshot.sequence_length == 1:
            snapshot.pattern_phase = "FORMING"
        elif snapshot.sequence_length >= 2 and snapshot.response_efficiency_state != "ABSORBED_OR_TRAP":
            if snapshot.sequence_length >= 4:
                snapshot.pattern_phase = "MATURE"
            else:
                snapshot.pattern_phase = "CONFIRMED"
                
        if snapshot.persistence_state in ("PERSISTENT", "EXTENDED") and snapshot.tempo_state == "DECELERATING" and snapshot.exhaustion_risk > 0.55:
            snapshot.pattern_phase = "EXHAUSTING"

    def _estimate_pattern_strength(self, snapshot: TPFMSnapshot) -> float:
        return min(1.0, (abs(snapshot.initiative_score) + abs(snapshot.inventory_score) + snapshot.agreement_score) / 3.0)
        
    def _estimate_pattern_quality(self, snapshot: TPFMSnapshot) -> float:
        q = snapshot.tradability_score
        if snapshot.response_efficiency_state == "FOLLOW_THROUGH":
            q += 0.2
        return min(1.0, q)
        
    def _estimate_pattern_failure_risk(self, snapshot: TPFMSnapshot) -> float:
        return min(1.0, snapshot.trap_risk * 0.5 + snapshot.exhaustion_risk * 0.5 + snapshot.conflict_score * 0.2)

    def _build_flow_pattern_event(self, snapshot: TPFMSnapshot) -> 'FlowPatternEvent':
        from cfte.tpfm.models import FlowPatternEvent
        return FlowPatternEvent(
            pattern_id=str(uuid.uuid4()),
            snapshot_id=snapshot.snapshot_id,
            symbol=snapshot.symbol,
            venue=snapshot.venue,
            timestamp=snapshot.window_end_ts,
            pattern_code=snapshot.pattern_code,
            pattern_alias_vi=snapshot.pattern_alias_vi,
            pattern_family=snapshot.pattern_family,
            pattern_phase=snapshot.pattern_phase,
            sequence_id=snapshot.sequence_id,
            sequence_signature=snapshot.sequence_signature,
            sequence_length=snapshot.sequence_length,
            tempo_state=snapshot.tempo_state,
            persistence_state=snapshot.persistence_state,
            pattern_strength=snapshot.pattern_strength,
            pattern_quality=snapshot.pattern_quality,
            pattern_failure_risk=snapshot.pattern_failure_risk,
            matrix_cell=snapshot.matrix_cell,
            flow_state_code=snapshot.flow_state_code,
            metadata={"history_depth": snapshot.history_depth}
        )
        
    # ==========================================
    # Phase B: Sequence Engine (Chain Tracking)
    # ==========================================
    
    def _derive_sequence_tracking(self, snapshot: TPFMSnapshot) -> Optional['FlowSequenceEvent']:
        """Identifies and tracks consecutive flow matrices (Sequences)."""
        from cfte.tpfm.models import FlowSequenceEvent  # local import for now
        
        current_cell = snapshot.matrix_cell
        if "NEUTRAL_INIT__NEUTRAL_INV" in current_cell:
            return None
            
        closed_sequence = None
        is_pivot = False
            
        if self._current_sequence is None:
            # Start new sequence
            is_pivot = True
            self._current_sequence = FlowSequenceEvent(
                sequence_id=str(uuid.uuid4()),
                symbol=snapshot.symbol,
                venue=snapshot.venue,
                started_ts=snapshot.window_start_ts,
                ended_ts=snapshot.window_end_ts,
                sequence_signature=current_cell,
                sequence_family=self._transition_target_code(snapshot),
                sequence_length=1,
                sequence_bias="LONG" if snapshot.initiative_score > 0 else "SHORT" if snapshot.initiative_score < 0 else "NEUTRAL",
                sequence_strength=abs(snapshot.initiative_score),
                sequence_maturity="NEW",
                sequence_quality=snapshot.tradability_score,
                stack_alignment_hint="UNKNOWN",
                current_cell=snapshot.matrix_cell,
                current_flow_state=snapshot.flow_state_code,
                resolution_hint="ONGOING",
                metadata={"cumulative_initiative": snapshot.initiative_score, "cumulative_inventory": snapshot.inventory_score, "max_energy": snapshot.energy_score, "is_active": True}
            )
        elif self._current_sequence.sequence_signature == current_cell:
            # Continue sequence
            self._current_sequence.ended_ts = snapshot.window_end_ts
            self._current_sequence.sequence_length += 1
            self._current_sequence.metadata["cumulative_initiative"] += snapshot.initiative_score
            self._current_sequence.metadata["cumulative_inventory"] += snapshot.inventory_score
            self._current_sequence.metadata["max_energy"] = max(self._current_sequence.metadata["max_energy"], snapshot.energy_score)
            self._current_sequence.sequence_strength = max(self._current_sequence.sequence_strength, abs(snapshot.initiative_score))
            if self._current_sequence.sequence_length >= 5:
                self._current_sequence.sequence_maturity = "MATURE"
            elif self._current_sequence.sequence_length >= 3:
                self._current_sequence.sequence_maturity = "ESTABLISHED"
            
            n = self._current_sequence.sequence_length
            self._current_sequence.sequence_quality = (self._current_sequence.sequence_quality * (n - 1) + snapshot.tradability_score) / n
        else:
            # Break sequence (PIVOT)
            is_pivot = True
            closed_sequence = self._current_sequence
            closed_sequence.metadata["is_active"] = False
            closed_sequence.resolution_hint = f"BROKEN_BY_{current_cell}"
            
            # Start new sequence
            self._current_sequence = FlowSequenceEvent(
                sequence_id=str(uuid.uuid4()),
                symbol=snapshot.symbol,
                venue=snapshot.venue,
                started_ts=snapshot.window_start_ts,
                ended_ts=snapshot.window_end_ts,
                sequence_signature=current_cell,
                sequence_family=self._transition_target_code(snapshot),
                sequence_length=1,
                sequence_bias="LONG" if snapshot.initiative_score > 0 else "SHORT" if snapshot.initiative_score < 0 else "NEUTRAL",
                sequence_strength=abs(snapshot.initiative_score),
                sequence_maturity="NEW",
                sequence_quality=snapshot.tradability_score,
                stack_alignment_hint="UNKNOWN",
                current_cell=snapshot.matrix_cell,
                current_flow_state=snapshot.flow_state_code,
                resolution_hint="ONGOING",
                metadata={"cumulative_initiative": snapshot.initiative_score, "cumulative_inventory": snapshot.inventory_score, "max_energy": snapshot.energy_score, "is_active": True}
            )
            
        # Phase 2: Populate snapshot temporal tracking attributes
        snapshot.sequence_id = self._current_sequence.sequence_id
        snapshot.sequence_signature = self._current_sequence.sequence_signature
        snapshot.sequence_length = self._current_sequence.sequence_length
        snapshot.sequence_family = self._current_sequence.sequence_family
        snapshot.sequence_quality = self._current_sequence.sequence_quality
        
        snapshot.sequence_start_ts = self._current_sequence.started_ts
        snapshot.sequence_duration_sec = (snapshot.window_end_ts - snapshot.sequence_start_ts) / 1000.0
        snapshot.is_sequence_pivot = is_pivot
        
        return closed_sequence

    def _derive_multi_timeframe_context(self, snapshot: TPFMSnapshot) -> None:
        """Fetches and integrates M30/H4 context for the matrix stack."""
        # Phase 2: MTF integration
        snapshot.parent_context = {
            "m30_regime": "UNKNOWN",
            "h4_structural_bias": "UNKNOWN",
            "m30_persistence": 0.0,
            "stack_alignment": "NEUTRAL"
        }

    def _estimate_outcome_targets(self, snapshot: TPFMSnapshot) -> None:
        """Predicts potential price levels for T+1, T+5, T+12 based on current momentum."""
        # Use last_trade_px if available, else fallback to snapshot price
        last_px = snapshot.metadata.get("last_trade_px", 0.0)
        if last_px <= 0:
            # Fallback to the latest TapeSnapshot mid_px if we have snapshots in the calculation
            # But the snapshot doesn't have the TapeSnapshots list. 
            # In calculate_m5_snapshot, we can pass it or use a default.
            # For now, let's just use mid_px if it was stored or passed.
            pass
        
        # Actually, let's just make it simpler: use a price from the snapshot metadata or a default
        # Since this is a skeleton for Phase 2.
        last_px = last_px or 50000.0 # Default for testing or use a real field if we add it
        
        snapshot.t_plus_1_price = last_px * (1 + snapshot.initiative_score * 0.0001)
        snapshot.t_plus_5_price = last_px * (1 + snapshot.initiative_score * 0.0003)
        snapshot.t_plus_12_price = last_px * (1 + snapshot.initiative_score * 0.0005)
    def _derive_probability_edge(self, snapshot: TPFMSnapshot) -> None:
        """Calculates the statistical edge for the current snapshot using ProbabilityEngine"""
        edge_data = self._probability_engine.evaluate_edge(
            snapshot.matrix_cell,
            sequence_length=snapshot.sequence_length
        )
        
        snapshot.historical_win_rate = edge_data.historical_win_rate
        snapshot.expected_rr = edge_data.expected_rr
        snapshot.edge_score = edge_data.edge_score
        snapshot.edge_confidence = edge_data.confidence

    async def sync_probability_stats(self, db_writer) -> bool:
        """Syncs ProbabilityEngine with real-world results from the database"""
        try:
            scorecard = await db_writer.get_matrix_scorecard()
            pattern_scorecard = await db_writer.get_pattern_scorecard()
            if scorecard:
                self._probability_engine.refresh_stats(scorecard, pattern_scorecard=pattern_scorecard)
                print(f"✅ TPFM Probability Engine synced with {len(scorecard)} cells and {len(pattern_scorecard)} patterns.")
                return True
            return False
        except Exception as e:
            print(f"❌ Error syncing probability stats: {str(e)}")
            return False

    def _derive_ai_brief(self, snapshot: TPFMSnapshot) -> None:
        """Generates a professional AI-driven trader brief for the snapshot"""
        snapshot.flow_decision_brief = self._ai_explainer.explain_m5_brief(snapshot)

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
            macro_posture=(
                "WAIT_FOR_REGIME"
                if persistence < 0.5 or dominant_regime == "NEUTRAL_CONSOLIDATION" or trap_rate >= 0.5
                else "FOLLOW_REGIME"
            )
        )

    def _soft_clamp(self, x: float) -> float:
        return max(-1.0, min(1.0, x))

    def _center_imbalance(self, value: float) -> float:
        return self._soft_clamp((value - 0.5) * 2.0)

    def _resolve_inventory_side(self, centered_imbalance: float, micro_gap_bps: float) -> float:
        if centered_imbalance >= 0.05:
            return 1.0
        if centered_imbalance <= -0.05:
            return -1.0
        if micro_gap_bps > 0:
            return 1.0
        if micro_gap_bps < 0:
            return -1.0
        return 0.0

    def _polarity_label(
        self,
        value: float,
        *,
        positive_threshold: float,
        negative_threshold: float,
        positive_label: str,
        negative_label: str,
        neutral_label: str,
    ) -> str:
        if value >= positive_threshold:
            return positive_label
        if value <= negative_threshold:
            return negative_label
        return neutral_label

    def _matrix_contract(self, matrix_cell: str) -> Dict[str, str]:
        return _MATRIX_ALIASES_VI.get(
            matrix_cell,
            {
                "alias": "Trung tính",
                "bias": "NEUTRAL",
                "posture": "WAIT",
                "decision": "Đứng ngoài và chờ cấu trúc rõ hơn.",
                "confirm_needed": "Chờ thêm xác nhận từ futures hoặc đa sàn",
                "invalidation": "Matrix không còn giữ được hướng hiện tại.",
                "entry_condition": "N/A",
                "avoid_if": "N/A",
            }
        )

    def _build_axis_confidence(
        self,
        *,
        avg_freshness: float,
        avg_gap: float,
        trade_count: int,
        avg_spread_bps: float,
    ) -> float:
        freshness_part = min(1.0, max(0.0, avg_freshness))
        gap_part = 1.0 if avg_gap <= 1.5 else max(0.0, 1.0 - min(1.0, (avg_gap - 1.5) / 2.0))
        count_part = min(1.0, trade_count / 20.0)
        spread_part = max(0.0, 1.0 - min(1.0, avg_spread_bps / 12.0))
        return round(self._soft_clamp(0.25 + 0.30 * freshness_part + 0.20 * gap_part + 0.15 * count_part + 0.10 * spread_part), 2)

    def _apply_input_quality_flags(self, snapshot: TPFMSnapshot, *, avg_freshness: float, avg_gap: float, trade_count: int) -> None:
        if avg_freshness < 0.35:
            self._append_unique(snapshot.blind_spot_flags, "FLOW_FRESHNESS_LOW")
            snapshot.degraded = True
        if avg_gap > 1.5:
            self._append_unique(snapshot.blind_spot_flags, "STREAM_GAP_HIGH")
            snapshot.degraded = True
        if trade_count < 8:
            self._append_unique(snapshot.blind_spot_flags, "THIN_M5_WINDOW")
            snapshot.degraded = True
        if snapshot.degraded and snapshot.health_state == "HEALTHY":
            snapshot.health_state = "DEGRADED_INPUT"

    def _build_setup_score_map(self, active_theses: List) -> Dict[str, float]:
        setup_score_map: Dict[str, float] = {}
        for item in active_theses:
            signal = getattr(item, "signal", None)
            if signal is None:
                continue
            setup_score_map[signal.setup] = max(setup_score_map.get(signal.setup, 0.0), float(signal.score))
        return setup_score_map

    def _assign_tradability_grade(self, snapshot: TPFMSnapshot) -> str:
        blind_spot_penalty = 0.08 * len(snapshot.blind_spot_flags)
        quality = (
            0.45 * snapshot.tradability_score
            + 0.30 * snapshot.market_quality_score
            + 0.25 * snapshot.axis_confidence
            - blind_spot_penalty
        )
        if quality >= 0.75:
            return "A"
        if quality >= 0.60:
            return "B"
        if quality >= 0.45:
            return "C"
        return "D"

    def _fmt_quote(self, value: float) -> str:
        abs_value = abs(value)
        if abs_value >= 1_000_000:
            body = f"{abs_value / 1_000_000:.2f}M"
        elif abs_value >= 1_000:
            body = f"{abs_value / 1_000:.2f}K"
        else:
            body = f"{abs_value:.0f}"
        prefix = "+" if value > 0 else "-" if value < 0 else ""
        return f"{prefix}{body}"

    def _append_unique(self, items: List[str], message: str) -> None:
        if message not in items:
            items.append(message)

    def _mean(self, values: List[float], *, default: float) -> float:
        return sum(values) / len(values) if values else default

    def _snapshot_meta_float(self, snapshot: TapeSnapshot, key: str, default: float) -> float:
        metadata = getattr(snapshot, "metadata", None)
        if not isinstance(metadata, dict):
            return default
        try:
            return float(metadata.get(key, default))
        except (TypeError, ValueError):
            return default

    def _snapshot_attr_float(self, snapshot: TapeSnapshot, key: str, default: float) -> float:
        value = getattr(snapshot, key, default)
        if isinstance(value, (int, float)):
            return float(value)
        return default

    def _derive_inventory_defense_state(self, snapshot: TPFMSnapshot) -> str:
        if snapshot.inventory_polarity == "POS_INV" and (
            snapshot.signed_absorption_score >= 0.10
            or snapshot.replenishment_bid_score > max(snapshot.replenishment_ask_score * 1.1, 2_500.0)
        ):
            return "BID_DEFENSE"
        if snapshot.inventory_polarity == "NEG_INV" and (
            snapshot.signed_absorption_score <= -0.10
            or snapshot.replenishment_ask_score > max(snapshot.replenishment_bid_score * 1.1, 2_500.0)
        ):
            return "ASK_DEFENSE"
        return "NONE"

    def _derive_flow_state_code(self, snapshot: TPFMSnapshot) -> str:
        base_map = {
            "POS_INIT__POS_INV": "LONG_CONTINUATION",
            "POS_INIT__NEG_INV": "LONG_TRAP_RISK",
            "NEG_INIT__POS_INV": "SHORT_TRAP_RISK",
            "NEG_INIT__NEG_INV": "SHORT_CONTINUATION",
            "POS_INIT__NEUTRAL_INV": "BUY_PRESSURE_BUILDUP",
            "NEG_INIT__NEUTRAL_INV": "SELL_PRESSURE_BUILDUP",
            "NEUTRAL_INIT__POS_INV": "PASSIVE_BID_SUPPORT",
            "NEUTRAL_INIT__NEG_INV": "PASSIVE_ASK_SUPPLY",
            "NEUTRAL_INIT__NEUTRAL_INV": "NEUTRAL_BALANCE",
        }
        state = base_map.get(snapshot.matrix_cell, "NEUTRAL_BALANCE")
        if snapshot.forced_flow_state == "SQUEEZE_LED":
            state = "FORCED_SHORT_SQUEEZE" if snapshot.liquidation_bias == "SHORTS_FLUSHED" else f"SQUEEZE_{state}"
        elif snapshot.forced_flow_state == "LIQUIDATION_LED":
            if snapshot.liquidation_bias == "LONGS_FLUSHED":
                state = "FORCED_LONG_FLUSH"
            elif snapshot.liquidation_bias == "SHORTS_FLUSHED":
                state = "FORCED_SHORT_SQUEEZE"
            else:
                state = f"FORCED_{state}"
        elif snapshot.forced_flow_state == "GAP_LED":
            state = f"GAP_{state}"

        if snapshot.response_efficiency_state == "ABSORBED_OR_TRAP":
            state = f"{state}__TRAP"
        elif snapshot.response_efficiency_state == "FOLLOW_THROUGH" and snapshot.agreement_score >= snapshot.conflict_score:
            state = f"{state}__FOLLOW_THROUGH"
        return state

    def _derive_observed_facts(self, snapshot: TPFMSnapshot) -> List[str]:
        observed = [
            f"Delta spot {self._fmt_quote(snapshot.delta_quote)}",
            f"Nhịp {snapshot.trade_burst:.2f}/s",
            f"Inventory L1 {snapshot.centered_imbalance_l1 * 100:+.0f}%",
            f"Hấp thụ ký hiệu {snapshot.signed_absorption_score:+.2f}",
        ]
        if snapshot.sweep_quote > 0:
            dominant_sweep = "mua" if snapshot.sweep_buy_quote >= snapshot.sweep_sell_quote else "bán"
            observed.append(
                f"Sweep {dominant_sweep} {self._fmt_quote(snapshot.sweep_quote)} | giữ lực {snapshot.burst_persistence:.2f}"
            )
        elif snapshot.burst_persistence >= 0.45:
            observed.append(f"Dòng tiền duy trì đều {snapshot.burst_persistence:.2f}")
        if abs(snapshot.microprice_drift_bps) >= 1.0:
            observed.append(f"Microprice drift {snapshot.microprice_drift_bps:+.1f} bps")
        if snapshot.inventory_defense_state == "BID_DEFENSE":
            observed.append("Bid đang hấp thụ và đỡ giá")
        elif snapshot.inventory_defense_state == "ASK_DEFENSE":
            observed.append("Ask đang hấp thụ và ghìm giá")
        if snapshot.replenishment_bid_score > max(snapshot.replenishment_ask_score, 2_500.0):
            observed.append(f"Bid refill {self._fmt_quote(snapshot.replenishment_bid_score)}")
        elif snapshot.replenishment_ask_score > max(snapshot.replenishment_bid_score, 2_500.0):
            observed.append(f"Ask refill {self._fmt_quote(snapshot.replenishment_ask_score)}")
        if snapshot.futures_delta_available:
            observed.append(
                f"Delta futures {self._fmt_quote(snapshot.futures_delta)} | aggr {snapshot.futures_aggression_ratio:.2f}"
            )
        if snapshot.oi_state != "STABLE":
            observed.append(f"OI {snapshot.oi_state.lower()} ({snapshot.oi_expansion_ratio:+.3f})")
        if abs(snapshot.basis_bps) >= 2.0:
            observed.append(f"Basis {snapshot.basis_bps:+.1f} bps | {snapshot.basis_state}")
        if snapshot.venue_confirmation_state == "CONFIRMED" and snapshot.leader_venue:
            observed.append(
                f"Đa sàn xác nhận, {snapshot.leader_venue} dẫn ({snapshot.leader_confidence:.2f}) | lệch VWAP {snapshot.venue_vwap_spread_bps:.1f} bps"
            )
        elif snapshot.venue_confirmation_state == "ALT_LEAD" and snapshot.leader_venue:
            observed.append(
                f"{snapshot.leader_venue} dẫn trước Binance ({snapshot.leader_confidence:.2f})"
            )
        if snapshot.liquidation_quote > 5_000:
            liq_label = {
                "LONGS_FLUSHED": "Longs bị flush",
                "SHORTS_FLUSHED": "Shorts bị squeeze",
                "MIXED": "Thanh lý hai chiều",
            }.get(snapshot.liquidation_bias, "Thanh lý")
            observed.append(
                f"{liq_label} {self._fmt_quote(snapshot.liquidation_quote)} / {snapshot.liquidation_count} cụm | intensity {snapshot.liquidation_intensity:.2f}"
            )
        return observed

    def _derive_inferred_facts(self, snapshot: TPFMSnapshot) -> List[str]:
        inferred = [snapshot.matrix_alias_vi, f"Flow state: {snapshot.flow_state_code}"]
        if snapshot.transition_event is not None:
            inferred.append(
                f"Transition: {snapshot.transition_event.transition_alias_vi} ({snapshot.transition_event.transition_code})"
            )
        if snapshot.micro_conclusion != "UNCERTAIN":
            inferred.append(snapshot.micro_conclusion)
        if snapshot.forced_flow_state != "NONE":
            inferred.append(f"Forced flow: {snapshot.forced_flow_state}")
        if snapshot.inventory_defense_state != "NONE":
            inferred.append(f"Inventory defense: {snapshot.inventory_defense_state}")
        if snapshot.spot_futures_relation not in {"NO_FUTURES_CONFIRM", "NO_FUTURES_DELTA"}:
            inferred.append(f"Quan hệ spot/futures: {snapshot.spot_futures_relation}")
        if snapshot.venue_confirmation_state == "ALT_LEAD" and snapshot.leader_venue:
            inferred.append(f"Dòng perp đang do {snapshot.leader_venue} dẫn nhịp")
        if snapshot.sweep_quote > 0 and snapshot.burst_persistence >= 0.45:
            sweep_sign = snapshot.sweep_buy_quote - snapshot.sweep_sell_quote
            if sweep_sign * snapshot.delta_quote > 0:
                inferred.append("Sweep và delta cùng hướng, continuation có nền tốt.")
            elif sweep_sign != 0:
                inferred.append("Sweep đi ngược delta, cần cảnh giác trap ngắn hạn.")
        if snapshot.basis_state in {"OVERHEATED_PREMIUM", "DEEP_DISCOUNT"}:
            inferred.append(f"Basis đang cực trị: {snapshot.basis_state}")
        return inferred

    def _derive_missing_context(self, snapshot: TPFMSnapshot) -> List[str]:
        missing = []
        if not snapshot.futures_context_available:
            missing.append("Chưa có context futures.")
        elif not snapshot.futures_delta_available:
            missing.append("Chưa có futures delta thật.")
        if snapshot.venue_confirmation_state == "UNCONFIRMED":
            missing.append("Chưa có xác nhận đa sàn.")
        elif snapshot.leader_confidence < 0.45:
            missing.append("Leader liên sàn chưa đủ tin cậy.")
        if not snapshot.liquidation_context_available:
            missing.append("Chưa có liquidation feed.")
        return missing

    def _derive_risk_flags(self, snapshot: TPFMSnapshot) -> List[str]:
        risks = list(snapshot.context_warning_flags)
        if snapshot.conflict_score >= 1.0:
            risks.append("Initiative và inventory đang lệch pha.")
        if snapshot.response_efficiency_state == "ABSORBED_OR_TRAP":
            risks.append("Phản ứng giá đang mang tính hấp thụ hoặc trap.")
        if snapshot.axis_confidence < 0.55:
            risks.append("Độ chắc của trục matrix còn thấp.")
        if snapshot.trap_risk >= 0.6:
            risks.append(f"Trap risk cao ({snapshot.trap_risk:.2f}).")
        if snapshot.venue_confirmation_state == "DIVERGENT":
            risks.append(
                f"Liên sàn phân kỳ, leader={snapshot.leader_venue or 'unknown'}, lệch VWAP {snapshot.venue_vwap_spread_bps:.1f} bps."
            )
        if snapshot.sweep_quote > 0 and snapshot.burst_persistence < 0.30:
            risks.append("Sweep mạnh nhưng thiếu persistence, dễ là one-shot move.")
        if snapshot.basis_state in {"OVERHEATED_PREMIUM", "DEEP_DISCOUNT"}:
            risks.append(f"Basis đang cực trị ({snapshot.basis_state}).")
        return risks

    def _build_decision_view(self, snapshot: TPFMSnapshot) -> FlowDecisionView:
        flow_bias = "NEUTRAL"
        if snapshot.initiative_score > 0 or "LONG" in snapshot.continuation_bias:
            flow_bias = "LONG"
        elif snapshot.initiative_score < 0 or "SHORT" in snapshot.continuation_bias:
            flow_bias = "SHORT"

        posture = snapshot.decision_posture
        summary_vi = snapshot.decision_summary_vi
        if snapshot.tradability_grade == "C":
            posture = "WAIT"
            summary_vi = "Chỉ theo dõi, chờ thêm xác nhận từ futures hoặc đa sàn trước khi hành động."
        elif snapshot.tradability_grade == "D":
            posture = "WAIT"
            summary_vi = "Đứng ngoài. Matrix chưa đủ sạch để hành động."

        snapshot.decision_posture = posture
        snapshot.decision_summary_vi = summary_vi
        snapshot.action_plan_vi = f"{posture}: {summary_vi}" if posture != "WAIT" else summary_vi
        snapshot.entry_condition = snapshot.entry_condition_vi
        snapshot.avoid_if = snapshot.avoid_if_vi
        snapshot.review_tags = {
            "flow_state_code": snapshot.flow_state_code,
            "matrix_cell": snapshot.matrix_cell,
            "continuation_bias": snapshot.continuation_bias,
            "spot_futures_relation": snapshot.spot_futures_relation,
            "forced_flow_state": snapshot.forced_flow_state,
            "venue_confirmation_state": snapshot.venue_confirmation_state,
            "tradability_grade": snapshot.tradability_grade,
            "basis_state": snapshot.basis_state,
            "inventory_defense_state": snapshot.inventory_defense_state,
        }
        if snapshot.transition_event is not None:
            snapshot.review_tags["transition_code"] = snapshot.transition_event.transition_code
            snapshot.review_tags["transition_family"] = snapshot.transition_event.transition_family
        snapshot.review_tags_json = json.dumps(snapshot.review_tags, ensure_ascii=False)

        decision_view = FlowDecisionView(
            decision_id=f"decision:{snapshot.snapshot_id}",
            snapshot_id=snapshot.snapshot_id,
            symbol=snapshot.symbol,
            venue=snapshot.venue,
            timestamp=snapshot.window_end_ts,
            flow_bias=flow_bias,
            continuation_bias=snapshot.continuation_bias,
            posture=posture,
            tradability_grade=snapshot.tradability_grade,
            entry_condition=snapshot.entry_condition_vi,
            confirm_needed=snapshot.confirm_needed_vi,
            avoid_if=snapshot.avoid_if_vi,
            invalid_if=snapshot.invalid_if,
            tp_path=[snapshot.action_plan_vi],
            risk_flags=list(snapshot.risk_flags),
            review_tags=dict(snapshot.review_tags),
        )
        snapshot.metadata["decision_view"] = asdict(decision_view)
        return decision_view

    def _finalize_output_contract(self, snapshot: TPFMSnapshot) -> None:
        snapshot.inventory_defense_state = self._derive_inventory_defense_state(snapshot)
        snapshot.flow_state_code = self._derive_flow_state_code(snapshot)
        snapshot.tradability_grade = self._assign_tradability_grade(snapshot)
        snapshot.observed_facts = self._derive_observed_facts(snapshot)[:7]
        snapshot.inferred_facts = self._derive_inferred_facts(snapshot)[:5]
        snapshot.missing_context = self._derive_missing_context(snapshot)[:4]
        snapshot.risk_flags = self._derive_risk_flags(snapshot)[:5]
        self._build_decision_view(snapshot)

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

    def _update_pattern_outcomes(self, snapshot: TPFMSnapshot, trades: List[NormalizedTrade]) -> List[Any]:
        from cfte.models.events import FlowPatternOutcome
        import uuid
        outcomes_to_save = []
        
        if not trades:
            return []
            
        interval_high = max(t.price for t in trades)
        interval_low = min(t.price for t in trades)
        current_px = snapshot.microprice
        
        # 1. Update existing pending
        remaining_pending = []
        for entry in self._pending_patterns:
            outcome = entry["outcome"]
            meta = entry["meta"]
            
            meta["max_px"] = max(meta["max_px"], interval_high)
            meta["min_px"] = min(meta["min_px"], interval_low)
            meta["bars_seen"] += 1
            
            start_px = outcome.start_px
            if start_px != 0:
                if meta["bars_seen"] == 1:
                    outcome.t1_px = current_px
                    outcome.r1_bps = (current_px - start_px) / start_px * 10000
                elif meta["bars_seen"] == 5:
                    outcome.t5_px = current_px
                    outcome.r5_bps = (current_px - start_px) / start_px * 10000
                elif meta["bars_seen"] == 12:
                    outcome.t12_px = current_px
                    outcome.r12_bps = (current_px - start_px) / start_px * 10000
                
                # Update MAE/MFE (raw deviation in bps)
                outcome.max_favorable_bps = max(outcome.max_favorable_bps, (meta["max_px"] - start_px) / start_px * 10000)
                outcome.max_adverse_bps = min(outcome.max_adverse_bps, (meta["min_px"] - start_px) / start_px * 10000)
            
            if meta["bars_seen"] in [1, 5, 12]:
                outcomes_to_save.append(outcome)
            
            if meta["bars_seen"] < 12:
                remaining_pending.append(entry)
        
        self._pending_patterns = remaining_pending
        
        # 2. Add current pattern state to pending
        if snapshot.pattern_code != "UNCLASSIFIED":
            new_outcome = FlowPatternOutcome(
                outcome_id=str(uuid.uuid4()),
                snapshot_id=snapshot.snapshot_id,
                symbol=snapshot.symbol,
                timestamp=snapshot.window_end_ts,
                pattern_code=snapshot.pattern_code,
                sequence_signature=snapshot.sequence_signature,
                start_px=current_px,
            )
            self._pending_patterns.append({
                "outcome": new_outcome,
                "meta": {
                    "max_px": interval_high,
                    "min_px": interval_low,
                    "bars_seen": 0,
                }
            })
                
        return outcomes_to_save

    def flush_all_pending_outcomes(self, last_snapshot: TPFMSnapshot) -> List[Any]:
        """Force outcomes for all pending patterns using last known price."""
        outcomes = []
        current_px = last_snapshot.microprice
        for entry in self._pending_patterns:
            outcome = entry["outcome"]
            meta = entry["meta"]
            start_px = outcome.start_px
            if start_px != 0:
                if outcome.t1_px == 0:
                    outcome.t1_px = current_px
                    outcome.r1_bps = (current_px - start_px) / start_px * 10000
                if outcome.t5_px == 0:
                    outcome.t5_px = current_px
                    outcome.r5_bps = (current_px - start_px) / start_px * 10000
                if outcome.t12_px == 0:
                    outcome.t12_px = current_px
                    outcome.r12_bps = (current_px - start_px) / start_px * 10000
                
                outcome.max_favorable_bps = max(outcome.max_favorable_bps, (meta["max_px"] - start_px) / start_px * 10000)
                outcome.max_adverse_bps = min(outcome.max_adverse_bps, (meta["min_px"] - start_px) / start_px * 10000)
            
            outcomes.append(outcome)
        
        self._pending_patterns = []
        return outcomes
