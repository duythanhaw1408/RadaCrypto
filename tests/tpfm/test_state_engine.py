import unittest
from unittest.mock import MagicMock
from dataclasses import asdict
from cfte.tpfm.engine import TPFMStateEngine
from cfte.tpfm.models import TPFM30mRegime, TPFMSnapshot, TPFM4hStructural
from cfte.models.events import NormalizedTrade, TapeSnapshot

class TestTPFMStateEngine(unittest.TestCase):
    def setUp(self):
        self.engine = TPFMStateEngine(symbol="BTCUSDT")

    def test_pos_init_pos_inv(self):
        # Scenario: Strong Buy Pressure + Support + High Activity
        trades = [
            NormalizedTrade(event_id=f"e{i}", venue="binance", instrument_key="BTCUSDT", venue_ts=i*500, price=50000 + i, qty=1.0, quote_qty=50000 + i, taker_side="BUY")
            for i in range(20) # 20 trades in a short burst
        ]
        snapshots = [
            MagicMock(mid_px=50000 + i, imbalance_l1=0.8, absorption_proxy=10000, spread_bps=1.0, microprice=49999 + i)
            for i in range(20)
        ]
        
        # First snap (establishes baseline)
        self.engine.calculate_m5_snapshot(0, 10000, trades, snapshots)
        
        # Second snap: Shift to NEG_INIT (novelty check)
        trades_shift = [
            NormalizedTrade(event_id=f"es{i}", venue="binance", instrument_key="BTCUSDT", venue_ts=10000+i*500, price=50020 - i, qty=2.0, quote_qty=100000, taker_side="SELL")
            for i in range(20)
        ]
        snap = self.engine.calculate_m5_snapshot(10000, 20000, trades_shift, snapshots)
        
        self.assertEqual(snap.initiative_polarity, "NEG_INIT")
        self.assertTrue(snap.should_escalate)
        self.assertIn("Shift", snap.escalation_reason[0])

    def test_rendering_card(self):
        from cfte.tpfm.cards import render_tpfm_m5_card
        snap = self.engine._empty_snapshot(0, 300000)
        snap.symbol = "BTCUSDT"
        snap.matrix_alias_vi = "Trung tính"
        snap.flow_state_code = "NEUTRAL_BALANCE"
        snap.tradability_grade = "C"
        snap.observed_facts = ["Delta spot +0"]
        snap.inferred_facts = ["Trung tính"]
        snap.missing_context = ["THIẾU CONTEXT FUTURES"]
        snap.action_plan_vi = "WAIT"
        snap.entry_condition_vi = "N/A"
        snap.confirm_needed_vi = "Chờ futures"
        snap.avoid_if_vi = "Spread giãn"
        snap.invalid_if = "Matrix flip"
        
        card = render_tpfm_m5_card(snap)
        self.assertIn("BTCUSDT", card)
        self.assertIn("Pattern :", card)
        self.assertIn("Observed:", card)
        self.assertIn("Decision: WAIT", card)
        self.assertIn("Entry   :", card)
        self.assertIn("Invalid :", card)

    def test_30m_regime_synthesis(self):
        snapshots = []
        for i in range(4):
            s = self.engine._empty_snapshot(i*300000, (i+1)*300000)
            s.matrix_cell = "POS_INIT__POS_INV"
            s.initiative_score = 0.5
            s.inventory_score = 0.5
            s.energy_score = 0.5
            s.delta_quote = 100000
            s.trade_burst = 10
            snapshots.append(s)
        for i in range(4, 6):
            s = self.engine._empty_snapshot(i*300000, (i+1)*300000)
            s.matrix_cell = "NEUTRAL_INIT__NEUTRAL_INV"
            s.initiative_score = 0.0
            s.inventory_score = 0.0
            s.energy_score = 0.1
            s.delta_quote = 0
            s.trade_burst = 1
            snapshots.append(s)
            
        regime = self.engine.calculate_30m_regime(snapshots)
        
        self.assertEqual(regime.dominant_cell, "POS_INIT__POS_INV")
        self.assertEqual(regime.dominant_regime, "STRONG_ACCUMULATION")
        self.assertAlmostEqual(regime.regime_persistence_score, 4/6)
        self.assertEqual(regime.macro_posture, "FOLLOW_REGIME")

    def test_inventory_axis_is_centered_around_half(self):
        trades = [
            NormalizedTrade(event_id=f"e{i}", venue="binance", instrument_key="BTCUSDT", venue_ts=i * 1_000, price=50000, qty=1.0, quote_qty=50000, taker_side="BUY")
            for i in range(10)
        ]
        snapshots = [
            MagicMock(
                mid_px=50000,
                last_trade_px=50000,
                imbalance_l1=0.5,
                absorption_proxy=10000,
                spread_bps=1.0,
                microprice=50000,
                metadata={},
            )
            for _ in range(10)
        ]

        snap = self.engine.calculate_m5_snapshot(0, 10_000, trades, snapshots)

        self.assertAlmostEqual(snap.centered_imbalance_l1, 0.0)
        self.assertEqual(snap.inventory_polarity, "NEUTRAL_INV")

    def test_context_refinement_confluence(self):
        # Scenario: Spot BUY (+1M), Futures BUY (+800k)
        snap = self.engine._empty_snapshot(0, 300000)
        snap.delta_quote = 1000000
        snap.tradability_score = 0.5
        snap.agreement_score = 0.5
        
        ctx = {
            "available": True,
            "fresh": True,
            "futures_delta": 800000,
            "oi_delta": 1000,
            "basis_bps": 2.0
        }
        
        self.engine._apply_context_overlay(snap, ctx)
        self.engine._derive_decision_view(snap, context=ctx)
        
        self.assertEqual(snap.context_score, 1.0)
        self.assertGreater(snap.tradability_score, 0.5*1.1) # Boosted
        self.assertGreater(snap.agreement_score, 0.5) # Boosted
        self.assertIn("Futures Xác Nhận", snap.escalation_reason[0])

    def test_context_refinement_divergence_absorption(self):
        # Scenario: Spot BUY (+1M), Futures SELL (-2.5M) -> ABSORPTION
        snap = self.engine._empty_snapshot(0, 300000)
        snap.delta_quote = 1000000
        snap.tradability_score = 0.5
        
        ctx = {
            "available": True,
            "fresh": True,
            "futures_delta": -2500000,
            "oi_delta": -500,
            "basis_bps": -5.0
        }
        
        self.engine._apply_context_overlay(snap, ctx)
        self.engine._derive_decision_view(snap, context=ctx)
        
        self.assertEqual(snap.context_score, -1.0) # Divergence
        self.assertLess(snap.tradability_score, 0.5) # Penalized
        self.assertEqual(snap.micro_conclusion, "ABSORBED_BY_FUTURES")
        self.assertIn("FUTURES LỆCH NHỊP (Divergence)", snap.context_warning_flags[0])

    def test_context_basis_and_oi_flags(self):
        snap = self.engine._empty_snapshot(0, 300000)
        ctx = {
            "available": True,
            "fresh": True,
            "futures_delta": 2000000, # Large move
            "oi_delta": 1000000,      # OI expanding
            "basis_bps": 15.0         # High basis
        }
        snap.delta_quote = 100000      # Spot leading by relative measure? No, wait.
                                       # abs(futures) > abs(spot) * 1.5 -> FUTURES_LED
        
        self.engine._apply_context_overlay(snap, ctx)
        self.engine._derive_decision_view(snap, context=ctx)
        
        self.assertEqual(snap.futures_bias_proxy, "FUTURES_LED")
        self.assertEqual(snap.micro_conclusion, "OI_DRIVEN_MOVE")
        self.assertEqual(snap.basis_divergence_state, "DIVERGING_POS")
        self.assertIn("BASIS PHÂN KỲ", snap.context_warning_flags)

    def test_safe_degradation(self):
        snap = self.engine._empty_snapshot(0, 300000)
        snap.tradability_score = 0.5
        
        # Context unavailable
        self.engine._apply_context_overlay(snap, {"available": False})
        
        self.assertFalse(snap.futures_context_available)
        self.assertEqual(snap.tradability_score, 0.5) # Unchanged
        self.assertIn("THIẾU CONTEXT FUTURES", snap.context_warning_flags)

    def test_missing_futures_delta_is_exposed_as_blind_spot(self):
        trades = [
            NormalizedTrade(event_id=f"e{i}", venue="binance", instrument_key="BTCUSDT", venue_ts=i * 500, price=50000 + i, qty=1.0, quote_qty=50000 + i, taker_side="BUY")
            for i in range(12)
        ]
        snapshots = [
            MagicMock(
                mid_px=50000 + i,
                last_trade_px=50001 + i,
                imbalance_l1=0.7,
                absorption_proxy=12000,
                spread_bps=1.0,
                microprice=49999 + i,
                metadata={"recent_quote_share": 0.9},
            )
            for i in range(12)
        ]
        snap = self.engine.calculate_m5_snapshot(
            0,
            6_000,
            trades,
            snapshots,
            futures_context={"available": True, "fresh": True, "futures_delta_available": False, "oi_delta": 1000, "basis_bps": 2.0},
        )

        self.assertEqual(snap.spot_futures_relation, "NO_FUTURES_DELTA")
        self.assertIn("NO_FUTURES_DELTA", snap.blind_spot_flags)
        self.assertTrue(any("futures delta" in item.lower() for item in snap.missing_context))

    def test_vnext_snapshot_contract_is_self_describing(self):
        trades = [
            NormalizedTrade(
                event_id=f"e{i}",
                venue="binance",
                instrument_key="BTCUSDT",
                venue_ts=i * 500,
                price=50000 + i,
                qty=1.0,
                quote_qty=50000 + i,
                taker_side="BUY",
            )
            for i in range(16)
        ]
        snapshots = [
            MagicMock(
                mid_px=50000 + i,
                last_trade_px=50002 + i,
                imbalance_l1=0.78,
                absorption_proxy=16000,
                spread_bps=1.0,
                microprice=49999 + i,
                metadata={"recent_quote_share": 0.95, "bid_replenishment": 14000, "ask_replenishment": 1000},
            )
            for i in range(16)
        ]

        snap = self.engine.calculate_m5_snapshot(
            0,
            8_000,
            trades,
            snapshots,
            futures_context={
                "available": True,
                "fresh": True,
                "futures_delta": 750000,
                "futures_delta_available": True,
                "oi_delta": 1200,
                "basis_bps": 2.5,
                "venue_confirmation_state": "CONFIRMED",
                "leader_venue": "binance",
                "liquidation_context_available": True,
                "liquidation_bias": "SHORTS_FLUSHED",
                "liquidation_quote": 120000,
                "liquidation_count": 3,
            },
        )

        self.assertNotEqual(snap.flow_state_code, "NEUTRAL")
        self.assertIn(snap.inventory_defense_state, {"BID_DEFENSE", "NONE"})
        self.assertTrue(snap.review_tags)
        self.assertNotEqual(snap.entry_condition_vi, "N/A")
        self.assertNotEqual(snap.confirm_needed_vi, "N/A")
        self.assertTrue(snap.action_plan_vi)

    def test_context_overlay_surfaces_liquidation_and_venue_confirmation(self):
        snap = self.engine._empty_snapshot(0, 300000)
        snap.delta_quote = 250000
        snap.tradability_score = 0.5

        ctx = {
            "available": True,
            "fresh": True,
            "futures_delta": 200000,
            "oi_delta": 500,
            "basis_bps": 3.0,
            "venue_confirmation_state": "CONFIRMED",
            "leader_venue": "binance",
            "venue_vwap_spread_bps": 1.4,
            "liquidation_context_available": True,
            "liquidation_bias": "SHORTS_FLUSHED",
            "liquidation_count": 3,
            "liquidation_quote": 90000,
        }

        self.engine._apply_context_overlay(snap, ctx)
        self.engine._derive_decision_view(snap, context=ctx)
        self.engine._finalize_output_contract(snap)

        self.assertEqual(snap.venue_confirmation_state, "CONFIRMED")
        self.assertEqual(snap.leader_venue, "binance")
        self.assertEqual(snap.liquidation_bias, "SHORTS_FLUSHED")
        self.assertGreater(snap.tradability_score, 0.5)
        self.assertTrue(any("Futures Xác Nhận" in item for item in snap.escalation_reason))

    def test_context_overlay_marks_alt_lead_as_inference_and_risk(self):
        snap = self.engine._empty_snapshot(0, 300000)
        snap.delta_quote = 120000
        snap.tradability_score = 0.5

        ctx = {
            "available": True,
            "fresh": True,
            "futures_delta": 100000,
            "oi_delta": 200,
            "basis_bps": 1.0,
            "venue_confirmation_state": "ALT_LEAD",
            "leader_venue": "bybit",
            "lagger_venue": "binance",
            "venue_vwap_spread_bps": 1.8,
        }

        self.engine._apply_context_overlay(snap, ctx)
        self.engine._derive_decision_view(snap, context=ctx)
        self.engine._finalize_output_contract(snap)

        self.assertEqual(snap.venue_confirmation_state, "ALT_LEAD")
        self.assertTrue(any("bybit" in item.lower() for item in snap.inferred_facts))
        self.assertIn("BINANCE KHÔNG DẪN NHỊP LIÊN SÀN", snap.risk_flags)

    def test_phase2_hidden_flow_evidence_is_exposed_on_snapshot(self):
        trades = [
            NormalizedTrade(
                event_id=f"e{i}",
                venue="binance",
                instrument_key="BTCUSDT",
                venue_ts=i * 500,
                price=50_000 + i,
                qty=1.0,
                quote_qty=50_000 + i,
                taker_side="BUY",
            )
            for i in range(12)
        ]
        snapshots = [
            MagicMock(
                mid_px=50_000 + i,
                last_trade_px=50_004 + i,
                imbalance_l1=0.72,
                absorption_proxy=18_000,
                spread_bps=1.2,
                microprice=50_002 + i,
                metadata={
                    "recent_quote_share": 0.95,
                    "sweep_quote": 25_000,
                    "sweep_buy_quote": 20_000,
                    "sweep_sell_quote": 5_000,
                    "burst_persistence": 0.72,
                    "microprice_drift_bps": 3.5,
                    "replenishment_bid_score": 11_000,
                    "replenishment_ask_score": 1_000,
                },
            )
            for i in range(12)
        ]

        snap = self.engine.calculate_m5_snapshot(
            0,
            6_000,
            trades,
            snapshots,
            futures_context={
                "available": True,
                "fresh": True,
                "futures_delta": 400_000,
                "futures_delta_available": True,
                "futures_aggression_ratio": 0.68,
                "oi_delta": 2_000,
                "oi_expansion_ratio": 0.012,
                "basis_bps": 6.5,
                "basis_state": "PREMIUM",
                "venue_confirmation_state": "CONFIRMED",
                "leader_venue": "binance",
                "leader_confidence": 0.81,
                "aligned_window_ms": 2_500,
                "liquidation_context_available": True,
                "liquidation_bias": "SHORTS_FLUSHED",
                "liquidation_quote": 95_000,
                "liquidation_intensity": 0.8,
            },
        )

        self.assertGreater(snap.sweep_buy_quote, snap.sweep_sell_quote)
        self.assertGreater(snap.burst_persistence, 0.5)
        self.assertGreater(snap.replenishment_bid_score, snap.replenishment_ask_score)
        self.assertEqual(snap.basis_state, "PREMIUM")
        self.assertGreater(snap.leader_confidence, 0.7)
        self.assertTrue(any("Sweep" in item for item in snap.observed_facts))
        self.assertIn(snap.spot_futures_relation, {"CONFLUENT", "FUTURES_LED", "SPOT_LED"})

    def test_transition_intelligence_builds_semantic_transition_event(self):
        first = self.engine._empty_snapshot(0, 300_000)
        first.matrix_cell = "POS_INIT__NEG_INV"
        first.flow_state_code = "LONG_TRAP_RISK__TRAP"
        first.initiative_score = 0.55
        first.inventory_score = -0.45
        first.energy_score = 0.45
        first.context_score = -1.0
        first.decision_posture = "CONSERVATIVE"
        first.tradability_grade = "C"
        self.engine._recent_snapshots.append(first)
        self.engine._prev_snapshot = first

        second = self.engine._empty_snapshot(300_000, 600_000)
        second.matrix_cell = "NEG_INIT__NEG_INV"
        second.flow_state_code = "SHORT_CONTINUATION__FOLLOW_THROUGH"
        second.initiative_score = -0.72
        second.inventory_score = -0.61
        second.energy_score = 0.73
        second.context_score = 1.0
        second.market_quality_score = 0.78
        second.tradability_score = 0.81
        second.axis_confidence = 0.74
        second.response_efficiency_state = "FOLLOW_THROUGH"
        second.decision_posture = "AGGRESSIVE"
        second.forced_flow_state = "NONE"
        second.trap_risk = 0.18

        self.engine._detect_transitions(second)

        self.assertTrue(second.transition_ready)
        self.assertIsNotNone(second.transition_event)
        event = second.transition_event
        assert event is not None
        self.assertEqual(event.transition_family, "FLIP")
        self.assertIn("TO_SHORT", event.transition_code)
        self.assertIn("Đảo chiều", event.transition_alias_vi)
        self.assertEqual(event.from_flow_state_code, first.flow_state_code)
        self.assertEqual(event.to_flow_state_code, second.flow_state_code)
        self.assertEqual(event.decision_shift, "CONSERVATIVE_TO_AGGRESSIVE")

    def test_temporal_tempo_state_acceleration_and_exhaustion(self):
        self.engine._recent_snapshots.clear()
        
        # Helper to create and drive a snapshot through temporal logic
        def mock_snap(init, inv, matrix, agreement=0.5, tradability=0.8, efficiency="MIXED"):
            snap = self.engine._empty_snapshot(0, 300_000)
            snap.initiative_score = init
            snap.inventory_score = inv
            snap.matrix_cell = matrix
            snap.agreement_score = agreement
            snap.tradability_score = tradability
            snap.response_efficiency_state = efficiency
            self.engine._derive_temporal_memory(snap)
            
            # Manually simulate sequence length based on history
            snap.sequence_length = len(list(self.engine._recent_snapshots)) + 1
            
            snap.tempo_state = self.engine._classify_tempo_state(snap)
            snap.persistence_state = self.engine._classify_persistence_state(snap)
            snap.exhaustion_risk = self.engine._estimate_exhaustion_risk(snap, list(self.engine._recent_snapshots))
            self.engine._recent_snapshots.append(snap)
            return snap
            
        s0 = mock_snap(0.1, 0.1, "POS_INIT__POS_INV")
        self.assertEqual(s0.tempo_state, "STABLE")
        self.assertEqual(s0.persistence_state, "EARLY")
        
        mock_snap(0.2, 0.2, "POS_INIT__POS_INV", agreement=0.6)
        mock_snap(0.3, 0.3, "POS_INIT__POS_INV", agreement=0.7)
        
        s3 = mock_snap(0.4, 0.4, "POS_INIT__POS_INV", agreement=0.8)
        # delta = (0.4 - avg(0.1, 0.2, 0.3)) = 0.4 - 0.2 = 0.2 >= 0.12 AND agreement 0.8-avg(...) >= 0.08 => ACCELERATING
        self.assertEqual(s3.tempo_state, "ACCELERATING")
        self.assertEqual(s3.persistence_state, "PERSISTENT")
        
        mock_snap(0.3, 0.3, "POS_INIT__POS_INV", tradability=0.7)
        s5 = mock_snap(0.1, 0.1, "POS_INIT__POS_INV", tradability=0.5)
        # delta_init_3 = 0.1 - avg(0.2,0.3,0.3) = 0.1 - 0.26 = -0.16. abs >= 0.08.
        # tradability dropped. So DECELERATING.
        self.assertEqual(s5.tempo_state, "DECELERATING")
        
        # In new taxonomy, sequence length 6 is PERSISTENT
        self.assertEqual(s5.persistence_state, "PERSISTENT")
        
        # Exhaustion risk must be high enough
        # exhaustion_risk logic: if persistent + decelerating -> 0.2 + 0.3 = 0.5. 
        # still need 0.55 for EXHAUSTING phase.
        # Let's add trap_risk or response_efficiency=ABSORBED_OR_TRAP
        s5.response_efficiency_state = "ABSORBED_OR_TRAP"
        s5.exhaustion_risk = self.engine._estimate_exhaustion_risk(s5, list(self.engine._recent_snapshots))
        
        self.engine._derive_pattern_phase(s5)
        self.assertEqual(s5.pattern_phase, "EXHAUSTING")
        self.assertGreater(s5.exhaustion_risk, 0.5)


if __name__ == "__main__":
    unittest.main()
