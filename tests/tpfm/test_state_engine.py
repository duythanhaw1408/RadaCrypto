import unittest
from unittest.mock import MagicMock
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
        snap.escalation_reason = ["Test Reason"]
        snap.futures_context_available = False
        card = render_tpfm_m5_card(snap)
        self.assertIn("TPFM M5 ESCALATION", card)
        self.assertIn("THIẾU CONTEXT FUTURES", card)

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
        
        self.assertEqual(snap.context_score, -1.0) # Divergence
        self.assertLess(snap.tradability_score, 0.5) # Penalized
        self.assertEqual(snap.micro_conclusion, "ABSORBED_BY_FUTURES")
        self.assertIn("FUTURES LỆCH NHỊP", snap.context_warning_flags[0])

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

if __name__ == "__main__":
    unittest.main()
