from cfte.storage.review_journal import (
    build_flow_state_tuning_suggestions,
    build_forced_flow_tuning_suggestions,
    build_matrix_tuning_suggestions,
    build_transition_tuning_suggestions,
    render_tuning_report_vi,
)


def test_build_matrix_tuning_suggestions_tightens_divergent_negative_bucket():
    rows = [
        {
            "matrix_cell": "NEG_INIT__POS_INV",
            "matrix_alias_vi": "Bán gặp hấp thụ mua",
            "spot_futures_relation": "DIVERGENT",
            "venue_confirmation_state": "DIVERGENT",
            "liquidation_bias": "MIXED",
            "tradability_grade": "D",
            "horizons": {"24h": {"count": 5, "wins": 1, "win_rate": 0.2, "avg_edge": -1.8, "avg_mae": 42.0, "avg_mfe": 10.0}},
        }
    ]

    suggestions = build_matrix_tuning_suggestions(rows, base_threshold=74.0)

    assert suggestions[0]["suggested_threshold"] > 74.0
    assert suggestions[0]["action"] == "tighten"
    assert any("đa sàn phân kỳ" in reason for reason in suggestions[0]["rationale_vi"])


def test_build_matrix_tuning_suggestions_loosens_confirmed_positive_bucket():
    rows = [
        {
            "matrix_cell": "POS_INIT__POS_INV",
            "matrix_alias_vi": "Thuận pha mua",
            "spot_futures_relation": "CONFLUENT",
            "venue_confirmation_state": "CONFIRMED",
            "liquidation_bias": "SHORTS_FLUSHED",
            "tradability_grade": "A",
            "horizons": {"24h": {"count": 6, "wins": 5, "win_rate": 0.83, "avg_edge": 2.1, "avg_mae": 14.0, "avg_mfe": 55.0}},
        }
    ]

    suggestions = build_matrix_tuning_suggestions(rows, base_threshold=74.0)

    assert suggestions[0]["suggested_threshold"] < 74.0
    assert suggestions[0]["action"] == "loosen"
    assert any("đa sàn xác nhận" in reason for reason in suggestions[0]["rationale_vi"])


def test_render_tuning_report_vi_includes_matrix_section():
    text = render_tuning_report_vi(
        [{"setup": "distribution", "current_threshold": 74.0, "suggested_threshold": 79.0, "avg_edge_24h": -1.2, "win_rate_24h": 0.3, "avg_mae": 35.0, "avg_mfe": 9.0, "rationale_vi": ["test"]}],
        [{
            "matrix_cell": "POS_INIT__POS_INV",
            "matrix_alias_vi": "Thuận pha mua",
            "spot_futures_relation": "CONFLUENT",
            "venue_confirmation_state": "CONFIRMED",
            "liquidation_bias": "SHORTS_FLUSHED",
            "tradability_grade": "A",
            "current_threshold": 74.0,
            "suggested_threshold": 71.0,
            "avg_edge": 2.1,
            "horizon": "24h",
            "win_rate": 0.83,
            "sample_size": 6,
            "rationale_vi": ["đa sàn xác nhận"],
        }],
        [],
        [{
            "flow_state_code": "LONG_CONTINUATION__FOLLOW_THROUGH",
            "forced_flow_state": "SQUEEZE_LED",
            "inventory_defense_state": "BID_DEFENSE",
            "decision_posture": "AGGRESSIVE",
            "tradability_grade": "A",
            "current_threshold": 74.0,
            "suggested_threshold": 72.0,
            "avg_edge": 2.4,
            "horizon": "24h",
            "win_rate": 0.8,
            "sample_size": 5,
            "rationale_vi": ["flow continuation rõ"],
        }],
        [{
            "forced_flow_state": "SQUEEZE_LED",
            "liquidation_bias": "SHORTS_FLUSHED",
            "basis_state": "BALANCED",
            "tradability_grade": "A",
            "current_threshold": 74.0,
            "suggested_threshold": 73.0,
            "avg_edge": 1.7,
            "horizon": "24h",
            "win_rate": 0.67,
            "sample_size": 5,
            "rationale_vi": ["squeeze-led đang hỗ trợ đúng hướng"],
        }],
    )

    assert "Theo setup" in text
    assert "Theo flow state" in text
    assert "Theo forced flow" in text
    assert "Theo matrix/context" in text
    assert "Thuận pha mua" in text


def test_build_flow_state_tuning_suggestions_tightens_high_trap_bucket():
    rows = [
        {
            "flow_state_code": "LONG_TRAP__FAILING_FOLLOW_THROUGH",
            "forced_flow_state": "GAP_LED",
            "inventory_defense_state": "NONE",
            "decision_posture": "AGGRESSIVE",
            "tradability_grade": "D",
            "avg_trap_risk": 0.71,
            "avg_forced_flow_intensity": 0.92,
            "avg_context_quality_score": 0.28,
            "horizons": {"24h": {"count": 5, "wins": 1, "win_rate": 0.2, "avg_edge": -1.7, "avg_mae": 39.0, "avg_mfe": 12.0}},
        }
    ]

    suggestions = build_flow_state_tuning_suggestions(rows, base_threshold=74.0)

    assert suggestions[0]["suggested_threshold"] > 74.0
    assert suggestions[0]["action"] == "tighten"
    assert any("trap" in reason.lower() for reason in suggestions[0]["rationale_vi"])


def test_build_forced_flow_tuning_suggestions_loosens_clean_squeeze_bucket():
    rows = [
        {
            "forced_flow_state": "SQUEEZE_LED",
            "liquidation_bias": "SHORTS_FLUSHED",
            "basis_state": "BALANCED",
            "tradability_grade": "A",
            "avg_forced_flow_intensity": 1.14,
            "avg_liquidation_intensity": 1.02,
            "avg_trap_risk": 0.22,
            "horizons": {"24h": {"count": 6, "wins": 5, "win_rate": 0.83, "avg_edge": 1.8, "avg_mae": 16.0, "avg_mfe": 57.0}},
        }
    ]

    suggestions = build_forced_flow_tuning_suggestions(rows, base_threshold=74.0)

    assert suggestions[0]["suggested_threshold"] < 74.0
    assert suggestions[0]["action"] == "loosen"
    assert any("SQUEEZE_LED" in reason for reason in suggestions[0]["rationale_vi"])


def test_build_transition_tuning_suggestions_respects_transition_family():
    rows = [
        {
            "transition_code": "TRAP_FLIP_TO_LONG",
            "transition_family": "TRAP_FLIP",
            "transition_alias_vi": "Lật sang mua nhưng mang tính bẫy",
            "avg_transition_quality": 0.31,
            "avg_transition_speed": 0.72,
            "avg_persistence_score": 0.18,
            "avg_trap_risk": 0.74,
            "forced_ratio": 0.0,
            "horizons": {"24h": {"count": 5, "wins": 1, "win_rate": 0.2, "avg_edge": -1.6, "avg_mae": 38.0, "avg_mfe": 12.0}},
        }
    ]

    suggestions = build_transition_tuning_suggestions(rows, base_threshold=74.0)

    assert suggestions[0]["suggested_threshold"] > 74.0
    assert suggestions[0]["action"] == "tighten"
    assert any("trap" in reason.lower() for reason in suggestions[0]["rationale_vi"])
