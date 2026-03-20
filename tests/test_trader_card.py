from cfte.models.events import ThesisSignal
from cfte.thesis.cards import render_trader_card


def test_render_trader_card_vietnamese_first_labels():
    signal = ThesisSignal(
        thesis_id="abc123",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="stealth_accumulation",
        direction="LONG_BIAS",
        stage="WATCHLIST",
        score=66.5,
        confidence=0.69,
        coverage=0.8,
        why_now=["Delta mua dương"],
        conflicts=["Spread giãn"],
        invalidation="Mất bid hỗ trợ dưới 100.00",
        entry_style="Canh hồi về bid",
        targets=["TP1", "TP2"],
    )

    rendered = render_trader_card(signal)

    assert "Tích lũy âm thầm" in rendered  # setup label
    assert "LONG 🔼" in rendered  # direction label
    assert "Đưa vào danh sách theo dõi" in rendered  # stage label
    assert "THEO DÕI" in rendered  # priority tag
    assert "Hành động:" in rendered
    assert "Chỉ số: Score" in rendered  # score label
    assert "Conf" in rendered  # confidence label
    assert "Cov" in rendered  # coverage label
    assert "Cơ sở:" in rendered  # why_now label
    assert "Rủi ro:" in rendered  # conflicts label
    assert "Vô hiệu:" in rendered  # invalidation label
    assert "Vào:" in rendered  # entry_style label
    assert "Mục tiêu:" in rendered  # targets label
    assert "Delta mua dương" in rendered
    assert "Spread giãn" in rendered
    assert "TP1" in rendered
    assert "TP2" in rendered


def test_render_trader_card_formats_market_metrics_for_fast_reading():
    signal = ThesisSignal(
        thesis_id="metric123",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="breakout_ignition",
        direction="LONG_BIAS",
        stage="ACTIONABLE",
        score=86.0,
        confidence=0.90,
        coverage=0.8,
        why_now=[
            "Dòng tiền chủ động theo hướng mua: 17399.39",
            "Lực đỡ bid rõ ràng: 0.57",
            "Spread nén thuận lợi cho bứt phá: 0.00 bps",
            "Thanh khoản hấp thụ tốt trước điểm nổ: 466117.35",
        ],
        conflicts=[],
        invalidation="Thất bại giữ trên microprice 70204.14",
        entry_style="Theo breakout có xác nhận volume và giữ cấu trúc bid",
        targets=["TP1: Mở rộng 1R", "TP2: Mở rộng 2R"],
    )

    rendered = render_trader_card(signal)

    assert "ƯU TIÊN CAO" in rendered
    assert "17.40K" in rendered
    assert "57%" in rendered
    assert "<0.01 bps" in rendered
    assert "+1 yếu tố nữa" in rendered
    assert "70,204.14" in rendered


def test_render_trader_card_prioritizes_flow_header_when_available():
    signal = ThesisSignal(
        thesis_id="flow1234",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="breakout_ignition",
        direction="LONG_BIAS",
        stage="ACTIONABLE",
        score=88.0,
        confidence=0.91,
        coverage=0.84,
        why_now=["Matrix TPFM: Thuận pha mua"],
        conflicts=[],
        invalidation="Initiative mất dương hoặc inventory không còn đỡ.",
        entry_style="Microprice retest + Bid replenishment",
        targets=["TP1", "TP2"],
        matrix_cell="POS_INIT__POS_INV",
        matrix_alias_vi="Thuận pha mua",
        flow_state="LONG_CONTINUATION__FOLLOW_THROUGH",
        tradability_grade="A",
        decision_posture="AGGRESSIVE",
        decision_summary_vi="Ưu tiên continuation long khi microprice giữ vững.",
    )

    rendered = render_trader_card(signal)

    assert "BINANCE:BTCUSDT:SPOT | Thuận pha mua | Grade A" in rendered
    assert "Flow: LONG_CONTINUATION__FOLLOW_THROUGH | Posture AGGRESSIVE" in rendered
    assert "Decision: Ưu tiên continuation long khi microprice giữ vững." in rendered
    assert "Setup tương thích: Kích hoạt bứt phá | LONG 🔼" in rendered
