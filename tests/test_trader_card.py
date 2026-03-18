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

    # Current compact card format uses inline labels
    assert "Tích lũy âm thầm" in rendered  # setup label
    assert "LONG 🔼" in rendered  # direction label
    assert "Đưa vào danh sách theo dõi" in rendered  # stage label
    assert "Điểm:" in rendered  # score label
    assert "Tin cậy:" in rendered  # confidence label
    assert "Độ phủ:" in rendered  # coverage label
    assert "Cơ sở:" in rendered  # why_now label
    assert "Rủi ro:" in rendered  # conflicts label
    assert "Vô hiệu:" in rendered  # invalidation label
    assert "Vào lệnh:" in rendered  # entry_style label
    assert "Mục tiêu:" in rendered  # targets label
    assert "Delta mua dương" in rendered
    assert "Spread giãn" in rendered
    assert "TP1" in rendered
    assert "TP2" in rendered
