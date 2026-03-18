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

    assert "Thiết lập" in rendered
    assert "Xu hướng" in rendered
    assert "Độ tin cậy" in rendered
    assert "Lý do lúc này" in rendered
    assert "Yếu tố mâu thuẫn" in rendered
    assert "Điểm vô hiệu" in rendered
    assert "Cách vào lệnh" in rendered
    assert "Mục tiêu" in rendered
    assert "Trạng thái luận điểm" in rendered
    assert "Tích lũy âm thầm" in rendered
    assert "Ưu tiên kịch bản tăng" in rendered
    assert "Đưa vào danh sách theo dõi" in rendered
