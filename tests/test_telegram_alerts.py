from cfte.alerts.telegram import try_send_text


def test_try_send_text_returns_vi_error_when_missing_credentials():
    ok, error = try_send_text("Xin chào", token=None, chat_id=None)

    assert ok is False
    assert error is not None
    assert "Thiếu cấu hình Telegram" in error
