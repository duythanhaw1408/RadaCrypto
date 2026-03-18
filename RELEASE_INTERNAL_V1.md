# Internal Release Note: v1-internal-rc1

Bản phát hành này đánh dấu cột mốc **Release Candidate 1** cho đội ngũ nội bộ. Hệ thống hiện đã sẵn sàng cho việc kiểm thử thực tế và đánh giá độ ổn định của các tín hiệu luận điểm (thesis signals).

## 1. Phạm vi v1 Nội bộ
Tập trung vào tính toàn vẹn của luồng dữ liệu (data integrity) từ Binance, khả năng tái hiện (replayability) và độ chính xác của các logic tính toán đặc tính (feature computation).

## 2. Những gì đã hỗ trợ
- **Collector**: Binance Public WS (aggTrade, bookTicker, depth).
- **Engine**: Order book reconstruction (LOB), Tape metrics, Stealth Accumulation & Distribution engines.
- **Localization**: Toàn bộ giao diện CLI, Telegram và Trader-card đã được bản địa hóa tiếng Việt.
- **Replay**: Cơ chế replay deterministic cho phép kiểm thử lại các kịch bản quá khứ với cùng một kết quả.
- **Storage**: Lưu trữ Parquet (Raw Lake) và SQLite (Local State) hoạt động ổn định.

## 3. Watch Items (Cần theo dõi, không phải blocker)
- **Binance Health**: Bề mặt giám sát sức khỏe (monitoring surface) còn mỏng hơn so với các adapter Bybit/OKX sắp tới.
- **Auto-resolve**: Cơ chế tự động đóng luận điểm (resolve) ở cuối stream replay hiện tại còn ở mức tối giản.
- **Localization Edge Cases**: Có thể vẫn còn một số chuỗi ký tự tiếng Anh ở các trường hợp lỗi sâu (edge cases).
- **On-chain Adapter**: Hiện tại là **Optional**, không ảnh hưởng đến hot-path của market data.

## 4. Cách chạy Smoke Test
Sau khi deploy hoặc update, hãy chạy chuỗi lệnh sau:
```bash
source .venv/bin/activate
# 1. Kiểm tra file hệ thống
python -m cfte.cli.main doctor
# 2. Chạy unit tests
pytest -q
# 3. Chạy simulation với dữ liệu mẫu
python scripts/replay_binance_public.py
```

## 5. Cách bật Telegram Alert
1. Cập nhật `TELEGRAM_BOT_TOKEN` và `TELEGRAM_CHAT_ID` vào file `.env`.
2. Hệ thống sẽ tự động gửi Trader-card bằng tiếng Việt khi có tín hiệu `CONFIRMED` hoặc `ACTIONABLE`.

## 6. Checklist xác nhận trước khi dùng nội bộ
- [ ] **SQLite Bootstrap**: Chạy lệnh `python scripts/init_sqlite_db.py` ổn định trên máy mục tiêu.
- [ ] **Replay Fingerprint**: Chạy `replay_binance_public.py` cho ra kết quả khớp với baseline (xem walkthrough).
- [ ] **Binance Connection**: Đảm bảo môi trường thật có thể kết nối đến `wss://stream.binance.com`.
- [ ] **Telegram Credentials**: Kiểm tra gửi tin nhắn test thành công nếu bật tính năng alert.

---
*Ghi chú: Giữ kiến trúc local-first và replay-first làm ưu tiên hàng đầu trong mọi điều chỉnh.*
