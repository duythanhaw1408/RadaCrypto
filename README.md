# Crypto Flow Thesis Engine Starter

Local-first, replay-first starter repo for a crypto flow thesis engine.

## What this starter includes
- Binance public stream collector skeleton
- Local order book reconstruction
- Raw parquet writer
- Tape feature computation
- Deterministic thesis engine
- Trader-card formatter
- SQLite bootstrap SQL
- Replay entrypoint
- Basic tests

## Quick start

1. Create a virtualenv
   python -m venv .venv
   source .venv/bin/activate

2. Install
   python3 -m pip install -e .

3. Initialize SQLite
   python scripts/init_sqlite_db.py

4. Run tests
   pytest -q

## Personal-use CLI flow

Mục tiêu của shell CLI là giúp trader cá nhân chạy một chu kỳ (flow) ổn định mỗi ngày.

### 1. Chuẩn bị (Setup)

```bash
# Cài đặt môi trường sạch (nếu chưa)
python -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
python scripts/init_sqlite_db.py
```

### 2. Bootstrap + Doctor

Khuyến nghị chạy bootstrap một lần sau mỗi lần pull/update để làm sạch các lỗi môi trường thường gặp:
```bash
cfte bootstrap
cfte doctor
```

- `bootstrap`: tạo thư mục cần thiết, khởi tạo SQLite schema và lưu `data/review/health_status.json`.
- `doctor`: kiểm tra Python runtime, dependency cốt lõi, profile, SQLite và các artifact vận hành.

### 3. Quy trình hàng ngày (Daily Workflow)

Sử dụng bộ hồ sơ cá nhân (`--profile`) để tự động hóa tham số:

| Lệnh | Ý nghĩa | Ví dụ |
| :--- | :--- | :--- |
| **bootstrap** | Chuẩn bị môi trường chạy hằng ngày | `cfte bootstrap` |
| **doctor** | Kiểm tra hệ thống + health report | `cfte doctor` |
| **run-scan** | Quét nhanh cơ hội | `cfte --profile configs/profiles/personal_binance.yaml run-scan` |
| **run-live** | Bám sát thị trường | `cfte --profile configs/profiles/personal_binance.yaml run-live` |
| **review-thesis** | Dashboard luận điểm | `cfte review-thesis` |
| **review-day** | Tổng kết cuối ngày | `cfte review-day` |
| **log-review** | Ghi quyết định vào/bỏ/phớt lờ từng thesis | `cfte log-review --thesis-id <id> --decision taken --usefulness useful` |
| **review-log** | Tổng hợp journal review cá nhân | `cfte review-log --start-date 2026-03-01 --end-date 2026-03-07` |
| **tune-profile** | Gợi ý siết/nới threshold cá nhân | `cfte tune-profile` |

### 4. Các hồ sơ hỗ trợ sẵn

- `configs/profiles/personal_binance.yaml`: Phổ thông (BTC/ETH)
- `configs/profiles/personal_binance_onchain.yaml`: Theo dõi hệ sinh thái (SOL)
- `configs/profiles/personal_replay.yaml`: Nghiên cứu & Backtest

## Notes
- Giữ nguyên kiến trúc local-first và replay-first.
- CLI chỉ đóng vai trò "Product Shell" cho trader cá nhân.
- Toàn bộ output người dùng mặc định là tiếng Việt.


## Stage 4 — Operate & Tune

Luồng vận hành mới tập trung vào review lặp lại và tuning tối thiểu:

1. `cfte run-scan` hoặc `cfte run-live` để tạo thesis.
2. `cfte log-review ...` để ghi nhanh từng thesis bạn **vào lệnh / bỏ qua / phớt lờ** và đánh giá **hữu ích / trung tính / nhiễu**.
3. `cfte review-day` để tạo daily summary có kèm review journal.
4. `cfte review-week` để tổng hợp weekly review + scorecard + tuning suggestions.
5. `cfte tune-profile` khi muốn xem riêng gợi ý ngưỡng threshold theo setup.

Các workflow GitHub Actions daily/weekly sẽ sinh artifact review JSON ổn định để bạn tải xuống và đối chiếu trong pilot 7–14 ngày.


## Stage 5 — Daily Reliability & Product Hardening

Các bổ sung vận hành cá nhân cho chặng 5:

- `cfte health` giờ sinh báo cáo rõ trạng thái **healthy / degraded / bad config** bằng tiếng Việt và lưu artifact `data/review/health_status.json`.
- `cfte run-live` có watchdog `watchdog_idle_seconds`, heartbeat `heartbeat_interval`, và luôn lưu runtime artifact `data/review/live_runtime.json` để bạn biết loop dừng vì timeout, runtime error hay hoàn tất bình thường.
- Các profile cá nhân có thêm khóa `review.health_report_path`, `review.live_runtime_path`, `live.watchdog_idle_seconds`, `live.heartbeat_interval` để giảm sửa tay khi chạy lặp lại.
- Khi update môi trường, hãy chạy lại tuần tự: `python3 -m pip install -e .` -> `cfte bootstrap` -> `cfte doctor`.

### Quy trình daily ngắn gọn

```bash
python3 -m pip install -e .
cfte bootstrap
cfte doctor
cfte run-scan
cfte run-live --max-events 500
cfte review-day
```

### Cách đọc degraded state

- **BAD CONFIG**: lỗi chặn chạy ổn định, thường là Python version hoặc dependency lõi.
- **DEGRADED**: vẫn chạy được nhưng thiếu artifact, replay mặc định, hoặc môi trường chưa đủ sạch.
- **Runtime artifact**: kiểm tra `status`, `last_error`, `stale_gap_seconds`, `processed_events` để biết loop live chết ở đâu.
