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

## Phase 6 — Launch Operating Model

Kiến trúc hiện tại coi `TPFM (Flow Intelligence)` là nguồn chân lý. `Thesis/Setup` vẫn tồn tại để tương thích người dùng cũ, nhưng quyết định vận hành nên bám vào `flow state`, `transition`, `forced flow`, và `runtime health`.

### Launch Checklist

Trước khi mở pilot hoặc public launch:

- [ ] Chạy `cfte bootstrap` để cập nhật schema và artifact mặc định.
- [ ] Chạy `cfte doctor` và xác nhận hệ thống ở trạng thái `HEALTHY` hoặc ít nhất không có `BAD CONFIG`.
- [ ] Xác nhận chỉ có **một** live loop ghi vào mỗi `live_runtime_path`; nếu cần song song nhiều loop, phải tách profile/artifact path riêng.
- [ ] Chạy thử `cfte --profile configs/profiles/personal_binance.yaml run-live --max-events 1` khi đã có loop nền; lệnh phải fail nhanh với thông báo lock conflict thay vì ghi đè artifact.
- [ ] Chạy `cfte --profile configs/profiles/personal_binance.yaml run-live --min-runtime-seconds 330 --run-until-first-m5`.
- [ ] Kiểm tra `data/review/live_runtime.json` có đủ `run_id`, `pid`, `first_m5_seen_at`, `latest_tpfm`, `latest_flow_grade`.
- [ ] Kiểm tra file lock `data/review/live_runtime.json.lock` đã tự biến mất sau khi phiên kết thúc `completed`.
- [ ] Kiểm tra `cfte watchdog` hiển thị được `Matrix gần nhất`, `Flow contract`, và `Transition gần nhất`.
- [ ] Chạy `cfte review-day` và xác nhận report có `flow state scorecard`, `forced flow scorecard`, `transition scorecard`.
- [ ] Chạy `cfte review-week` và `cfte tune-profile` để xác nhận tuning ưu tiên `flow state`.
- [ ] Nếu `doctor` chỉ còn `DEGRADED` do `okx_stale`, quyết định rõ có chấp nhận launch với `Binance + Bybit` làm cặp venue chính hay không.

### Pilot Checklist

Trong 7-14 ngày đầu:

- [ ] Không dùng threshold mặc định làm chân lý; theo dõi `tuning_report.json` mỗi tuần.
- [ ] Mỗi ngày ghi ít nhất 5-10 dòng `log-review` để flow calibration có dữ liệu thật.
- [ ] Đối chiếu `first_m5_seen_at` với thời gian thị trường thực để phát hiện cycle quá ngắn.
- [ ] Nếu `latest_flow_grade` thường xuyên là `D`, ưu tiên đứng ngoài thay vì cố nới threshold.
- [ ] Nếu `latest_transition.transition_family` thiên về `TRAP` hoặc `FORCED`, giảm tần suất vào lệnh đuổi.
- [ ] Kiểm tra `degraded_flags` sau mỗi phiên live; nếu còn lặp lại nhiều ngày thì chưa nên launch rộng.

### Rollback Checklist

Khi cần quay về trạng thái an toàn:

1. Backup `data/state/state.db`.
2. Backup `data/review/live_runtime.json`, `data/review/weekly_review.json`, `data/review/tuning_report.json`.
3. Quay về tag hoặc nhánh ổn định gần nhất.
4. Chạy lại `cfte bootstrap` rồi `cfte doctor`.
5. Chỉ mở pilot lại khi `run-live -> review-day -> watchdog` đã pass trọn chu kỳ.

### Scheduled Cycle Contract

Cycle mặc định qua `scripts/run_cycle.py` hiện được xem là hợp lệ khi:

- `run-live` chạy đủ `min_runtime_seconds`
- phiên live sinh được `first_m5_seen_at`
- runtime artifact có `latest_tpfm.matrix_cell`
- watchdog không báo thiếu flow contract

Nếu thiếu một trong các điều kiện trên, cycle nên bị xem là `degraded` hoặc `fail`, không nên dùng để cập nhật dashboard hay tuning.
