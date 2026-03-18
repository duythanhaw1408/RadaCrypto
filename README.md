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

Mục tiêu của shell CLI là giúp trader cá nhân chạy một flow ổn định mỗi ngày mà không phải nhớ script rời rạc.

### Entrypoint ổn định

Sau khi cài `pip install -e .`, bạn có thể dùng trực tiếp:

```bash
cfte doctor
cfte run-scan
cfte run-live --max-events 25
cfte review-day
cfte health
```

Nếu muốn chạy bằng module:

```bash
python -m cfte.cli.main doctor
python -m cfte.cli.main run-scan
```

### Hồ sơ cấu hình cá nhân

CLI mặc định dùng hồ sơ `configs/profiles/personal.default.yaml`.

Bạn có thể tạo một hồ sơ riêng rồi truyền vào bằng `--profile`:

```bash
cfte --profile configs/profiles/personal.default.yaml doctor
```

Các phần cấu hình cá nhân hiện hỗ trợ:
- trader display name
- symbol mặc định
- đường dẫn replay mặc định
- ngưỡng scan cá nhân
- đường dẫn log thesis cho scan/live
- tham số ingest live cơ bản

## Daily workflow đề xuất

### 1) Kiểm tra hệ thống

```bash
cfte doctor
```

### 2) Chạy replay deterministic

```bash
cfte replay \
  --events fixtures/replay/btcusdt_normalized.jsonl \
  --summary-out data/replay/summary_btcusdt.json
```

### 3) Quét cơ hội theo hồ sơ cá nhân

```bash
cfte run-scan --events fixtures/replay/btcusdt_normalized.jsonl --limit 3
```

`run-scan` sẽ tự động ghi summary replay theo hồ sơ cá nhân và append log thesis JSONL để trader xem lại sau.

### 4) Chạy ingest live cho phiên theo dõi

```bash
cfte run-live --symbol BTCUSDT --max-events 25
```

`run-live` giữ tối thiểu ingest thật từ Binance public, đánh giá thesis theo cửa sổ trade gần nhất, ghi raw parquet, và append thesis log JSONL cho từng nhịp trade. Nếu snapshot đầu vào lỗi, shell sẽ báo trạng thái suy giảm thay vì crash mơ hồ.

### 5) Xem review cuối ngày

```bash
cfte review-day --summary data/replay/summary_btcusdt.json
```

### 6) Kiểm tra trạng thái shell trước live

```bash
cfte health
```

## Notes
- Giữ nguyên kiến trúc local-first và replay-first.
- CLI chỉ productize shell sử dụng cá nhân, không thay đổi roadmap lõi.
- Các module features/thesis/replay hiện có vẫn được giữ nguyên.
