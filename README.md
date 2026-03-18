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

### 2. Kiểm tra (Doctor)

Đảm bảo các tệp cấu hình và dữ liệu cơ bản đã sẵn sàng:
```bash
cfte doctor
```

### 3. Quy trình hàng ngày (Daily Workflow)

Sử dụng bộ hồ sơ cá nhân (`--profile`) để tự động hóa tham số:

| Lệnh | Ý nghĩa | Ví dụ |
| :--- | :--- | :--- |
| **doctor** | Kiểm tra hệ thống | `cfte doctor` |
| **run-scan** | Quét nhanh cơ hội | `cfte --profile configs/profiles/personal_binance.yaml run-scan` |
| **run-live** | Bám sát thị trường | `cfte --profile configs/profiles/personal_binance.yaml run-live` |
| **review-thesis** | Dashboard luận điểm | `cfte review-thesis` |
| **review-day** | Tổng kết cuối ngày | `cfte review-day` |

### 4. Các hồ sơ hỗ trợ sẵn

- `configs/profiles/personal_binance.yaml`: Phổ thông (BTC/ETH)
- `configs/profiles/personal_binance_onchain.yaml`: Theo dõi hệ sinh thái (SOL)
- `configs/profiles/personal_replay.yaml`: Nghiên cứu & Backtest

## Notes
- Giữ nguyên kiến trúc local-first và replay-first.
- CLI chỉ đóng vai trò "Product Shell" cho trader cá nhân.
- Toàn bộ output người dùng mặc định là tiếng Việt.
