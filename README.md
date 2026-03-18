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
   pip install -e .

3. Copy env
   cp .env.example .env

4. Initialize SQLite
   python scripts/init_sqlite_db.py

5. Run tests
   pytest -q

6. Doctor check
   python -m cfte.cli.main doctor

## How to Run
To run the project again once set up:

```bash
cd crypto-flow-thesis-engine-starter
source .venv/bin/activate
# Kiểm tra trạng thái
python3 -m cfte.cli.main doctor
# Chạy tests
pytest
# Chạy simulation
python3 scripts/replay_binance_public.py
```

### Source-Path Execution (Workaround)
Nếu `pip install -e .` chưa nhận diện đúng package, bạn có thể chạy trực tiếp bằng cách chỉ định `PYTHONPATH`:

```bash
source .venv/bin/activate
PYTHONPATH=src pytest -q
PYTHONPATH=src python3 -m cfte.cli.main doctor
PYTHONPATH=src python3 scripts/replay_binance_public.py
```
*Lưu ý: Đây là source-path execution, không thay thế hoàn toàn cho việc kiểm tra packaging/installation.*

## Phase 1 goal
Binance-centric vertical slice:
- market data normalization
- order book reconstruction
- tape metrics
- initial deterministic thesis signals
- replayable research path

## Notes
This is a starter scaffold, not a production terminal.
Keep the architecture local-first and replay-first.


## Replay research workflow (Phase 1C)
- Dữ liệu replay mẫu: `fixtures/replay/btcusdt_normalized.jsonl`
- Chạy replay deterministic:
  `python -m cfte.cli.main replay-research --events fixtures/replay/btcusdt_normalized.jsonl --summary-out data/replay/summary_btcusdt.json`
- Kết quả tóm tắt được lưu JSON để phục vụ phân tích lặp lại.
