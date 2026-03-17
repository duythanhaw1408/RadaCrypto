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
