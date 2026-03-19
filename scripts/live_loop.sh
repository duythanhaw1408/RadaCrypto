#!/bin/bash
# CFTE Live Loop — Auto-start script
# Chạy live loop liên tục, tự restart khi crash

cd /Users/nguyenduythanh/Downloads/crypto-flow-thesis-engine-starter/crypto-flow-thesis-engine-starter
source .venv/bin/activate
export PYTHONPATH=src

while true; do
    echo "[$(date)] Bắt đầu live loop..."
    python3 -m cfte.cli.main run-live --symbol BTCUSDT 2>&1 | tee -a /tmp/cfte_live.log
    echo "[$(date)] Live loop kết thúc. Restart sau 10s..."
    sleep 10
done
