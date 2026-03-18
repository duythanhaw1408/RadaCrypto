#!/bin/bash

# Crypto Flow Thesis Engine - Personal Bootstrap Script
echo "🚀 Đang khởi tạo môi trường cfte cho sử dụng cá nhân..."

# 1. Create Virtual Environment if not exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# 2. Activate and Install dependencies
echo "Installing dependencies..."
source .venv/bin/activate
pip install --upgrade pip
pip install -e .

# 3. Initialize necessary directories
echo "Creating data directories..."
mkdir -p data/state data/raw data/replay data/thesis

# 4. Initialize SQLite DB
if [ ! -f "data/state/state.db" ]; then
    echo "Initializing SQLite database..."
    python3 scripts/init_sqlite_db.py
else
    echo "SQLite database already exists."
fi

# 5. Create .env if not exists
if [ ! -f ".env" ]; then
    echo "Creating .env template..."
    cp .env.example .env
    echo "TIP: Please edit .env with your TELEGRAM_BOT_TOKEN and other keys."
fi

# 6. Run doctor check
echo "Running doctor health check..."
python3 -m cfte.cli.main doctor

echo "✅ Hoàn tất cài đặt! Bạn có thể bắt đầu sử dụng lệnh 'cfte'."
echo "Gợi ý: Thêm 'alias cfte=\"python3 -m cfte.cli.main\"' vào ~/.zshrc hoặc ~/.bashrc để tiện dùng."
