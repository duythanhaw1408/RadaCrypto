#!/bin/bash

# RadaCrypto One-Click Startup Script
# ----------------------------------

# 1. Chuyển vào thư mục dự án
cd "$(dirname "$0")"

echo "🚀 Đang khởi động RadaCrypto Professional..."

# Kiểm tra Gemini API Key
if [ -z "$GEMINI_API_KEY" ] && [ ! -f .env ]; then
  echo "⚠️  CẢNH BÁO: Chưa tìm thấy GEMINI_API_KEY."
  echo "💡 AI Specialist sẽ không hoạt động. Hãy copy .env.example thành .env và dán Key vào."
  echo "--------------------------------------------------------------------------"
fi

# 2. Dọn dẹp các phiên cũ đang chạy (tránh lỗi Lock)
echo "🧹 Đang dọn dẹp các phiên cũ..."
# Tìm và dừng các process đang chạy cfte run-live
ps aux | grep "cfte/cli/main.py run-live" | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null
# Xóa file lock thủ công để đảm bảo chắc chắn
rm -f data/review/live_runtime.json.lock

# 3. Khởi động Dashboard Server (Chạy ngầm)
echo "🌐 Đang mở Dashboard tại http://localhost:8686"
lsof -ti:8686 | xargs kill -9 2>/dev/null
python3 -m http.server 8686 --directory docs > /dev/null 2>&1 &
SERVER_PID=$!

# Đợi server lên
sleep 2

# 3. Khởi động Live Engine (Chạy chính)
echo "⚡ Đang kết nối dòng tiền BTCUSDT..."
echo "💡 Nhấn Ctrl+C để dừng toàn bộ hệ thống."

# Sử dụng PYTHONPATH để đảm bảo import đúng
export PYTHONPATH=$PYTHONPATH:$(pwd)/src

# Chạy loop liên tục (bỏ --max-events để chạy vô hạn, hoặc để 500 nếu muốn check nhanh)
python3 src/cfte/cli/main.py run-live --symbol BTCUSDT --max-events 3000

# Khi người dùng Ctrl+C, script sẽ dừng cả server
kill $SERVER_PID
echo "👋 Đã dừng hệ thống."
