# Hướng dẫn Deploy — Free Tier (Không cần thẻ Visa)

## Chiến lược tổng quan

Tool này có 2 chế độ chạy:
- **Scan cycle** (2-5 phút): bootstrap → scan → review → watchdog
- **Live loop** (liên tục): bám sát thị trường real-time qua WebSocket

Free-tier tối ưu nhất: **GitHub Actions (scan) + Local (live khi cần)**

---

## 🏆 Option 1: GitHub Actions — Khuyến nghị nhất

**Chi phí: $0** | **Không cần thẻ** | **Chạy 4 lần/ngày tự động**

Đây là cách tối ưu nhất vì:
- 2000 phút miễn phí/tháng (mỗi cycle ~5 phút = 600 phút/tháng cho 4x/ngày)
- Tự động, không cần bật máy
- Lưu artifact để download về review
- Data được cache giữa các lần chạy

### Step 1: Push code lên GitHub

```bash
cd crypto-flow-thesis-engine-starter/crypto-flow-thesis-engine-starter
git add .
git commit -m "Add deployment configs"
git push origin main
```

### Step 2: Enable GitHub Actions

1. Vào repo trên GitHub → **Settings** → **Actions** → General
2. Chọn **Allow all actions**
3. Lưu

### Step 3: Kiểm tra workflows

Vào tab **Actions** sẽ thấy 3 workflows:
- **CFTE Scan Cycle** — chạy mỗi 6 giờ (07:00, 13:00, 19:00, 01:00 ICT)
- **CFTE Weekly Review** — chạy Chủ nhật 08:00 ICT
- **Smoke Test** — chạy khi push code

### Step 4: Chạy thử ngay

1. Vào **Actions** → **CFTE Scan Cycle** → **Run workflow** → chọn branch `main` → **Run**
2. Đợi ~3 phút
3. Khi xong, vào run → **Artifacts** → download `cfte-scan-xxx`

### Step 5: Xem kết quả

Download artifact ZIP, trong đó có:
- `summary_btcusdt.json` — kết quả replay
- `daily_summary.json` — tổng kết ngày
- `health_status.json` — trạng thái hệ thống
- `thesis_log.jsonl` — lịch sử thesis

### Ước tính chi phí thời gian

| Workflow | Tần suất | Thời gian/lần | Tổng/tháng |
|:---|:---|:---|:---|
| Scan Cycle (4x/ngày) | 120 lần/tháng | ~5 phút | ~600 phút |
| Weekly Review | 4 lần/tháng | ~3 phút | ~12 phút |
| Smoke Test | ~10 lần/tháng | ~3 phút | ~30 phút |
| **Tổng** | | | **~642 phút** |
| **Giới hạn free** | | | **2000 phút** |

→ Dư **1358 phút/tháng**, có thể tăng lên 6x/ngày nếu cần.

---

## 🚂 Option 2: Railway — Live Loop Ngắn

**Chi phí: $5 credit free/tháng** | **Không cần thẻ** (đăng nhập bằng GitHub)

Railway cho phép chạy cron job với Dockerfile. Phù hợp cho live loop ngắn (200-500 events).

### Step 1: Đăng ký Railway

1. Vào [railway.app](https://railway.app)
2. **Đăng nhập bằng GitHub** (không cần thẻ)
3. Nhận $5 credit miễn phí/tháng

### Step 2: Tạo project từ GitHub repo

1. **New Project** → **Deploy from GitHub Repo**
2. Chọn repo `crypto-flow-thesis-engine-starter`
3. Railway tự phát hiện `Dockerfile` và `railway.json`
4. Đợi build (~2-3 phút)

### Step 3: Cấu hình

Railway sẽ đọc `railway.json` và tự thiết lập:
- **Build**: Dockerfile
- **Schedule**: Mỗi 4 giờ chạy `run_cycle.py --max-events 200`
- **Restart**: Tự restart khi lỗi (max 3 lần)

### Step 4: Kiểm tra logs

Vào project → **Deployments** → click vào deployment mới nhất → xem **Logs**

### Ước tính chi phí

Railway tính theo thời gian chạy:
- Mỗi cycle: ~5 phút (scan only) hoặc ~15 phút (có live 200 events)
- 6 lần/ngày × 15 phút = 90 phút/ngày = 2700 phút/tháng
- Free tier: ~$5 ≈ 500 giờ vCPU (quá dư)

### Tối ưu: Chỉ chạy live vào giờ cao điểm

Sửa `railway.json` để chỉ chạy live vào khung giờ giao dịch:

```json
{
  "deploy": {
    "startCommand": "PYTHONPATH=src python scripts/run_cycle.py --max-events 300",
    "cronSchedule": "0 1,3,7,9,11,13 * * *"
  }
}
```

(Chạy lúc 08:00, 10:00, 14:00, 16:00, 18:00, 20:00 ICT)

---

## 🖥️ Option 3: Local + Crontab

**Chi phí: $0** | **Cần máy bật**

Phù hợp nhất nếu bạn có máy tính bật thường xuyên.

### Crontab setup

```bash
crontab -e
```

Thêm các dòng sau:

```crontab
# Scan mỗi 4 giờ (07:00, 11:00, 15:00, 19:00, 23:00 ICT)
0 0,4,8,12,16 * * * cd /path/to/crypto-flow-thesis-engine-starter/crypto-flow-thesis-engine-starter && source .venv/bin/activate && PYTHONPATH=src python3 scripts/run_cycle.py --skip-live >> /tmp/cfte_cycle.log 2>&1

# Live burst mỗi 2 giờ trong giờ giao dịch (09:00-21:00 ICT = 02:00-14:00 UTC)
0 2,4,6,8,10,12,14 * * * cd /path/to/crypto-flow-thesis-engine-starter/crypto-flow-thesis-engine-starter && source .venv/bin/activate && PYTHONPATH=src python3 scripts/run_cycle.py --max-events 300 >> /tmp/cfte_live.log 2>&1

# Weekly review Chủ nhật 08:00 ICT
0 1 * * 0 cd /path/to/crypto-flow-thesis-engine-starter/crypto-flow-thesis-engine-starter && source .venv/bin/activate && PYTHONPATH=src python3 -m cfte.cli.main review-week >> /tmp/cfte_weekly.log 2>&1
```

---

## 🧩 Combo tối ưu: GitHub Actions + Local

Kết hợp tốt nhất cho trader cá nhân:

| Nhiệm vụ | Nền tảng | Tần suất |
|:---|:---|:---|
| Scan tự động | GitHub Actions | 4x/ngày (tự động) |
| Weekly review | GitHub Actions | 1x/tuần (Chủ nhật) |
| Live loop | Local (khi đang giao dịch) | Khi cần |
| Review nhanh | Local CLI | Bất kỳ lúc nào |
| Smoke test | GitHub Actions | Khi push code |

### Quy trình hàng ngày

```
Sáng (tự động):    GitHub Actions chạy scan → bạn check artifact
Trong ngày:        Mở terminal → cfte run-live khi muốn bám sát
Tối:               Check daily summary từ GitHub artifact
Cuối tuần:         Download weekly review artifact
```

---

## ❌ Các nền tảng KHÔNG khuyến nghị

| Nền tảng | Lý do |
|:---|:---|
| **Render Free** | Background worker không free, web service ngủ 15 phút |
| **Fly.io** | Cần thẻ Visa |
| **Heroku** | Không còn free tier |
| **Vercel** | Serverless, không phù hợp long-running |
| **Netlify Functions** | 10s timeout, quá ngắn |

---

## Troubleshooting

### Railway build fail?
```bash
# Test Dockerfile locally trước
docker build -t cfte .
docker run --rm cfte
```

### GitHub Actions fail?
- Kiểm tra tab Actions → click vào run fail → xem logs
- Thường do Python version hoặc dependency

### Data bị mất giữa các lần chạy (GitHub Actions)?
- Workflow đã dùng `actions/cache` để giữ data
- Nếu cache hết hạn (7 ngày), SQLite sẽ được init lại
- Thesis log (JSONL) được upload artifact → không mất

### Binance bị chặn trên GitHub Actions?
- Một số runner có thể bị block bởi Binance
- Workaround: dùng `--skip-live` và chỉ chạy live trên local
