#!/usr/bin/env python3
"""
CFTE Pro Dashboard — Giao diện web cao cấp theo dõi Crypto Flow Thesis Engine.
Chạy: PYTHONPATH=src python3 scripts/dashboard.py
Truy cập: http://localhost:8686
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "data"
STATE_DB = DATA / "state" / "state.db"
PORT = 8686


def _read_json(path: Path) -> dict | list | None:
    try:
        if not path.exists(): return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_jsonl_tail(path: Path, n: int = 50) -> list[dict]:
    try:
        if not path.exists(): return []
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        return [json.loads(l) for l in lines[-n:]]
    except Exception:
        return []


def _read_log_tail(path: Path, n: int = 100) -> str:
    try:
        if not path.exists(): return "(Chưa có log)"
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        return "\n".join(lines[-n:])
    except Exception:
        return "(Lỗi đọc log)"


def _query_db():
    if not STATE_DB.exists():
        return {"thesis_count": 0, "outcome_count": 0, "event_count": 0, "theses": [], "tpfm_m5": [], "tpfm_m30": [], "tpfm_4h": []}
    try:
        conn = sqlite3.connect(STATE_DB)
        conn.row_factory = sqlite3.Row
        
        tc = conn.execute("SELECT COUNT(*) FROM thesis").fetchone()[0]
        oc = conn.execute("SELECT COUNT(*) FROM thesis_outcome").fetchone()[0]
        ec = conn.execute("SELECT COUNT(*) FROM thesis_event").fetchone()[0]
        
        theses = [dict(r) for r in conn.execute(
            "SELECT * FROM thesis ORDER BY opened_ts DESC LIMIT 50"
        ).fetchall()]
        
        tpfm_m5 = [dict(r) for r in conn.execute(
            "SELECT * FROM tpfm_m5_snapshot ORDER BY window_end_ts DESC LIMIT 100"
        ).fetchall()]
        
        tpfm_m30 = [dict(r) for r in conn.execute(
            "SELECT * FROM tpfm_m30_regime ORDER BY window_end_ts DESC LIMIT 50"
        ).fetchall()]
        
        tpfm_4h = [dict(r) for r in conn.execute(
            "SELECT * FROM tpfm_4h_structural ORDER BY window_end_ts DESC LIMIT 10"
        ).fetchall()]
        
        conn.close()
        return {
            "thesis_count": tc, "outcome_count": oc, "event_count": ec, 
            "theses": theses, "tpfm_m5": tpfm_m5, "tpfm_m30": tpfm_m30, "tpfm_4h": tpfm_4h
        }
    except Exception as e:
        return {"error": str(e)}


def _build_api_data():
    health = _read_json(DATA / "review" / "health_status.json")
    daily = _read_json(DATA / "review" / "daily_summary.json")
    weekly = _read_json(DATA / "review" / "weekly_review.json")
    thesis_log = _read_jsonl_tail(DATA / "thesis" / "thesis_log.jsonl", 50)
    live_log = _read_log_tail(Path("/tmp/cfte_live_stdout.log"), 100)
    db = _query_db()
    
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "health": health,
        "daily": daily,
        "weekly": weekly,
        "thesis_log": thesis_log,
        "live_log": live_log,
        "db": db,
    }


HTML_PAGE = """<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CFTE Pro Dashboard — Crypto Flow Thesis Engine</title>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0b0e14; --bg-glass: rgba(17, 25, 40, 0.75); --card: rgba(23, 31, 46, 0.8);
  --border: rgba(255, 255, 255, 0.1); --text: #f0f3f8; --muted: #94a3b8;
  --accent: #38bdf8; --green: #10b981; --red: #ef4444; --yellow: #f59e0b;
  --purple: #818cf8; --cyan: #22d3ee; --grad: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { 
  background: var(--bg); color: var(--text); font-family: 'Outfit', sans-serif; 
  font-size: 14px; min-height: 100vh; overflow-x: hidden;
  background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b0e14 100%);
}
.container { max-width: 1440px; margin: 0 auto; padding: 24px; }

/* Header & Glassmorphism */
header {
  backdrop-filter: blur(16px) saturate(180%); -webkit-backdrop-filter: blur(16px) saturate(180%);
  background-color: var(--bg-glass); border-radius: 16px; border: 1px solid var(--border);
  padding: 24px; display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px;
}
header h1 { font-size: 24px; font-weight: 700; background: var(--grad); -webkit-background-clip: text; -webkit-fill-color: transparent; }
.header-meta { display: flex; align-items: center; gap: 20px; font-size: 13px; color: var(--muted); }

/* KPI Cards */
.grid-kpi { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; margin-bottom: 24px; }
.card-kpi {
  background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 24px;
  backdrop-filter: blur(8px); transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}
.card-kpi:hover { transform: translateY(-4px); border-color: var(--accent); }
.kpi-label { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: var(--muted); margin-bottom: 8px; }
.kpi-value { font-size: 36px; font-weight: 800; }
.kpi-sub { font-size: 13px; margin-top: 6px; color: var(--muted); }

/* Navigation Tab */
.tabs { display: flex; gap: 8px; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 12px; margin-bottom: 24px; width: fit-content; }
.tab {
  padding: 10px 24px; cursor: pointer; color: var(--muted); border-radius: 8px;
  font-weight: 600; font-size: 13px; transition: all 0.2s ease;
}
.tab:hover { background: rgba(255,255,255,0.05); color: var(--text); }
.tab.active { background: var(--grad); color: white; }

/* Sections */
.tab-content { display: none; animation: fadeIn 0.4s ease; }
.tab-content.active { display: block; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }

.glass-panel {
  background: var(--card); border: 1px solid var(--border); border-radius: 20px;
  padding: 24px; backdrop-filter: blur(12px); overflow: hidden;
}
.glass-panel h2 { font-size: 18px; margin-bottom: 20px; display: flex; align-items: center; gap: 10px; }

/* Table Pro */
table { width: 100%; border-collapse: separate; border-spacing: 0 8px; }
th { text-align: left; padding: 12px; color: var(--muted); font-weight: 600; font-size: 12px; text-transform: uppercase; }
td { padding: 16px 12px; background: rgba(255,255,255,0.02); }
td:first-child { border-radius: 8px 0 0 8px; }
td:last-child { border-radius: 0 8px 8px 0; }
tr:hover td { background: rgba(255,255,255,0.05); }

/* Custom Components */
.badge-pro { padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700; white-space: nowrap; }
.badge-pos { background: rgba(16, 185, 129, 0.15); color: var(--green); }
.badge-neg { background: rgba(239, 68, 68, 0.15); color: var(--red); }
.badge-neutral { background: rgba(148, 163, 184, 0.15); color: var(--muted); }

.polarity-pill { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 600; }
.pill-dot { width: 8px; height: 8px; border-radius: 50%; }

.progress-track { width: 80px; height: 6px; background: rgba(255,255,255,0.1); border-radius: 10px; overflow: hidden; }
.progress-fill { height: 100%; border-radius: 10px; }

.ai-exp-card { background: rgba(129, 140, 248, 0.05); border: 1px dashed var(--purple); border-radius: 12px; padding: 16px; line-height: 1.6; color: #cbd5e1; }

/* Responsive */
@media (max-width: 768px) {
  .container { padding: 12px; }
  header { flex-direction: column; align-items: flex-start; gap: 16px; }
  .grid-kpi { grid-template-columns: 1fr; }
  .tabs { width: 100%; overflow-x: auto; }
}
</style>
</head>
<body>

<div class="container">
  <header>
    <div>
      <h1>⚡ CFTE Pro Matrix</h1>
      <p style="color:var(--muted); font-size:12px; margin-top:4px">Trader-Grade Flow Analysis Engine</p>
    </div>
    <div class="header-meta">
      <div style="display:flex; align-items:center; gap:8px">
        <div style="width:10px; height:10px; background:var(--green); border-radius:50%; box-shadow:0 0 8px var(--green)"></div>
        <span id="statusLabel">SYSTEM HEALTHY</span>
      </div>
      <span id="lastUpdated"></span>
    </div>
  </header>

  <div id="kpi-area" class="grid-kpi"></div>

  <div class="tabs">
    <div class="tab active" onclick="switchTab(this,'m5')">TPFM M5</div>
    <div class="tab" onclick="switchTab(this,'m30')">Tâm thế 30P</div>
    <div class="tab" onclick="switchTab(this,'structural')">Cấu trúc 4H</div>
    <div class="tab" onclick="switchTab(this,'theses')">Luận điểm</div>
    <div class="tab" onclick="switchTab(this,'live')">Live Engine</div>
  </div>

  <div id="tab-m5" class="tab-content active">
    <div class="glass-panel">
      <h2>🌊 TPFM M5 Snapshots (Vi cấu trúc)</h2>
      <div id="m5-table-area"></div>
    </div>
  </div>

  <div id="tab-m30" class="tab-content">
    <div class="glass-panel">
      <h2>🌀 TPFM 30m Regime Synthesis (Tâm thế)</h2>
      <div id="m30-table-area"></div>
    </div>
  </div>

  <div id="tab-structural" class="tab-content">
    <div class="glass-panel">
      <h2>🏛️ TPFM 4h Structural Report (Báo cáo tổng quát)</h2>
      <div id="structural-area"></div>
    </div>
  </div>

  <div id="tab-theses" class="tab-content">
    <div class="glass-panel">
      <h2>📊 Live Theses & Outcomes</h2>
      <div id="theses-table-area"></div>
    </div>
  </div>

  <div id="tab-live" class="tab-content">
    <div class="glass-panel">
      <h2>🔴 Engine Events & Logs</h2>
      <pre id="live-log" class="log" style="font-family:monospace; background:#000; padding:20px; border-radius:12px; margin-top:10px"></pre>
    </div>
  </div>
</div>

<script>
const STATE_MAP = {DETECTED:'🔍 DETECTED',WATCHLIST:'👀 WATCHLIST',CONFIRMED:'✅ CONFIRMED',ACTIONABLE:'🔥 ACTIONABLE',INVALIDATED:'❌ INVALIDATED',RESOLVED:'🏁 RESOLVED'};

function pill(polarity) {
  const p = polarity || 'NEUTRAL';
  const color = p.includes('POS') ? 'var(--green)' : p.includes('NEG') ? 'var(--red)' : 'var(--muted)';
  return `<div class="polarity-pill"><div class="pill-dot" style="background:${color}"></div>${p}</div>`;
}

function badge(cls, text) { return `<span class="badge-pro ${cls}">${text||'N/A'}</span>`; }

function tBar(val, max=1.0) {
  const v = val || 0;
  const percent = Math.min(100, (Math.abs(v) / max) * 100);
  const color = v >= 0 ? 'var(--accent)' : 'var(--purple)';
  return `<div class="progress-track"><div class="progress-fill" style="width:${percent}%; background:${color}"></div></div>`;
}

function timeShort(ts) {
  if (!ts) return 'N/A';
  const d = new Date(ts);
  return d.toLocaleTimeString('vi-VN', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

function renderKPIs(data) {
  const db = data.db || {};
  const d = data.daily || {};
  
  const html = `
    <div class="card-kpi">
      <div class="kpi-label">Luận điểm tích lũy</div>
      <div class="kpi-value accent">${db.thesis_count || 0}</div>
      <div class="kpi-sub">+${d.new_thesis_count || 0} hôm nay</div>
    </div>
    <div class="card-kpi">
      <div class="kpi-label">Hiệu suất Edge</div>
      <div class="kpi-value ${d.avg_edge >= 0 ? 'green' : 'red'}">${(d.avg_edge || 0).toFixed(2)}%</div>
      <div class="kpi-sub">Dựa trên ${data.weekly?.completed_outcomes || 0} kết quả</div>
    </div>
    <div class="card-kpi">
      <div class="kpi-label">TPFM Tần suất</div>
      <div class="kpi-value yellow">${(db.tpfm_m5 || []).length}</div>
      <div class="kpi-sub">5m snapshots trong buffer</div>
    </div>
    <div class="card-kpi">
      <div class="kpi-label">Sự kiện hệ thống</div>
      <div class="kpi-value purple">${db.event_count || 0}</div>
      <div class="kpi-sub">Triggered events</div>
    </div>
  `;
  document.getElementById('kpi-area').innerHTML = html;
  document.getElementById('lastUpdated').textContent = 'Last update: ' + timeShort(data.generated_at);
}

function renderM5(data) {
  const list = data.db?.tpfm_m5 || [];
  if (list.length === 0) {
    document.getElementById('m5-table-area').innerHTML = '<p style="color:var(--muted)">Chưa có dữ liệu M5.</p>';
    return;
  }
  
  let html = `<table>
    <thead><tr><th>Thời gian</th><th>Matrix Cell</th><th>Initiative</th><th>Inventory</th><th>Energy</th><th>Efficiency</th><th>Tradability</th><th>Context</th></tr></thead>
    <tbody>
    ${list.map(s => `<tr>
      <td style="font-weight:600">${timeShort(s.window_end_ts)}</td>
      <td><span class="badge-pro ${(s.matrix_cell||'').includes('POS')?'badge-pos':'badge-neg'}">${s.matrix_cell || 'N/A'}</span></td>
      <td>${pill(s.initiative_polarity)}</td>
      <td>${pill(s.inventory_polarity)}</td>
      <td>${badge('badge-neutral', s.energy_state)}</td>
      <td>${badge('badge-neutral', s.response_efficiency_state)}</td>
      <td>${tBar(s.tradability_score)}</td>
      <td><div style="font-size:10px; color:var(--muted)">${s.micro_conclusion || 'Normal'}</div></td>
    </tr>`).join('')}
    </tbody>
  </table>`;
  document.getElementById('m5-table-area').innerHTML = html;
}

function renderM30(data) {
  const list = data.db?.tpfm_m30 || [];
  if (list.length === 0) {
    document.getElementById('m30-table-area').innerHTML = '<p style="color:var(--muted)">Chưa có dữ liệu 30m.</p>';
    return;
  }
  
  let html = `<table>
    <thead><tr><th>Window</th><th>Dominant Regime</th><th>Dominant Cell</th><th>Persistence</th><th>Posture</th><th>Delta Quote</th></tr></thead>
    <tbody>
    ${list.map(r => `<tr>
      <td style="color:var(--muted); font-size:11px">${timeShort(r.window_start_ts)} - ${timeShort(r.window_end_ts)}</td>
      <td><strong>${r.dominant_regime || 'N/A'}</strong></td>
      <td>${badge('badge-neutral', r.dominant_cell)}</td>
      <td>${tBar(r.regime_persistence_score)}</td>
      <td>${badge(r.macro_posture==='FOLLOW_REGIME'?'badge-pos':'badge-neutral', r.macro_posture)}</td>
      <td style="color:${(r.net_delta_quote||0) >= 0 ? 'var(--green)' : 'var(--red)'}">${(r.net_delta_quote || 0).toLocaleString()}</td>
    </tr>`).join('')}
    </tbody>
  </table>`;
  document.getElementById('m30-table-area').innerHTML = html;
}

function renderStructural(data) {
  const list = data.db?.tpfm_4h || [];
  if (list.length === 0) {
    document.getElementById('structural-area').innerHTML = '<p style="color:var(--muted)">Chưa có báo cáo 4h.</p>';
    return;
  }
  
  let html = list.map(report => `
    <div style="margin-bottom:30px; border-bottom:1px solid var(--border); padding-bottom:20px">
      <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:16px">
        <div>
          <span class="badge-pro ${ (report.structural_bias||'').includes('POS') ? 'badge-pos' : 'badge-neg'}" style="font-size:14px; padding:6px 16px">${report.structural_bias || 'NEUTRAL'}</span>
          <p style="color:var(--muted); font-size:12px; margin-top:8px">Window: ${new Date(report.window_start_ts).toLocaleString()} - ${new Date(report.window_end_ts).toLocaleString()}</p>
        </div>
        <div style="text-align:right">
          <div style="font-size:20px; font-weight:700; color:${(report.net_delta_quote||0) >= 0 ? 'var(--green)' : 'var(--red)'}">${(report.net_delta_quote || 0).toLocaleString()} USDT</div>
          <div style="font-size:12px; color:var(--muted)">Net Structural Delta</div>
        </div>
      </div>
      <div class="ai-exp-card">
        <strong style="color:white; display:block; margin-bottom:8px">🤖 AI Structural Analysis:</strong>
        ${report.ai_analysis_vi || 'Đang chờ phân tích AI...'}
      </div>
    </div>
  `).join('');
  document.getElementById('structural-area').innerHTML = html;
}

function renderTheses(data) {
  const list = data.db?.theses || [];
  let html = `<table>
    <thead><tr><th>Instrument</th><th>Setup</th><th>Direction</th><th>Stage</th><th>Score</th><th>Conf</th><th>Opened</th></tr></thead>
    <tbody>
    ${list.map(t => `<tr>
      <td><strong>${t.instrument_key}</strong></td>
      <td style="color:var(--accent)">${t.setup}</td>
      <td>${badge(t.direction.includes('LONG')?'badge-pos':'badge-neg', t.direction)}</td>
      <td>${badge('badge-neutral', t.stage)}</td>
      <td>${tBar(t.score, 100)}</td>
      <td>${((t.confidence||0)*100).toFixed(0)}%</td>
      <td style="color:var(--muted); font-size:11px">${timeShort(t.opened_ts)}</td>
    </tr>`).join('')}
    </tbody>
  </table>`;
  document.getElementById('theses-table-area').innerHTML = html;
}

function switchTab(el, id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('tab-' + id).classList.add('active');
}

async function refresh() {
  try {
    const r = await fetch('/api/data');
    const data = await r.json();
    renderKPIs(data);
    renderM5(data);
    renderM30(data);
    renderStructural(data);
    renderTheses(data);
    document.getElementById('live-log').textContent = data.live_log || 'Logs empty.';
  } catch(e) { console.error(e); }
}

setInterval(refresh, 30000);
refresh();
</script>
</body>
</html>
"""


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/data":
            data = _build_api_data()
            body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/" or path == "/index.html":
            body = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, format, *args):
        pass


def main():
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"🚀 CFTE Pro Dashboard đang chạy tại: http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()


if __name__ == "__main__":
    main()
