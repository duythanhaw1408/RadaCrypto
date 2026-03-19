#!/usr/bin/env python3
"""
CFTE Dashboard — Giao diện web theo dõi Crypto Flow Thesis Engine.
Chạy: PYTHONPATH=src python3 scripts/dashboard.py
Truy cập: http://localhost:8686
Không cần cài thêm dependency — dùng Python built-in.
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
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_jsonl_tail(path: Path, n: int = 20) -> list[dict]:
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        return [json.loads(l) for l in lines[-n:]]
    except Exception:
        return []


def _read_log_tail(path: Path, n: int = 50) -> str:
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        return "\n".join(lines[-n:])
    except Exception:
        return "(Chưa có log)"


def _query_db():
    if not STATE_DB.exists():
        return {"thesis_count": 0, "outcome_count": 0, "event_count": 0, "theses": [], "recent_events": []}
    try:
        conn = sqlite3.connect(STATE_DB)
        conn.row_factory = sqlite3.Row
        tc = conn.execute("SELECT COUNT(*) FROM thesis").fetchone()[0]
        oc = conn.execute("SELECT COUNT(*) FROM thesis_outcome").fetchone()[0]
        ec = conn.execute("SELECT COUNT(*) FROM thesis_event").fetchone()[0]
        theses = [dict(r) for r in conn.execute(
            "SELECT thesis_id, instrument_key, setup, direction, stage, score, confidence, opened_ts, closed_ts FROM thesis ORDER BY opened_ts DESC LIMIT 20"
        ).fetchall()]
        events = [dict(r) for r in conn.execute(
            "SELECT thesis_id, event_type, delta_score, reason_json, event_ts FROM thesis_event ORDER BY event_ts DESC LIMIT 20"
        ).fetchall()]
        conn.close()
        return {"thesis_count": tc, "outcome_count": oc, "event_count": ec, "theses": theses, "recent_events": events}
    except Exception as e:
        return {"thesis_count": 0, "outcome_count": 0, "event_count": 0, "theses": [], "recent_events": [], "error": str(e)}


def _build_api_data():
    health = _read_json(DATA / "review" / "health_status.json")
    daily = _read_json(DATA / "review" / "daily_summary.json")
    weekly = _read_json(DATA / "review" / "weekly_review.json")
    tuning = _read_json(DATA / "review" / "tuning_report.json")
    replay = _read_json(DATA / "replay" / "summary_btcusdt.json")
    thesis_log = _read_jsonl_tail(DATA / "thesis" / "thesis_log.jsonl", 30)
    live_log = _read_log_tail(Path("/tmp/cfte_live_stdout.log"), 60)
    db = _query_db()
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "health": health,
        "daily": daily,
        "weekly": weekly,
        "tuning": tuning,
        "replay": replay,
        "thesis_log": thesis_log,
        "live_log": live_log,
        "db": db,
    }


HTML_PAGE = """<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CFTE Dashboard — Crypto Flow Thesis Engine</title>
<style>
:root {
  --bg: #0d1117; --card: #161b22; --border: #30363d; --text: #e6edf3;
  --muted: #8b949e; --accent: #58a6ff; --green: #3fb950; --red: #f85149;
  --yellow: #d29922; --purple: #bc8cff;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; }
.container { max-width: 1200px; margin: 0 auto; padding: 16px; }
header { display: flex; align-items: center; justify-content: space-between; padding: 16px 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
header h1 { font-size: 20px; color: var(--accent); }
header .meta { color: var(--muted); font-size: 12px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin-bottom: 16px; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
.card h3 { font-size: 13px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 10px; }
.stat { font-size: 28px; font-weight: 700; }
.stat.green { color: var(--green); }
.stat.red { color: var(--red); }
.stat.yellow { color: var(--yellow); }
.stat.accent { color: var(--accent); }
.stat-label { color: var(--muted); font-size: 12px; margin-top: 2px; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.badge.healthy { background: #0d3117; color: var(--green); }
.badge.degraded { background: #3d2e00; color: var(--yellow); }
.badge.bad { background: #3d1111; color: var(--red); }
.badge.long { background: #0d3117; color: var(--green); }
.badge.short { background: #3d1111; color: var(--red); }
.stage { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; }
.stage.DETECTED { background: #1a1f2e; color: var(--accent); }
.stage.WATCHLIST { background: #1a2e1a; color: #7ee787; }
.stage.CONFIRMED { background: #0d3117; color: var(--green); }
.stage.ACTIONABLE { background: #3d2e00; color: var(--yellow); }
.stage.INVALIDATED { background: #3d1111; color: var(--red); }
.stage.RESOLVED { background: #1c1a2e; color: var(--purple); }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; color: var(--muted); font-weight: 600; padding: 8px 6px; border-bottom: 1px solid var(--border); }
td { padding: 8px 6px; border-bottom: 1px solid var(--border); }
tr:hover { background: rgba(88,166,255,0.05); }
.section { margin-bottom: 16px; }
.section > h2 { font-size: 16px; margin-bottom: 8px; color: var(--text); }
pre.log { background: #0d1117; border: 1px solid var(--border); border-radius: 6px; padding: 12px; font-size: 12px; font-family: 'SF Mono', monospace; overflow-x: auto; max-height: 400px; overflow-y: auto; white-space: pre-wrap; color: var(--muted); }
.refresh-bar { text-align: center; color: var(--muted); font-size: 11px; padding: 8px; }
.tabs { display: flex; border-bottom: 1px solid var(--border); margin-bottom: 12px; }
.tab { padding: 8px 16px; cursor: pointer; color: var(--muted); border-bottom: 2px solid transparent; }
.tab.active { color: var(--accent); border-bottom-color: var(--accent); }
.tab-content { display: none; }
.tab-content.active { display: block; }
.score-bar { height: 6px; border-radius: 3px; background: var(--border); overflow: hidden; }
.score-fill { height: 100%; border-radius: 3px; transition: width 0.5s; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
.live-dot { width: 8px; height: 8px; background: var(--green); border-radius: 50%; display: inline-block; animation: pulse 2s infinite; margin-right: 6px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>⚡ CFTE Dashboard</h1>
    <div class="meta">
      <span class="live-dot"></span>Auto-refresh 30s
      <span id="lastUpdate"></span>
    </div>
  </header>
  <div id="app">Đang tải...</div>
</div>

<script>
const SETUP_VI = {stealth_accumulation:'Tích lũy âm thầm',breakout_ignition:'Kích hoạt bứt phá',distribution:'Phân phối',failed_breakout:'Bứt phá thất bại'};
const DIR_VI = {LONG_BIAS:'LONG 🔼',SHORT_BIAS:'SHORT 🔽'};
const STAGE_EMOJI = {DETECTED:'🔍',WATCHLIST:'👀',CONFIRMED:'✅',ACTIONABLE:'🔥',INVALIDATED:'❌',RESOLVED:'🏁'};

function statusBadge(s) {
  if (!s) return '<span class="badge degraded">N/A</span>';
  const c = s === 'healthy' ? 'healthy' : s === 'degraded' ? 'degraded' : 'bad';
  return `<span class="badge ${c}">${s.toUpperCase()}</span>`;
}
function stageBadge(s) {
  return `<span class="stage ${s}">${STAGE_EMOJI[s]||''} ${s}</span>`;
}
function scoreColor(s) {
  if (s >= 80) return 'var(--green)';
  if (s >= 60) return 'var(--yellow)';
  return 'var(--red)';
}
function scoreBar(s) {
  return `<div class="score-bar"><div class="score-fill" style="width:${Math.min(100,s)}%;background:${scoreColor(s)}"></div></div>`;
}
function timeAgo(iso) {
  if (!iso) return '';
  const d = new Date(iso), now = new Date();
  const m = Math.floor((now - d) / 60000);
  if (m < 1) return 'vừa xong';
  if (m < 60) return m + ' phút trước';
  const h = Math.floor(m / 60);
  if (h < 24) return h + ' giờ trước';
  return Math.floor(h / 24) + ' ngày trước';
}

function render(data) {
  const h = data.health || {};
  const d = data.daily || {};
  const db = data.db || {};
  const w = data.weekly || {};
  
  const status = h.overall_status || 'unknown';
  const checks = (h.checks || []);
  const okCount = checks.filter(c => c.status === 'ok').length;

  document.getElementById('lastUpdate').textContent = ' | ' + timeAgo(data.generated_at);

  let html = `
  <div class="grid">
    <div class="card">
      <h3>Trạng thái hệ thống</h3>
      <div>${statusBadge(status)}</div>
      <div class="stat-label" style="margin-top:8px">${okCount}/${checks.length} kiểm tra OK</div>
    </div>
    <div class="card">
      <h3>Cơ sở dữ liệu</h3>
      <div class="stat accent">${db.thesis_count || 0}</div>
      <div class="stat-label">luận điểm | ${db.outcome_count||0} outcome | ${db.event_count||0} sự kiện</div>
    </div>
    <div class="card">
      <h3>Ngày hôm nay</h3>
      <div class="stat ${(d.new_thesis_count||0)>0?'green':'yellow'}">${d.new_thesis_count || 0}</div>
      <div class="stat-label">thesis mới | edge: ${d.avg_edge ? (d.avg_edge > 0?'+':'') + (d.avg_edge*1).toFixed(2)+'%' : 'N/A'}</div>
    </div>
    <div class="card">
      <h3>Tuần này</h3>
      <div class="stat-label">Outcome: ${w.completed_outcomes||0} | Edge TB: ${w.avg_edge ? (w.avg_edge>0?'+':'')+(w.avg_edge*1).toFixed(2)+'%':'N/A'}</div>
      <div class="stat-label">Setup tốt nhất: ${SETUP_VI[w.best_setup]||w.best_setup||'chưa có'}</div>
    </div>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="switchTab(this,'theses')">📊 Luận điểm</div>
    <div class="tab" onclick="switchTab(this,'events')">📝 Sự kiện</div>
    <div class="tab" onclick="switchTab(this,'live')">🔴 Live Log</div>
    <div class="tab" onclick="switchTab(this,'health')">🏥 Health</div>
    <div class="tab" onclick="switchTab(this,'scan')">🔍 Scan</div>
  </div>

  <div id="tab-theses" class="tab-content active">
    <table>
      <tr><th>ID</th><th>Setup</th><th>Hướng</th><th>Stage</th><th>Score</th><th>Conf</th><th>Thời gian</th></tr>
      ${(db.theses||[]).map(t => `<tr>
        <td style="font-family:monospace;font-size:12px">${(t.thesis_id||'').slice(0,8)}</td>
        <td>${SETUP_VI[t.setup]||t.setup||''}</td>
        <td><span class="badge ${t.direction==='LONG_BIAS'?'long':'short'}">${DIR_VI[t.direction]||t.direction||''}</span></td>
        <td>${stageBadge(t.stage||'')}</td>
        <td>${scoreBar(t.score||0)}<span style="font-size:12px">${(t.score||0).toFixed(1)}</span></td>
        <td>${((t.confidence||0)*100).toFixed(0)}%</td>
        <td style="color:var(--muted);font-size:12px">${t.opened_ts||''}</td>
      </tr>`).join('')}
    </table>
    ${(db.theses||[]).length===0 ? '<p style="color:var(--muted);padding:20px;text-align:center">Chưa có luận điểm</p>' : ''}
  </div>

  <div id="tab-events" class="tab-content">
    <table>
      <tr><th>Thesis</th><th>Loại</th><th>Δ Score</th><th>Thời gian</th></tr>
      ${(db.recent_events||[]).map(e => `<tr>
        <td style="font-family:monospace;font-size:12px">${(e.thesis_id||'').slice(0,8)}</td>
        <td>${e.event_type||''}</td>
        <td style="color:${(e.delta_score||0)>=0?'var(--green)':'var(--red)'}">${(e.delta_score||0)>0?'+':''}${(e.delta_score||0).toFixed(1)}</td>
        <td style="font-size:12px;color:var(--muted)">${e.event_ts||''}</td>
      </tr>`).join('')}
    </table>
    ${(db.recent_events||[]).length===0 ? '<p style="color:var(--muted);padding:20px;text-align:center">Chưa có sự kiện</p>' : ''}
  </div>

  <div id="tab-live" class="tab-content">
    <pre class="log">${escapeHtml(data.live_log || '(Chưa có log)')}</pre>
  </div>

  <div id="tab-health" class="tab-content">
    <table>
      <tr><th>Kiểm tra</th><th>Trạng thái</th><th>Chi tiết</th></tr>
      ${checks.map(c => `<tr>
        <td>${c.key}</td>
        <td>${c.status==='ok'?'✅':c.status==='warn'?'⚠️':'❌'} ${c.status}</td>
        <td style="font-size:12px;color:var(--muted)">${c.summary_vi||''}</td>
      </tr>`).join('')}
    </table>
  </div>

  <div id="tab-scan" class="tab-content">
    ${data.thesis_log && data.thesis_log.length > 0 ?
      `<table>
        <tr><th>Flow</th><th>Setup</th><th>Count</th><th>Thời gian</th></tr>
        ${data.thesis_log.slice(-15).reverse().map(t => `<tr>
          <td><span class="badge ${t.flow==='live'?'healthy':'degraded'}">${t.flow}</span></td>
          <td>${t.signals?t.signals.map(s=>SETUP_VI[s.setup]||s.setup).join(', '):t.setup||''}</td>
          <td>${t.selected_count||t.total_count||''}</td>
          <td style="font-size:12px;color:var(--muted)">${t.timestamp||''}</td>
        </tr>`).join('')}
      </table>` :
      '<p style="color:var(--muted);padding:20px;text-align:center">Chưa có dữ liệu scan</p>'}
  </div>

  <div class="refresh-bar">Tự động cập nhật mỗi 30 giây</div>`;

  document.getElementById('app').innerHTML = html;
}

function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
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
    render(data);
  } catch(e) {
    document.getElementById('app').innerHTML = '<p style="color:var(--red)">Lỗi kết nối: ' + e.message + '</p>';
  }
}

refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>"""


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/data":
            data = _build_api_data()
            body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
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
        pass  # Suppress access logs


def main():
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"🚀 CFTE Dashboard đang chạy tại: http://localhost:{PORT}")
    print(f"   Dữ liệu từ: {DATA}")
    print(f"   Ctrl+C để tắt")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nĐã tắt dashboard.")
        server.server_close()


if __name__ == "__main__":
    main()
