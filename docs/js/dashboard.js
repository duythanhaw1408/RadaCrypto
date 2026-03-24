/**
 * RadaCrypto Pro Dashboard v2
 * Professional Flow Intelligence UI
 */

class RadaDashboard {
  constructor() {
    this.charts = {};
    this.series = {};
    this.data = { status: {}, m5: [], m30: [], h4: [], summary: {}, daily: {}, health: {}, logs: [], liveRuntime: {} };
    this.init();
  }

  async init() {
    this.initCharts();
    this.renderMatrixGrid();
    await this.updateLoop();
    setInterval(() => this.updateLoop(), 30000);
  }

  /* ═══ Charts ═══ */
  initCharts() {
    const opts = {
      layout: { background: { color: "#080810" }, textColor: "#555e6e", fontSize: 11, fontFamily: "Inter, sans-serif" },
      grid: { vertLines: { color: "#161625" }, horzLines: { color: "#161625" } },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: "rgba(255,255,255,0.06)" },
      timeScale: { borderColor: "rgba(255,255,255,0.06)", timeVisible: true, secondsVisible: false },
    };

    const mainEl = document.getElementById("main-chart");
    const volEl = document.getElementById("volume-chart");

    this.charts.main = LightweightCharts.createChart(mainEl, opts);
    this.series.candle = this.charts.main.addSeries(LightweightCharts.CandlestickSeries, {
      upColor: "#00e68a", downColor: "#ff4757", borderVisible: false,
      wickUpColor: "#00e68a", wickDownColor: "#ff4757",
    });
    this.series.line = this.charts.main.addSeries(LightweightCharts.LineSeries, {
      color: "#3b82f6", lineWidth: 2, visible: false,
    });

    this.charts.volume = LightweightCharts.createChart(volEl, { ...opts, height: 120 });
    this.series.volume = this.charts.volume.addSeries(LightweightCharts.HistogramSeries, {
      priceFormat: { type: "volume" }, priceScaleId: "vol",
    });
    this.charts.volume.priceScale("vol").applyOptions({ scaleMargins: { top: 0.1, bottom: 0.1 } });

    // Sync timescales
    this.charts.main.timeScale().subscribeVisibleTimeRangeChange(r => this.charts.volume.timeScale().setVisibleRange(r));
    this.charts.volume.timeScale().subscribeVisibleTimeRangeChange(r => this.charts.main.timeScale().setVisibleRange(r));

    window.addEventListener("resize", () => {
      this.charts.main.resize(mainEl.clientWidth, mainEl.clientHeight);
      this.charts.volume.resize(volEl.clientWidth, 120);
    });
  }

  /* ═══ Data Loading ═══ */
  async updateLoop() {
    const f = async (p) => { try { const r = await fetch(`${p}?${Date.now()}`); return r.ok ? r.json() : null; } catch { return null; } };

    const status = await f("data/actions_status.json");
    const mode = status?.data_mode || "scan";
    const sfx = mode === "live" ? "_live" : "";

    const [m5, m30, h4, summary, daily, health, logs, lr] = await Promise.all([
      f(`data/tpfm_m5${sfx}.json`), f("data/tpfm_m30.json"), f("data/tpfm_4h.json"),
      f(`data/summary_btcusdt${sfx}.json`), f("data/daily_summary.json"),
      f("data/health_status.json"), f(`data/thesis_log${sfx}.json`), f("data/live_runtime.json"),
    ]);

    this.data = {
      status: status || {}, m5: Array.isArray(m5) ? m5 : [],
      m30: Array.isArray(m30) ? m30 : [], h4: Array.isArray(h4) ? h4 : [],
      summary: summary || {}, daily: daily || {}, health: health || {},
      logs: Array.isArray(logs) ? logs : [], liveRuntime: lr || {},
    };

    this.renderAll();
  }

  renderAll() {
    this.renderHeader();
    this.renderCharts();
    this.renderFlowMatrix();
    this.renderDecision();
    this.renderGauges();
    this.renderPattern();
    this.renderFacts();
    this.renderRisks();
    this.renderMTF();
    this.renderSignals();
  }

  /* ═══ Helpers ═══ */
  snap() {
    const { m5, summary } = this.data;
    if (m5.length > 0) return m5[m5.length - 1];
    return summary?.latest_tpfm || {};
  }

  modeStr() {
    const s = this.data.status;
    if (s.data_mode === "live" || s.live_enabled) return "live";
    return "scan";
  }

  fmtNum(n, d = 2) {
    if (n == null || isNaN(n)) return "--";
    return Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  }

  relTime(iso) {
    if (!iso) return "--:--";
    try {
      const d = new Date(iso);
      const now = Date.now();
      const diff = Math.floor((now - d.getTime()) / 1000);
      if (diff < 60) return `${diff}s trước`;
      if (diff < 3600) return `${Math.floor(diff / 60)}p trước`;
      if (diff < 86400) return `${Math.floor(diff / 3600)}h trước`;
      return d.toLocaleDateString("vi-VN");
    } catch { return "--:--"; }
  }

  /* ═══ HEADER ═══ */
  renderHeader() {
    const s = this.data.status;
    const snap = this.snap();
    const mode = this.modeStr();
    const grade = snap.tradability_grade || s.latest_flow_grade || "D";

    document.getElementById("header-symbol").textContent = this.data.summary?.instrument_key || snap.symbol || "BTCUSDT";

    const price = snap.microprice || snap.close_px;
    document.getElementById("header-price").textContent = price ? this.fmtNum(price, 2) : "--,---";
    document.getElementById("floating-price").textContent = price ? this.fmtNum(price, 2) : "--,---";
    document.getElementById("floating-change").textContent = snap.matrix_alias_vi || "--";

    // Grade
    const pill = document.getElementById("header-grade");
    document.getElementById("grade-letter").textContent = grade;
    pill.className = `grade-pill grade-${grade}`;

    // Mode
    const badge = document.getElementById("mode-badge");
    const dot = document.getElementById("mode-dot");
    const modeText = document.getElementById("mode-text");
    if (mode === "live") {
      badge.classList.add("mode-live");
      modeText.textContent = "TRỰC TIẾP";
    } else {
      badge.classList.remove("mode-live");
      modeText.textContent = "QUÉT DỮ LIỆU";
    }

    // Status
    const health = this.data.health;
    const overall = String(health.overall_status || "").toLowerCase();
    const statusEl = document.getElementById("system-status");
    if (mode === "scan") statusEl.textContent = "Snapshot flow";
    else if (overall === "ok" || overall === "healthy") statusEl.textContent = "Hệ thống ổn định";
    else if (overall === "degraded") statusEl.textContent = "Suy giảm";
    else statusEl.textContent = "Đang khởi tạo...";

    const t = s.artifact_generated_at || s.last_run || health.generated_at;
    document.getElementById("last-update").textContent = this.relTime(t);
  }

  /* ═══ CHARTS ═══ */
  renderCharts() {
    const snaps = this.data.m5;
    if (!snaps.length) return;

    const candles = [];
    const volumes = [];

    for (const s of snaps) {
      const ts = Math.floor((s.window_end_ts || 0) / 1000);
      if (!ts) continue;
      candles.push({
        time: ts,
        open: s.open_px || s.microprice || 0,
        high: s.high_px || s.microprice || 0,
        low: s.low_px || s.microprice || 0,
        close: s.close_px || s.microprice || 0,
      });
      const delta = s.delta_quote || 0;
      volumes.push({
        time: ts,
        value: Math.abs(s.volume_quote || delta || 0),
        color: delta >= 0 ? "rgba(0, 230, 138, 0.5)" : "rgba(255, 71, 87, 0.5)",
      });
    }

    if (candles.length) {
      const valid = candles.filter(c => c.open > 0);
      if (valid.length) {
        this.series.candle.setData(valid);
        this.series.line.setData(valid.map(c => ({ time: c.time, value: c.close })));
      }
    }
    if (volumes.length) this.series.volume.setData(volumes);
  }

  /* ═══ FLOW MATRIX 3×3 ═══ */
  renderMatrixGrid() {
    const grid = document.getElementById("matrix-grid");
    grid.innerHTML = "";

    // Rows: POS_INV (top), NEUTRAL_INV (mid), NEG_INV (bottom)
    // Cols: NEG_INIT (left), NEUTRAL_INIT (mid), POS_INIT (right)
    const labels = [
      { init: "NEG", inv: "POS", code: "NEG_INIT__POS_INV", short: "−I +V" },
      { init: "NEU", inv: "POS", code: "NEUTRAL_INIT__POS_INV", short: "○I +V" },
      { init: "POS", inv: "POS", code: "POS_INIT__POS_INV", short: "+I +V" },
      { init: "NEG", inv: "NEU", code: "NEG_INIT__NEUTRAL_INV", short: "−I ○V" },
      { init: "NEU", inv: "NEU", code: "NEUTRAL_INIT__NEUTRAL_INV", short: "○I ○V" },
      { init: "POS", inv: "NEU", code: "POS_INIT__NEUTRAL_INV", short: "+I ○V" },
      { init: "NEG", inv: "NEG", code: "NEG_INIT__NEG_INV", short: "−I −V" },
      { init: "NEU", inv: "NEG", code: "NEUTRAL_INIT__NEG_INV", short: "○I −V" },
      { init: "POS", inv: "NEG", code: "POS_INIT__NEG_INV", short: "+I −V" },
    ];

    for (const l of labels) {
      const cell = document.createElement("div");
      cell.className = "matrix-cell";
      cell.dataset.code = l.code;
      cell.textContent = l.short;
      cell.title = l.code;
      grid.appendChild(cell);
    }
  }

  renderFlowMatrix() {
    const snap = this.snap();
    const activeCell = snap.matrix_cell || "";
    const cells = document.querySelectorAll(".matrix-cell");

    const isSell = (activeCell.startsWith("NEG_INIT") && !activeCell.includes("POS_INV"));

    for (const cell of cells) {
      cell.classList.remove("active", "active-sell");
      if (cell.dataset.code === activeCell) {
        cell.classList.add(isSell ? "active-sell" : "active");
      }
    }

    document.getElementById("matrix-alias").textContent = snap.matrix_alias_vi || "--";
    document.getElementById("matrix-flow").textContent = snap.flow_state_code
      ? snap.flow_state_code.replace(/_/g, " ")
      : "--";
  }

  /* ═══ DECISION ═══ */
  renderDecision() {
    const snap = this.snap();
    const posture = snap.decision_posture || "WAIT";
    const card = document.getElementById("decision-card");

    card.className = "decision-card";
    if (posture === "LONG" || posture === "AGGRESSIVE") card.classList.add("posture-long");
    else if (posture === "SHORT") card.classList.add("posture-short");
    else card.classList.add("posture-wait");

    const postureMap = {
      LONG: "TIẾP TỤC MUA",
      SHORT: "TIẾP TỤC BÁN",
      AGGRESSIVE: "TÍCH CỰC MUA",
      WAIT: "ĐỨNG NGOÀI",
      EXIT: "THOÁT VỊ THẾ",
    };
    document.getElementById("decision-posture").textContent = postureMap[posture] || posture;
    document.getElementById("decision-summary").textContent = snap.decision_summary_vi || snap.action_plan_vi || "Đang tải...";
  }

  /* ═══ GAUGES ═══ */
  renderGauges() {
    const snap = this.snap();
    const circumference = 2 * Math.PI * 34; // r=34

    const setGauge = (id, valId, pct) => {
      const el = document.getElementById(id);
      const valEl = document.getElementById(valId);
      const p = Math.max(0, Math.min(1, pct || 0));
      el.style.strokeDashoffset = circumference * (1 - p);
      valEl.textContent = `${Math.round(p * 100)}%`;
    };

    setGauge("gauge-quality", "gauge-quality-val", snap.market_quality_score || 0);
    setGauge("gauge-agreement", "gauge-agreement-val", snap.agreement_score || 0);
    setGauge("gauge-edge", "gauge-edge-val", snap.edge_score || 0);
  }

  /* ═══ PATTERN ═══ */
  renderPattern() {
    const snap = this.snap();
    document.getElementById("pattern-name").textContent = snap.pattern_alias_vi || snap.pattern_code || "--";
    document.getElementById("pattern-phase").textContent = snap.pattern_phase || "--";
    document.getElementById("entry-condition").textContent = snap.entry_condition_vi || snap.entry_condition || "--";
    document.getElementById("avoid-if").textContent = snap.avoid_if_vi || snap.avoid_if || "--";
    document.getElementById("invalid-if").textContent = snap.invalid_if || "--";
  }

  /* ═══ FACTS ═══ */
  renderFacts() {
    const snap = this.snap();
    const container = document.getElementById("observed-facts");
    container.innerHTML = "";

    const observed = snap.observed_facts || [];
    const inferred = snap.inferred_facts || [];
    const missing = snap.missing_context || [];

    const add = (text, dotClass) => {
      const item = document.createElement("div");
      item.className = "fact-item fade-enter";
      item.innerHTML = `<span class="fact-dot ${dotClass}"></span><span>${this.esc(text)}</span>`;
      container.appendChild(item);
    };

    for (const t of observed) add(t, "");
    for (const t of inferred) add(t, "dot-inferred");
    for (const t of missing) add(t, "dot-missing");

    if (!observed.length && !inferred.length && !missing.length) {
      container.innerHTML = '<div class="fact-item"><span class="fact-dot"></span><span>Đang chờ dữ liệu...</span></div>';
    }
  }

  /* ═══ RISKS ═══ */
  renderRisks() {
    const snap = this.snap();
    const container = document.getElementById("risk-flags");
    container.innerHTML = "";
    const flags = snap.risk_flags || snap.context_warning_flags || [];

    for (const f of flags) {
      const item = document.createElement("div");
      item.className = "risk-item fade-enter";
      item.innerHTML = `<span class="risk-dot"></span><span>${this.esc(f)}</span>`;
      container.appendChild(item);
    }

    if (!flags.length) {
      container.innerHTML = '<div class="risk-item"><span class="risk-dot"></span><span>Không có cảnh báo nổi bật</span></div>';
    }
  }

  /* ═══ MTF ═══ */
  renderMTF() {
    const snap = this.snap();
    const m30 = this.data.m30;
    const h4 = this.data.h4;

    // M5
    const m5Card = document.getElementById("mtf-m5");
    m5Card.className = "mtf-card " + this.biasClass(snap.continuation_bias);
    document.getElementById("mtf-m5-alias").textContent = snap.matrix_alias_vi || "--";
    document.getElementById("mtf-m5-grade").textContent = snap.tradability_grade ? `Hạng ${snap.tradability_grade}` : "--";

    // M30
    const m30Data = Array.isArray(m30) && m30.length ? m30[m30.length - 1] : {};
    const m30Card = document.getElementById("mtf-m30");
    m30Card.className = "mtf-card " + this.biasClass(m30Data.macro_conclusion_code);
    document.getElementById("mtf-m30-alias").textContent = m30Data.dominant_regime || m30Data.macro_conclusion_code || "--";
    document.getElementById("mtf-m30-grade").textContent = m30Data.tradability_score != null ? `Chất lượng ${Math.round(m30Data.tradability_score * 100)}%` : "--";

    // 4H
    const h4Data = Array.isArray(h4) && h4.length ? h4[h4.length - 1] : {};
    const h4Card = document.getElementById("mtf-4h");
    h4Card.className = "mtf-card " + this.biasClass(h4Data.structural_bias);
    document.getElementById("mtf-4h-alias").textContent = h4Data.structural_bias || "--";
    document.getElementById("mtf-4h-grade").textContent = h4Data.structural_quality || "--";
  }

  biasClass(bias) {
    if (!bias) return "bias-neutral";
    const b = bias.toUpperCase();
    if (b.includes("LONG") || b.includes("BULL") || b.includes("POS")) return "bias-long";
    if (b.includes("SHORT") || b.includes("BEAR") || b.includes("NEG")) return "bias-short";
    return "bias-neutral";
  }

  /* ═══ SIGNALS TABLE ═══ */
  renderSignals() {
    const tbody = document.getElementById("signal-tbody");
    tbody.innerHTML = "";

    const signals = this.data.summary?.top_signals || [];

    for (const sig of signals) {
      const row = document.createElement("tr");
      row.className = "fade-enter";

      const setupNames = {
        stealth_accumulation: "Tích lũy ẩn",
        breakout_ignition: "Bứt phá",
        distribution: "Phân phối",
        failed_breakout: "Breakout thất bại",
      };

      const dirClass = sig.direction === "LONG_BIAS" ? "dir-long" : "dir-short";
      const dirText = sig.direction === "LONG_BIAS" ? "MUA" : "BÁN";
      const grade = sig.tradability_grade || "D";
      const posture = sig.decision_posture || "WAIT";
      const flowPct = Math.round((sig.flow_alignment_score || 0) * 100);

      row.innerHTML = `
        <td><strong>${setupNames[sig.setup] || sig.setup}</strong></td>
        <td><span class="dir-badge ${dirClass}">${dirText}</span></td>
        <td>
          <div class="score-bar">
            <span>${Math.round(sig.score || 0)}</span>
            <div class="score-track"><div class="score-fill" style="width:${Math.min(100, sig.score || 0)}%"></div></div>
          </div>
        </td>
        <td>${sig.matrix_alias_vi || "--"}</td>
        <td><span class="tag-grade tag-${grade}">${grade}</span></td>
        <td><span class="posture-tag posture-${posture}">${this.postureLabel(posture)}</span></td>
        <td>
          <div class="flow-bar"><div class="flow-fill" style="width:${flowPct}%"></div></div>
          <span style="font-size:0.7rem;margin-left:4px">${flowPct}%</span>
        </td>
      `;
      tbody.appendChild(row);
    }

    if (!signals.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:20px">Chưa có tín hiệu</td></tr>';
    }
  }

  postureLabel(p) {
    const map = { LONG: "Mua", SHORT: "Bán", AGGRESSIVE: "Tích cực", WAIT: "Chờ", EXIT: "Thoát" };
    return map[p] || p;
  }

  esc(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }
}

// ═══ Bootstrap ═══
document.addEventListener("DOMContentLoaded", () => new RadaDashboard());
