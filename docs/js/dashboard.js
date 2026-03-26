/**
 * RadaCrypto Pro Dashboard v3
 * Market Timeline Anchored architecture
 */

class RadaDashboard {
  constructor() {
    this.charts = {};
    this.series = {};
    this.data = { 
      status: {}, 
      m5: [], m30: [], h4: [], 
      flowFrames: [], flowTimeline: [], flowStack: [],
      summary: {}, daily: {}, health: {}, logs: [], liveRuntime: {}, realtimeEvents: [] 
    };
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

    this.charts.volume = LightweightCharts.createChart(volEl, { ...opts, height: 150 });
    this.series.volume = this.charts.volume.addSeries(LightweightCharts.HistogramSeries, {
      priceFormat: { type: "volume" }
    });
    this.charts.volume.priceScale("right").applyOptions({ scaleMargins: { top: 0.1, bottom: 0.1 } });

    // Sync timescales
    this.charts.main.timeScale().subscribeVisibleTimeRangeChange(r => {
      if (r) this.charts.volume.timeScale().setVisibleRange(r);
    });
    this.charts.volume.timeScale().subscribeVisibleTimeRangeChange(r => {
      if (r) this.charts.main.timeScale().setVisibleRange(r);
    });

    window.addEventListener("resize", () => {
      this.charts.main.resize(mainEl.clientWidth, mainEl.clientHeight);
      this.charts.volume.resize(volEl.clientWidth, 150);
    });
  }

  /* ═══ Data Loading ═══ */
  async updateLoop() {
    const f = async (path) => {
      try {
        const resp = await fetch(path + "?t=" + Date.now());
        if (!resp.ok) return null;
        const text = await resp.text();
        if (!text || text.trim() === "") return null;
        return JSON.parse(text);
      } catch (err) {
        return null;
      }
    };

    const status = await f("data/actions_status.json");
    const mode = status?.data_mode || "scan";
    const sfx = mode === "scan" ? "_scan" : "_live";
    const manifest = await f(`data/current${sfx}.json`);
    const manifestPath = (key, fallback) => {
      const rel = manifest?.paths?.[key];
      return rel ? `data/${rel}` : fallback;
    };
    const activeRunId = manifest?.run_id || status?.artifact_run_id || "";

    const [
      m5, m30, h4, 
      frames, timeline, stack,
      summary, daily, health, logs, lr, rtEvents
    ] = await Promise.all([
      f(manifestPath("m5", `data/tpfm_m5${sfx}.json`)),
      f(manifestPath("m30", `data/tpfm_m30${sfx}.json`)),
      f(manifestPath("h4", `data/tpfm_4h${sfx}.json`)),
      f(manifestPath("frames", `data/flow_frames${sfx}.json`)),
      f(manifestPath("timeline", `data/flow_timeline${sfx}.json`)),
      f(manifestPath("stack", `data/flow_stack${sfx}.json`)),
      f(manifestPath("summary", `data/summary_btcusdt${sfx}.json`)),
      f("data/daily_summary.json"),
      f("data/health_status.json"),
      f(manifestPath("logs", `data/thesis_log${sfx}.json`)),
      f("data/live_runtime.json"),
      f(manifestPath("realtime", `data/realtime_events${sfx}.json`)),
    ]);

    // Patch 2 & 6: Unified Normalization & Active Run Filter
    const normalizeFrame = i => this.normalizeFrameItem(i);
    const normalizeTimeline = i => this.normalizeTimelineItem(i);
    const normalizeStack = i => this.normalizeStackItem(i);

    const rawFrames = (frames?.items || []).map(normalizeFrame);
    const rawTimeline = (timeline?.items || []).map(normalizeTimeline);
    const rawStack = (stack?.items || []).map(normalizeStack);
    const rawRT = (rtEvents?.items || (Array.isArray(rtEvents) ? rtEvents : [])).map(i => ({...i, run_id: i.run_id || activeRunId}));

    const filterRun = i => !activeRunId || i.run_id === activeRunId;

    this.data = {
      status: status || {}, 
      m5: (Array.isArray(m5) ? m5 : []).filter(filterRun),
      m30: Array.isArray(m30) ? m30 : [], 
      h4: Array.isArray(h4) ? h4 : [],
      flowFrames: rawFrames.filter(filterRun),
      flowTimeline: rawTimeline.filter(filterRun),
      flowStack: rawStack.filter(filterRun),
      summary: summary || {}, 
      daily: daily || {}, 
      health: health || {},
      logs: (Array.isArray(logs) ? logs : []), 
      liveRuntime: lr || {},
      realtimeEvents: rawRT.filter(filterRun),
    };

    this.renderAll();
  }

  renderAll() {
    const methods = [
      "renderHeader", "renderCharts", "renderFlowMatrix", "renderDecision",
      "renderGauges", "renderPattern", "renderFacts", "renderRisks",
      "renderMTF", "renderSignals", "renderRealtimeEvents", "renderAIHistory"
    ];
    for (const m of methods) {
      try {
        if (typeof this[m] === "function") this[m]();
      } catch (e) {
        console.error(`Render error in ${m}:`, e);
      }
    }
  }

  /* ═══ Helpers ═══ */
  normalizeFrameItem(i) {
    if (!i) return {};
    return {
      ...i,
      snapshot_id: i.snapshot_id || i.source?.ref_id || i.frame_state_id,
      window_start_ts: i.window?.start_ts || i.window_start_ts,
      window_end_ts: i.window?.end_ts || i.window_end_ts,
      open_px: i.prices?.open || i.open_px,
      high_px: i.prices?.high || i.high_px,
      low_px: i.prices?.low || i.low_px,
      close_px: i.prices?.close || i.close_px,
      matrix_cell: i.flow?.matrix_cell || i.matrix_cell,
      matrix_alias_vi: i.flow?.matrix_alias_vi || i.matrix_alias_vi,
      flow_state_code: i.flow?.flow_state_code || i.flow_state_code,
      flow_bias: i.flow?.flow_bias || i.flow_bias,
      pattern_code: i.flow?.pattern_code || i.pattern_code,
      pattern_phase: i.flow?.pattern_phase || i.pattern_phase,
      tradability_grade: i.decision?.tradability_grade || i.tradability_grade,
      decision_posture: i.decision?.posture || i.decision_posture,
      action_label_vi: i.decision?.action_label_vi || i.action_label_vi,
      invalid_if_vi: i.decision?.invalid_if_vi || i.invalid_if_vi,
      stack_alignment: i.decision?.stack_alignment || i.stack_alignment,
      stack_quality: i.decision?.stack_quality || i.stack_quality,
    };
  }

  normalizeTimelineItem(i) {
    if (!i) return {};
    return {
      ...i,
      ...i.context,
      ...i.message,
      window_start_ts: i.window?.start_ts || i.window_start_ts,
      window_end_ts: i.window?.end_ts || i.window_end_ts,
    };
  }

  normalizeStackItem(i) {
    if (!i) return {};
    return {
      ...i,
      anchor_frame: i.anchor?.frame || i.anchor_frame,
      anchor_window_end_ts: i.anchor?.window_end_ts || i.anchor_window_end_ts,
    };
  }

  primarySnap() {
    const frames = this.data.flowFrames || [];
    if (frames.length > 0) {
      return frames[frames.length - 1]; // Already normalized in updateLoop
    }
    const m5Arr = this.data.m5 || [];
    if (m5Arr.length > 0) {
      const raw = m5Arr[m5Arr.length - 1];
      return { ...raw.metadata, ...raw };
    }
    return {};
  }

  latestTPFM() {
    return this.data.summary?.latest_tpfm || {};
  }

  topSignal() {
    return (this.data.summary?.top_signals || [])[0] || {};
  }

  timelineForSnap(snap = this.primarySnap()) {
    const timeline = this.data.flowTimeline || [];
    if (!timeline.length) return {};
    const bySnapshot = timeline.find(t => t.snapshot_id && snap.snapshot_id && t.snapshot_id === snap.snapshot_id);
    if (bySnapshot) return bySnapshot;
    const byWindow = timeline.find(t => t.frame === "M5" && t.window_end_ts && snap.window_end_ts && t.window_end_ts === snap.window_end_ts);
    if (byWindow) return byWindow;
    return timeline[timeline.length - 1] || {};
  }

  decoratedSnap() {
    const snap = this.primarySnap();
    const latest = this.latestTPFM();
    const signal = this.topSignal();
    const timeline = this.timelineForSnap(snap);
    const whyNowList = Array.isArray(signal.why_now) ? signal.why_now : [];
    const conflictList = Array.isArray(signal.conflicts) ? signal.conflicts : [];

    return {
      ...latest,
      ...signal,
      ...timeline,
      ...snap,
      matrix_alias_vi: snap.matrix_alias_vi || latest.matrix_alias_vi || signal.matrix_alias_vi || timeline.matrix_alias_vi,
      flow_state_code: snap.flow_state_code || latest.flow_state_code || signal.flow_state || timeline.flow_state_code,
      flow_bias: snap.flow_bias || latest.continuation_bias || signal.direction || "NEUTRAL",
      tradability_grade: snap.tradability_grade || signal.tradability_grade || latest.tradability_grade || "D",
      decision_posture: snap.decision_posture || signal.decision_posture || latest.preferred_posture || "WAIT",
      decision_summary_vi:
        snap.decision_summary_vi ||
        signal.decision_summary_vi ||
        timeline.summary_vi ||
        latest.decision_summary_vi ||
        latest.preferred_posture ||
        "",
      action_label_vi:
        snap.action_label_vi ||
        timeline.action_label_vi ||
        signal.entry_style ||
        latest.preferred_posture ||
        "",
      invalid_if_vi:
        snap.invalid_if_vi ||
        timeline.invalid_if_vi ||
        signal.invalidation ||
        snap.invalid_if ||
        "",
      flow_decision_brief:
        snap.flow_decision_brief ||
        signal.ai_brief_vi ||
        timeline.why_now_vi ||
        whyNowList.join(" | ") ||
        latest.micro_conclusion ||
        "",
      pattern_code: snap.pattern_code || signal.pattern_code || timeline.pattern_code || "--",
      pattern_phase: snap.pattern_phase || signal.pattern_phase || timeline.pattern_phase || "--",
      pattern_alias_vi: snap.pattern_alias_vi || signal.setup || "",
      entry_condition_vi:
        snap.entry_condition_vi ||
        signal.entry_style ||
        timeline.action_label_vi ||
        latest.preferred_posture ||
        "--",
      avoid_if_vi:
        snap.avoid_if_vi ||
        conflictList.join(" | ") ||
        "--",
      observed_facts:
        snap.observed_facts ||
        latest.observed_facts ||
        whyNowList,
      inferred_facts:
        snap.inferred_facts ||
        latest.inferred_facts ||
        (signal.matrix_alias_vi ? [signal.matrix_alias_vi] : []),
      missing_context:
        snap.missing_context ||
        latest.missing_context ||
        [],
      risk_flags:
        snap.risk_flags ||
        latest.risk_flags ||
        conflictList,
      agreement_score:
        snap.agreement_score ??
        latest.agreement_score ??
        0,
      market_quality_score:
        snap.market_quality_score ??
        latest.market_quality_score ??
        0,
      edge_score:
        snap.edge_score ??
        signal.edge_score ??
        latest.tradability_score ??
        snap.stack_quality ??
        0,
      pattern_sample_count:
        snap.pattern_sample_count ||
        signal.pattern_sample_count ||
        latest.pattern_sample_count ||
        snap.sequence_length,
    };
  }

  modeStr() {
    const s = this.data.status || {};
    if (s.data_mode === "live" || s.live_enabled) return "live";
    return "scan";
  }

  fmtNum(n, d = 2) {
    if (n == null || isNaN(n) || n === "") return "--";
    return Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  }

  relTime(ts) {
    if (!ts) return "--:--";
    try {
      const date = new Date(ts);
      const now = new Date();
      const diff = Math.floor((now - date) / 1000);
      if (diff < 60) return "Vừa xong";
      if (diff < 3600) return `${Math.floor(diff / 60)} phút trước`;
      if (diff < 86400) return `${Math.floor(diff / 3600)} giờ trước`;
      return date.toLocaleDateString("vi-VN");
    } catch (e) { return "--:--"; }
  }

  esc(s) {
    if (!s) return "";
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  /* ═══ Components ═══ */
  renderHeader() {
    const s = this.data.status;
    const snap = this.primarySnap();
    const mode = this.modeStr();
    const grade = snap.tradability_grade || "D";

    document.getElementById("header-symbol").textContent = snap.symbol || "BTCUSDT";

    const price = snap.microprice || snap.close_px;
    document.getElementById("header-price").textContent = price ? this.fmtNum(price, 2) : "--,---";
    const fPrice = document.getElementById("floating-price");
    if (fPrice) fPrice.textContent = price ? this.fmtNum(price, 2) : "--,---";
    const fChange = document.getElementById("floating-change");
    if (fChange) fChange.textContent = snap.matrix_alias_vi || "--";

    const sourceEl = document.getElementById("header-source");
    if (sourceEl) {
      const modeLabel = mode === "live" ? "Live Timeline" : "Scan";
      const lastTs = snap.window_end_ts ? new Date(snap.window_end_ts).toLocaleTimeString("vi-VN", {hour:"2-digit",minute:"2-digit"}) : "--:--";
      sourceEl.textContent = `Nguồn: ${modeLabel} · ${lastTs}`;
    }

    const pill = document.getElementById("header-grade");
    const letter = document.getElementById("grade-letter");
    if (letter) letter.textContent = grade;
    if (pill) pill.className = `grade-pill grade-${grade}`;

    // Mode Badge
    const badge = document.getElementById("mode-badge");
    const modeText = document.getElementById("mode-text");
    if (mode === "live") {
      badge?.classList.add("mode-live");
      if (modeText) modeText.textContent = "TRỰC TIẾP";
    } else {
      badge?.classList.remove("mode-live");
      if (modeText) modeText.textContent = "QUÉT DỮ LIỆU";
    }

    const t = s.artifact_generated_at || snap.window_end_ts;
    const updateEl = document.getElementById("last-update");
    if (updateEl) updateEl.textContent = this.relTime(t);
  }

  renderCharts() {
    // Priority: flowFrames (Table 1) then fallback to raw M5 (History)
    const snaps = this.data.flowFrames.length ? this.data.flowFrames : this.data.m5;
    if (!snaps.length) return;
    const sorted = [...snaps].sort((a, b) => (a.market_ts || a.window_end_ts || 0) - (b.market_ts || b.window_end_ts || 0));
    
    const candles = [];
    const volumes = [];
    const seen = new Set();

    for (const s of sorted) {
      const ts = Math.floor((s.window_end_ts || 0) / 1000);
      if (!ts || seen.has(ts)) continue;
      seen.add(ts);

      const open = s.open_px || s.microprice || 0;
      const close = s.close_px || s.microprice || 0;
      if (open > 0) {
        candles.push({
          time: ts, open, high: s.high_px || Math.max(open, close),
          low: s.low_px || Math.min(open, close), close
        });
      }
      const vol = s.volume_quote || Math.abs(s.delta_quote || 0);
      if (vol > 0) {
        volumes.push({
          time: ts, value: vol,
          color: (s.delta_quote || (close - open)) >= 0 ? "rgba(0, 230, 138, 0.4)" : "rgba(255, 71, 87, 0.4)",
        });
      }
    }
    if (candles.length) {
      this.series.candle.setData(candles);
      this.series.line.setData(candles.map(c => ({ time: c.time, value: c.close })));
    }
    if (volumes.length) this.series.volume.setData(volumes);
  }

  renderMatrixGrid() {
    const grid = document.getElementById("matrix-grid");
    if (!grid) return;
    grid.innerHTML = "";
    const labels = [
      { code: "NEG_INIT__POS_INV", short: "−I +V" }, { code: "NEUTRAL_INIT__POS_INV", short: "○I +V" }, { code: "POS_INIT__POS_INV", short: "+I +V" },
      { code: "NEG_INIT__NEUTRAL_INV", short: "−I ○V" }, { code: "NEUTRAL_INIT__NEUTRAL_INV", short: "○I ○V" }, { code: "POS_INIT__NEUTRAL_INV", short: "+I ○V" },
      { code: "NEG_INIT__NEG_INV", short: "−I −V" }, { code: "NEUTRAL_INIT__NEG_INV", short: "○I −V" }, { code: "POS_INIT__NEG_INV", short: "+I −V" },
    ];
    for (const l of labels) {
      const cell = document.createElement("div");
      cell.className = "matrix-cell";
      cell.dataset.code = l.code;
      cell.textContent = l.short;
      grid.appendChild(cell);
    }
  }

  renderFlowMatrix() {
    const snap = this.primarySnap();
    const activeCell = snap.matrix_cell || "";
    const cells = document.querySelectorAll(".matrix-cell");
    const isSell = activeCell.includes("NEG_INIT") && !activeCell.includes("POS_INV");

    for (const cell of cells) {
      cell.classList.remove("active", "active-sell");
      if (cell.dataset.code === activeCell) {
        cell.classList.add(isSell ? "active-sell" : "active");
      }
    }
    document.getElementById("matrix-alias").textContent = snap.matrix_alias_vi || "--";
    document.getElementById("matrix-flow").textContent = snap.flow_state_code?.replace(/_/g, " ") || "--";
  }

  renderDecision() {
    const snap = this.decoratedSnap();
    const posture = snap.decision_posture || "WAIT";
    const bias = snap.flow_bias || snap.continuation_bias || "NEUTRAL";
    const card = document.getElementById("decision-card");
    if (!card) return;

    card.className = "decision-card";
    if (posture === "LONG" || (posture === "AGGRESSIVE" && bias !== "SHORT")) card.classList.add("posture-long");
    else if (posture === "SHORT" || (posture === "AGGRESSIVE" && bias === "SHORT")) card.classList.add("posture-short");
    else card.classList.add("posture-wait");

    const postureMap = {
      LONG: "TIẾP TỤC MUA", SHORT: "TIẾP TỤC BÁN", AGGRESSIVE: bias === "SHORT" ? "TÍCH CỰC BÁN" : "TÍCH CỰC MUA",
      WAIT: "ĐỨNG NGOÀI", EXIT: "THOÁT VỊ THẾ", CONSERVATIVE: "THẬN TRỌNG"
    };
    document.getElementById("decision-posture").textContent = postureMap[posture] || posture;
    document.getElementById("decision-summary").textContent = snap.decision_summary_vi || snap.action_plan_vi || "Đang phân tích...";
    
    const briefEl = document.getElementById("ai-brief");
    if (briefEl) briefEl.textContent = snap.flow_decision_brief || "";
    const invEl = document.getElementById("decision-invalid-if");
    if (invEl) invEl.textContent = snap.invalid_if_vi || snap.invalid_if || "--";
  }

  renderGauges() {
    const snap = this.decoratedSnap();
    const circum = 2 * Math.PI * 34;
    const setG = (id, valId, score) => {
      const el = document.getElementById(id);
      const valEl = document.getElementById(valId);
      if (!el || !valEl) return;
      const p = Math.max(0, Math.min(1, score || 0));
      el.style.strokeDashoffset = circum * (1 - p);
      valEl.textContent = `${Math.round(p * 100)}%`;
    };
    setG("gauge-quality", "gauge-quality-val", snap.market_quality_score);
    setG("gauge-agreement", "gauge-agreement-val", snap.agreement_score);
    setG("gauge-edge", "gauge-edge-val", snap.edge_score || snap.stack_quality);

    const sEl = document.getElementById("gauge-edge-sample");
    if (sEl) sEl.textContent = `n=${snap.pattern_sample_count || snap.sequence_length || "--"}`;
  }

  renderPattern() {
    const snap = this.decoratedSnap();
    document.getElementById("pattern-name").textContent = snap.pattern_alias_vi || snap.pattern_code || "--";
    document.getElementById("pattern-phase").textContent = snap.pattern_phase || "--";
    document.getElementById("entry-condition").textContent = snap.entry_condition_vi || snap.entry_condition || "--";
    document.getElementById("avoid-if").textContent = snap.avoid_if_vi || snap.avoid_if || "--";
    document.getElementById("invalid-if").textContent = snap.invalid_if_vi || snap.invalid_if || "--";
  }

  renderFacts() {
    const snap = this.decoratedSnap();
    const container = document.getElementById("observed-facts");
    if (!container) return;
    container.innerHTML = "";
    const items = [
      ...(snap.observed_facts || []).map(t => ({ t, cls: "" })),
      ...(snap.inferred_facts || []).map(t => ({ t, cls: "dot-inferred" })),
      ...(snap.missing_context || []).map(t => ({ t, cls: "dot-missing" }))
    ];
    if (items.length === 0) {
      container.innerHTML = '<div class="fact-item"><span class="fact-dot"></span><span>Đang tải luận điểm...</span></div>';
      return;
    }
    items.forEach(item => {
      const div = document.createElement("div");
      div.className = "fact-item fade-enter";
      div.innerHTML = `<span class="fact-dot ${item.cls}"></span><span>${this.esc(item.t)}</span>`;
      container.appendChild(div);
    });
  }

  renderRisks() {
    const snap = this.decoratedSnap();
    const container = document.getElementById("risk-flags");
    if (!container) return;
    container.innerHTML = "";
    const flags = snap.risk_flags || snap.context_warning_flags || [];
    if (flags.length === 0) {
      container.innerHTML = '<div class="risk-item"><span class="risk-dot"></span><span>Không có cảnh báo rủi ro</span></div>';
      return;
    }
    flags.forEach(f => {
      const div = document.createElement("div");
      div.className = "risk-item fade-enter";
      div.innerHTML = `<span class="risk-dot"></span><span>${this.esc(f)}</span>`;
      container.appendChild(div);
    });
  }

  renderMTF() {
    const container = document.getElementById("mtf-strip");
    if (!container) return;

    // Phase 24: Use flowStack for tiered MTF data
    const latestStack = this.data.flowStack?.[0] || {};
    const frames = latestStack.frames || this.primarySnap().frames || {};
    
    // Ordered timeframes for the strip
    const tfs = ["M5", "M30", "H1", "H4", "H12", "D1"];
    
    container.innerHTML = tfs.map(tf => {
      // Find frame data. Priority: exact match in latest stack -> fallback for M5
      const fd = frames[tf] || (tf === "M5" ? this.primarySnap() : null);
      
      const bias = (fd?.flow_bias || "NEUTRAL").toLowerCase();
      const grade = fd?.tradability_grade || "--";
      const alias = fd?.alias_vi || (tf === "M5" ? fd?.matrix_alias_vi : "") || "Chưa có dữ liệu";
      
      // Sync with CSS classes: .mtf-card, .mtf-tf, .mtf-alias, .mtf-grade
      return `
        <div class="mtf-card mtf-${bias}" id="mtf-${tf.toLowerCase()}">
          <div class="mtf-tf">${tf}</div>
          <div class="mtf-alias">${alias}</div>
          <div class="mtf-grade">${grade}</div>
        </div>
      `;
    }).join("");
  }

  renderSignals() {
    const container = document.getElementById("signal-tbody");
    if (!container) return;

    // Use normalized flowFrames (M5 only) for the signal log
    const history = (this.data.flowFrames || []).filter(f => f.frame === "M5").slice(0, 20).reverse();
    
    if (!history.length) {
      container.innerHTML = '<tr><td colspan="7" class="empty-state">Đang chờ dữ liệu timeline...</td></tr>';
      return;
    }

    container.innerHTML = history.map(s => {
      const ts = new Date(s.market_ts).toLocaleTimeString("vi-VN", { hour12: false });
      
      // Patch 10: Trader-facing columns alignment
      // 1. Bias | 2. Pattern · Pha | 3. Hạng | 4. Hành động | 5. Vô hiệu khi | 6. Chip
      const bias = s.flow_bias || "NEUTRAL";
      const patternDesc = `${s.pattern_alias_vi || s.pattern_code || "--"} · ${s.pattern_phase || "--"}`;
      const action = s.action_label_vi || "Theo dõi";
      const inv = s.invalid_if_vi || "--";
      
      // Chips: alignment, quality alerts
      const alignClass = (s.stack_alignment || "").toLowerCase().includes("lead") ? "chip-lead" : "chip-neutral";
      const chip = `<span class="signal-chip ${alignClass}">${s.stack_alignment || "WAIT"}</span>`;

      return `
        <tr>
          <td class="col-time">${ts}</td>
          <td><span class="bias-badge badge-${bias.toLowerCase()}">${bias}</span></td>
          <td class="col-pattern">${this.esc(patternDesc)}</td>
          <td><span class="grade-chip grade-${(s.tradability_grade || 'D').toLowerCase()}">${s.tradability_grade || "D"}</span></td>
          <td class="col-action">${this.esc(action)}</td>
          <td class="col-invalid">${this.esc(inv)}</td>
          <td class="col-chip">${chip}</td>
        </tr>
      `;
    }).join("");
  }

  renderRealtimeEvents() {
    const container = document.getElementById("realtime-events-list");
    if (!container) return;
    
    // Priority 1: dedicated realtimeEvents (from Phase 20-H/F)
    // Priority 2: flowTimeline (the raw event log)
    const events = this.data.realtimeEvents?.length ? this.data.realtimeEvents : this.data.flowTimeline;
    const sortedEvents = [...events].slice(-50).reverse();

    if (!sortedEvents.length) {
      container.innerHTML = '<div class="empty-feed">Chờ sự kiện từ timeline...</div>';
      return;
    }
    
    const typeLabel = { stage_transition: "DIỄN BIẾN", EVENT_LIQUIDATION_LARGE: "THANH LÝ" };
    container.innerHTML = sortedEvents.map(e => {
      const ts = new Date(e.timestamp || e.event_ts || e.market_ts).toLocaleTimeString("vi-VN", { hour12: false });
      const label = typeLabel[e.event_type] || e.type || "SỰ KIỆN";
      const desc = e.summary_vi || e.description_vi || e.transition_alias_vi || "";
      return `
        <div class="feed-item event-${(e.to_stage || e.to_flow_state_code || 'info').toLowerCase()}">
          <span class="feed-time">${ts}</span>
          <span class="feed-label">${label}</span>
          <span class="feed-desc">${desc}</span>
        </div>
      `;
    }).join("");
  }

  renderAIHistory() {
    const list = document.getElementById("ai-history-list");
    if (!list) return;
    const items = (this.data.m5 || []).filter(s => s.flow_decision_brief).slice(-20).reverse();
    if (!items.length) {
      list.innerHTML = '<div class="ai-history-item">Chưa có nhật ký phân tích...</div>';
      return;
    }
    list.innerHTML = items.map(s => `
      <div class="ai-history-item">
        <div class="ai-history-time">${this.relTime(s.window_end_ts)}</div>
        <div class="ai-history-content">${this.esc(s.flow_decision_brief)}</div>
      </div>
    `).join("");
  }
}

document.addEventListener("DOMContentLoaded", () => new RadaDashboard());
