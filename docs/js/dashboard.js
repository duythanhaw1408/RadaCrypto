/**
 * RadaCrypto Pro Dashboard
 * Render flow-first UI from docs/data artifacts without inventing missing market data.
 */

class RadaDashboard {
    constructor() {
        this.charts = {};
        this.series = {};
        this.data = {
            status: {},
            m5: [],
            m30: [],
            h4: [],
            summary: {},
            daily: {},
            health: {},
            logs: [],
            liveRuntime: {},
        };

        this.init();
    }

    async init() {
        this.initCharts();
        await this.updateLoop();
        setInterval(() => this.updateLoop(), 60000);
    }

    initCharts() {
        const chartOptions = {
            layout: {
                backgroundColor: "#0a0a0f",
                textColor: "#94a3b8",
                fontSize: 12,
                fontFamily: "Inter, sans-serif",
            },
            grid: {
                vertLines: { color: "#1c1c28" },
                horzLines: { color: "#1c1c28" },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: "#2d2d3d",
            },
            timeScale: {
                borderColor: "#2d2d3d",
                timeVisible: true,
                secondsVisible: false,
            },
        };

        this.charts.main = LightweightCharts.createChart(
            document.getElementById("main-chart"),
            chartOptions,
        );
        this.series.candle = this.charts.main.addSeries(LightweightCharts.CandlestickSeries, {
            upColor: "#00ffa3",
            downColor: "#ff4d4d",
            borderVisible: false,
            wickUpColor: "#00ffa3",
            wickDownColor: "#ff4d4d",
            visible: true,
        });
        this.series.line = this.charts.main.addSeries(LightweightCharts.LineSeries, {
            color: "#3b82f6",
            lineWidth: 2,
            visible: false,
        });

        this.charts.volume = LightweightCharts.createChart(
            document.getElementById("volume-chart"),
            {
                ...chartOptions,
                height: 150,
            },
        );
        this.series.volume = this.charts.volume.addSeries(LightweightCharts.HistogramSeries, {
            color: "#3b82f6",
            priceFormat: { type: "volume" },
            priceScaleId: "volume",
        });
        this.charts.volume.priceScale("volume").applyOptions({
            scaleMargins: { top: 0.1, bottom: 0.1 },
        });

        this.charts.main.timeScale().subscribeVisibleTimeRangeChange((range) => {
            this.charts.volume.timeScale().setVisibleRange(range);
        });
        this.charts.volume.timeScale().subscribeVisibleTimeRangeChange((range) => {
            this.charts.main.timeScale().setVisibleRange(range);
        });

        window.addEventListener("resize", () => {
            const main = document.getElementById("main-chart");
            const volume = document.getElementById("volume-chart");
            this.charts.main.resize(main.clientWidth, main.clientHeight);
            this.charts.volume.resize(volume.clientWidth, 150);
        });
    }

    async updateLoop() {
        const fetchJson = async (path) => {
            try {
                const response = await fetch(`${path}?${Date.now()}`);
                if (!response.ok) {
                    return null;
                }
                return await response.json();
            } catch (_err) {
                return null;
            }
        };

        const [
            status,
            m5,
            m30,
            h4,
            summary,
            daily,
            health,
            logs,
            liveRuntime,
        ] = await Promise.all([
            fetchJson("data/actions_status.json"),
            fetchJson("data/tpfm_m5.json"),
            fetchJson("data/tpfm_m30.json"),
            fetchJson("data/tpfm_4h.json"),
            fetchJson("data/summary_btcusdt.json"),
            fetchJson("data/daily_summary.json"),
            fetchJson("data/health_status.json"),
            fetchJson("data/thesis_log.json"),
            fetchJson("data/live_runtime.json"),
        ]);

        this.data = {
            status: status || {},
            m5: Array.isArray(m5) ? m5 : [],
            m30: Array.isArray(m30) ? m30 : [],
            h4: Array.isArray(h4) ? h4 : [],
            summary: summary || {},
            daily: daily || {},
            health: health || {},
            logs: Array.isArray(logs) ? logs : [],
            liveRuntime: liveRuntime || {},
        };

        this.renderHeader();
        this.renderCharts();
        this.renderDecisionPanel();
        this.renderMTFStack();
        this.renderSignalLog();
    }

    renderHeader() {
        const status = this.data.status || {};
        const summary = this.data.summary || {};
        const health = this.data.health || {};
        const latest = this.getPrimarySnapshot();
        const mode = this.normalizeMode(status.data_mode, status.live_enabled);
        const overall = String(health.overall_status || "").toLowerCase();

        document.getElementById("symbol-display").innerText =
            summary.instrument_key || latest.symbol || "BTCUSDT";
        document.getElementById("header-grade").innerText =
            `Hạng ${latest.tradability_grade || status.latest_flow_grade || "D"}`;
        document.getElementById("mode-label").innerText =
            mode === "live" ? "TRỰC TIẾP" : "QUÉT DỮ LIỆU";

        const systemLabel = document.getElementById("system-label");
        const systemDot = document.getElementById("system-dot");
        if (mode === "scan") {
            systemLabel.innerText = "Scan snapshot flow";
            systemDot.style.background = "#ffaa00";
            systemDot.style.boxShadow = "0 0 8px #ffaa00";
        } else if (overall === "ok" || overall === "healthy") {
            systemLabel.innerText = "Hệ thống ổn định";
            systemDot.style.background = "#00ffa3";
            systemDot.style.boxShadow = "0 0 8px #00ffa3";
        } else if (overall === "degraded" || overall === "warn") {
            systemLabel.innerText = "Hệ thống suy giảm";
            systemDot.style.background = "#ffaa00";
            systemDot.style.boxShadow = "0 0 8px #ffaa00";
        } else {
            systemLabel.innerText = "Đang khởi tạo...";
            systemDot.style.background = "#64748b";
            systemDot.style.boxShadow = "0 0 8px #64748b";
        }

        const lastRun = status.artifact_generated_at || status.last_run || status.last_scan_time || health.generated_at || summary.generated_at;
        document.getElementById("last-update").innerText = lastRun
            ? this.formatRelativeTime(lastRun)
            : "--:--";
    }

    renderCharts() {
        if (!this.series.candle || !this.series.line || !this.series.volume) {
            return;
        }

        const ordered = [...this.data.m5].sort(
            (left, right) => Number(left.window_start_ts || 0) - Number(right.window_start_ts || 0),
        );
        const mode = this.normalizeMode(this.data.status.data_mode, this.data.status.live_enabled);
        const latestSummary = this.getSummarySnapshot();
        const primarySnapshot = this.getPrimarySnapshot();
        if (!ordered.length) {
            this.series.candle.setData([]);
            this.series.line.setData([]);
            this.series.volume.setData([]);
            this.updateOverlayNoPrice("Chưa có snapshot M5", "Đang chờ dữ liệu flow mới.");
            return;
        }

        if (
            mode === "scan"
            && latestSummary.window_end_ts
            && primarySnapshot.window_end_ts === latestSummary.window_end_ts
            && !this.isM5HistoryAlignedWithSummary(ordered, latestSummary)
        ) {
            this.series.candle.applyOptions({ visible: false });
            this.series.line.applyOptions({ visible: false });
            this.series.candle.setData([]);
            this.series.line.setData([]);
            this.series.volume.setData([]);
            this.renderMarkers([]);
            this.updateOverlayNoPrice(
                "Chế độ scan",
                "Chưa có chuỗi M5 đồng bộ để vẽ chart giá. Đang dùng panel flow ở bên phải.",
            );
            return;
        }

        const hasCandles = ordered.every((row) => this.hasOhlc(row));
        if (hasCandles) {
            const candleData = ordered.map((row) => ({
                time: Math.floor(Number(row.window_start_ts || 0) / 1000),
                open: Number(row.open_px),
                high: Number(row.high_px),
                low: Number(row.low_px),
                close: Number(row.close_px),
            }));
            this.series.candle.applyOptions({ visible: true });
            this.series.line.applyOptions({ visible: false });
            this.series.candle.setData(candleData);
            this.series.line.setData([]);
            this.renderMarkers(ordered);

            const latest = candleData[candleData.length - 1];
            const delta = latest.close - latest.open;
            const pct = latest.open ? (delta / latest.open) * 100.0 : 0.0;
            this.updateOverlayPrice(latest.close, pct, delta, "M5");
        } else {
            const lineData = ordered
                .map((row) => {
                    const fallbackValue = this.numberOrNull(row.microprice)
                        ?? this.numberOrNull(row.mid_px)
                        ?? this.numberOrNull(row.delta_quote);
                    if (fallbackValue === null) {
                        return null;
                    }
                    return {
                        time: Math.floor(Number(row.window_start_ts || 0) / 1000),
                        value: fallbackValue,
                    };
                })
                .filter(Boolean);
            this.series.candle.applyOptions({ visible: false });
            this.series.candle.setData([]);
            this.series.line.applyOptions({ visible: true });
            this.series.line.setData(lineData);
            this.renderMarkers([]);

            const latestLine = lineData[lineData.length - 1];
            if (latestLine) {
                this.updateOverlayNoPrice(
                    "Thiếu OHLC M5",
                    `Đang hiển thị fallback từ ${this.describeFallbackSeries(ordered[0])}.`,
                );
            } else {
                this.updateOverlayNoPrice(
                    "Thiếu dữ liệu giá",
                    "Cần export thêm microprice hoặc OHLC để vẽ biểu đồ giá.",
                );
            }
        }

        const volumeData = ordered.map((row) => ({
            time: Math.floor(Number(row.window_start_ts || 0) / 1000),
            value: Number(row.delta_quote || 0),
            color: Number(row.delta_quote || 0) >= 0 ? "#00ffa3" : "#ff4d4d",
        }));
        this.series.volume.setData(volumeData);
    }

    renderMarkers(rows) {
        if (!Array.isArray(rows) || !rows.length || typeof this.series.candle.setMarkers !== "function") {
            if (typeof this.series.candle.setMarkers === "function") {
                this.series.candle.setMarkers([]);
            }
            return;
        }

        const markers = [];
        rows.forEach((row) => {
            const time = Math.floor(Number(row.window_start_ts || 0) / 1000);
            const defense = String(row.inventory_defense_state || "");
            const forced = String(row.forced_flow_state || "");
            const flow = String(row.flow_state_code || "");
            const pattern = String(row.pattern_code || "");

            if (defense === "BID_DEFENSE") {
                markers.push({ time, position: "belowBar", color: "#00ffa3", shape: "arrowUp", text: "BID" });
            } else if (defense === "ASK_DEFENSE") {
                markers.push({ time, position: "aboveBar", color: "#ff4d4d", shape: "arrowDown", text: "ASK" });
            }

            if (flow.includes("TRAP")) {
                markers.push({ time, position: "aboveBar", color: "#ffaa00", shape: "circle", text: "TRAP" });
            }
            if (forced.includes("SQUEEZE")) {
                markers.push({ time, position: "belowBar", color: "#3b82f6", shape: "circle", text: "SQZ" });
            }
            if (pattern.includes("CONTI_LONG")) {
                markers.push({ time, position: "belowBar", color: "#00ffa3", shape: "arrowUp", text: "CTN" });
            } else if (pattern.includes("CONTI_SHORT")) {
                markers.push({ time, position: "aboveBar", color: "#ff4d4d", shape: "arrowDown", text: "CTN" });
            }
        });

        this.series.candle.setMarkers(markers);
    }

    renderDecisionPanel() {
        const latest = this.getPrimarySnapshot();
        const topSignal = this.getPrimarySignal();

        document.getElementById("matrix-title").innerText =
            latest.matrix_alias_vi || topSignal.matrix_alias_vi || "Chưa rõ ma trận";
        document.getElementById("matrix-desc").innerText = this.buildMatrixDescription(latest, topSignal);

        document.getElementById("pattern-title").innerText = this.buildPatternTitle(latest, topSignal);
        document.getElementById("pattern-desc").innerText = this.buildPatternDescription(latest, topSignal);

        const actionCard = document.getElementById("action-card");
        const action = this.buildAction(latest, topSignal);
        actionCard.innerText = action.label;
        actionCard.className = `action-card ${action.className}`;

        this.renderList(
            document.getElementById("reasoning-list"),
            this.buildReasoningItems(latest, topSignal),
        );
        this.renderList(
            document.getElementById("risk-list"),
            this.buildRiskItems(latest, topSignal),
        );

        const winRate = Number(latest.historical_win_rate ?? topSignal.hit_rate ?? 0);
        const rr = Number(latest.expected_rr ?? topSignal.edge_score ?? 0);
        document.getElementById("prob-title").innerText =
            winRate > 0
                ? `${Math.round(winRate * 100)}% Win Rate | RR ${rr.toFixed(2)}`
                : "Chưa đủ mẫu xác suất";

        const confidence = String(latest.edge_confidence || topSignal.edge_confidence || "LOW").toUpperCase();
        document.getElementById("prob-desc").innerText =
            confidence === "HIGH"
                ? "Độ tin cậy cao từ pattern hiện tại."
                : confidence === "MEDIUM"
                    ? "Độ tin cậy vừa, cần xác nhận thêm context."
                    : "Độ tin cậy còn thấp hoặc đang dùng fallback heuristic.";
    }

    renderMTFStack() {
        const container = document.getElementById("mtf-stack");
        if (!container) {
            return;
        }
        container.innerHTML = "";

        const mode = this.normalizeMode(this.data.status.data_mode, this.data.status.live_enabled);
        const latestM5 = this.getPrimarySnapshot();
        const latestM30 = this.getLatestSnapshot(this.data.m30) || {};
        const latestH4 = this.getLatestSnapshot(this.data.h4) || {};
        const topSignal = this.getPrimarySignal();

        const cards = [
            {
                id: "M5",
                alias: latestM5.matrix_alias_vi || "Chưa rõ",
                grade: latestM5.tradability_grade || "D",
                bias: this.detectBias(latestM5.matrix_cell || latestM5.flow_state_code || ""),
            },
            {
                id: "30M",
                alias: mode === "scan"
                    ? (latestM5.parent_context?.m30_regime || latestM30.dominant_regime || "Chưa rõ")
                    : (latestM30.dominant_regime || latestM5.parent_context?.m30_regime || "Chưa rõ"),
                grade: latestM30.health_state === "HEALTHY" ? "B" : "C",
                bias: this.detectBias(
                    mode === "scan"
                        ? (latestM5.parent_context?.m30_regime || latestM30.dominant_regime || "")
                        : (latestM30.dominant_regime || ""),
                ),
            },
            {
                id: "H1",
                alias: topSignal.pattern_code
                    ? `${this.humanizePattern(topSignal.pattern_code)}`
                    : topSignal.matrix_alias_vi || "Theo flow",
                grade: topSignal.tradability_grade || "D",
                bias: this.detectBias(topSignal.direction || topSignal.matrix_cell || ""),
            },
            {
                id: "H4",
                alias: mode === "scan"
                    ? (latestM5.parent_context?.h4_structural_bias || latestH4.structural_bias || "Chưa rõ")
                    : (latestH4.structural_bias || latestM5.parent_context?.h4_structural_bias || "Chưa rõ"),
                grade: this.scoreToGrade(latestH4.structural_score),
                bias: this.detectBias(
                    mode === "scan"
                        ? (latestM5.parent_context?.h4_structural_bias || latestH4.structural_bias || "")
                        : (latestH4.structural_bias || ""),
                ),
            },
            {
                id: "H12",
                alias: "N/A",
                grade: "--",
                bias: "neutral",
            },
            {
                id: "D1",
                alias: "N/A",
                grade: "--",
                bias: "neutral",
            },
        ];

        cards.forEach((card) => {
            const element = document.createElement("div");
            element.className = `mtf-card active-bias-${card.bias}`;
            element.innerHTML = `
                <span class="mtf-timeframe">${card.id}</span>
                <span class="mtf-alias">${card.alias}</span>
                <span class="mtf-grade">Hạng ${card.grade}</span>
            `;
            container.appendChild(element);
        });
    }

    renderSignalLog() {
        const body = document.getElementById("signal-log-body");
        if (!body) {
            return;
        }
        body.innerHTML = "";

        const rows = this.buildSignalRows();
        rows.slice(0, 15).forEach((row) => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${row.timeLabel}</td>
                <td><span class="log-setup">${row.setupLabel}</span></td>
                <td>${row.matrixLabel}</td>
                <td><span class="grade-badge-small">${row.grade}</span></td>
                <td><span class="log-action action-${row.actionTone}">${row.actionLabel}</span></td>
            `;
            body.appendChild(tr);
        });
    }

    buildSignalRows() {
        const topSignals = Array.isArray(this.data.summary.top_signals) ? this.data.summary.top_signals : [];
        const summaryFresh = this.isSummaryFreshAgainstM5();
        const signalById = new Map(
            topSignals
                .filter((signal) => signal && signal.thesis_id)
                .map((signal) => [signal.thesis_id, signal]),
        );

        const rows = [];
        const seen = new Set();
        [...this.data.logs].reverse().forEach((record) => {
            if (!record?.thesis_id || seen.has(record.thesis_id)) {
                return;
            }
            seen.add(record.thesis_id);
            const enriched = summaryFresh ? (signalById.get(record.thesis_id) || record) : record;
            
            // In scan mode, if it's not in topSignals, it's likely from a previous run
            if (this.normalizeMode(this.data.status.data_mode, this.data.status.live_enabled) === "scan" && !signalById.has(record.thesis_id)) {
                return;
            }

            rows.push({
                timeLabel: record.event_ts ? this.formatClock(record.event_ts) : (enriched.timeframe || "--"),
                setupLabel: this.humanizeSetup(enriched.setup || record.setup),
                matrixLabel: enriched.matrix_alias_vi || enriched.matrix_cell || "Chưa có matrix",
                grade: enriched.tradability_grade || this.scoreToGrade(enriched.score),
                actionLabel: this.humanizeStage(record.to_stage || enriched.stage || "DETECTED"),
                actionTone: this.detectBias(enriched.direction || enriched.matrix_cell || ""),
            });
        });

        if (rows.length) {
            return rows;
        }

        return topSignals.map((signal) => ({
            timeLabel: signal.timeframe || "--",
            setupLabel: `${summaryFresh ? "" : "[SCAN] "}${this.humanizeSetup(signal.setup)}`,
            matrixLabel: signal.matrix_alias_vi || signal.matrix_cell || "Chưa có matrix",
            grade: signal.tradability_grade || this.scoreToGrade(signal.score),
            actionLabel: this.humanizeStage(signal.stage || "DETECTED"),
            actionTone: this.detectBias(signal.direction || signal.matrix_cell || ""),
        }));
    }

    buildReasoningItems(latest, topSignal) {
        const items = [];
        const observed = Array.isArray(latest.observed_facts) ? latest.observed_facts : [];
        const inferred = Array.isArray(latest.inferred_facts) ? latest.inferred_facts : [];
        const missing = Array.isArray(latest.missing_context) ? latest.missing_context : [];

        if (observed.length) {
            items.push(...observed.map(text => `Đã thấy: ${text}`));
        }
        if (inferred.length) {
            items.push(...inferred.map(text => `Suy ra: ${text}`));
        }
        if (missing.length) {
            items.push(...missing.map(text => `Chưa thấy: ${text}`));
        }

        if (!items.length && topSignal.why_now) {
            items.push(...(Array.isArray(topSignal.why_now) ? topSignal.why_now : [topSignal.why_now]));
        }

        return items.length ? items.slice(0, 8) : ["Đang chờ thêm dữ liệu flow để kết luận sâu hơn."];
    }

    buildRiskItems(latest, topSignal) {
        const risks = []
            .concat(Array.isArray(latest.risk_flags) ? latest.risk_flags : [])
            .concat(Array.isArray(latest.missing_context) ? latest.missing_context : [])
            .concat(Array.isArray(topSignal.conflicts) ? topSignal.conflicts : [])
            .filter(Boolean);
        return [...new Set(risks)].slice(0, 5).length
            ? [...new Set(risks)].slice(0, 5)
            : ["Chưa ghi nhận rủi ro nổi bật ngoài các blind spot hiện có."];
    }

    buildMatrixDescription(latest, topSignal) {
        const parts = [
            latest.flow_state_code || topSignal.flow_state || "",
            latest.tradability_grade ? `Grade ${latest.tradability_grade}` : "",
            latest.decision_posture ? `Posture ${latest.decision_posture}` : topSignal.decision_posture ? `Posture ${topSignal.decision_posture}` : "",
        ].filter(Boolean);
        return parts.join(" • ") || "Đang chờ snapshot flow cập nhật.";
    }

    buildPatternTitle(latest, topSignal) {
        const code = latest.pattern_code || topSignal.pattern_code || "";
        const phase = latest.pattern_phase || topSignal.pattern_phase || "";
        if (code && code !== "UNCLASSIFIED") {
            return phase ? `${this.humanizePattern(code)} | ${phase}` : this.humanizePattern(code);
        }
        if (latest.sequence_signature) {
            return latest.sequence_signature;
        }
        return "Đang chờ pattern rõ hơn";
    }

    buildPatternDescription(latest, topSignal) {
        if (latest.decision_summary_vi) {
            return latest.decision_summary_vi;
        }
        if (topSignal.decision_summary_vi) {
            return topSignal.decision_summary_vi;
        }
        if (Array.isArray(topSignal.why_now) && topSignal.why_now.length) {
            return topSignal.why_now.slice(0, 3).join(" • ");
        }
        return "Chưa đủ dữ liệu để mô tả pattern hiện tại.";
    }

    buildAction(latest, topSignal) {
        const posture = String(latest.decision_posture || topSignal.decision_posture || "");
        const stage = String(topSignal.stage || "");
        const direction = String(topSignal.direction || latest.continuation_bias || latest.matrix_cell || "");

        if (posture.includes("LONG") || (stage === "ACTIONABLE" && direction.includes("LONG"))) {
            return { label: "ƯU TIÊN LONG CÓ CHỌN LỌC", className: "action-buy" };
        }
        if (posture.includes("SHORT") || (stage === "ACTIONABLE" && direction.includes("SHORT"))) {
            return { label: "ƯU TIÊN SHORT CÓ CHỌN LỌC", className: "action-sell" };
        }
        if (stage === "WATCHLIST" || posture === "CONSERVATIVE") {
            return { label: "THEO DÕI SÁT", className: "action-neutral" };
        }
        return { label: "ĐỨNG NGOÀI", className: "action-neutral" };
    }

    renderList(container, items) {
        if (!container) {
            return;
        }
        container.innerHTML = "";
        items.forEach((text) => {
            const item = document.createElement("div");
            item.className = "list-item";
            item.innerText = text;
            container.appendChild(item);
        });
    }

    getPrimarySignal() {
        const signals = Array.isArray(this.data.summary.top_signals) ? this.data.summary.top_signals : [];
        if (signals.length) {
            return signals[0];
        }
        return {};
    }

    getSummarySnapshot() {
        return (this.data.summary || {}).latest_tpfm || {};
    }

    getLatestM5Snapshot() {
        return this.getLatestSnapshot(this.data.m5) || {};
    }

    getPrimarySnapshot() {
        const summarySnapshot = this.getSummarySnapshot();
        const latestM5 = this.getLatestM5Snapshot();
        const summaryTs = Number(summarySnapshot.window_end_ts || 0);
        const m5Ts = Number(latestM5.window_end_ts || 0);
        const artifactRunId = String(this.data.status.artifact_run_id || this.data.summary?.artifact_contract?.run_id || "");
        const m5RunId = String(latestM5.run_id || "");
        const summaryRunId = String(summarySnapshot.run_id || this.data.summary?.artifact_contract?.run_id || "");

        if (artifactRunId && summaryRunId && artifactRunId === summaryRunId) {
            if (artifactRunId && m5RunId && artifactRunId !== m5RunId) {
                return summarySnapshot;
            }
        }

        if (!Number.isFinite(summaryTs) || summaryTs <= 0) {
            return latestM5;
        }
        if (!Number.isFinite(m5Ts) || m5Ts <= 0) {
            return summarySnapshot;
        }
        return m5Ts > summaryTs ? latestM5 : summarySnapshot;
    }

    isSummaryFreshAgainstM5() {
        const summarySnapshot = this.getSummarySnapshot();
        const latestM5 = this.getLatestM5Snapshot();
        const summaryTs = Number(summarySnapshot.window_end_ts || 0);
        const m5Ts = Number(latestM5.window_end_ts || 0);
        if (!summaryTs || !m5Ts) {
            return Boolean(summaryTs || m5Ts);
        }
        const summaryRunId = String(summarySnapshot.run_id || this.data.summary?.artifact_contract?.run_id || "");
        const m5RunId = String(latestM5.run_id || "");
        const sameRun = summaryRunId && m5RunId ? summaryRunId === m5RunId : true;
        return Math.abs(summaryTs - m5Ts) <= 300000 && sameRun;
    }

    getLatestSnapshot(list) {
        if (!Array.isArray(list) || !list.length) {
            return null;
        }
        return [...list].sort(
            (left, right) => Number(right.window_end_ts || 0) - Number(left.window_end_ts || 0),
        )[0];
    }

    normalizeMode(dataMode, liveEnabled) {
        const mode = String(dataMode || "").toLowerCase();
        if (mode === "scan" || mode === "scan_only") {
            return "scan";
        }
        return liveEnabled ? "live" : "scan";
    }

    hasOhlc(row) {
        return ["open_px", "high_px", "low_px", "close_px"].every((key) => {
            const value = Number(row?.[key]);
            return Number.isFinite(value) && value > 0;
        });
    }

    isM5HistoryAlignedWithSummary(ordered, latestSummary) {
        if (!latestSummary || !latestSummary.window_end_ts) {
            return true;
        }
        const latestRow = ordered[ordered.length - 1];
        if (!latestRow) {
            return false;
        }
        const latestRowTs = Number(latestRow.window_end_ts || 0);
        const latestSummaryTs = Number(latestSummary.window_end_ts || 0);
        if (!Number.isFinite(latestRowTs) || !Number.isFinite(latestSummaryTs) || latestRowTs <= 0 || latestSummaryTs <= 0) {
            return false;
        }
        const tsGap = Math.abs(latestRowTs - latestSummaryTs);
        const sameMatrix = String(latestRow.matrix_cell || "") === String(latestSummary.matrix_cell || "");
        const sameRun = latestRow.run_id && latestSummary.run_id ? latestRow.run_id === latestSummary.run_id : true;
        return tsGap <= 300000 && sameMatrix && sameRun;
    }

    describeFallbackSeries(row) {
        if (this.numberOrNull(row?.microprice) !== null) {
            return "microprice";
        }
        if (this.numberOrNull(row?.mid_px) !== null) {
            return "mid price";
        }
        return "delta_quote";
    }

    updateOverlayPrice(close, pct, delta, timeframe) {
        document.getElementById("current-price").innerText = close.toFixed(2);
        const pctText = `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
        const deltaText = `${delta >= 0 ? "+" : ""}${delta.toFixed(2)}`;
        const label = document.getElementById("price-change");
        label.innerText = `${pctText} (${deltaText}) | ${timeframe}`;
        label.style.color = pct >= 0 ? "#00ffa3" : "#ff4d4d";
    }

    updateOverlayNoPrice(title, subtitle) {
        document.getElementById("current-price").innerText = title;
        const label = document.getElementById("price-change");
        label.innerText = subtitle;
        label.style.color = "#94a3b8";
    }

    humanizeSetup(setup) {
        const labels = {
            stealth_accumulation: "Tích lũy âm thầm",
            breakout_ignition: "Kích hoạt bứt phá",
            failed_breakout: "Bứt phá thất bại",
            distribution: "Phân phối",
        };
        return labels[setup] || String(setup || "Snapshot");
    }

    humanizeStage(stage) {
        const labels = {
            ACTIONABLE: "Có thể hành động",
            WATCHLIST: "Theo dõi sát",
            CONFIRMED: "Đã xác nhận",
            DETECTED: "Mới phát hiện",
            INVALIDATED: "Đã vô hiệu",
            RESOLVED: "Đã chốt",
        };
        return labels[stage] || String(stage || "Snapshot");
    }

    humanizePattern(code) {
        const labels = {
            CONTI_LONG: "Tiếp diễn mua",
            CONTI_SHORT: "Tiếp diễn bán",
            TRAP_LONG: "Bẫy mua",
            TRAP_SHORT: "Bẫy bán",
            ABSORB_LONG: "Hấp thụ rồi đảo mua",
            ABSORB_SHORT: "Hấp thụ rồi đảo bán",
            EXHAUSTION_LONG: "Mua cạn lực",
            EXHAUSTION_SHORT: "Bán cạn lực",
            SQUEEZE_LONG: "Squeeze mua",
            FLUSH_SHORT: "Xả bán ép buộc",
            BALANCE: "Cân bằng dòng tiền",
            UNCLASSIFIED: "Chưa phân loại pattern",
        };
        return labels[code] || code || "Chưa phân loại pattern";
    }

    scoreToGrade(score) {
        const value = Number(score || 0);
        if (value >= 0.8 || value >= 85) {
            return "A";
        }
        if (value >= 0.6 || value >= 70) {
            return "B";
        }
        if (value > 0) {
            return "C";
        }
        return "D";
    }

    detectBias(text) {
        const value = String(text || "").toLowerCase();
        if (value.includes("long") || value.includes("mua") || value.includes("bid") || value.includes("bull")) {
            return "buy";
        }
        if (value.includes("short") || value.includes("bán") || value.includes("ask") || value.includes("bear")) {
            return "sell";
        }
        return "neutral";
    }

    formatClock(timestampMs) {
        const date = new Date(Number(timestampMs));
        return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }

    formatRelativeTime(isoOrTs) {
        const timestamp = typeof isoOrTs === "number"
            ? Number(isoOrTs)
            : Date.parse(String(isoOrTs));
        if (!Number.isFinite(timestamp)) {
            return "--:--";
        }
        const diffMinutes = Math.max(0, Math.floor((Date.now() - timestamp) / 60000));
        if (diffMinutes < 1) {
            return "Vừa xong";
        }
        if (diffMinutes < 60) {
            return `${diffMinutes} phút trước`;
        }
        const diffHours = Math.floor(diffMinutes / 60);
        return `${diffHours} giờ trước`;
    }

    numberOrNull(value) {
        const number = Number(value);
        return Number.isFinite(number) ? number : null;
    }
}

document.addEventListener("DOMContentLoaded", () => {
    window.dashboard = new RadaDashboard();
});
