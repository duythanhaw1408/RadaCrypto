// RadaCrypto Dashboard Logic - Pages-friendly snapshot mode

let realDataLoaded = false;

document.addEventListener("DOMContentLoaded", () => {
    initSessionTimer();
    // initMockLiveFeed(); // Finding 2: Disable demo masquerading
    loadRealData();

    // Refresh data every 1 minute
    setInterval(loadRealData, 60000);
});

async function fetchJson(path) {
    try {
        const response = await fetch(path);
        if (!response.ok) {
            return null;
        }
        return await response.json();
    } catch (_err) {
        return null;
    }
}

async function loadRealData() {
    const [status, m5List, summary, thesisLog] = await Promise.all([
        fetchJson("data/actions_status.json"),
        fetchJson("data/tpfm_m5.json"),
        fetchJson("data/summary_btcusdt.json"),
        fetchJson("data/thesis_log.json"),
    ]);

    const liveSnapshot = Array.isArray(m5List) ? m5List.find((entry) => entry && entry.matrix_cell) : null;
    const replaySignals = extractSignalsFromThesisLog(thesisLog);
    const summarySignals = Array.isArray(summary?.top_signals) ? summary.top_signals : [];
    const signals = replaySignals.length ? replaySignals : summarySignals;
    const primarySignal = signals[0] || status?.top_signal || null;

    updateSystemHealth(status, Boolean(liveSnapshot), Boolean(primarySignal));

    if (liveSnapshot) {
        updateIntelligenceHeroFromLive(liveSnapshot);
        updateMatrixGridFromLive(liveSnapshot);
        realDataLoaded = true;
    } else if (primarySignal) {
        updateIntelligenceHeroFromReplay(primarySignal);
        updateMatrixGridFromReplay(primarySignal, status);
        realDataLoaded = true;
    }

    if (signals.length > 0) {
        updateSignalsTable(signals);
        realDataLoaded = true;
    }
}

function extractSignalsFromThesisLog(thesisLog) {
    if (!Array.isArray(thesisLog) || thesisLog.length === 0) {
        return [];
    }
    for (let index = thesisLog.length - 1; index >= 0; index -= 1) {
        const record = thesisLog[index];
        if (record && Array.isArray(record.signals) && record.signals.length > 0) {
            return record.signals.slice(0, 5);
        }
    }
    return [];
}

function updateSystemHealth(status, hasLiveSnapshot, hasSnapshotData) {
    const healthValues = document.querySelectorAll(".h-value");
    if (healthValues.length >= 3) {
        healthValues[0].textContent = hasLiveSnapshot ? "Live M5" : hasSnapshotData ? "Scan snapshot" : "Khởi tạo";
        healthValues[0].className = `h-value ${hasLiveSnapshot || hasSnapshotData ? "ok" : "warning"}`;

        const grade = status?.latest_flow_grade || "N/A";
        healthValues[1].textContent = hasLiveSnapshot ? `Grade ${grade}` : "Không dùng live CI";
        healthValues[1].className = `h-value ${hasLiveSnapshot ? "ok" : "warning"}`;

        healthValues[2].textContent = hasLiveSnapshot ? "Flow thật" : hasSnapshotData ? "Replay thật" : "Thiếu dữ liệu";
        healthValues[2].className = `h-value ${hasSnapshotData ? "ok" : "warning"}`;
    }

    const systemStatus = document.querySelector(".status-text");
    if (systemStatus) {
        systemStatus.textContent = hasLiveSnapshot
            ? "Hệ thống: Live M5 sẵn sàng"
            : hasSnapshotData
                ? "Hệ thống: Chế độ scan snapshot"
                : "Hệ thống: Đang khởi tạo";
    }

    const gradeEl = document.querySelector(".metric .m-value");
    if (gradeEl && status?.latest_flow_grade) {
        const grade = String(status.latest_flow_grade);
        gradeEl.textContent = grade;
        gradeEl.className = `m-value grade-${grade.toLowerCase().startsWith("a") ? "a" : "b"}`;
    }

    const sessionTimer = document.querySelector(".session-timer .value");
    if (status?.last_run && sessionTimer) {
        const lastRun = new Date(status.last_run);
        const now = new Date();
        const diffMs = now - lastRun;
        const diffMins = Math.max(0, Math.floor(diffMs / 60000));
        sessionTimer.textContent = `Cách đây ${diffMins} phút`;
    }

    if (ownerRows.length >= 2) {
        ownerRows[0].textContent = status?.data_mode === "scan" ? "GitHub Actions" : "Live runtime";
        ownerRows[1].textContent = status?.data_mode === "scan" ? "scan" : "live";
    }
}

function updateIntelligenceHeroFromLive(m5) {
    const headline = document.querySelector(".status-headline h2");
    const conclusion = document.querySelector(".conclusion-text");
    const biasEl = document.querySelector(".bias-long, .bias-short");
    const indicator = document.querySelector(".indicator");
    const badges = document.querySelectorAll(".intel-badges .badge");

    const isBuy = Number(m5.initiative_score || 0) >= 0;
    const alias = m5.matrix_alias_vi || m5.matrix_cell || "Flow M5";
    const summary = m5.decision_summary_vi || "Dòng tiền live đang sẵn sàng cho trader đọc trực tiếp.";

    if (headline) {
        headline.textContent = alias;
    }
    if (conclusion) {
        conclusion.textContent = summary;
    }
    if (biasEl) {
        biasEl.textContent = isBuy ? "THUẬN MUA" : "THUẬN BÁN";
        biasEl.className = isBuy ? "m-value bias-long" : "m-value bias-short";
        biasEl.style.color = isBuy ? "var(--buy)" : "var(--sell)";
    }
    if (indicator) {
        indicator.className = isBuy ? "indicator buy" : "indicator sell";
    }
    if (badges.length >= 3) {
        badges[0].textContent = `Flow: ${m5.flow_state_code || "LIVE"}`;
        badges[1].textContent = `Matrix: ${alias}`;
        badges[2].textContent = `Tư thế: ${m5.decision_posture || "THEO DÕI"}`;
    }
}

function updateIntelligenceHeroFromReplay(signal) {
    const headline = document.querySelector(".status-headline h2");
    const conclusion = document.querySelector(".conclusion-text");
    const biasEl = document.querySelector(".bias-long, .bias-short");
    const indicator = document.querySelector(".indicator");
    const badges = document.querySelectorAll(".intel-badges .badge");

    const isBuy = String(signal.direction || "").includes("LONG");
    const setupLabel = humanizeSetup(signal.setup);
    const summary = signal.decision_summary_vi || summarizeSignal(signal);

    if (headline) {
        headline.innerHTML = `<span class="scan-tag">[SCAN]</span> ${setupLabel}`;
    }
    if (conclusion) {
        conclusion.textContent = summary;
    }
    if (biasEl) {
        biasEl.textContent = isBuy ? "THUẬN MUA" : "THUẬN BÁN";
        biasEl.className = isBuy ? "m-value bias-long" : "m-value bias-short";
        biasEl.style.color = isBuy ? "var(--buy)" : "var(--sell)";
    }
    if (indicator) {
        indicator.className = isBuy ? "indicator buy" : "indicator sell";
    }
    if (badges.length >= 3) {
        badges[0].textContent = `Flow: ${signal.flow_state || "SCAN SNAPSHOT"}`;
        badges[1].textContent = `Setup: ${setupLabel}`;
        badges[2].textContent = `Trạng thái: ${humanizeStage(signal.stage)}`;
    }

    const gradeMetric = document.querySelector(".intel-metrics .metric .m-value.grade-a, .intel-metrics .metric .m-value.grade-b");
    if (gradeMetric) {
        const grade = deriveGrade(signal);
        gradeMetric.textContent = grade;
        gradeMetric.className = `m-value grade-${grade.toLowerCase().startsWith("a") ? "a" : "b"}`;
    }
}

function updateMatrixGridFromLive(m5) {
    const matrixColumns = document.querySelectorAll(".matrix-overview .matrix-column");
    if (matrixColumns.length === 0) {
        return;
    }
    updateMatrixColumn(matrixColumns[0], {
        title: "Matrix M5",
        state: m5.matrix_alias_vi || m5.matrix_cell || "Live M5",
        grade: m5.tradability_grade || "A",
        bias: Number(m5.initiative_score || 0) >= 0 ? "Mua" : "Bán",
        className: Number(m5.initiative_score || 0) >= 0 ? "acc" : "distribution",
    });
}

function updateMatrixGridFromReplay(signal, status) {
    const matrixColumns = document.querySelectorAll(".matrix-overview .matrix-column");
    if (matrixColumns.length === 0) {
        return;
    }
    updateMatrixColumn(matrixColumns[0], {
        title: "[SCAN] Matrix Snapshot",
        state: humanizeSetup(signal.setup),
        grade: deriveGrade(signal),
        bias: String(signal.direction || "").includes("LONG") ? "Mua" : "Bán",
        className: String(signal.direction || "").includes("LONG") ? "acc" : "distribution",
    });
        updateMatrixColumn(matrixColumns[1], {
            title: "Nguồn dữ liệu",
            state: status?.data_mode === "scan" ? "Chế độ Scan" : "Khởi tạo",
            grade: status?.latest_flow_grade || "N/A",
            bias: "Scan",
            className: "neutral",
        });
}

function updateMatrixColumn(column, payload) {
    const title = column.querySelector(".m-title");
    const state = column.querySelector(".m-state");
    const grade = column.querySelector(".m-grade");
    const bias = column.querySelector(".m-bias");

    if (title) {
        title.textContent = payload.title;
    }
    if (state) {
        state.textContent = payload.state;
        state.className = `m-state ${payload.className}`;
    }
    if (grade) {
        grade.textContent = `Grade: ${payload.grade}`;
    }
    if (bias) {
        bias.textContent = `Bias: ${payload.bias}`;
    }
}

function updateSignalsTable(signals) {
    const tableBody = document.querySelector(".signals-table tbody");
    if (!tableBody) {
        return;
    }

    tableBody.innerHTML = "";
    signals.slice(0, 5).forEach((signal) => {
        const row = document.createElement("tr");
        const isBuy = String(signal.direction || "").includes("LONG");
        row.innerHTML = `
            <td>#${String(signal.thesis_id || "").slice(0, 6)}</td>
            <td>${humanizeSetup(signal.setup)}</td>
            <td><span class="badge ${isBuy ? "buy" : "sell"}">${isBuy ? "LONG" : "SHORT"}</span></td>
            <td>${signal.matrix_alias_vi || humanizeStage(signal.stage)}</td>
            <td><span class="grade">${deriveGrade(signal)}</span></td>
            <td>${truncateText(signal.entry_style || summarizeSignal(signal), 42)}</td>
            <td>${truncateText(signal.invalidation || "Chờ xác nhận", 30)}</td>
        `;
        tableBody.appendChild(row);
    });
}

function humanizeSetup(setup) {
    const labels = {
        stealth_accumulation: "Tích lũy âm thầm",
        breakout_ignition: "Kích hoạt bứt phá",
        failed_breakout: "Bứt phá thất bại",
        distribution: "Phân phối",
    };
    return labels[setup] || String(setup || "Snapshot replay");
}

function humanizeStage(stage) {
    const labels = {
        ACTIONABLE: "Có thể hành động",
        WATCHLIST: "Danh sách theo dõi",
        CONFIRMED: "Đã xác nhận",
        DETECTED: "Mới phát hiện",
        INVALIDATED: "Đã vô hiệu",
    };
    return labels[stage] || String(stage || "Snapshot");
}

function deriveGrade(signal) {
    if (signal.tradability_grade) {
        return signal.tradability_grade;
    }
    const score = Number(signal.score || 0);
    if (score >= 85) {
        return "A";
    }
    if (score >= 70) {
        return "B";
    }
    if (score > 0) {
        return "C";
    }
    return "N/A";
}

function summarizeSignal(signal) {
    const why = Array.isArray(signal.why_now) ? signal.why_now.slice(0, 2).join(" | ") : "";
    if (why) {
        return why;
    }
    if (signal.entry_style) {
        return signal.entry_style;
    }
    return "Snapshot replay mới nhất từ scan-cycle.";
}

function truncateText(value, maxLength) {
    const text = String(value || "");
    if (text.length <= maxLength) {
        return text;
    }
    return `${text.slice(0, maxLength - 1)}…`;
}

function initSessionTimer() {
    const timerValue = document.querySelector(".session-timer .value");
    if (!timerValue || timerValue.textContent.includes("phút")) {
        return;
    }

    let seconds = 0;
    setInterval(() => {
        if (timerValue.textContent.includes("phút")) {
            return;
        }
        seconds += 1;
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = seconds % 60;
        timerValue.textContent =
            `${h.toString().padStart(2, "0")}:${m.toString().padStart(2, "0")}:${s.toString().padStart(2, "0")}`;
    }, 1000);
}

function initMockLiveFeed() {
    const tableBody = document.querySelector(".signals-table tbody");
    if (!tableBody) {
        return;
    }

    setInterval(() => {
        if (realDataLoaded) {
            return;
        }
        const id = Math.floor(Math.random() * 9000) + 1000;
        const setup = "Flow Confirmation";
        const isLong = Math.random() > 0.5;
        const row = document.createElement("tr");
        row.innerHTML = `
            <td>#${id}</td>
            <td>${setup}</td>
            <td><span class="badge ${isLong ? "buy" : "sell"}">${isLong ? "LONG" : "SHORT"}</span></td>
            <td>Chế độ demo</td>
            <td><span class="grade">A</span></td>
            <td>Theo dõi Vol</td>
            <td>Vỡ cấu trúc</td>
        `;
        if (tableBody.firstChild) {
            tableBody.insertBefore(row, tableBody.firstChild);
        } else {
            tableBody.appendChild(row);
        }
        if (tableBody.children.length > 8) {
            tableBody.removeChild(tableBody.lastChild);
        }
    }, 30000);
}
