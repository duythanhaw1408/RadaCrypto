// RadaCrypto Dashboard Logic - Live & Data Driven

document.addEventListener('DOMContentLoaded', () => {
    initSessionTimer();
    initMockLiveFeed();
    loadRealData();
    
    // Refresh data every 1 minute
    setInterval(loadRealData, 60000);
});

async function loadRealData() {
    try {
        const [statusRes, m5Res] = await Promise.all([
            fetch('data/actions_status.json').catch(() => null),
            fetch('data/tpfm_m5.json').catch(() => null)
        ]);

        if (statusRes && statusRes.ok) {
            const status = await statusRes.json();
            updateSystemHealth(status);
        }

        if (m5Res && m5Res.ok) {
            const m5List = await m5Res.json();
            if (m5List && m5List.length > 0) {
                updateIntelligenceHero(m5List[0]);
                updateMatrixGrid(m5List[0]);
            }
        }
    } catch (err) {
        console.warn("Dữ liệu live chưa sẵn sàng, đang dùng chế độ demo.", err);
    }
}

function updateSystemHealth(status) {
    const healthValues = document.querySelectorAll('.h-value');
    if (healthValues.length >= 3) {
        // Runtime
        healthValues[0].textContent = status.scan_count > 0 ? "Ổn định" : "Khởi tạo";
        healthValues[0].className = "h-value ok";
        
        // Grade
        const grade = status.latest_flow_grade || "N/A";
        const gradeEl = document.querySelector('.metric .grade-a');
        if (gradeEl) {
            gradeEl.textContent = grade;
            gradeEl.className = `m-value grade-${grade.toLowerCase().startsWith('a') ? 'a' : 'b'}`;
        }
    }
    
    // Last run timer
    const sessionTimer = document.querySelector('.session-timer .value');
    if (status.last_run && sessionTimer) {
        const lastRun = new Date(status.last_run);
        const now = new Date();
        const diffMs = now - lastRun;
        const diffMins = Math.floor(diffMs / 60000);
        sessionTimer.textContent = `Cách đây ${diffMins} phút`;
    }
}

function updateIntelligenceHero(m5) {
    const headline = document.querySelector('.status-headline h2');
    const conclusion = document.querySelector('.conclusion-text');
    const biasEl = document.querySelector('.bias-long');
    const indicator = document.querySelector('.indicator');

    if (m5.matrix_cell) {
        const isPos = m5.buy_pressure > 0.5;
        headline.textContent = m5.matrix_cell;
        conclusion.textContent = `Dòng tiền đang ở trạng thái ${m5.matrix_cell}. Mức độ áp lực mua: ${(m5.buy_pressure * 100).toFixed(1)}%.`;
        
        if (biasEl) {
            biasEl.textContent = isPos ? "THUẬN MUA" : "THUẬN BÁN";
            biasEl.className = isPos ? "m-value bias-long" : "m-value bias-short";
            biasEl.style.color = isPos ? "var(--buy)" : "var(--sell)";
        }
        
        if (indicator) {
            indicator.className = isPos ? "indicator buy" : "indicator sell";
        }
    }
}

function updateMatrixGrid(m5) {
    const m5State = document.querySelector('.matrix-overview .matrix-column:nth-child(1) .m-state');
    if (m5State) {
        m5State.textContent = m5.matrix_cell;
        m5State.className = `m-state ${m5.buy_pressure > 0.5 ? 'acc' : 'distribution'}`;
    }
}

/**
 * Hiệu ứng đếm thời gian phiên (Fallback if no real data)
 */
function initSessionTimer() {
    const timerValue = document.querySelector('.session-timer .value');
    if (timerValue.textContent.includes('phút')) return; 
    
    let seconds = 0;
    setInterval(() => {
        if (timerValue.textContent.includes('phút')) return;
        seconds++;
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = seconds % 60;
        timerValue.textContent = `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
    }, 1000);
}

/**
 * Giả lập đẩy tín hiệu mới vào bảng (Demo mode)
 */
function initMockLiveFeed() {
    const tableBody = document.querySelector('.signals-table tbody');
    setInterval(() => {
        if (tableBody.children.length > 5) return; // Don't overflow if real data exists
        const id = Math.floor(Math.random() * 9000) + 1000;
        const setup = "Flow Confirmation";
        const isLong = Math.random() > 0.5;
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>#${id}</td>
            <td>${setup}</td>
            <td><span class="badge ${isLong ? 'buy' : 'sell'}">${isLong ? 'LONG' : 'SHORT'}</span></td>
            <td>Matrix Meta</td>
            <td><span class="grade">A</span></td>
            <td>Theo dõi Vol</td>
            <td>Vỡ cấu trúc</td>
        `;
        if (tableBody.firstChild) tableBody.insertBefore(row, tableBody.firstChild);
        else tableBody.appendChild(row);
    }, 30000);
}
