// RadaCrypto Dashboard Logic - Multi-View & Data Driven

document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initSessionTimer();
    initMockLiveFeed();
    loadRealData();
    
    // Refresh data every 1 minute
    setInterval(loadRealData, 60000);
});

/**
 * Điều hướng giữa các View
 */
function initNavigation() {
    const navItems = document.querySelectorAll('.nav-item');
    const views = document.querySelectorAll('.app-view');
    
    const viewMap = {
        'Tổng quan': 'view-overview',
        'Thị trường Live': 'view-live',
        'Review nhật ký': 'view-journal',
        'Tuning hệ thống': 'view-tuning'
    };

    navItems.forEach(item => {
        item.addEventListener('click', () => {
            // Update Active Nav
            navItems.forEach(n => n.classList.remove('active'));
            item.classList.add('active');
            
            // Switch View
            const targetId = viewMap[item.textContent.trim()];
            views.forEach(v => v.classList.remove('active'));
            const targetView = document.getElementById(targetId);
            if (targetView) {
                targetView.classList.add('active');
                
                // Trigger specific loaders
                if (targetId === 'view-journal') loadJournalData();
                if (targetId === 'view-live') loadLiveMarketData();
            }
        });
    });
}

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

async function loadJournalData() {
    const list = document.getElementById('journal-list');
    try {
        const res = await fetch('data/thesis_log.json');
        if (!res.ok) throw new Error("File not found");
        const logs = await res.json();
        
        list.innerHTML = '';
        logs.reverse().slice(0, 50).forEach(entry => {
            const div = document.createElement('div');
            div.className = 'journal-entry';
            const time = new Date(entry.timestamp).toLocaleTimeString('vi-VN');
            div.innerHTML = `
                <span class="entry-time">${time}</span>
                <span class="entry-message">${entry.message || 'Cập nhật hệ thống'}</span>
                <span class="badge ${entry.bias === 'LONG' ? 'buy' : 'sell'}">${entry.bias || 'N/A'}</span>
                <span class="entry-grade">Grade: ${entry.flow_grade || 'B'}</span>
                <span style="color: var(--text-secondary)">#${entry.id || '---'}</span>
            `;
            list.appendChild(div);
        });
    } catch (err) {
        list.innerHTML = '<div class="loading">Chưa có dữ liệu nhật ký thực tế. Đang chờ GitHub Actions...</div>';
    }
}

function loadLiveMarketData() {
    loadLiveSignals();
    
    // Fallback UI for symbol grid
    const grid = document.querySelector('.live-symbols-grid');
    if (grid && grid.children.length === 0) {
        grid.innerHTML = `
            <div class="symbol-mini-card glass highlight"><span>BTCUSDT</span><span class="pos">+2.4%</span></div>
            <div class="symbol-mini-card glass"><span>ETHUSDT</span><span class="pos">+1.8%</span></div>
        `;
    }
}

async function loadLiveSignals() {
    const tableBody = document.querySelector('#live-signals-table tbody');
    if (!tableBody) return;

    try {
        // Try multiple paths to support both Local and GitHub Pages
        const paths = ['data/thesis_log.json', '../data/thesis/thesis_log.jsonl', 'data/thesis/thesis_log.jsonl'];
        let logs = [];

        for (const path of paths) {
            try {
                const res = await fetch(path);
                if (!res.ok) continue;
                const text = await res.text();
                // Handle both JSON array and JSONL
                if (text.trim().startsWith('[')) {
                    logs = JSON.parse(text);
                } else {
                    logs = text.trim().split('\n').map(l => JSON.parse(l));
                }
                break;
            } catch (e) { continue; }
        }

        if (logs.length === 0) return;

        // Map setup codes to display names
        const setupMap = {
            'stealth_accumulation': 'Tích lũy âm thầm',
            'breakout_ignition': 'Kích hoạt bứt phá',
            'distribution': 'Phân phối / Xả hàng',
            'failed_breakout': 'Bứt phá thất bại',
            'absorption_play': 'Vùng hấp thụ'
        };

        // Filter for actionable signals or just latest transitions
        const signals = logs.filter(l => l.event_type === 'stage_transition')
                           .reverse()
                           .slice(0, 10);

        tableBody.innerHTML = '';
        signals.forEach(sig => {
            const isLong = sig.direction === 'LONG_BIAS' || (sig.summary_vi && sig.summary_vi.includes('Mua'));
            const grade = sig.score >= 90 ? 'A+' : sig.score >= 80 ? 'A' : sig.score >= 70 ? 'B+' : 'B';
            
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>#${sig.thesis_id ? sig.thesis_id.substring(0, 4) : '---'}</td>
                <td>${setupMap[sig.setup] || sig.setup || 'N/A'}</td>
                <td><span class="badge ${isLong ? 'buy' : 'sell'}">${isLong ? 'LONG' : 'SHORT'}</span></td>
                <td>${sig.matrix_cell || 'N/A'}</td>
                <td><span class="grade grade-${grade.toLowerCase().replace('+', 'plus')}">${grade}</span></td>
                <td>${sig.summary_vi ? sig.summary_vi.split('.')[0] : 'Đang theo dõi'}</td>
                <td>Cấu trúc vỡ</td>
            `;
            tableBody.appendChild(row);
        });
    } catch (err) {
        console.warn("Failed to load live signals:", err);
    }
}

function updateSystemHealth(status) {
    const healthValues = document.querySelectorAll('.h-value');
    if (healthValues.length >= 3) {
        healthValues[0].textContent = status.scan_count > 0 ? "Ổn định" : "Khởi tạo";
        healthValues[0].className = "h-value ok";
        
        const grade = status.latest_flow_grade || "N/A";
        const gradeEl = document.querySelector('.metric .grade-a');
        if (gradeEl) {
            gradeEl.textContent = grade;
            gradeEl.className = `m-value grade-${grade.toLowerCase().startsWith('a') ? 'a' : 'b'}`;
        }
    }
    
    // Session timer (distance from last run)
    const sessionTimerValue = document.querySelector('.session-timer .value');
    if (status.last_run && sessionTimerValue) {
        const lastRun = new Date(status.last_run);
        const now = new Date();
        const diffMs = now - lastRun;
        const diffMins = Math.floor(diffMs / 60000);
        sessionTimerValue.textContent = diffMins < 1 ? "Vừa mới đây" : `${diffMins} phút trước`;
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
        conclusion.textContent = `Dòng tiền đang ở trạng thái ${m5.matrix_cell}. Thuận pha ${(isPos ? 'Mua' : 'Bán')} mạnh.`;
        
        if (biasEl) {
            biasEl.textContent = isPos ? "THUẬN MUA" : "THUẬN BÁN";
            biasEl.className = isPos ? "m-value bias-long" : "m-value bias-short";
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

function initSessionTimer() {
    const timerValue = document.querySelector('.session-timer .value');
    if (timerValue && (timerValue.textContent.includes('phút') || timerValue.textContent.includes('Vừa'))) return;
    
    let seconds = 0;
    setInterval(() => {
        if (timerValue.textContent.includes('phút') || timerValue.textContent.includes('Vừa')) return;
        seconds++;
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = seconds % 60;
        timerValue.textContent = `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
    }, 1000);
}

function initMockLiveFeed() {
    // Disabled - switched to real data via loadLiveSignals()
}
