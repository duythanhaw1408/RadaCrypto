/**
 * RadaCrypto Professional Dashboard
 * Centered on Intelligence Charts & Decision Logic
 */

class RadaDashboard {
    constructor() {
        this.charts = {};
        this.series = {};
        this.data = {
            m5: [],
            m30: [],
            h4: [],
            summary: {},
            daily: {},
            health: {},
            logs: []
        };
        
        this.init();
    }

    async init() {
        this.initCharts();
        await this.updateLoop();
        setInterval(() => this.updateLoop(), 60000); // 1 min sync
    }

    initCharts() {
        const chartOptions = {
            layout: {
                backgroundColor: '#0a0a0f',
                textColor: '#94a3b8',
                fontSize: 12,
                fontFamily: 'Inter, sans-serif',
            },
            grid: {
                vertLines: { color: '#1c1c28' },
                horzLines: { color: '#1c1c28' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: '#2d2d3d',
            },
            timeScale: {
                borderColor: '#2d2d3d',
                timeVisible: true,
                secondsVisible: false,
            },
        };

        // Main Candlestick Chart
        this.charts.main = LightweightCharts.createChart(document.getElementById('main-chart'), chartOptions);
        this.series.candle = this.charts.main.addCandlestickSeries({
            upColor: '#00ffa3', downColor: '#ff4d4d', borderVisible: false,
            wickUpColor: '#00ffa3', wickDownColor: '#ff4d4d',
        });

        // Volume Delta Chart (Independent below main)
        this.charts.volume = LightweightCharts.createChart(document.getElementById('volume-chart'), {
            ...chartOptions,
            height: 150,
        });
        this.series.volume = this.charts.volume.addHistogramSeries({
            color: '#3b82f6',
            priceFormat: { type: 'volume' },
            priceScaleId: 'volume',
        });
        
        this.charts.volume.priceScale('volume').applyOptions({
            scaleMargins: { top: 0.1, bottom: 0.1 },
        });

        // Sync timescales
        this.charts.main.timeScale().subscribeVisibleTimeRangeChange(range => {
            this.charts.volume.timeScale().setVisibleRange(range);
        });
        this.charts.volume.timeScale().subscribeVisibleTimeRangeChange(range => {
            this.charts.main.timeScale().setVisibleRange(range);
        });

        window.addEventListener('resize', () => {
            this.charts.main.resize(document.getElementById('main-chart').clientWidth, document.getElementById('main-chart').clientHeight);
            this.charts.volume.resize(document.getElementById('volume-chart').clientWidth, 150);
        });
    }

    async updateLoop() {
        try {
            const [m5Res, m30Res, h4Res, summaryRes, dailyRes, healthRes, logsRes] = await Promise.all([
                fetch('data/tpfm_m5.json?' + Date.now()),
                fetch('data/tpfm_m30.json?' + Date.now()),
                fetch('data/tpfm_4h.json?' + Date.now()),
                fetch('data/replay/summary_btcusdt.json?' + Date.now()),
                fetch('data/review/daily_summary.json?' + Date.now()),
                fetch('data/review/health_status.json?' + Date.now()),
                fetch('data/thesis_log.json?' + Date.now())
            ]);

            if (m5Res.ok) this.data.m5 = await m5Res.json();
            if (m30Res.ok) this.data.m30 = await m30Res.json();
            if (h4Res.ok) this.data.h4 = await h4Res.json();
            if (summaryRes.ok) this.data.summary = await summaryRes.json();
            if (dailyRes.ok) this.data.daily = await dailyRes.json();
            if (healthRes.ok) this.data.health = await healthRes.json();
            if (logsRes.ok) this.data.logs = await logsRes.json();

            this.render();
        } catch (e) {
            console.error("Dashboard Sync Error:", e);
        }
    }

    render() {
        this.renderCharts();
        this.renderHeader();
        this.renderDecisionPanel();
        this.renderMTFStack();
        this.renderSignalLog();
    }

    renderCharts() {
        if (!this.data.m5.length || !this.series.candle) return;

        const candleData = this.data.m5.map(d => ({
            time: d.window_start_ts / 1000,
            open: d.open_px,
            high: d.high_px,
            low: d.low_px,
            close: d.close_px
        })).sort((a, b) => a.time - b.time);

        const volumeData = this.data.m5.map(d => ({
            time: d.window_start_ts / 1000,
            value: Math.abs(d.delta_quote || d.volume_quote),
            color: (d.delta_quote || 0) >= 0 ? '#00ffa3' : '#ff4d4d'
        })).sort((a, b) => a.time - b.time);

        this.series.candle.setData(candleData);
        this.series.volume.setData(volumeData);
        this.renderMarkers();

        // Update Price Overlay
        const latest = candleData[candleData.length - 1];
        if (latest) {
            document.getElementById('current-price').innerText = latest.close.toFixed(2);
            const change = ((latest.close - latest.open) / latest.open * 100).toFixed(2);
            document.getElementById('price-change').innerText = `${change}% (${(latest.close - latest.open).toFixed(2)}) | m5`;
            document.getElementById('price-change').style.color = change >= 0 ? '#00ffa3' : '#ff4d4d';
        }
    }

    renderMarkers() {
        if (!this.data.m5.length) return;

        const markers = [];
        this.data.m5.forEach(d => {
            const time = d.window_start_ts / 1000;
            const matrix = d.matrix_cell || "";
            const flow = d.flow_state_code || "";

            if (matrix.includes('PASSIVE_BID') || matrix.includes('GOM')) {
                markers.push({
                    time: time,
                    position: 'belowBar',
                    color: '#00ffa3',
                    shape: 'arrowUp',
                    text: 'GOM',
                });
            } else if (matrix.includes('PASSIVE_ASK') || matrix.includes('XA')) {
                markers.push({
                    time: time,
                    position: 'aboveBar',
                    color: '#ff4d4d',
                    shape: 'arrowDown',
                    text: 'XA',
                });
            }

            if (flow.includes('TRAP')) {
                markers.push({
                    time: time,
                    position: 'aboveBar',
                    color: '#ffaa00',
                    shape: 'circle',
                    text: 'TRAP',
                });
            }
            
            if (flow.includes('SQUEEZE')) {
                markers.push({
                    time: time,
                    position: 'belowBar',
                    color: '#3b82f6',
                    shape: 'circle',
                    text: 'SQZ',
                });
            }
        });

        this.series.candle.setMarkers(markers);
    }

    renderHeader() {
        const s = this.data.summary;
        const h = this.data.health;
        
        document.getElementById('symbol-display').innerText = s.instrument_key || 'BTC/USDT';
        document.getElementById('header-grade').innerText = `Hạng ${this.data.daily.latest_flow_grade || 'D'}`;
        
        const mode = h.system_status || 'SCAN';
        document.getElementById('mode-label').innerText = mode === 'GOOD' ? 'TRỰC TIẾP' : 'QUÉT DỮ LIỆU';
        document.getElementById('system-label').innerText = mode === 'GOOD' ? 'Hệ thống ổn định' : 'Hệ thống suy giảm';
        document.getElementById('system-dot').style.background = mode === 'GOOD' ? '#00ffa3' : '#ffaa00';
        document.getElementById('system-dot').style.boxShadow = `0 0 8px ${mode === 'GOOD' ? '#00ffa3' : '#ffaa00'}`;
        
        document.getElementById('last-update').innerText = new Date().toLocaleTimeString();
    }

    renderDecisionPanel() {
        const s = this.data.summary;
        const d = this.data.daily;

        document.getElementById('matrix-title').innerText = d.matrix_alias_vi || 'Chưa rõ ma trận';
        document.getElementById('matrix-desc').innerText = `Flow State: ${d.latest_flow_state || 'N/A'}`;

        const topSignal = (s.top_signals || [])[0] || {};
        document.getElementById('pattern-title').innerText = topSignal.setup || 'Đang quét mẫu hình...';
        document.getElementById('pattern-desc').innerText = Array.isArray(topSignal.why_now) ? topSignal.why_now.join(' • ') : 'Đang chờ xác nhận dòng tiền.';

        const actionCard = document.getElementById('action-card');
        actionCard.innerText = topSignal.stage === 'ACTIONABLE' ? 'CÓ THỂ HÀNH ĐỘNG' : 
                          topSignal.stage === 'WATCHLIST' ? 'THEO DÕI SÁT' : 'ĐỨNG NGOÀI';
        
        actionCard.className = 'action-card ' + (topSignal.stage === 'ACTIONABLE' ? 'action-buy' : 
                                            topSignal.stage === 'WATCHLIST' ? 'action-sell' : 'action-neutral');

        // Reasoning
        const reasonList = document.getElementById('reasoning-list');
        reasonList.innerHTML = '';
        const reasons = Array.isArray(topSignal.why_now) ? topSignal.why_now : ['Phân tích dòng tiền đang diễn ra...'];
        reasons.forEach(r => {
            const item = document.createElement('div');
            item.className = 'list-item';
            item.innerText = r;
            reasonList.appendChild(item);
        });

        // Probability
        document.getElementById('prob-title').innerText = `${((d.hit_rate || 0) * 100).toFixed(0)}% Win Rate`;
        if (d.pattern_scorecard_vi && d.pattern_scorecard_vi.includes('Lưu ý')) {
            document.getElementById('prob-desc').innerText = 'Dữ liệu mẫu còn mỏng (n < 10).';
        } else {
            document.getElementById('prob-desc').innerText = 'Mẫu đủ tham chiếu.';
        }
    }

    renderMTFStack() {
        const container = document.getElementById('mtf-stack');
        container.innerHTML = '';
        
        const latestM5 = this.data.m5[0] || {};
        const latestM30 = this.data.m30[0] || {};
        const latestH4 = this.data.h4[0] || {};

        const timeframes = [
            { id: 'M5', label: '5 Phút', grade: this.data.daily.latest_flow_grade || 'D', alias: latestM5.matrix_cell || 'N/A' },
            { id: '30M', label: '30 Phút', grade: 'C', alias: latestM30.matrix_cell || 'Chưa rõ' },
            { id: 'H1', label: '1 Giờ', grade: 'B', alias: 'Đang đè' },
            { id: 'H4', label: '4 Giờ', grade: 'C', alias: latestH4.matrix_cell || 'Chưa rõ' }
        ];

        timeframes.forEach(tf => {
            const card = document.createElement('div');
            const bias = tf.alias.toLowerCase().includes('buy') || tf.alias.toLowerCase().includes('mua') ? 'buy' : 
                         tf.alias.toLowerCase().includes('sell') || tf.alias.toLowerCase().includes('bán') ? 'sell' : 'neutral';
            
            card.className = `mtf-card active-bias-${bias}`;
            card.innerHTML = `
                <span class="mtf-timeframe">${tf.id}</span>
                <span class="mtf-alias">${tf.alias}</span>
                <span class="mtf-grade">Hạng ${tf.grade}</span>
            `;
            container.appendChild(card);
        });
    }

    renderSignalLog() {
        const tbody = document.getElementById('signal-log-body');
        if (!tbody) return;
        tbody.innerHTML = '';

        const logs = this.data.logs.slice(-50).reverse();
        logs.forEach(log => {
            const tr = document.createElement('tr');
            const grade = (log.metadata?.flow_grade_at_entry || 'D').toLowerCase();
            const timeStr = new Date(log.opened_ts * 1000).toLocaleTimeString();

            tr.innerHTML = `
                <td>${timeStr}</td>
                <td>${log.setup}</td>
                <td>${log.metadata?.matrix_cell_at_entry || log.matrix_cell || '--'}</td>
                <td><span class="tag-grade tag-grade-${grade}">${grade.toUpperCase()}</span></td>
                <td>${log.decision_summary_vi || '--'}</td>
            `;
            tbody.appendChild(tr);
        });
    }
}

// Start Dashboard
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new RadaDashboard();
});
