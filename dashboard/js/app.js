// RadaCrypto Dashboard Logic

document.addEventListener('DOMContentLoaded', () => {
    initSessionTimer();
    initMockLiveFeed();
    initIntelligenceUpdates();
});

/**
 * Hiệu ứng đếm ngược/tiến thời gian phiên
 */
function initSessionTimer() {
    const timerValue = document.querySelector('.session-timer .value');
    let seconds = 0;
    
    setInterval(() => {
        seconds++;
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = seconds % 60;
        
        timerValue.textContent = 
            `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
    }, 1000);
}

/**
 * Giả lập đẩy tín hiệu mới vào bảng
 */
function initMockLiveFeed() {
    const tableBody = document.querySelector('.signals-table tbody');
    
    // Mỗi 15-30 giây thêm một signal ảo để tạo cảm giác live
    setInterval(() => {
        const id = Math.floor(Math.random() * 9000) + 1000;
        const setups = ["Breakout Confirm", "Trend Conti.", "Mean Reversion", "Absorption Play"];
        const setup = setups[Math.floor(Math.random() * setups.length)];
        const isLong = Math.random() > 0.4;
        
        const row = document.createElement('tr');
        row.style.opacity = '0';
        row.style.transform = 'translateY(10px)';
        row.style.transition = 'all 0.5s ease';
        
        row.innerHTML = `
            <td>#${id}</td>
            <td>${setup}</td>
            <td><span class="badge ${isLong ? 'buy' : 'sell'}">${isLong ? 'LONG' : 'SHORT'}</span></td>
            <td>${isLong ? 'Accumulation' : 'Distribution'}</td>
            <td><span class="grade">${['A', 'A+', 'B', 'B+'][Math.floor(Math.random() * 4)]}</span></td>
            <td>${isLong ? 'Mua nhẹ' : 'Bán nhẹ'} tại Market</td>
            <td>${isLong ? 'Thủng đáy' : 'Vượt đỉnh'} m5</td>
        `;
        
        // Thêm vào đầu bảng
        if (tableBody.firstChild) {
            tableBody.insertBefore(row, tableBody.firstChild);
        } else {
            tableBody.appendChild(row);
        }

        // Animate in
        setTimeout(() => {
            row.style.opacity = '1';
            row.style.transform = 'translateY(0)';
        }, 50);

        // Giới hạn max 10 dòng
        if (tableBody.children.length > 10) {
            tableBody.removeChild(tableBody.lastChild);
        }
    }, 20000);
}

/**
 * Cập nhật các trạng thái trí tuệ định kỳ
 */
function initIntelligenceUpdates() {
    // Các câu kết luận mẫu
    const catchphrases = [
        "Dòng tiền Mua đang dẫn dắt, Inventory Bid hấp thụ tốt.",
        "Áp lực Bán đang gia tăng, lực đỡ tại M5 đang suy yếu.",
        "Thị trường đang trong vùng tranh chấp, ưu tiên đứng ngoài.",
        "Phát hiện bẫy giá (Bull Trap) tại vùng kháng cự m30.",
        "Dòng tiền thông minh (Smart Flow) đang âm thầm tích lũy."
    ];

    setInterval(() => {
        const headline = document.querySelector('.status-headline h2');
        const conclusion = document.querySelector('.conclusion-text');
        
        if (headline && Math.random() > 0.7) {
            const index = Math.floor(Math.random() * catchphrases.length);
            headline.style.opacity = '0.5';
            setTimeout(() => {
                headline.textContent = catchphrases[index];
                headline.style.opacity = '1';
            }, 300);
        }
    }, 15000);
}
