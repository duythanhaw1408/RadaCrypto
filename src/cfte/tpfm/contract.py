from datetime import datetime
from cfte.tpfm.models import TPFMSnapshot, FlowDecisionView

class FlowDecisionContract:
    @staticmethod
    def generate_brief(snapshot: TPFMSnapshot, decision: FlowDecisionView) -> str:
        """Generates a professional, Telegram-friendly Flow Decision Brief"""
        
        # Determine Header Emoji based on flow_bias and tradability
        header_emoji = "🔥" if decision.flow_bias == "LONG" else "🥶" if decision.flow_bias == "SHORT" else "⚖️"
        grade_emoji = "🟢" if decision.tradability_grade in ["A", "B"] else "🟡" if decision.tradability_grade == "C" else "🔴"
        
        # Format Time
        ts_formatted = datetime.fromtimestamp(snapshot.window_end_ts / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Sequence & Stack text
        seq_text = "N/A"
        if snapshot.sequence_length > 1:
            seq_text = f"{snapshot.sequence_length} nến {snapshot.sequence_family} liên tiếp"
            
        stack_text = "N/A"
        if snapshot.stack_state:
            stack_text = f"M5 vs H4: {snapshot.stack_state.micro_vs_macro} | Cấu trúc: {snapshot.stack_state.stack_alignment}"
            
        # Probability Edge text
        edge_text = ""
        if snapshot.edge_profile:
            edge_text = f"\n🎯 **Xác suất Lịch sử (Beta)**\n"
            edge_text += f"Win Rate: {snapshot.edge_profile.historical_win_rate * 100:.1f}%\n"
            edge_text += f"Kỳ vọng R:R: {snapshot.edge_profile.expected_rr}\n"
            edge_text += f"Độ tin cậy: {snapshot.edge_profile.confidence} ({snapshot.edge_profile.sample_size} setups)\n"
            
        brief = f"""
{header_emoji} **[TPFM] FLOW DECISION BRIEF - {snapshot.symbol}** {header_emoji}
🕒 {ts_formatted}

**1. TRẠNG THÁI DÒNG TIỀN (SNAPSHOT)** 📊
• Matrix Cell: {snapshot.matrix_cell} ({snapshot.matrix_alias_vi})
• Bias Dòng Tiền: {decision.flow_bias} | Grade: {decision.tradability_grade} {grade_emoji}
• Sức ép (Initiative): {snapshot.initiative_score:.2f}
• Áp đảo (Inventory): {snapshot.inventory_score:.2f}

**2. BỐI CẢNH THỜI GIAN (TEMPORAL & SEQUENCE)** ⏳
• Chuỗi M5 hiện tại: {seq_text}
• Gia tốc: {snapshot.tempo_state}
• M5/H4 Stack: {stack_text}
{edge_text}
**3. QUYẾT ĐỊNH GIAO DỊCH (DECISION POSTURE)** ⚔️
• Chiến lược định hướng: {snapshot.action_plan_vi}
• Điểm vào lệnh (Entry Condition): {decision.entry_condition}
• Tín hiệu Xác nhận bắt buộc: {decision.confirm_needed}
• Bộ lọc rủi ro (Avoid If): {decision.avoid_if}
• Điều kiện Bãi bỏ (Invalidation): {decision.invalid_if}

**4. DẤU HIỆU CẢNH BÁO (RISK RADAR)** ⚠️
"""
        if not decision.risk_flags:
            brief += "• Không phát hiện rủi ro nghiêm trọng.\n"
        else:
            for risk in decision.risk_flags:
                brief += f"• ❗ {risk}\n"
                
        brief += f"\n*ID: {snapshot.snapshot_id[:8]}*"
        return brief
