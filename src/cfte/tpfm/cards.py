from cfte.tpfm.models import TPFMSnapshot

def render_tpfm_m5_card(snap: TPFMSnapshot) -> str:
    """Renders a TPFM M5 Escalation Card for the CLI"""
    
    # Emoji Mapping for Polarity
    init_emoji = "🟢" if "POS" in snap.initiative_polarity else "🔴" if "NEG" in snap.initiative_polarity else "⚪"
    inv_emoji = "🛡️" if "POS" in snap.inventory_polarity else "🧱" if "NEG" in snap.inventory_polarity else "⚖️"
    
    # Energy State Mapping
    energy_emoji = {
        "COMPRESSION": "🗜️ NÉN",
        "EXPANDING": "💥 BÙNG NỔ",
        "EXHAUSTING": "⚠️ KIỆT SỨC"
    }.get(snap.energy_state, snap.energy_state)
    
    # Efficiency Mapping
    eff_emoji = {
        "FOLLOW_THROUGH": "✅ TIẾP DIỄN",
        "MIXED": "🔄 HỖN LOẠN",
        "ABSORBED_OR_TRAP": "🪤 BỊ HẤP THỤ / BẪY"
    }.get(snap.response_efficiency_state, snap.response_efficiency_state)

    # Context Badges
    ctx_badges = []
    if not snap.futures_context_available:
        ctx_badges.append("⚪ THIẾU CONTEXT FUTURES")
    else:
        if snap.context_score == 1.0: ctx_badges.append("🔵 FUTURES XÁC NHẬN")
        elif snap.context_score == -1.0: ctx_badges.append("🟠 FUTURES LỆCH NHỊP")
        
        if "BASIS PHÂN KỲ" in snap.context_warning_flags: ctx_badges.append("⚠️ BASIS PHÂN KỲ")
        if "OI Mở Rộng Ủng Hộ" in snap.escalation_reason: ctx_badges.append("💎 OI MỞ RỘNG ỦNG HỘ")
        if "FUNDING QUÁ NÓNG" in snap.context_warning_flags: ctx_badges.append("🔥 FUNDING QUÁ NÓNG")

    ctx_str = " | ".join(ctx_badges) if ctx_badges else "N/A"
    reasons = "\n".join([f"  • {r}" for r in snap.escalation_reason])
    
    card = f"""
┌──────────────────────────────────────────────────────────┐
│  🚨 TPFM M5 ESCALATION - {snap.symbol} 🚨
├──────────────────────────────────────────────────────────┤
│ 🗺️  Matrix Cell: {snap.matrix_cell}
│ ⚡ Energy:      {energy_emoji} (Score: {snap.energy_score:.2f})
│ 📊 Efficiency:  {eff_emoji}
├──────────────────────────────────────────────────────────┤
│ 🏹 Initiative:  {init_emoji} {snap.initiative_polarity:<12} (Score: {snap.initiative_score:+.2f})
│ 📥 Inventory:   {inv_emoji} {snap.inventory_polarity:<12} (Score: {snap.inventory_score:+.2f})
├──────────────────────────────────────────────────────────┤
│ 🌐 Context:     {ctx_str}
├──────────────────────────────────────────────────────────┤
│ 🔔 Lý do cảnh báo:
{reasons}
├──────────────────────────────────────────────────────────┤
│ 📈 Actionables:  {snap.actionable_count} tín hiệu đang chờ
└──────────────────────────────────────────────────────────┘
"""
    return card
