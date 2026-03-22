from cfte.tpfm.models import TPFMSnapshot


def _join_items(items: list[str], fallback: str = "N/A") -> str:
    if not items:
        return fallback
    return " | ".join(items)


def render_tpfm_m5_card(snap: TPFMSnapshot) -> str:
    """Renders a concise trader-first TPFM M5 contract for vNext with Pattern Intelligence."""
    
    # Simple temporal indicators
    i_emoji = "📈" if snap.initiative_delta_1 > 0 else "📉" if snap.initiative_delta_1 < 0 else "➡️"
    v_emoji = "📈" if snap.inventory_delta_1 > 0 else "📉" if snap.inventory_delta_1 < 0 else "➡️"
    
    return (
        f"━━━ {snap.symbol} | {snap.matrix_alias_vi} | Grade {snap.tradability_grade} ━━━\n"
        f"Pattern : {snap.pattern_alias_vi} | Phase: {snap.pattern_phase}\n"
        f"Sequence: {snap.sequence_signature} | L: {snap.sequence_length}\n"
        f"Edge    : Score {snap.edge_score:.2f} ({snap.edge_confidence}) | WR {snap.historical_win_rate:.0%} | RR {snap.expected_rr:.1f}\n"
        f"Tempo   : {snap.tempo_state} | Persistence: {snap.persistence_state} | Exh: {snap.exhaustion_risk:.2f}\n"
        f"Deltas  : Init {snap.initiative_score:.2f} ({i_emoji}) | Inv {snap.inventory_score:.2f} ({v_emoji})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Observed: {_join_items(snap.observed_facts)}\n"
        f"Inferred: {_join_items(snap.inferred_facts)}\n"
        f"Decision: {snap.action_plan_vi}\n"
        f"Entry   : {snap.entry_condition_vi}\n"
        f"Invalid : {snap.invalid_if}\n"
        + (f"💡 Brief : {snap.flow_decision_brief}\n" if snap.flow_decision_brief else "") +
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
