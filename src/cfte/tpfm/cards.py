from cfte.tpfm.models import TPFMSnapshot


def _join_items(items: list[str], fallback: str = "N/A") -> str:
    if not items:
        return fallback
    return " | ".join(items)


def render_tpfm_m5_card(snap: TPFMSnapshot) -> str:
    """Renders a concise trader-first TPFM M5 contract for vNext."""
    return (
        f"━━━ {snap.symbol} | {snap.matrix_alias_vi} | Grade {snap.tradability_grade} ━━━\n"
        f"Matrix: {snap.matrix_cell} ({snap.matrix_alias_vi}) | Flow: {snap.flow_state_code}\n"
        f"Observed: {_join_items(snap.observed_facts)}\n"
        f"Inferred: {_join_items(snap.inferred_facts)}\n"
        f"Missing: {_join_items(snap.missing_context)}\n"
        f"Decision: {snap.action_plan_vi}\n"
        f"Entry: {snap.entry_condition_vi}\n"
        f"Confirm: {snap.confirm_needed_vi}\n"
        f"Avoid: {snap.avoid_if_vi}\n"
        f"Invalidation: {snap.invalid_if}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
