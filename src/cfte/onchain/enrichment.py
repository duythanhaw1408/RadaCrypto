from __future__ import annotations

from dataclasses import replace

from cfte.models.events import ThesisSignal
from cfte.onchain.adapters import OnchainProviderAdapter
from cfte.onchain.models import ContextBundle


def collect_optional_context(
    symbol: str,
    chain: str,
    token_address: str,
    adapters: list[OnchainProviderAdapter],
) -> ContextBundle:
    bundle = ContextBundle()
    for adapter in adapters:
        ok = True
        try:
            bundle.pools.extend(adapter.fetch_pool_context(symbol=symbol, chain=chain))
            bundle.wallets.extend(adapter.fetch_wallet_context(symbol=symbol, chain=chain))
            bundle.holders.extend(adapter.fetch_holder_context(token_address=token_address, chain=chain))
        except Exception:
            ok = False
        bundle.provider_status[adapter.provider_name] = ok
    return bundle


def enrich_thesis_signal(signal: ThesisSignal, context: ContextBundle) -> ThesisSignal:
    why_now = list(signal.why_now)
    conflicts = list(signal.conflicts)

    top_pool = max(context.pools, key=lambda item: item.liquidity_usd, default=None)
    if top_pool is not None:
        why_now.append(
            f"Thanh khoản on-chain nổi bật ({top_pool.source}): {top_pool.liquidity_usd:,.0f} USD"
        )

    if context.wallets:
        total_netflow = sum(item.netflow_24h_usd for item in context.wallets)
        why_now.append(f"Dòng ví theo dõi 24h: {total_netflow:,.0f} USD")

    holder = context.holders[0] if context.holders else None
    if holder is not None and holder.top10_holder_pct >= 60:
        conflicts.append(f"Tập trung holder cao: top 10 nắm {holder.top10_holder_pct:.1f}%")

    unavailable = sorted(name for name, status in context.provider_status.items() if not status)
    if unavailable:
        conflicts.append(f"Một số nguồn on-chain tạm thiếu dữ liệu: {', '.join(unavailable)}")

    coverage_bonus = min(0.15, 0.03 * len(context.pools) + 0.04 * len(context.wallets) + 0.05 * len(context.holders))
    coverage = min(1.0, round(signal.coverage + coverage_bonus, 2))

    return replace(signal, why_now=why_now, conflicts=conflicts, coverage=coverage)
