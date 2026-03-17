from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class OnchainPoolContext:
    source: str
    pool_id: str
    chain: str
    base_symbol: str
    quote_symbol: str
    liquidity_usd: float
    volume_24h_usd: float
    price_change_24h_pct: float
    txns_24h: int
    buys_24h: int
    sells_24h: int


@dataclass(slots=True)
class WalletContext:
    source: str
    wallet: str
    chain: str
    label: str
    netflow_24h_usd: float
    buy_volume_24h_usd: float
    sell_volume_24h_usd: float
    tx_count_24h: int


@dataclass(slots=True)
class HolderContext:
    source: str
    token_address: str
    chain: str
    total_holders: int
    top10_holder_pct: float
    new_holders_24h: int
    whale_holders: int


@dataclass(slots=True)
class ContextBundle:
    pools: list[OnchainPoolContext] = field(default_factory=list)
    wallets: list[WalletContext] = field(default_factory=list)
    holders: list[HolderContext] = field(default_factory=list)
    provider_status: dict[str, bool] = field(default_factory=dict)
