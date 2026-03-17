from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

import requests

from cfte.onchain.models import HolderContext, OnchainPoolContext, WalletContext

JsonFetcher = Callable[[str, dict[str, str], dict[str, str]], dict[str, Any]]


def default_json_fetcher(url: str, headers: dict[str, str], params: dict[str, str]) -> dict[str, Any]:
    response = requests.get(url, headers=headers, params=params, timeout=8)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        return payload
    return {"data": payload}


class OnchainProviderAdapter(Protocol):
    provider_name: str

    def fetch_pool_context(self, symbol: str, chain: str) -> list[OnchainPoolContext]: ...

    def fetch_wallet_context(self, symbol: str, chain: str) -> list[WalletContext]: ...

    def fetch_holder_context(self, token_address: str, chain: str) -> list[HolderContext]: ...


@dataclass(slots=True)
class HeliusAdapter:
    api_key: str
    fetcher: JsonFetcher = default_json_fetcher
    provider_name: str = "helius"

    def fetch_pool_context(self, symbol: str, chain: str) -> list[OnchainPoolContext]:
        return []

    def fetch_wallet_context(self, symbol: str, chain: str) -> list[WalletContext]:
        payload = self.fetcher(
            "https://api.helius.xyz/v0/addresses",
            headers={"x-api-key": self.api_key},
            params={"symbol": symbol, "chain": chain},
        )
        return normalize_helius_wallets(payload, chain=chain)

    def fetch_holder_context(self, token_address: str, chain: str) -> list[HolderContext]:
        return []


@dataclass(slots=True)
class JupiterAdapter:
    fetcher: JsonFetcher = default_json_fetcher
    provider_name: str = "jupiter"

    def fetch_pool_context(self, symbol: str, chain: str) -> list[OnchainPoolContext]:
        payload = self.fetcher(
            "https://quote-api.jup.ag/v6/pools",
            headers={},
            params={"symbol": symbol, "chain": chain},
        )
        return normalize_jupiter_pools(payload, chain=chain)

    def fetch_wallet_context(self, symbol: str, chain: str) -> list[WalletContext]:
        return []

    def fetch_holder_context(self, token_address: str, chain: str) -> list[HolderContext]:
        return []


@dataclass(slots=True)
class GeckoTerminalAdapter:
    fetcher: JsonFetcher = default_json_fetcher
    provider_name: str = "geckoterminal"

    def fetch_pool_context(self, symbol: str, chain: str) -> list[OnchainPoolContext]:
        payload = self.fetcher(
            f"https://api.geckoterminal.com/api/v2/networks/{chain}/search/pools",
            headers={},
            params={"query": symbol},
        )
        return normalize_geckoterminal_pools(payload, chain=chain)

    def fetch_wallet_context(self, symbol: str, chain: str) -> list[WalletContext]:
        return []

    def fetch_holder_context(self, token_address: str, chain: str) -> list[HolderContext]:
        return []


@dataclass(slots=True)
class DexScreenerAdapter:
    fetcher: JsonFetcher = default_json_fetcher
    provider_name: str = "dexscreener"

    def fetch_pool_context(self, symbol: str, chain: str) -> list[OnchainPoolContext]:
        payload = self.fetcher(
            "https://api.dexscreener.com/latest/dex/search",
            headers={},
            params={"q": symbol, "chain": chain},
        )
        return normalize_dexscreener_pools(payload, chain=chain)

    def fetch_wallet_context(self, symbol: str, chain: str) -> list[WalletContext]:
        return []

    def fetch_holder_context(self, token_address: str, chain: str) -> list[HolderContext]:
        return []


@dataclass(slots=True)
class SimByDuneAdapter:
    api_key: str
    fetcher: JsonFetcher = default_json_fetcher
    provider_name: str = "sim_by_dune"

    def fetch_pool_context(self, symbol: str, chain: str) -> list[OnchainPoolContext]:
        return []

    def fetch_wallet_context(self, symbol: str, chain: str) -> list[WalletContext]:
        return []

    def fetch_holder_context(self, token_address: str, chain: str) -> list[HolderContext]:
        payload = self.fetcher(
            "https://api.sim.dune.com/v1/token/holders",
            headers={"x-dune-api-key": self.api_key},
            params={"token_address": token_address, "chain": chain},
        )
        return normalize_sim_holders(payload, chain=chain, token_address=token_address)


def normalize_helius_wallets(payload: dict[str, Any], chain: str) -> list[WalletContext]:
    wallets: list[WalletContext] = []
    for row in payload.get("data", []):
        wallets.append(
            WalletContext(
                source="helius",
                wallet=str(row.get("wallet", "")),
                chain=chain,
                label=str(row.get("label", "unknown")),
                netflow_24h_usd=float(row.get("netflow_24h_usd", 0.0)),
                buy_volume_24h_usd=float(row.get("buy_volume_24h_usd", 0.0)),
                sell_volume_24h_usd=float(row.get("sell_volume_24h_usd", 0.0)),
                tx_count_24h=int(row.get("tx_count_24h", 0)),
            )
        )
    return wallets


def normalize_jupiter_pools(payload: dict[str, Any], chain: str) -> list[OnchainPoolContext]:
    pools: list[OnchainPoolContext] = []
    for row in payload.get("data", []):
        pools.append(
            OnchainPoolContext(
                source="jupiter",
                pool_id=str(row.get("id", "")),
                chain=chain,
                base_symbol=str(row.get("base_symbol", "")),
                quote_symbol=str(row.get("quote_symbol", "USDC")),
                liquidity_usd=float(row.get("liquidity_usd", 0.0)),
                volume_24h_usd=float(row.get("volume_24h_usd", 0.0)),
                price_change_24h_pct=float(row.get("price_change_24h_pct", 0.0)),
                txns_24h=int(row.get("txns_24h", 0)),
                buys_24h=int(row.get("buys_24h", 0)),
                sells_24h=int(row.get("sells_24h", 0)),
            )
        )
    return pools


def normalize_geckoterminal_pools(payload: dict[str, Any], chain: str) -> list[OnchainPoolContext]:
    pools: list[OnchainPoolContext] = []
    for row in payload.get("data", []):
        attrs = row.get("attributes", {})
        pools.append(
            OnchainPoolContext(
                source="geckoterminal",
                pool_id=str(row.get("id", "")),
                chain=chain,
                base_symbol=str(attrs.get("base_token_symbol", "")),
                quote_symbol=str(attrs.get("quote_token_symbol", "USDC")),
                liquidity_usd=float(attrs.get("reserve_in_usd", 0.0)),
                volume_24h_usd=float(attrs.get("volume_usd", {}).get("h24", 0.0)),
                price_change_24h_pct=float(attrs.get("price_change_percentage", {}).get("h24", 0.0)),
                txns_24h=int(attrs.get("transactions", {}).get("h24", {}).get("buys", 0))
                + int(attrs.get("transactions", {}).get("h24", {}).get("sells", 0)),
                buys_24h=int(attrs.get("transactions", {}).get("h24", {}).get("buys", 0)),
                sells_24h=int(attrs.get("transactions", {}).get("h24", {}).get("sells", 0)),
            )
        )
    return pools


def normalize_dexscreener_pools(payload: dict[str, Any], chain: str) -> list[OnchainPoolContext]:
    pools: list[OnchainPoolContext] = []
    for row in payload.get("pairs", []):
        txns = row.get("txns", {}).get("h24", {})
        pools.append(
            OnchainPoolContext(
                source="dexscreener",
                pool_id=str(row.get("pairAddress", "")),
                chain=chain,
                base_symbol=str(row.get("baseToken", {}).get("symbol", "")),
                quote_symbol=str(row.get("quoteToken", {}).get("symbol", "USDC")),
                liquidity_usd=float(row.get("liquidity", {}).get("usd", 0.0)),
                volume_24h_usd=float(row.get("volume", {}).get("h24", 0.0)),
                price_change_24h_pct=float(row.get("priceChange", {}).get("h24", 0.0)),
                txns_24h=int(txns.get("buys", 0)) + int(txns.get("sells", 0)),
                buys_24h=int(txns.get("buys", 0)),
                sells_24h=int(txns.get("sells", 0)),
            )
        )
    return pools


def normalize_sim_holders(payload: dict[str, Any], chain: str, token_address: str) -> list[HolderContext]:
    summary = payload.get("summary", {})
    return [
        HolderContext(
            source="sim_by_dune",
            token_address=token_address,
            chain=chain,
            total_holders=int(summary.get("total_holders", 0)),
            top10_holder_pct=float(summary.get("top10_holder_pct", 0.0)),
            new_holders_24h=int(summary.get("new_holders_24h", 0)),
            whale_holders=int(summary.get("whale_holders", 0)),
        )
    ]
