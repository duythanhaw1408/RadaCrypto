from cfte.models.events import ThesisSignal
from cfte.onchain.adapters import (
    normalize_dexscreener_pools,
    normalize_geckoterminal_pools,
    normalize_helius_wallets,
    normalize_jupiter_pools,
    normalize_sim_holders,
)
from cfte.onchain.enrichment import collect_optional_context, enrich_thesis_signal


class _HealthyAdapter:
    provider_name = "healthy"

    def fetch_pool_context(self, symbol: str, chain: str):
        assert symbol == "SOL"
        return [
            normalize_jupiter_pools(
                {
                    "data": [
                        {
                            "id": "pool-1",
                            "base_symbol": "SOL",
                            "quote_symbol": "USDC",
                            "liquidity_usd": 1500000,
                            "volume_24h_usd": 340000,
                            "price_change_24h_pct": 3.1,
                            "txns_24h": 1200,
                            "buys_24h": 700,
                            "sells_24h": 500,
                        }
                    ]
                },
                chain=chain,
            )[0]
        ]

    def fetch_wallet_context(self, symbol: str, chain: str):
        return normalize_helius_wallets(
            {
                "data": [
                    {
                        "wallet": "abc",
                        "label": "smart_money",
                        "netflow_24h_usd": 120000,
                        "buy_volume_24h_usd": 200000,
                        "sell_volume_24h_usd": 80000,
                        "tx_count_24h": 44,
                    }
                ]
            },
            chain=chain,
        )

    def fetch_holder_context(self, token_address: str, chain: str):
        return normalize_sim_holders(
            {
                "summary": {
                    "total_holders": 12000,
                    "top10_holder_pct": 66.5,
                    "new_holders_24h": 420,
                    "whale_holders": 34,
                }
            },
            chain=chain,
            token_address=token_address,
        )


class _PartiallyFailingAdapter:
    provider_name = "partial"

    def fetch_pool_context(self, symbol: str, chain: str):
        return normalize_dexscreener_pools(
            {
                "pairs": [
                    {
                        "pairAddress": "pair-partial",
                        "baseToken": {"symbol": symbol},
                        "quoteToken": {"symbol": "USDC"},
                        "liquidity": {"usd": 950000},
                        "volume": {"h24": 250000},
                        "priceChange": {"h24": 0.9},
                        "txns": {"h24": {"buys": 300, "sells": 240}},
                    }
                ]
            },
            chain=chain,
        )

    def fetch_wallet_context(self, symbol: str, chain: str):
        raise RuntimeError("wallet endpoint timeout")

    def fetch_holder_context(self, token_address: str, chain: str):
        return []


class _FailingAdapter:
    provider_name = "failing"

    def fetch_pool_context(self, symbol: str, chain: str):
        raise RuntimeError("provider unavailable")

    def fetch_wallet_context(self, symbol: str, chain: str):
        raise RuntimeError("provider unavailable")

    def fetch_holder_context(self, token_address: str, chain: str):
        raise RuntimeError("provider unavailable")


def _sample_signal() -> ThesisSignal:
    return ThesisSignal(
        thesis_id="abc123",
        instrument_key="BINANCE:SOLUSDT:SPOT",
        setup="breakout_ignition",
        direction="LONG_BIAS",
        stage="WATCHLIST",
        score=74.0,
        confidence=0.7,
        coverage=0.8,
        why_now=["Xung lực tape đang tăng"],
        conflicts=[],
        invalidation="Thủng microprice",
        entry_style="Theo breakout có retest",
        targets=["TP1", "TP2"],
    )


def test_normalizers_map_provider_payloads_into_context_models():
    gecko = normalize_geckoterminal_pools(
        {
            "data": [
                {
                    "id": "solana_pool",
                    "attributes": {
                        "base_token_symbol": "SOL",
                        "quote_token_symbol": "USDC",
                        "reserve_in_usd": 2100000,
                        "volume_usd": {"h24": 510000},
                        "price_change_percentage": {"h24": 2.4},
                        "transactions": {"h24": {"buys": 600, "sells": 440}},
                    },
                }
            ]
        },
        chain="solana",
    )
    dex = normalize_dexscreener_pools(
        {
            "pairs": [
                {
                    "pairAddress": "pair1",
                    "baseToken": {"symbol": "SOL"},
                    "quoteToken": {"symbol": "USDC"},
                    "liquidity": {"usd": 1800000},
                    "volume": {"h24": 700000},
                    "priceChange": {"h24": 1.8},
                    "txns": {"h24": {"buys": 900, "sells": 650}},
                }
            ]
        },
        chain="solana",
    )

    assert gecko[0].source == "geckoterminal"
    assert gecko[0].txns_24h == 1040
    assert dex[0].source == "dexscreener"
    assert dex[0].buys_24h == 900


def test_enrichment_is_optional_and_keeps_core_signal_working_when_providers_fail():
    context = collect_optional_context(
        symbol="SOL",
        chain="solana",
        token_address="token-1",
        adapters=[_HealthyAdapter(), _FailingAdapter()],
    )

    enriched = enrich_thesis_signal(_sample_signal(), context)

    assert context.provider_status["healthy"] is True
    assert context.provider_status["failing"] is False
    assert enriched.score == 74.0
    assert enriched.confidence == 0.7
    assert enriched.coverage > 0.8
    assert any("Thanh khoản on-chain" in line for line in enriched.why_now)
    assert any("tạm thiếu dữ liệu" in line for line in enriched.conflicts)


def test_fallback_without_adapters_returns_original_signal_with_no_crash():
    context = collect_optional_context(
        symbol="SOL",
        chain="solana",
        token_address="token-1",
        adapters=[],
    )
    original = _sample_signal()
    enriched = enrich_thesis_signal(original, context)

    assert context.provider_status == {}
    assert enriched.why_now == original.why_now
    assert enriched.conflicts == original.conflicts
    assert enriched.coverage == original.coverage


def test_partial_provider_failure_keeps_successful_context_slices():
    context = collect_optional_context(
        symbol="SOL",
        chain="solana",
        token_address="token-1",
        adapters=[_PartiallyFailingAdapter()],
    )

    assert context.provider_status["partial"] is False
    assert len(context.pools) == 1
    assert context.pools[0].source == "dexscreener"
    assert context.wallets == []
    assert context.holders == []
