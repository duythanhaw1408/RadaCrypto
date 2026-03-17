# AGENTS.md

## Project identity
This repository implements a local-first, replay-first crypto flow thesis engine for trader-grade research and alerting.

Primary goals:
1. Multi-venue tape-reading terminal
2. Desk-grade execution monitor
3. On-chain intelligence context layer

## Non-negotiables
- local-first ingest
- replay-first architecture
- deterministic features and scoring
- lowcode configs via YAML
- AI is explainer-only, never the source of truth
- do not use D1 or KV for hot-path tick ingest
- do not collapse accumulation and distribution into one score

## Current required setups
- stealth_accumulation
- breakout_ignition
- distribution
- failed_breakout

## Stage lifecycle
- DETECTED
- WATCHLIST
- CONFIRMED
- ACTIONABLE
- INVALIDATED
- RESOLVED

## Implementation order
Phase 1:
- Binance public collector
- local order book
- parquet raw writer
- tape features
- thesis engine
- trader-card formatter
- replay pipeline

Phase 2:
- Bybit
- OKX free-safe feeds
- execution ledger
- fill quality metrics

Phase 3:
- Helius
- Jupiter
- GeckoTerminal
- DEX Screener
- Sim
- cloud sync and dashboard

## Coding rules
- prefer small typed modules
- prefer pure functions for features/scoring
- do not add speculative abstractions
- add tests for all meaningful logic
