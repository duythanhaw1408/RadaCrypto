# AGENTS.md

## Project identity
This repository implements a local-first, replay-first crypto flow thesis engine for trader-grade research and alerting.

This is NOT a generic indicator bot.
This is NOT a sentiment bot.
This is NOT a cloud-first dashboard project.

The system is intended to evolve toward:
1. multi-venue tape-reading
2. desk-grade execution monitoring
3. on-chain intelligence context

Primary product goal:
maximize trader edge under free-tier constraints through architecture, selective coverage, and replay.

## Core principles
Always preserve these:
- local-first ingest
- replay-first design
- deterministic feature computation
- deterministic thesis scoring
- append-only raw event logging
- graceful degradation when optional providers are unavailable
- lowcode configuration through YAML
- AI is explainer-only and never the source of truth

## Localization policy
This product is built for Vietnamese users.

Language rules:
- Keep internal code identifiers in English:
  - file names
  - module names
  - class names
  - function names
  - variable names
  - database/table/column names
  - internal enums and event types
- Default all user-facing output to Vietnamese:
  - Telegram alerts
  - trader-card rendering
  - dashboard labels
  - CLI user-facing summaries
  - end-user instructions
  - operational help text
- Prefer natural Vietnamese suitable for Vietnamese crypto traders.
- Avoid rigid machine-translated phrasing.
- Vietnamese must remain the default locale.

## Storage architecture
Maintain 3 storage layers:
1. raw immutable event lake in Parquet
2. mutable local state in SQLite
3. cloud summary data in D1/R2/KV

Do not use D1 or KV for hot-path tick ingest.
Do not store raw market streams in cloud-first storage.

## Bounded modules
Keep responsibilities separated across these areas:
- collectors
- normalizers
- books
- storage
- features
- thesis
- execution
- alerts
- replay
- cloud summaries

Do not collapse these layers into one large module.

## Scope control
Do not optimize for full-market coverage.
Prefer selective symbol coverage and deeper insight.

Universe design:
- Tier A: highest-conviction symbols with deepest coverage
- Tier B: Binance-first scouting universe
- Tier C: on-chain/context watchlist universe

## Required setup engines
The thesis layer must support setup-specific engines.
Do not emit one generic score.

Current required setups:
- stealth_accumulation
- breakout_ignition
- distribution
- failed_breakout

Each setup result must include:
- setup
- direction
- score
- confidence
- coverage
- why_now
- conflicts
- invalidation
- entry_style
- targets
- stage

## Thesis lifecycle
Every thesis must move through:
- DETECTED
- WATCHLIST
- CONFIRMED
- ACTIONABLE
- INVALIDATED
- RESOLVED

Deduplication must not be symbol-only.
Use a thesis identity derived from:
symbol + venue + setup + direction + timeframe + regime bucket

## Coding rules
Prefer:
- small typed modules
- pure functions for feature computation and scoring
- explicit data models
- minimal hidden state
- deterministic interfaces
- named constants instead of magic numbers

Avoid:
- giant classes
- direct provider coupling inside feature logic
- cloud-dependent core logic
- speculative abstractions
- AI-generated business logic
- storing critical state only in memory

## Testing rules
Every meaningful module must include tests or smoke coverage.

Prioritize tests for:
- normalization
- local book reconstruction
- tape feature computation
- thesis scoring
- execution reconciliation
- replay determinism

If something cannot be fully tested yet, add the smallest possible smoke test and leave a clear TODO.

## Work discipline
For every task:
1. inspect existing files first
2. propose the smallest complete implementation slice
3. implement
4. run relevant tests
5. summarize changed files, tradeoffs, and next steps

Do not rewrite unrelated files.
Do not add broad frameworks without immediate need.

## Delivery order
Unless explicitly overridden, implement in this order:

Phase 1:
- Binance public collector
- local order book
- parquet raw writer
- tape features
- initial thesis engine
- trader-card formatter
- replay pipeline

Phase 2:
- Bybit integration
- OKX free-safe integration
- execution ledger
- fill quality metrics
- cross-venue leader/lagger logic

Phase 3:
- Helius/Jupiter integration
- GeckoTerminal/DEX Screener integration
- Sim integration
- cloud summary sync
- dashboard ranking and expectancy views

## Non-negotiables
Do not:
- optimize for full-market coverage
- build fancy UI before core signal integrity
- replace replayable pipelines with ad hoc scripts
- treat generic sentiment as a core signal
- collapse accumulation and distribution into one score
- make AI responsible for truth
