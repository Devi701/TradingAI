# Trading AI Plan (Paper Trading First)

## Scope (Phase 0)
- Paper trading only (Alpaca paper endpoint).
- No live-money execution.
- Three-layer architecture:
  - Grandmother: risk and capital allocation.
  - Mother: strategy evolution.
  - Children: signal + execution workers.
- Fixed topology:
  - 1 Grandmother
  - 5 Mothers (`Speedy`, `Techie`, `Bargain`, `Chaos`, `Hedge`)
  - 10 Children per Mother (50 total)

## Phase 1: Environment and Foundations
- Define config contract in `.env.example`.
- Set up secret handling (`.env`, never committed).
- Choose runtime stack (Python recommended for rapid iteration).
- Add base logging, DB connectivity, and health checks.
- Add dry-run startup that validates all required env vars.

## Phase 2: Core Runtime Skeleton
- Process model:
  - one `grandmother` orchestration loop.
  - five logical `mother` managers (asyncio-first, optional process isolation).
  - child workers per strategy genome (asyncio tasks, not separate processes).
- Centralized data proxy:
  - only Grandmother can call Alpaca market-data endpoints
  - mothers/children consume local cache snapshots via broadcast/subscriber model
- Shared database schema for:
  - allocations
  - orders
  - fills
  - pnl snapshots
  - genomes and generation history
  - risk events and kill-switch events
- Add idempotent task scheduling for hourly and nightly jobs.
- Add weekly Sunday prune/breed schedule.

## Phase 3: Grandmother Logic
- Pull live account + positions from Alpaca.
- Pull bulk snapshots for all symbols into local cache.
- Enforce account-level drawdown stop (`5%` default).
- Trigger total market exit on kill switch.
- Compute allowance per mother based on recent performance.
- Apply macro risk multiplier (manual input first, auto-news later).
- Operate dual clocks:
  - crypto lane (live, 5-second refresh)
  - stock lane (15-minute delayed, 60-second refresh)
- On startup, execute resurrection flow from persisted ledger state.
- Net child intents per symbol before routing to Alpaca (internal exchange model).
- Freeze new trading actions when Alpaca stream disconnects; resume only after reconnect and sync.
- Heartbeat each Mother every `60s`; restart only failed Mother runtime.

## Phase 4: Mother Logic
- Maintain `N` children per mother (`10` default).
- Nightly selection at `23:59`:
  - remove bottom `K` performers (`3` default)
  - keep top performers
  - spawn mutated children from winners
- Weekly Sunday crossover:
  - combine top parent genomes (signal logic + parameters)
  - mutate offspring within Mother-specific risk bounds
- Mother fitness is profile-specific:
  - aggressive profiles favor net profit
  - stable profiles favor Sharpe/risk-adjusted returns
- Hourly report of subtotal P&L to Grandmother.

## Phase 5: Child Logic
- Bind each child to ticker set and rule DNA.
- Consume only Grandmother-distributed cache snapshots (no direct Alpaca calls).
- Evaluate trigger conditions.
- Submit buy/sell requests to Mother risk gate.
- Persist full trade lifecycle records with friction-adjusted fills:
  - slippage percentage model (`0.05%` each side)
  - randomized latency model (`50ms` to `200ms`)
  - virtual commission model

## Phase 6: Safety, Testing, and Ops
- End-to-end simulation backfill in paper mode.
- Failure handling:
  - API disconnect retry
  - stale prices
  - duplicate signal suppression
- Unit/integration tests for:
  - drawdown kill switch
  - mutation bounds
  - position sizing limits
  - slippage + commission accounting
  - restart/resurrection consistency
  - intent-netting correctness (collision scenarios)
  - Mother fitness ranking correctness (profit vs Sharpe)
  - heartbeat restart behavior for hung Mother
  - trading freeze/unfreeze behavior during API disconnects
- Observability:
  - logs
  - metrics (P&L, risk usage, fill latency)
  - alert conditions

## Initial Decisions To Finalize Before Coding
1. Language/runtime (`Python 3.11+` recommended).
2. DB backend (`SQLite` for local, `Postgres` for cloud).
3. Scheduling (`APScheduler` or equivalent).
4. Macro input source:
   - manual flag at first
   - then automated news classifier.
5. Universe by Mother:
   - Speedy: `BTC/USD, ETH/USD, SOL/USD`
   - Techie: `NVDA, TSLA, AAPL`
   - Bargain: `WMT, KO, PG`
   - Chaos: `META, AMZN, GME`
   - Hedge: `SPY, GLD, TLT`
