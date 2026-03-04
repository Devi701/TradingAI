# TradingAI Hive Spec (Paper Trading)

## Mission
Build a paper-trading AI with a strict hierarchy:
- Grandmother: capital allocator, global risk officer, and orchestration loop.
- Mothers: strategy managers that evolve children.
- Children: rule-driven execution agents.

This design is optimized to avoid API rate-limit bans and machine overload.

## Fixed Topology
- `GRANDMOTHER_COUNT=1`
- `MOTHER_COUNT=5`
- `CHILDREN_PER_MOTHER=10`
- `TOTAL_CHILDREN=50`

## Mother Personalities and Universes
1. `Speedy` (Momentum Scalper)
- Symbols: `BTC/USD`, `ETH/USD`, `SOL/USD`
- Uses live crypto clock.

2. `Techie` (Trend Follower)
- Symbols: `NVDA`, `TSLA`, `AAPL`
- Uses delayed stock clock.

3. `Bargain` (Mean Reversion)
- Symbols: `WMT`, `KO`, `PG`
- Uses delayed stock clock.

4. `Chaos` (Volatility Breakout)
- Symbols: `META`, `AMZN`, `GME`
- Uses delayed stock clock.

5. `Hedge` (Defensive / Inverse)
- Symbols: `SPY`, `GLD`, `TLT`
- Uses delayed stock clock.

## Mother as Species (Genetic Environment)
- A Mother is a species-level environment, not a direct trader.
- Each Mother applies a personality-specific fitness function to all children.
- Fitness objective examples:
  - Aggressive profile: maximize total net profit, tolerate larger drawdowns.
  - Stable profile: maximize risk-adjusted returns (Sharpe-focused), penalize volatile equity curves.
- Child parameter bounds (position size, stop distance, leverage style) are constrained by Mother profile.

## Child as DNA Variation
- Children are individual strategy variants under the Mother species.
- Children can differ by signal family (RSI, MACD, volume-spike, or hybrids).
- On weekly evolution, Mothers run crossover + mutation:
  - select top-performing children by Mother fitness function
  - combine parent logic/parameters into offspring
  - mutate bounded parameters to preserve exploration
- This enables ongoing strategy discovery without manual recoding.

## API Rate-Limit Strategy
- Alpaca request limit target: `<= 200 requests/min`.
- Grandmother performs one bulk snapshot pull per cycle for all symbols.
- Children never call Alpaca directly.
- Children read price data from a local in-memory cache populated by Grandmother.

Expected behavior:
- Low outbound API pressure.
- Deterministic shared market view across all children.

## Data Proxy (Centralized Distribution)
- Grandmother is the only internet-facing market-data client.
- Snapshot payloads are normalized and written to a shared local cache.
- Preferred backend for first version: in-memory Python dictionary.
- Optional backend for scale/failover: Redis.
- Children only perform local cache reads (RAM-speed), never remote API reads.

## Dual Clock Model (2026 Free-Tier Assumption)
- Crypto data: real-time feed cadence (`5s` updates).
- Stock data: 15-minute delayed feed cadence (`60s` updates), marked as `T-15`.

Execution discipline:
- Stock children must evaluate and place paper orders only against delayed (`T-15`) stock snapshots.
- Crypto children evaluate against live snapshots.

## Honesty Engine (Virtual Trading Friction)
To avoid unrealistic paper-trading results, each simulated fill includes friction:
- Slippage:
  - Buy orders fill above observed price.
  - Sell orders fill below observed price.
  - Default model: percentage slippage of `0.05%` each side.
- Virtual commission:
  - Subtract `0.05%` from notional each side of every trade.
- Latency simulation:
  - Add randomized `50ms` to `200ms` delay before fill confirmation.
- Performance evaluation must use friction-adjusted P&L only.

Goal:
- Strategies that survive slippage/fees are more robust for live deployment.

## Grandmother Orchestration Heartbeat
Single process heartbeat loop:
1. Fetch snapshots (bulk symbols).
2. Split payload into crypto-live and stock-delayed lanes.
3. Distribute normalized snapshots to all 5 mothers.
4. Allocate capital from account buying power based on mother performance.
5. Enforce kill switch:
- If total account equity drawdown reaches `5%`, force full market exit.
6. Weekly evolution trigger:
- Every Sunday, instruct each mother to prune losers and breed next generation.
7. Heartbeat supervision:
- Ping each Mother every `60s`; if unhealthy, restart that Mother runtime and reload state.

## Macro Overlay
- Grandmother fetches macro/business RSS feeds on a fixed cadence.
- Headline sentiment is converted into a macro regime score (`risk_on`, `neutral`, `risk_off`).
- Regime drives allocation multipliers:
  - `risk_off`: reduce aggressive mother capital and increase hedge/bargain share.
  - `risk_on`: increase growth/momentum allocations.
- Macro regime snapshots are persisted in the state ledger.

## Grandmother as Netting Engine (Internal Exchange)
- Children submit intents (`buy/sell`, symbol, qty, confidence) to Grandmother.
- Grandmother nets all intents per symbol before external execution.
- Only the residual net order is sent to Alpaca.
- Example: `+1.0 BTC` and `-0.8 BTC` intents become one `+0.2 BTC` order.
- Goal: reduce churn, spread leakage, and unnecessary fee/slippage drag.

## Persistence Layer (Hive Memory)
- Every order, fill, mutation, allocation, and mother P&L snapshot is persisted immediately.
- Primary ledger backend: SQLite (`DATABASE_URL`).
- Required snapshot cadence:
  - Grandmother equity/balance snapshot every minute
  - Mother child-DNA snapshot every hour
- On startup, Grandmother runs resurrection:
  - load last equity and allocation state
  - restore open child positions and active genomes
  - re-sync runtime state before new decisions are made
- Restart behavior target: continue trial from prior state, never reset to day 1 unless explicitly requested.

## System Safety and Runtime Model
- Runtime target is hybrid orchestration:
  - asyncio event loop for child-level concurrency
  - optional multiprocessing isolation for Mother runtimes
- Do not run 50 standalone Python processes.
- Keep per-child logic lightweight and stateless between ticks where possible.

## Scheduled Events
- Hourly: Mother -> Grandmother P&L reporting.
- Daily at `23:59`: Mother-level selection/mutation cycle.
- Weekly on Sunday: Grandmother-enforced global prune/breed pass.
