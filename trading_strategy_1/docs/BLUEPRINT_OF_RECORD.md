# Blueprint of Record (Technical Spec)

This is the canonical implementation blueprint for the TradingAI hive.

## Phase 1: Shared Memory Infrastructure
- Inter-agent communication must use high-speed in-memory transport, not text-file IPC.
- Grandmother is the only market-data ingress point.
- Grandmother fetches bulk Alpaca snapshots and writes normalized ticks to a data bus:
  - default: in-process shared dictionary
  - optional: Redis for multi-process scaling
- Mothers and Children follow a subscriber model:
  - no direct polling from Alpaca
  - consume Grandmother broadcast updates from cache

## Phase 2: Logic Stack (3-Tier DNA)

### Tier 1: Grandmother (State Machine)
- Responsibility: global risk, execution arbitration, persistence, and orchestration.
- State ledger: `hive_state.db` (SQLite) persists allocations, open positions, genomes, and equity snapshots.
- Circuit breaker:
  - `MAX_DRAWDOWN = 0.05`
  - if equity drops from `100,000` to `95,000` threshold, halt trading and execute `close_all_positions()`.
- Recovery authority:
  - on restart, load persisted state and re-sync with Alpaca before resuming child execution.

### Tier 2: Mothers (Fitness Environments)
- Each Mother is a class derived from a shared base Mother class.
- Mother personality is enforced by a constraint mask.
- Example:
  - Bargain Mother rejects child buy requests when RSI is above 40.
- Evolution cadence:
  - every Sunday, run genetic tournament
  - rank 10 children by Mother fitness objective
  - delete bottom 3
  - replace with mutations/crossover from top 2

### Tier 3: Children (Execution Drones)
- Each Child is a lightweight asyncio task with DNA parameters.
- Child DNA is serialized JSON (example):
```json
{
  "ticker": "BTC/USD",
  "entry_signal": "EMA_CROSS",
  "params": [12, 26],
  "stop_loss": 0.02,
  "take_profit": 0.06
}
```
- Order routing path:
  - Child -> Mother (`OrderRequest` policy check) -> Grandmother (netting + execution)
- Children never call Alpaca directly.

## Phase 3: Honesty Filter (Realistic Paper Trading)
- Slippage simulation:
  - add `0.05%` to effective buy price
  - subtract `0.05%` from effective sell price
- Latency simulation:
  - random delay between `50ms` and `200ms` before trade confirmation
- Internal netting:
  - offset opposing child intents before external execution
  - only net residual is sent to Alpaca

## Phase 4: Error Handling and Recovery
- Heartbeat supervision:
  - Grandmother pings each Mother every `60s`
  - on heartbeat timeout, restart only that Mother runtime and reload state from SQLite
- API/WebSocket disconnect handling:
  - freeze all new trading decisions while disconnected
  - resume only after successful reconnect + state re-sync

## Final Summary
| Component | Architecture | Critical Duty |
|---|---|---|
| Orchestration | asyncio + multiprocessing (hybrid) | Run 56 agents efficiently on a single Mac |
| Communication | Shared memory cache | Prevent rate-limit blowups via centralized market data |
| Intelligence | Genetic algorithms | Evolve child DNA through tournament, crossover, mutation |
| Safety | Global circuit breaker | Protect 100k account principal |

