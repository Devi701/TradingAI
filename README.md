# TradingAI Hive

Hierarchical paper-trading system with three tiers:
- `Grandmother`: risk, capital allocation, macro overlay, intent netting, Alpaca execution.
- `Mothers`: style-constrained fitness environments with evolutionary child selection.
- `Children`: asyncio strategy agents with DNA-based signals and position logic.

## What Is Implemented
- One-process asyncio hive runtime with 5 Mothers x 10 Children.
- Centralized snapshot data proxy (Grandmother-only Alpaca access).
- Internal intent netting before external Alpaca order routing.
- Honest paper friction model (slippage, commission, latency).
- Macro-news analyzer (RSS sentiment -> risk-on/risk-off regime).
- Dynamic Mother allowance rebalancing with macro/style overlays.
- Nightly + weekly evolution cycles with crossover/mutation.
- SQLite state ledger with resurrection-ready child DNA + positions.
- Heartbeat monitoring and Mother restart logic.
- L2 orderflow layer with whale-wall detection + tape large-print/sweep metrics.
- Optional Alpaca WebSocket orderflow stream (`trades`, `quotes`, crypto `orderbooks`) with auto reconnect.

## Quickstart

```bash
cd /Users/pardeepkumarmaheshwari/Desktop/TradingAI
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# fill ALPACA_API_KEY + ALPACA_SECRET_KEY
python scripts/healthcheck.py
python run_hive.py
```

## Run Detached On macOS (Recommended)

Use the launchd helper so the hive keeps running even if VS Code/terminal closes:

```bash
scripts/hive_service.sh start
scripts/hive_service.sh status
scripts/hive_service.sh logs
```

Turn display off immediately while keeping the service alive:

```bash
scripts/hive_service.sh screenoff
```

Stop it with:

```bash
scripts/hive_service.sh stop
```

Notes:
- This service wraps the bot in `caffeinate -ims` to prevent idle sleep while running.
- If the laptop is forced to sleep (for example lid closed without clamshell setup), macOS pauses processes.

## Runtime Artifacts
- State DB: `data/hive_state.db`
- Main entrypoint: `run_hive.py`
- Health check: `scripts/healthcheck.py`

## L2 + Tape (Whale Wall) Controls

Enable collection (default on) and optional WebSocket ingestion:

```bash
GRANDMOTHER_L2_TAPE_ENABLED=true
GRANDMOTHER_ORDERFLOW_WS_ENABLED=true
GRANDMOTHER_ORDERFLOW_WS_ASSETS=crypto,stock
```

Tune depth + whale-wall thresholding:

```bash
GRANDMOTHER_L2_DEPTH_LEVELS=20
GRANDMOTHER_L2_WHALE_WALL_MIN_NOTIONAL=500000
GRANDMOTHER_L2_WHALE_WALL_RATIO=2.0
```

Tape large-print/sweep thresholds:

```bash
GRANDMOTHER_TAPE_LARGE_PRINT_MIN_NOTIONAL=100000
GRANDMOTHER_TAPE_BUY_SWEEP_MIN_COUNT=2
GRANDMOTHER_TAPE_SELL_SWEEP_MIN_COUNT=2
```

To let L2/tape signals actively veto intents, enable:

```bash
GRANDMOTHER_L2_VETO_ENABLED=true
```

By default, L2/tape metrics are collected but vetoing is off, so baseline behavior stays stable.

## Quality Scanner + Sector Caps

Grandmother can now enforce a stock quality gate, run a multi-symbol scanner, and block correlated concentration:

```bash
GRANDMOTHER_QUALITY_FILTER_ENABLED=true
GRANDMOTHER_QUALITY_MIN_DAILY_DOLLAR_VOLUME=500000000
GRANDMOTHER_QUALITY_MAX_SPREAD_BPS=12
GRANDMOTHER_QUALITY_REQUIRE_INSTITUTIONAL_OWNERSHIP=false
GRANDMOTHER_QUALITY_MIN_INSTITUTIONAL_OWNERSHIP_PCT=50
GRANDMOTHER_INSTITUTIONAL_OWNERSHIP_PCT_BY_SYMBOL=
```

Scanner controls (top-N active + orderflow anomaly handoff):

```bash
GRANDMOTHER_SCANNER_ENABLED=true
GRANDMOTHER_SCANNER_TOP_N=100
GRANDMOTHER_SCANNER_MIN_ABS_DELTA_NOTIONAL=500000
GRANDMOTHER_SCANNER_FOCUS_TTL_SEC=180
GRANDMOTHER_SCANNER_UNIVERSE_SYMBOLS=AAPL,MSFT,GOOGL,AMZN,META,NVDA,TSLA,AMD,TSM,JPM,GS,XLE,SPY,GLD,TLT
```

Correlated risk cap:

```bash
GRANDMOTHER_SECTOR_DIVERSITY_ENABLED=true
GRANDMOTHER_MAX_POSITIONS_PER_SECTOR=2
GRANDMOTHER_SYMBOL_SECTOR_MAP=AAPL:tech,MSFT:tech,GOOGL:tech,NVDA:semis,AMD:semis,JPM:finance,GS:finance,XLE:energy
```

Buy intents are dropped when:
- symbol fails quality gate
- symbol is outside current scanner focus (when focus exists)
- sector already hit the max position count

## Safety Controls
- Global kill switch: `GRANDMOTHER_MAX_ACCOUNT_DRAWDOWN_PCT`
- Trading freeze on disconnect: `FREEZE_TRADING_ON_ALPACA_DISCONNECT`
- Internal netting toggle: `GRANDMOTHER_INTENT_NETTING_ENABLED`
- Macro risk reduction: `GRANDMOTHER_MACRO_RISK_REDUCTION_PCT`

## 24/7 Supervisor Behavior
- Runtime supervisor in `trading_ai/runtime.py` keeps the process running continuously.
- If a `ConnectionError` is detected by Grandmother loops, supervisor waits 30 seconds and restarts the hive.
- If Alpaca returns `401`/`Unauthorized`, all children are stopped and a macOS desktop alert is sent.
- When portfolio profit crosses each +5% milestone from baseline equity, a macOS desktop notification is sent.
