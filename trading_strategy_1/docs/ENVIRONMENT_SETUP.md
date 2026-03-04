# Environment Setup (Pre-Code)

## 1) Create local env file
Run from project root:

```bash
cp .env.example .env
```

## 2) Fill required secrets
Set these in `.env`:
- `ALPACA_API_KEY`
- `ALPACA_SECRET_KEY`

Keep:
- `ALPACA_BASE_URL=https://paper-api.alpaca.markets`

## 3) Keep paper-trading safety defaults
- `GRANDMOTHER_KILL_SWITCH_ENABLED=true`
- `GRANDMOTHER_MAX_ACCOUNT_DRAWDOWN_PCT=0.05`

## 4) Pick initial universe
Defaults now use 5 fixed mother universes:
- `MOTHER_SPEEDY_SYMBOLS=BTC/USD,ETH/USD,SOL/USD`
- `MOTHER_TECHIE_SYMBOLS=NVDA,TSLA,AAPL`
- `MOTHER_BARGAIN_SYMBOLS=WMT,KO,PG`
- `MOTHER_CHAOS_SYMBOLS=META,AMZN,GME`
- `MOTHER_HEDGE_SYMBOLS=SPY,GLD,TLT`

## 5) Validate config before coding
Checklist:
- `.env` exists and is not committed.
- API keys are present.
- Drawdown kill switch is enabled.
- Mutation and child-count parameters match intended experiments.
- Data proxy mode is enabled (children do not call Alpaca directly).
- Slippage and commission values are set for honest paper simulation.
- Persistence and resurrection are enabled.
- Heartbeat/restart controls are enabled for Mothers.
- Trading freeze on Alpaca stream disconnect is enabled.

## 6) Final pre-code decisions
- Runtime: Python 3.11+
- DB local: SQLite file in `./data`
- Scheduler cadence:
  - hourly reporting
  - nightly evolution at `23:59`
  - weekly Sunday prune/breed
- Market-data cadence:
  - crypto refresh every `5s` (live)
  - stock refresh every `60s` (`T-15` delayed)
- Honesty defaults:
  - slippage model: percentage `0.05%` each side
  - latency model: random `50-200ms`
  - virtual commission: `0.05%` per trade side
- Persistence defaults:
  - backend: SQLite
  - write mode: immediate
  - resurrection on startup: enabled

## 7) Macro overlay and warmup
- `GRANDMOTHER_MACRO_NEWS_ENABLED=true`
- `GRANDMOTHER_MACRO_NEWS_REFRESH_MIN=15`
- `GRANDMOTHER_MACRO_NEWS_SOURCES=...` (RSS list)
- `GRANDMOTHER_MACRO_RISK_ON_BOOST_PCT=0.15`
- `GRANDMOTHER_WARMUP_BARS=120`

## 8) Startup validation commands
```bash
python scripts/healthcheck.py
python run_hive.py
```

## 9) Run 24/7 in background on macOS
Start as a launchd service:

```bash
scripts/hive_service.sh start
scripts/hive_service.sh status
```

View logs:

```bash
scripts/hive_service.sh logs
```

Turn display off while hive keeps running:

```bash
scripts/hive_service.sh screenoff
```

Stop service:

```bash
scripts/hive_service.sh stop
```

Important:
- The service uses `caffeinate -ims` so idle sleep is blocked while the hive is active.
- If the machine is explicitly put to sleep, running processes pause until wake.
