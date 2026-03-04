from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


FitnessObjective = Literal["net_profit", "sharpe_ratio", "sortino_ratio"]


@dataclass(frozen=True)
class MotherSpec:
    id: str
    name: str
    style: str
    fitness_objective: FitnessObjective
    symbols: list[str]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "development"
    app_timezone: str = "UTC"
    log_level: str = "INFO"
    orchestration_mode: str = "asyncio_multiprocessing_hybrid"

    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_base_url: str = "https://paper-api.alpaca.markets"
    alpaca_data_url: str = "https://data.alpaca.markets"
    # Explicit streaming host (WebSocket) to use for market data streams. If you leave this
    # empty the runtime will try to derive a websocket URL from `alpaca_data_url`, but the
    # official streaming host is `wss://stream.data.alpaca.markets` and using that avoids
    # HTTP-level rejections when connecting to the data stream.
    alpaca_data_stream_url: str = "wss://stream.data.alpaca.markets"

    database_url: str = "sqlite:///./data/trading_ai.db"
    hive_state_db_path: str = "./data/hive_state.db"

    grandmother_count: int = 1
    grandmother_max_account_drawdown_pct: float = 0.05
    grandmother_macro_risk_reduction_pct: float = 0.50
    grandmother_rebalance_interval_min: int = 60
    grandmother_kill_switch_enabled: bool = True
    grandmother_prune_day: str = "SUNDAY"
    grandmother_prune_time: str = "23:59"
    grandmother_intent_netting_enabled: bool = True
    grandmother_intent_netting_interval_sec: int = 5
    grandmother_heartbeat_interval_sec: int = 60
    grandmother_restart_mother_on_heartbeat_fail: bool = True
    grandmother_circuit_breaker_action: str = "close_all_positions_and_halt"
    grandmother_macro_news_enabled: bool = True
    grandmother_macro_news_refresh_min: int = 15
    grandmother_macro_news_sources: str = (
        "https://feeds.reuters.com/reuters/businessNews,"
        "https://feeds.a.dj.com/rss/RSSMarketsMain.xml,"
        "https://www.marketwatch.com/rss/topstories"
    )
    grandmother_macro_risk_on_boost_pct: float = 0.15
    grandmother_warmup_bars: int = 120
    grandmother_overseer_enabled: bool = True
    grandmother_overseer_review_time: str = "23:55"
    grandmother_overseer_trade_churn_threshold: int = 100
    grandmother_overseer_cooldown_minutes: int = 5
    grandmother_overseer_symbol_lock_hours: int = 4
    grandmother_overseer_daily_budget_pct: float = 0.01
    grandmother_overseer_spread_reward_ratio: float = 3.0
    grandmother_overseer_vix_threshold: float = 25.0
    grandmother_overseer_high_vol_min_confidence: float = 0.85
    grandmother_overseer_low_vol_min_confidence: float = 0.60
    grandmother_overseer_high_vol_risk_scale: float = 0.20
    grandmother_overseer_low_vol_risk_scale: float = 1.00
    grandmother_overseer_min_trade_cooldown_sec: int = 180
    grandmother_overseer_max_trade_cooldown_sec: int = 900
    grandmother_overseer_global_trade_cooldown_sec: int = 20
    grandmother_overseer_penalty_lookback_hours: int = 6
    grandmother_overseer_penalty_buffer_mult: float = 1.25
    grandmother_overseer_max_orders_per_hour: int = 40
    grandmother_orderflow_enabled: bool = True
    grandmother_orderflow_imbalance_veto_threshold: float = -0.30
    grandmother_orderflow_delta_lookback_sec: int = 60
    grandmother_orderflow_min_delta_alignment: bool = True
    grandmother_orderflow_buy_pressure_ratio: float = 3.0
    grandmother_orderflow_spread_spike_multiplier: float = 2.0
    grandmother_orderflow_spread_avg_window_sec: int = 300
    grandmother_orderflow_spread_halt_sec: int = 120
    grandmother_orderflow_price_window_sec: int = 120
    grandmother_orderflow_price_spike_pct: float = 0.003
    grandmother_orderflow_open_interest_required_for_exhaustion: bool = False
    grandmother_l2_tape_enabled: bool = True
    grandmother_l2_veto_enabled: bool = False
    grandmother_l2_depth_levels: int = 20
    grandmother_l2_whale_wall_min_notional: float = 500000.0
    grandmother_l2_whale_wall_ratio: float = 2.0
    grandmother_tape_large_print_min_notional: float = 100000.0
    grandmother_tape_buy_sweep_min_count: int = 2
    grandmother_tape_sell_sweep_min_count: int = 2
    grandmother_orderflow_ws_enabled: bool = False
    grandmother_orderflow_ws_assets: str = "crypto"
    grandmother_orderflow_ws_reconnect_sec: int = 5
    grandmother_orderflow_ws_max_staleness_sec: int = 20
    # Starting capital for the hive. If set (>0) and no baseline equity exists in the
    # ledger, Grandmother will initialize baseline equity to this value. Useful for
    # backtesting or starting the system with a notional capital amount.
    grandmother_starting_capital: float = 0.0
    # Comma-separated base weight map for mothers. Example: "hedge:0.6,bargain:0.3,speedy:0.1"
    # Grandmother will multiply these base weights with dynamic quality weights to
    # compute final allocation. If omitted, equal treatment is used.
    grandmother_base_weights: str = ""
    grandmother_quality_filter_enabled: bool = True
    grandmother_quality_min_daily_dollar_volume: float = 500000000.0
    grandmother_quality_max_spread_bps: float = 12.0
    grandmother_quality_require_institutional_ownership: bool = False
    grandmother_quality_min_institutional_ownership_pct: float = 50.0
    grandmother_institutional_ownership_pct_by_symbol: str = ""
    grandmother_scanner_enabled: bool = True
    grandmother_scanner_refresh_sec: int = 60
    grandmother_scanner_top_n: int = 100
    grandmother_scanner_min_abs_delta_notional: float = 500000.0
    grandmother_scanner_focus_ttl_sec: int = 180
    grandmother_scanner_universe_symbols: str = (
        "AAPL,MSFT,GOOGL,AMZN,META,NVDA,TSLA,AMD,TSM,AVGO,ORCL,CRM,ADBE,INTC,QCOM,NFLX,CSCO,IBM,TXN,AMAT,KLAC,MU,PANW,UBER,SNOW,PLTR,SHOP,"
        "JPM,GS,BAC,WFC,MS,C,BLK,SPGI,USB,SCHW,AXP,CB,BX,"
        "XLE,CVX,XOM,COP,SLB,EOG,MPC,VLO,OXY,KMI,"
        "SPY,QQQ,IWM,DIA,XLK,XLF,XLI,XLV,XLP,XLY,XLC,GLD,TLT,XLU,"
        "UNH,LLY,JNJ,PFE,MRK,ABBV,TMO,DHR,ISRG,"
        "WMT,COST,HD,LOW,NKE,SBUX,MCD,DIS,KO,PEP,PG,"
        "TMUS,T,VZ,CMCSA,CHTR,"
        "CAT,DE,GE,BA,HON,UNP,UPS,FDX,LMT,RTX,GME"
    )
    grandmother_sector_diversity_enabled: bool = True
    grandmother_max_positions_per_sector: int = 2
    grandmother_symbol_sector_map: str = (
        "AAPL:tech,MSFT:tech,GOOGL:tech,AMZN:tech,META:tech,NVDA:semis,TSM:semis,AMD:semis,AVGO:semis,"
        "TSLA:tech,ORCL:tech,CRM:tech,"
        "JPM:finance,GS:finance,BAC:finance,WFC:finance,MS:finance,C:finance,BLK:finance,SPGI:finance,"
        "XLE:energy,CVX:energy,XOM:energy,COP:energy,SLB:energy,EOG:energy,"
        "SPY:index,QQQ:index,IWM:index,DIA:index,GLD:metals,TLT:rates,"
        "WMT:consumer_staples,KO:consumer_staples,PG:consumer_staples,"
        "GME:consumer_discretionary"
    )

    mother_count: int = 5
    mother_children_per_strategy: int = 10
    mother_nightly_selection_time: str = "23:59"
    mother_weekly_crossover_day: str = "SUNDAY"
    mother_weekly_crossover_time: str = "23:59"
    mother_crossover_enabled: bool = True
    mother_kill_count_per_cycle: int = 3
    mother_mutation_rate: float = 0.20
    mother_mutation_step: float = 0.10
    mother_report_interval_min: int = 60

    child_execution_mode: str = "asyncio"
    child_order_size_mode: str = "percent_of_mother_allowance"
    child_max_position_pct: float = 0.10
    child_max_open_positions: int = 5
    child_default_timeframe: str = "1Min"
    child_signal_cooldown_sec: int = 60
    child_intent_timeout_sec: int = 20
    child_max_holding_bars: int = 180
    child_atr_period: int = 14
    child_atr_stop_multiplier: float = 2.0
    child_atr_floor_pct: float = 0.001
    child_ladder_first_target_pct: float = 0.015
    child_ladder_second_target_pct: float = 0.03
    child_ladder_first_fraction: float = 0.3333
    child_ladder_second_fraction: float = 0.3333
    child_l2_ask_bid_ratio_threshold: float = 3.0
    child_l2_ask_bid_hold_sec: int = 10

    api_max_requests_per_min: int = 200
    snapshot_cache_enabled: bool = True
    snapshot_cache_backend: str = "memory"
    redis_url: str = "redis://localhost:6379/0"
    alpaca_stock_snapshot_interval_sec: int = 60
    alpaca_crypto_snapshot_interval_sec: int = 5
    stock_data_delay_label: str = "T-15"

    paper_enable_virtual_friction: bool = True
    paper_slippage_model: str = "percent"
    paper_slippage_pct: float = 0.0005
    paper_virtual_commission_pct: float = 0.0005
    paper_latency_simulation_enabled: bool = True
    paper_latency_min_ms: int = 50
    paper_latency_max_ms: int = 200

    ledger_backend: str = "sqlite"
    ledger_write_mode: str = "immediate"
    ledger_balance_snapshot_interval_sec: int = 60
    ledger_dna_snapshot_interval_min: int = 60
    resurrection_enabled: bool = True

    freeze_trading_on_alpaca_disconnect: bool = True
    unfreeze_only_after_resync: bool = True

    scheduler_enabled: bool = True
    market_exit_on_shutdown: bool = False

    mother_speedy_name: str = "Speedy"
    mother_speedy_style: str = "momentum_scalper"
    mother_speedy_fitness_objective: FitnessObjective = "net_profit"
    mother_speedy_symbols: str = "BTC/USD,ETH/USD,SOL/USD"

    mother_techie_name: str = "Techie"
    mother_techie_style: str = "trend_follower"
    mother_techie_fitness_objective: FitnessObjective = "sharpe_ratio"
    mother_techie_symbols: str = "NVDA,TSLA,AAPL,MSFT,GOOGL,TSM,AMD"

    mother_bargain_name: str = "Bargain"
    mother_bargain_style: str = "mean_reversion"
    mother_bargain_fitness_objective: FitnessObjective = "sharpe_ratio"
    # Alpha (mean_reversion) — high quality names to buy the dip
    mother_bargain_symbols: str = "AAPL,MSFT,GOOGL,AMZN,JNJ,WMT,PG"

    mother_chaos_name: str = "Chaos"
    mother_chaos_style: str = "volatility_breakout"
    mother_chaos_fitness_objective: FitnessObjective = "net_profit"
    mother_chaos_symbols: str = "META,AMZN,GME,MSFT,GOOGL,AMD"

    mother_hedge_name: str = "Hedge"
    mother_hedge_style: str = "defensive_inverse"
    mother_hedge_fitness_objective: FitnessObjective = "sharpe_ratio"
    # Core (macro_trend): holds SPY, QQQ, NVDA — long horizon macro exposure
    mother_hedge_symbols: str = "SPY,QQQ,NVDA,GLD,TLT"

    @field_validator("alpaca_base_url")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        return value.removesuffix("/v2").rstrip("/")

    @field_validator("hive_state_db_path")
    @classmethod
    def normalize_path(cls, value: str) -> str:
        return str(Path(value))

    @property
    def all_crypto_symbols(self) -> list[str]:
        return [s for s in self.all_symbols if "/" in s]

    @property
    def all_stock_symbols(self) -> list[str]:
        return [s for s in self.all_symbols if "/" not in s]

    @property
    def all_symbols(self) -> list[str]:
        merged: list[str] = []
        for spec in self.mother_specs:
            merged.extend(spec.symbols)
        return sorted(set(merged))

    @property
    def mother_specs(self) -> list[MotherSpec]:
        return [
            MotherSpec(
                id="speedy",
                name=self.mother_speedy_name,
                style=self.mother_speedy_style,
                fitness_objective=self.mother_speedy_fitness_objective,
                symbols=_split_csv(self.mother_speedy_symbols),
            ),
            MotherSpec(
                id="techie",
                name=self.mother_techie_name,
                style=self.mother_techie_style,
                fitness_objective=self.mother_techie_fitness_objective,
                symbols=_split_csv(self.mother_techie_symbols),
            ),
            MotherSpec(
                id="bargain",
                name=self.mother_bargain_name,
                style=self.mother_bargain_style,
                fitness_objective=self.mother_bargain_fitness_objective,
                symbols=_split_csv(self.mother_bargain_symbols),
            ),
            MotherSpec(
                id="chaos",
                name=self.mother_chaos_name,
                style=self.mother_chaos_style,
                fitness_objective=self.mother_chaos_fitness_objective,
                symbols=_split_csv(self.mother_chaos_symbols),
            ),
            MotherSpec(
                id="hedge",
                name=self.mother_hedge_name,
                style=self.mother_hedge_style,
                fitness_objective=self.mother_hedge_fitness_objective,
                symbols=_split_csv(self.mother_hedge_symbols),
            ),
        ]

    @property
    def macro_news_sources(self) -> list[str]:
        return _split_csv(self.grandmother_macro_news_sources)

    @property
    def grandmother_orderflow_ws_assets_list(self) -> list[str]:
        normalized = [asset.strip().lower() for asset in self.grandmother_orderflow_ws_assets.split(",")]
        return [asset for asset in normalized if asset in {"stock", "crypto"}]

    @property
    def grandmother_base_weight_map(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for item in _split_csv(self.grandmother_base_weights):
            if ":" not in item:
                continue
            key, raw = item.split(":", 1)
            try:
                out[key.strip()] = float(raw.strip())
            except Exception:
                continue
        return out

    @property
    def grandmother_scanner_universe(self) -> list[str]:
        return [symbol for symbol in _split_csv(self.grandmother_scanner_universe_symbols) if "/" not in symbol]

    @property
    def grandmother_institutional_ownership_map(self) -> dict[str, float]:
        return _parse_symbol_value_map(self.grandmother_institutional_ownership_pct_by_symbol)

    @property
    def grandmother_sector_map(self) -> dict[str, str]:
        mapping = _parse_symbol_string_map(self.grandmother_symbol_sector_map)
        return {symbol: sector.lower() for symbol, sector in mapping.items() if sector}



def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_symbol_value_map(value: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for item in _split_csv(value):
        if ":" not in item:
            continue
        symbol, raw = item.split(":", 1)
        symbol = symbol.strip().upper()
        if not symbol:
            continue
        try:
            out[symbol] = float(raw.strip())
        except ValueError:
            continue
    return out


def _parse_symbol_string_map(value: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in _split_csv(value):
        if ":" not in item:
            continue
        symbol, raw = item.split(":", 1)
        symbol = symbol.strip().upper()
        label = raw.strip()
        if symbol and label:
            out[symbol] = label
    return out


settings = Settings()
