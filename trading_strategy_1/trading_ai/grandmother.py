from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import statistics
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

import aiosqlite
import websockets

from .alpaca_gateway import AccountView, AlpacaGateway
from .config import Settings
from .data_bus import MemoryDataBus
from .ledger import Ledger
from .macro import MacroNewsAnalyzer
from .models import MacroRegime, OrderIntent, Snapshot, VirtualFill
from .mother import MotherRuntime
from .notify import notify_mac

logger = logging.getLogger("trading_ai.grandmother")


class GrandmotherRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bus = MemoryDataBus()
        self.ledger = Ledger(settings.hive_state_db_path)
        self.gateway = AlpacaGateway(settings)
        self.macro_analyzer = MacroNewsAnalyzer(settings.macro_news_sources)

        self.intent_queue: asyncio.Queue[OrderIntent] = asyncio.Queue()
        self.mothers: dict[str, MotherRuntime] = {}
        self.tasks: list[asyncio.Task] = []

        self.running = False
        self.trading_frozen = False
        self.restart_requested = False
        self.restart_reason = ""
        self.unauthorized_halt = False
        self.initial_equity = 0.0
        self.last_account: AccountView | None = None
        self.macro_regime = MacroRegime(score=0.0, mode="neutral", summary="Startup neutral", headline_count=0)
        self.last_profit_milestone_sent = 0
        self.overseer_last_review_date: str | None = None
        self.overseer_min_confidence = settings.grandmother_overseer_low_vol_min_confidence
        self.overseer_risk_scale = settings.grandmother_overseer_low_vol_risk_scale
        self.overseer_trade_cooldown_sec = self._clamp_trade_cooldown(settings.child_signal_cooldown_sec)
        self.overseer_symbol_locks: dict[str, str] = {}
        self.overseer_aggressive_whitelist: dict[str, str] = {}
        self.overseer_aggressive_blacklist: dict[str, str] = {}
        self.overseer_last_trade_ts_by_symbol: dict[str, str] = {}
        self.overseer_last_global_trade_ts: str | None = None
        self.overseer_symbol_penalty_mult: dict[str, float] = {}
        self.overseer_morning_briefing: dict[str, str] = {}
        self.orderflow_metrics: dict[str, dict[str, float | bool | None]] = {}
        self.orderflow_spread_halt_until: str | None = None
        self._orderflow_spread_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._orderflow_price_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._orderflow_last_open_interest: dict[str, float] = {}
        self._ws_orderbook_metrics: dict[str, dict[str, float]] = {}
        self._ws_trade_events: dict[str, deque[tuple[datetime, str, float, float]]] = defaultdict(deque)
        self._ws_last_update_by_symbol: dict[str, datetime] = {}
        self._ws_last_trade_price_by_symbol: dict[str, float] = {}
        self.quality_watchlist: set[str] = set()
        self.scanner_focus_symbols: set[str] = set()
        self._scanner_focus_expiry_by_symbol: dict[str, datetime] = {}
        self._scanner_last_top_symbols: list[str] = []
        self._last_overseer_checkpoint: datetime | None = None
        self._last_penalty_refresh: datetime | None = None

        self._last_hourly_rebalance: str | None = None

    async def start(self) -> None:
        await self.ledger.init()
        account = await self._get_account()
        self.last_account = account
        await self._restore_initial_equity(account)
        await self._restore_macro_state()
        await self._restore_profit_milestone_state()
        await self._restore_overseer_state()
        self.trading_frozen = True

        for spec in self.settings.mother_specs:
            mother = MotherRuntime(
                settings=self.settings,
                spec=spec,
                ledger=self.ledger,
                bus=self.bus,
                forward_intent=self.intent_queue.put,
                frozen_getter=lambda: self.trading_frozen,
                macro_mode_getter=lambda: self.macro_regime.mode,
            )
            restored = await self._restore_children(spec.id)
            await mother.start(restored_children=restored)
            self.mothers[spec.id] = mother

        self._apply_overseer_controls_to_mothers()
        await self._refresh_penalty_state(force=True)
        await self._resync_children_with_exchange_positions()
        await self._warmup_data()
        await self._rebalance_allowances(account, reason="startup")
        self.trading_frozen = False
        self.running = True

        self.tasks = [
            asyncio.create_task(self._crypto_snapshot_loop(), name="snapshot-crypto"),
            asyncio.create_task(self._stock_snapshot_loop(), name="snapshot-stock"),
            asyncio.create_task(self._netting_loop(), name="intent-netting"),
            asyncio.create_task(self._equity_loop(), name="equity-safety"),
            asyncio.create_task(self._heartbeat_loop(), name="mother-heartbeat"),
            asyncio.create_task(self._schedule_loop(), name="schedule-loop"),
        ]
        if self.settings.grandmother_macro_news_enabled:
            self.tasks.append(asyncio.create_task(self._macro_loop(), name="macro-loop"))
        if self.settings.grandmother_overseer_enabled:
            self.tasks.append(asyncio.create_task(self._overseer_loop(), name="overseer-loop"))
        if self.settings.grandmother_scanner_enabled:
            self.tasks.append(asyncio.create_task(self._scanner_loop(), name="scanner-loop"))
        if self.settings.grandmother_orderflow_enabled and self.settings.grandmother_orderflow_ws_enabled:
            for asset in self.settings.grandmother_orderflow_ws_assets_list:
                symbols = self.settings.all_crypto_symbols if asset == "crypto" else self.settings.all_stock_symbols
                if not symbols:
                    continue
                self.tasks.append(
                    asyncio.create_task(
                        self._orderflow_ws_asset_loop(asset=asset, symbols=symbols),
                        name=f"orderflow-ws-{asset}",
                    )
                )

        await self.ledger.log_event(
            "INFO",
            "grandmother",
            "runtime_started",
            {
                "account": account.account_number,
                "equity": account.equity,
                "mother_count": len(self.mothers),
                "macro_mode": self.macro_regime.mode,
            },
        )

    async def run_forever(self) -> None:
        await self.start()
        await asyncio.gather(*self.tasks)

    async def stop(self) -> None:
        self.running = False
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for mother in self.mothers.values():
            await mother.stop()
        if self.settings.market_exit_on_shutdown:
            await asyncio.to_thread(self.gateway.close_all_positions)
        try:
            await self._persist_overseer_state()
        except Exception:
            pass
        await self.ledger.log_event("INFO", "grandmother", "runtime_stopped", {})
        await self.ledger.close()

    async def _restore_initial_equity(self, account: AccountView) -> None:
        state = await self.ledger.get_state("baseline_equity")
        if state and "value" in state:
            self.initial_equity = float(state["value"])
            return

        # If a starting capital override is configured, use it as the initial equity
        # for allocation and budgeting. Otherwise fall back to the actual account equity.
        if getattr(self.settings, "grandmother_starting_capital", 0.0) and self.settings.grandmother_starting_capital > 0:
            self.initial_equity = float(self.settings.grandmother_starting_capital)
        else:
            self.initial_equity = float(account.equity)

        await self.ledger.set_state("baseline_equity", {"value": self.initial_equity})

    async def _restore_macro_state(self) -> None:
        state = await self.ledger.get_state("macro_regime")
        if not state:
            return
        try:
            self.macro_regime = MacroRegime(
                score=float(state.get("score", 0.0)),
                mode=state.get("mode", "neutral"),
                summary=state.get("summary", "Recovered macro regime"),
                headline_count=int(state.get("headline_count", 0)),
                fetched_at=datetime.fromisoformat(state.get("fetched_at").replace("Z", "+00:00"))
                if state.get("fetched_at")
                else datetime.now(timezone.utc),
            )
        except Exception:
            pass

    async def _restore_profit_milestone_state(self) -> None:
        state = await self.ledger.get_state("profit_milestone_state")
        if not state:
            return
        try:
            self.last_profit_milestone_sent = int(state.get("last_milestone", 0))
        except Exception:
            self.last_profit_milestone_sent = 0

    async def _restore_overseer_state(self) -> None:
        state = await self.ledger.get_state("grandmother_overseer_state")
        if not state:
            return
        try:
            self.overseer_last_review_date = state.get("last_review_date")
            self.overseer_min_confidence = float(
                state.get("min_confidence", self.settings.grandmother_overseer_low_vol_min_confidence)
            )
            self.overseer_risk_scale = float(
                state.get("risk_scale", self.settings.grandmother_overseer_low_vol_risk_scale)
            )
            self.overseer_trade_cooldown_sec = self._clamp_trade_cooldown(
                int(state.get("trade_cooldown_sec", self.settings.child_signal_cooldown_sec))
            )
            self.overseer_symbol_locks = dict(state.get("symbol_locks", {}))
            self.overseer_aggressive_whitelist = dict(state.get("aggressive_whitelist", {}))
            self.overseer_aggressive_blacklist = dict(state.get("aggressive_blacklist", {}))
            self.overseer_last_trade_ts_by_symbol = dict(state.get("last_trade_ts_by_symbol", {}))
            self.overseer_last_global_trade_ts = state.get("last_global_trade_ts")
            raw_penalties = dict(state.get("symbol_penalty_mult", {}))
            self.overseer_symbol_penalty_mult = {
                symbol: max(1.0, float(mult)) for symbol, mult in raw_penalties.items()
            }
            self.orderflow_spread_halt_until = state.get("orderflow_spread_halt_until")
            self.overseer_morning_briefing = dict(state.get("morning_briefing", {}))
            self._prune_overseer_expired()
        except Exception:
            self.overseer_last_review_date = None
            self.overseer_min_confidence = self.settings.grandmother_overseer_low_vol_min_confidence
            self.overseer_risk_scale = self.settings.grandmother_overseer_low_vol_risk_scale
            self.overseer_trade_cooldown_sec = self._clamp_trade_cooldown(self.settings.child_signal_cooldown_sec)
            self.overseer_symbol_locks = {}
            self.overseer_aggressive_whitelist = {}
            self.overseer_aggressive_blacklist = {}
            self.overseer_last_trade_ts_by_symbol = {}
            self.overseer_last_global_trade_ts = None
            self.overseer_symbol_penalty_mult = {}
            self.orderflow_spread_halt_until = None
            self.overseer_morning_briefing = {}

    async def _persist_overseer_state(self) -> None:
        await self.ledger.set_state(
            "grandmother_overseer_state",
            {
                "last_review_date": self.overseer_last_review_date,
                "min_confidence": self.overseer_min_confidence,
                "risk_scale": self.overseer_risk_scale,
                "trade_cooldown_sec": self.overseer_trade_cooldown_sec,
                "symbol_locks": self.overseer_symbol_locks,
                "aggressive_whitelist": self.overseer_aggressive_whitelist,
                "aggressive_blacklist": self.overseer_aggressive_blacklist,
                "last_trade_ts_by_symbol": self.overseer_last_trade_ts_by_symbol,
                "last_global_trade_ts": self.overseer_last_global_trade_ts,
                "symbol_penalty_mult": self.overseer_symbol_penalty_mult,
                "orderflow_spread_halt_until": self.orderflow_spread_halt_until,
                "morning_briefing": self.overseer_morning_briefing,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _apply_overseer_controls_to_mothers(self) -> None:
        whitelist = self._active_symbol_set(self.overseer_aggressive_whitelist)
        blacklist = self._active_symbol_set(self.overseer_aggressive_blacklist)
        for mother in self.mothers.values():
            mother.set_dynamic_controls(self.overseer_min_confidence, self.overseer_risk_scale)
            mother.set_aggressive_guidance(whitelist, blacklist)

    def _prune_overseer_expired(self) -> None:
        now = datetime.now(timezone.utc)
        self.overseer_symbol_locks = {
            symbol: expiry
            for symbol, expiry in self.overseer_symbol_locks.items()
            if self._parse_ts(expiry) > now
        }
        self.overseer_aggressive_whitelist = {
            symbol: expiry
            for symbol, expiry in self.overseer_aggressive_whitelist.items()
            if self._parse_ts(expiry) > now
        }
        self.overseer_aggressive_blacklist = {
            symbol: expiry
            for symbol, expiry in self.overseer_aggressive_blacklist.items()
            if self._parse_ts(expiry) > now
        }

    def _active_symbol_set(self, mapping: dict[str, str]) -> set[str]:
        now = datetime.now(timezone.utc)
        return {symbol for symbol, expiry in mapping.items() if self._parse_ts(expiry) > now}

    async def _restore_children(self, mother_id: str) -> list[tuple[str, object, float, float, float]] | None:
        from .models import ChildDNA

        rows = await self.ledger.list_children(mother_id)
        alive = [row for row in rows if row[3] == "alive"]
        if not alive:
            return None

        restored: list[tuple[str, ChildDNA, float, float, float]] = []
        for child_id, dna_json, pnl, _, qty, entry in alive:
            restored.append(
                (
                    child_id,
                    ChildDNA(
                        ticker=dna_json["ticker"],
                        entry_signal=dna_json["entry_signal"],
                        params=[float(v) for v in dna_json.get("params", [])],
                        stop_loss=float(dna_json.get("stop_loss", 0.02)),
                        take_profit=float(dna_json.get("take_profit", 0.06)),
                        max_position_pct=float(dna_json.get("max_position_pct", self.settings.child_max_position_pct)),
                        risk_bias=float(dna_json.get("risk_bias", 1.0)),
                    ),
                    float(pnl),
                    float(qty or 0.0),
                    float(entry or 0.0),
                )
            )
        return restored

    async def _rebalance_allowances(self, account: AccountView, reason: str) -> None:
        style_mult = self._macro_style_multipliers()
        weights: dict[str, float] = {}
        for mother_id, mother in self.mothers.items():
            metrics = mother.portfolio_metrics()
            pnl = metrics["pnl"]
            sharpe = metrics["sharpe"]
            win_rate = metrics["win_rate"]
            drawdown = metrics["drawdown"]

            quality = 1.0 + (pnl / 5000.0) + (sharpe * 0.35) + (win_rate * 0.25) - (drawdown * 0.8)
            quality = max(0.05, quality)
            # Apply base weights (user-specified target split) if provided in settings.
            base_map = getattr(self.settings, "grandmother_base_weight_map", {})
            base_w = float(base_map.get(mother_id, 1.0)) if base_map else 1.0
            weights[mother_id] = quality * style_mult.get(mother_id, 1.0) * base_w

        total_weight = sum(weights.values())
        if total_weight <= 0:
            for mother in self.mothers.values():
                mother.set_allowance(account.buying_power / max(len(self.mothers), 1))
            return

        macro_reduction = self.settings.grandmother_macro_risk_reduction_pct * max(0.0, -self.macro_regime.score)
        macro_boost = self.settings.grandmother_macro_risk_on_boost_pct * max(0.0, self.macro_regime.score)
        deploy_multiplier = max(0.2, min(1.25, 1.0 - macro_reduction + macro_boost))
        deploy_capital = account.buying_power * deploy_multiplier

        for mother_id, mother in self.mothers.items():
            allowance = deploy_capital * (weights[mother_id] / total_weight)
            mother.set_allowance(allowance)
            await self.ledger.save_mother_allocation(mother_id, allowance)

        await self.ledger.log_event(
            "INFO",
            "grandmother",
            "allowance_rebalanced",
            {
                "reason": reason,
                "macro_mode": self.macro_regime.mode,
                "macro_score": self.macro_regime.score,
                "deploy_multiplier": deploy_multiplier,
                "deploy_capital": deploy_capital,
            },
        )

    def _macro_style_multipliers(self) -> dict[str, float]:
        if self.macro_regime.mode == "risk_off":
            return {
                "speedy": 0.55,
                "chaos": 0.55,
                "techie": 0.85,
                "bargain": 1.2,
                "hedge": 1.75,
            }
        if self.macro_regime.mode == "risk_on":
            return {
                "speedy": 1.35,
                "chaos": 1.35,
                "techie": 1.15,
                "bargain": 0.9,
                "hedge": 0.65,
            }
        return {mother_id: 1.0 for mother_id in self.mothers.keys()}

    async def _crypto_snapshot_loop(self) -> None:
        interval = max(1, self.settings.alpaca_crypto_snapshot_interval_sec)
        while self.running:
            await self._snapshot_cycle(asset="crypto")
            await asyncio.sleep(interval)

    async def _stock_snapshot_loop(self) -> None:
        interval = max(1, self.settings.alpaca_stock_snapshot_interval_sec)
        while self.running:
            await self._snapshot_cycle(asset="stock")
            await asyncio.sleep(interval)

    async def _snapshot_cycle(self, asset: str) -> None:
        try:
            if asset == "crypto":
                payload = await asyncio.to_thread(self.gateway.get_crypto_snapshots, self.settings.all_crypto_symbols)
            else:
                payload = await asyncio.to_thread(self.gateway.get_stock_snapshots, self.settings.all_stock_symbols)

            if payload:
                await self.bus.publish(payload)
                if self.settings.grandmother_orderflow_enabled:
                    await self._update_orderflow_cache(asset, payload)

            if self.trading_frozen and self.settings.unfreeze_only_after_resync:
                self.trading_frozen = False
                await self.ledger.log_event("INFO", "grandmother", "trading_unfrozen_after_resync", {"asset": asset})
        except Exception as exc:  # noqa: BLE001
            if self._is_unauthorized_error(exc):
                await self._trigger_unauthorized_shutdown(exc)
                return
            if self._is_connection_error(exc):
                self._request_restart(f"ConnectionError in {asset} snapshot cycle")
            if self.settings.freeze_trading_on_alpaca_disconnect:
                self.trading_frozen = True
            await self.ledger.log_event("ERROR", "grandmother", "snapshot_cycle_failed", {"asset": asset, "error": str(exc)})

    async def _scanner_loop(self) -> None:
        interval = max(30, self.settings.grandmother_scanner_refresh_sec)
        while self.running:
            await self._scanner_cycle()
            await asyncio.sleep(interval)

    async def _scanner_cycle(self) -> None:
        universe = self.settings.grandmother_scanner_universe
        if not universe:
            return
        try:
            snapshots = await asyncio.to_thread(self.gateway.get_stock_snapshots, universe)
        except Exception as exc:  # noqa: BLE001
            if self._is_unauthorized_error(exc):
                await self._trigger_unauthorized_shutdown(exc)
                return
            if self._is_connection_error(exc):
                self._request_restart("ConnectionError in scanner cycle")
            await self.ledger.log_event("WARNING", "grandmother", "scanner_cycle_failed", {"error": str(exc)})
            return
        if not snapshots:
            return

        inst_map = self.settings.grandmother_institutional_ownership_map
        candidates: list[tuple[str, float]] = []
        for symbol, snapshot in snapshots.items():
            symbol_upper = symbol.upper()
            if not self._passes_quality_filter(symbol_upper, snapshot, inst_map):
                continue
            candidates.append((symbol_upper, snapshot.price * max(snapshot.volume, 0.0)))
        candidates.sort(key=lambda row: row[1], reverse=True)
        top_n = max(1, self.settings.grandmother_scanner_top_n)
        top_symbols = [symbol for symbol, _ in candidates[:top_n]]
        self.quality_watchlist = set(top_symbols)
        self._scanner_last_top_symbols = top_symbols[:15]

        focus_symbols: set[str] = set()
        if top_symbols:
            try:
                lookback = max(10, self.settings.grandmother_orderflow_delta_lookback_sec)
                trade_delta_map = await asyncio.to_thread(self.gateway.get_stock_trade_delta, top_symbols, lookback)
            except Exception:
                trade_delta_map = {}

            abs_delta_threshold = max(0.0, self.settings.grandmother_scanner_min_abs_delta_notional)
            buy_sweep_threshold = max(1, self.settings.grandmother_tape_buy_sweep_min_count)
            sell_sweep_threshold = max(1, self.settings.grandmother_tape_sell_sweep_min_count)
            for symbol in top_symbols:
                metrics = trade_delta_map.get(symbol, {})
                delta_notional = abs(float(metrics.get("delta_notional", 0.0)))
                buy_sweeps = float(metrics.get("large_buy_print_count", 0.0))
                sell_sweeps = float(metrics.get("large_sell_print_count", 0.0))
                if (
                    delta_notional >= abs_delta_threshold
                    or buy_sweeps >= buy_sweep_threshold
                    or sell_sweeps >= sell_sweep_threshold
                ):
                    focus_symbols.add(symbol)

        now = datetime.now(timezone.utc)
        ttl = max(15, self.settings.grandmother_scanner_focus_ttl_sec)
        previous_focus = set(self.scanner_focus_symbols)
        for symbol in focus_symbols:
            self._scanner_focus_expiry_by_symbol[symbol] = now + timedelta(seconds=ttl)
        self._scanner_focus_expiry_by_symbol = {
            symbol: expiry
            for symbol, expiry in self._scanner_focus_expiry_by_symbol.items()
            if expiry > now
        }
        self.scanner_focus_symbols = set(self._scanner_focus_expiry_by_symbol.keys())

        if previous_focus != self.scanner_focus_symbols:
            await self.ledger.log_event(
                "INFO",
                "grandmother",
                "scanner_focus_updated",
                {
                    "quality_watchlist_count": len(self.quality_watchlist),
                    "focus_count": len(self.scanner_focus_symbols),
                    "top_symbols": self._scanner_last_top_symbols,
                },
            )

    def _passes_quality_filter(
        self, symbol: str, snapshot: Snapshot, institutional_map: dict[str, float]
    ) -> bool:
        if not self.settings.grandmother_quality_filter_enabled:
            return True
        if snapshot.price <= 0:
            return False

        daily_notional = snapshot.price * max(snapshot.volume, 0.0)
        if daily_notional < max(0.0, self.settings.grandmother_quality_min_daily_dollar_volume):
            return False

        spread = snapshot.spread
        if spread is None and snapshot.bid_price and snapshot.ask_price:
            spread = max(0.0, snapshot.ask_price - snapshot.bid_price)
        spread_bps = (spread / snapshot.price) * 10000 if spread and snapshot.price > 0 else 0.0
        if spread_bps > max(0.0, self.settings.grandmother_quality_max_spread_bps):
            return False

        if self.settings.grandmother_quality_require_institutional_ownership:
            threshold = max(0.0, self.settings.grandmother_quality_min_institutional_ownership_pct)
            ownership = institutional_map.get(symbol.upper())
            if ownership is None or ownership < threshold:
                return False
        return True

    async def _warmup_data(self) -> None:
        bars = max(0, int(self.settings.grandmother_warmup_bars))
        if bars <= 0:
            return
        try:
            stock_task = asyncio.to_thread(self.gateway.get_stock_bars, self.settings.all_stock_symbols, bars)
            crypto_task = asyncio.to_thread(self.gateway.get_crypto_bars, self.settings.all_crypto_symbols, bars)
            stock_map, crypto_map = await asyncio.gather(stock_task, crypto_task)
            merged: dict[str, list] = {}
            merged.update(stock_map)
            merged.update(crypto_map)

            # If history endpoints return sparse payloads, synthesize short warmup series from latest snapshot.
            if len(merged) < len(self.settings.all_symbols):
                stock_snaps = await asyncio.to_thread(self.gateway.get_stock_snapshots, self.settings.all_stock_symbols)
                crypto_snaps = await asyncio.to_thread(self.gateway.get_crypto_snapshots, self.settings.all_crypto_symbols)
                all_snaps = {}
                all_snaps.update(stock_snaps)
                all_snaps.update(crypto_snaps)
                for symbol in self.settings.all_symbols:
                    if symbol in merged:
                        continue
                    snap = all_snaps.get(symbol)
                    if not snap:
                        continue
                    merged[symbol] = self._synthetic_history(snap, bars)

            if not merged:
                return

            for symbol, seq in merged.items():
                merged[symbol] = sorted(seq, key=lambda s: s.timestamp)

            for mother in self.mothers.values():
                for child in mother.children.values():
                    history = merged.get(child.dna.ticker, [])
                    if not history:
                        continue
                    for snapshot in history[-600:]:
                        child._push_snapshot(snapshot)

            latest_updates = {symbol: seq[-1] for symbol, seq in merged.items() if seq}
            published = 0
            if latest_updates:
                await self.bus.publish(latest_updates)
                published = 1

            await self.ledger.log_event(
                "INFO",
                "grandmother",
                "warmup_complete",
                {"bars": bars, "symbols": len(merged), "publish_batches": published, "mode": "direct_seed"},
            )
        except Exception as exc:  # noqa: BLE001
            await self.ledger.log_event("WARNING", "grandmother", "warmup_failed", {"error": str(exc)})

    @staticmethod
    def _synthetic_history(seed: Snapshot, bars: int) -> list[Snapshot]:
        price = max(seed.price, 0.0001)
        out: list[Snapshot] = []
        for _ in range(bars):
            # Conservative jitter to avoid fabricating extreme volatility.
            price *= 1 + random.uniform(-0.0025, 0.0025)
            out.append(
                Snapshot(
                    symbol=seed.symbol,
                    price=max(0.0001, price),
                    volume=max(seed.volume, 0.0),
                    timestamp=seed.timestamp,
                    asset_class=seed.asset_class,
                    delayed_label=seed.delayed_label,
                )
            )
        return out

    async def _resync_children_with_exchange_positions(self) -> None:
        try:
            exchange_positions = await asyncio.to_thread(self.gateway.get_positions_map)
        except Exception as exc:  # noqa: BLE001
            await self.ledger.log_event("WARNING", "grandmother", "position_resync_failed", {"error": str(exc)})
            return

        children_by_symbol: dict[str, list[tuple[MotherRuntime, object]]] = {}
        for mother in self.mothers.values():
            for child in mother.children.values():
                children_by_symbol.setdefault(child.dna.ticker, []).append((mother, child))

        for symbol, entries in children_by_symbol.items():
            exchange_qty = exchange_positions.get(symbol, 0.0)
            internal_total = sum(max(0.0, child.position.qty) for _, child in entries)

            if exchange_qty <= 0:
                for mother, child in entries:
                    if child.position.qty <= 0:
                        continue
                    child.position.qty = 0.0
                    child.position.entry_price = 0.0
                    await self.ledger.upsert_child(
                        child.id,
                        mother.spec.id,
                        child.dna,
                        "alive",
                        child.performance.pnl,
                        child.position.qty,
                        child.position.entry_price,
                    )
                continue

            if internal_total <= 0:
                allocation = exchange_qty / len(entries)
                current_px = self.bus.get(symbol).price if self.bus.get(symbol) else 0.0
                for mother, child in entries:
                    child.position.qty = allocation
                    if child.position.entry_price <= 0 and current_px > 0:
                        child.position.entry_price = current_px
                    await self.ledger.upsert_child(
                        child.id,
                        mother.spec.id,
                        child.dna,
                        "alive",
                        child.performance.pnl,
                        child.position.qty,
                        child.position.entry_price,
                    )
                continue

            scale = exchange_qty / internal_total
            for mother, child in entries:
                child.position.qty = max(0.0, child.position.qty * scale)
                await self.ledger.upsert_child(
                    child.id,
                    mother.spec.id,
                    child.dna,
                    "alive",
                    child.performance.pnl,
                    child.position.qty,
                    child.position.entry_price,
                )

        await self.ledger.log_event(
            "INFO",
            "grandmother",
            "position_resync_complete",
            {"symbols": len(children_by_symbol), "exchange_positions": len(exchange_positions)},
        )

    async def _netting_loop(self) -> None:
        interval = max(1, self.settings.grandmother_intent_netting_interval_sec)
        while self.running:
            await asyncio.sleep(interval)
            await self._process_intents_batch()

    async def _process_intents_batch(self) -> None:
        intents: list[OrderIntent] = []
        while True:
            try:
                intents.append(self.intent_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if not intents:
            return
        if self.unauthorized_halt:
            for intent in intents:
                await self.ledger.update_intent_status(intent.id, "dropped_unauthorized")
            return
        self._prune_overseer_expired()
        self._apply_overseer_controls_to_mothers()
        if self._is_orderflow_spread_halt_active():
            sell_intents: list[OrderIntent] = []
            for intent in intents:
                if intent.side == "sell":
                    sell_intents.append(intent)
                    continue
                await self.ledger.update_intent_status(intent.id, "dropped_orderflow_spread_halt")
            intents = sell_intents
            if not intents:
                return
            await self.ledger.log_event(
                "INFO",
                "grandmother",
                "orderflow_spread_halt_sell_override",
                {"sell_intents": len(intents)},
            )
        try:
            await self._refresh_penalty_state()
        except Exception as exc:  # noqa: BLE001
            await self.ledger.log_event(
                "WARNING",
                "grandmother",
                "overseer_penalty_refresh_failed",
                {"error": str(exc)},
            )

        max_orders_per_hour = max(0, self.settings.grandmother_overseer_max_orders_per_hour)
        recent_orders = 0
        if max_orders_per_hour > 0:
            try:
                recent_orders = await self._count_orders_since(datetime.now(timezone.utc) - timedelta(hours=1))
            except Exception as exc:  # noqa: BLE001
                await self.ledger.log_event(
                    "WARNING",
                    "grandmother",
                    "orders_per_hour_count_failed",
                    {"error": str(exc)},
                )
                recent_orders = 0

        grouped: dict[str, list[OrderIntent]] = {}
        for intent in intents:
            grouped.setdefault(intent.symbol, []).append(intent)
        try:
            positions_map = await asyncio.to_thread(self.gateway.get_positions_map)
        except Exception as exc:  # noqa: BLE001
            if self._is_unauthorized_error(exc):
                await self._trigger_unauthorized_shutdown(exc)
                return
            if self._is_connection_error(exc):
                self._request_restart("ConnectionError while fetching positions")
            await self.ledger.log_event("ERROR", "grandmother", "positions_fetch_failed", {"error": str(exc)})
            return

        sector_counts, held_stock_symbols = self._stock_sector_position_counts(positions_map)
        pending_new_stock_symbols: set[str] = set()
        grouped_symbols = sorted(
            grouped.keys(),
            key=lambda sym: max((intent.confidence for intent in grouped.get(sym, [])), default=0.0),
            reverse=True,
        )

        for symbol in grouped_symbols:
            bucket = grouped[symbol]
            snapshot = self.bus.get(symbol)
            observed_price = snapshot.price if snapshot else 0.0

            if self.trading_frozen or observed_price <= 0:
                for intent in bucket:
                    await self.ledger.update_intent_status(intent.id, "dropped_frozen")
                continue

            valid_bucket: list[OrderIntent] = []
            for intent in bucket:
                if self._is_symbol_locked(intent.symbol):
                    await self.ledger.update_intent_status(intent.id, "dropped_symbol_lock")
                    continue
                scanner_veto = self._scanner_veto_status(intent)
                if scanner_veto:
                    await self.ledger.update_intent_status(intent.id, scanner_veto)
                    continue
                orderflow_veto = self._orderflow_veto_status(intent)
                if orderflow_veto:
                    await self.ledger.update_intent_status(intent.id, orderflow_veto)
                    continue
                if self._is_global_cooldown_active():
                    await self.ledger.update_intent_status(intent.id, "dropped_global_cooldown")
                    continue
                if self._is_cooldown_active(intent.symbol):
                    await self.ledger.update_intent_status(intent.id, "dropped_overseer_cooldown")
                    continue
                if max_orders_per_hour > 0 and recent_orders >= max_orders_per_hour:
                    await self.ledger.update_intent_status(intent.id, "dropped_hourly_order_cap")
                    continue
                if self._is_aggressive_blacklisted(intent):
                    await self.ledger.update_intent_status(intent.id, "dropped_aggressive_blacklist")
                    continue
                if not self._passes_edge_filter(intent):
                    await self.ledger.update_intent_status(intent.id, "dropped_low_reward_to_spread")
                    continue
                sector_veto = self._sector_diversity_veto_status(
                    intent,
                    sector_counts,
                    held_stock_symbols,
                    pending_new_stock_symbols,
                )
                if sector_veto:
                    await self.ledger.update_intent_status(intent.id, sector_veto)
                    continue
                valid_bucket.append(intent)

            if not valid_bucket:
                continue

            net_qty = 0.0
            for intent in valid_bucket:
                net_qty += intent.qty if intent.side == "buy" else -intent.qty

            # Internal exchange first: every intent gets a virtual fill for honest accounting.
            for intent in valid_bucket:
                fill = await self._apply_virtual_fill(intent)
                mother = self.mothers.get(intent.mother_id)
                if mother:
                    await mother.apply_virtual_fill(intent.child_id, fill)
                await self.ledger.update_intent_status(intent.id, "filled_virtual")

            if abs(net_qty) < 1e-8:
                await self.ledger.log_event(
                    "INFO",
                    "grandmother",
                    "netting_internal_match",
                    {"symbol": symbol, "intent_count": len(valid_bucket)},
                )
                logger.info("netting_internal_match symbol=%s intents=%s", symbol, len(valid_bucket))
                continue

            side = "buy" if net_qty > 0 else "sell"
            qty = abs(net_qty)
            if side == "sell":
                available = positions_map.get(symbol, 0.0)
                qty = min(qty, available)
                if qty <= 1e-8:
                    await self.ledger.log_event(
                        "INFO",
                        "grandmother",
                        "sell_skipped_no_inventory",
                        {"symbol": symbol, "requested_qty": abs(net_qty)},
                    )
                    logger.info("sell_skipped_no_inventory symbol=%s requested_qty=%.6f", symbol, abs(net_qty))
                    continue
            if max_orders_per_hour > 0 and recent_orders >= max_orders_per_hour:
                for intent in valid_bucket:
                    await self.ledger.update_intent_status(intent.id, "dropped_hourly_order_cap")
                continue
            try:
                # Emit an easy-to-spot highlighted log when the grandmother has accepted
                # the netted intents and is about to submit an external order. This
                # gives immediate feedback on acceptance (green), separate from the
                # post-submission execution message.
                trade_logger = logging.getLogger("trading_ai.trade")
                try:
                    H_START = "\033[1;32m"  # bold green
                    H_END = "\033[0m"
                    trade_logger.info(
                        f"{H_START}TRADE ACCEPTED symbol={symbol} side={side} qty={qty:.6f} intents={len(valid_bucket)}{H_END}"
                    )
                except Exception:
                    trade_logger.info("TRADE ACCEPTED symbol=%s side=%s qty=%.6f intents=%s", symbol, side, qty, len(valid_bucket))

                raw = await asyncio.to_thread(self.gateway.submit_market_order, symbol, side, qty)
                order_id = str(raw.get("id") or raw.get("order_id") or f"local-{datetime.now(timezone.utc).timestamp()}")
                await self.ledger.persist_order(
                    order_id=order_id,
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    intent_count=len(valid_bucket),
                    submitted_price=observed_price,
                    status="submitted",
                    raw=raw,
                )
                logger.info(
                    "order_submitted symbol=%s side=%s qty=%.6f intents=%s order_id=%s",
                    symbol,
                    side,
                    qty,
                    len(valid_bucket),
                    order_id,
                )
                # Emit a highlighted, easy-to-spot console log for actual external executions.
                trade_logger = logging.getLogger("trading_ai.trade")
                try:
                    H_START = "\033[1;32m"  # bold green
                    H_END = "\033[0m"
                    trade_logger.info(
                        f"{H_START}TRADE EXECUTED symbol={symbol} side={side} qty={qty:.6f} intents={len(valid_bucket)} order_id={order_id}{H_END}"
                    )
                except Exception:
                    # If coloring or formatting fails, fall back to plain informative log
                    trade_logger.info("TRADE EXECUTED symbol=%s side=%s qty=%.6f intents=%s order_id=%s", symbol, side, qty, len(valid_bucket), order_id)
                now_iso = datetime.now(timezone.utc).isoformat()
                self.overseer_last_trade_ts_by_symbol[symbol] = now_iso
                self.overseer_last_global_trade_ts = now_iso
                if side == "buy":
                    positions_map[symbol] = positions_map.get(symbol, 0.0) + qty
                else:
                    positions_map[symbol] = max(0.0, positions_map.get(symbol, 0.0) - qty)
                if max_orders_per_hour > 0:
                    recent_orders += 1
            except Exception as exc:  # noqa: BLE001
                if self._is_unauthorized_error(exc):
                    await self._trigger_unauthorized_shutdown(exc)
                    return
                if self._is_connection_error(exc):
                    self._request_restart(f"ConnectionError submitting order {symbol}")
                await self.ledger.log_event(
                    "ERROR",
                    "grandmother",
                    "order_submit_failed",
                    {"symbol": symbol, "side": side, "qty": qty, "error": str(exc)},
                )
                logger.error("order_submit_failed symbol=%s side=%s qty=%.6f error=%s", symbol, side, qty, exc)

    async def _apply_virtual_fill(self, intent: OrderIntent) -> VirtualFill:
        if self.settings.paper_latency_simulation_enabled:
            delay_ms = random.randint(self.settings.paper_latency_min_ms, self.settings.paper_latency_max_ms)
            await asyncio.sleep(delay_ms / 1000)
        else:
            delay_ms = 0

        slip_pct = self.settings.paper_slippage_pct if self.settings.paper_enable_virtual_friction else 0.0
        comm_pct = self.settings.paper_virtual_commission_pct if self.settings.paper_enable_virtual_friction else 0.0

        if intent.side == "buy":
            effective = intent.observed_price * (1 + slip_pct)
            slippage = effective - intent.observed_price
        else:
            effective = intent.observed_price * (1 - slip_pct)
            slippage = intent.observed_price - effective

        commission = abs(effective * intent.qty) * comm_pct

        fill = VirtualFill(
            intent_id=intent.id,
            symbol=intent.symbol,
            side=intent.side,
            qty=intent.qty,
            observed_price=intent.observed_price,
            effective_price=effective,
            commission=commission,
            slippage=slippage,
            latency_ms=delay_ms,
        )
        await self.ledger.persist_fill(fill)
        return fill

    async def _equity_loop(self) -> None:
        interval = max(10, self.settings.ledger_balance_snapshot_interval_sec)
        while self.running:
            try:
                account = await self._get_account()
                self.last_account = account

                drawdown = 0.0
                if self.initial_equity > 0:
                    drawdown = max(0.0, (self.initial_equity - account.equity) / self.initial_equity)

                await self.ledger.snapshot_equity(
                    equity=account.equity,
                    cash=account.cash,
                    buying_power=account.buying_power,
                    drawdown_pct=drawdown,
                )
                await self._check_profit_milestones(account)

                if (
                    self.settings.grandmother_kill_switch_enabled
                    and drawdown >= self.settings.grandmother_max_account_drawdown_pct
                ):
                    self.trading_frozen = True
                    await asyncio.to_thread(self.gateway.close_all_positions)
                    await self.ledger.log_event(
                        "CRITICAL",
                        "grandmother",
                        "kill_switch_triggered",
                        {"drawdown": drawdown, "equity": account.equity},
                    )

                hour_key = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
                if hour_key != self._last_hourly_rebalance:
                    await self._rebalance_allowances(account, reason="hourly")
                    self._last_hourly_rebalance = hour_key

                now = datetime.now(timezone.utc)
                if (
                    self.settings.grandmother_overseer_enabled
                    and (
                        self._last_overseer_checkpoint is None
                        or (now - self._last_overseer_checkpoint).total_seconds() >= 60
                    )
                ):
                    await self._persist_overseer_state()
                    self._last_overseer_checkpoint = now

            except Exception as exc:  # noqa: BLE001
                if self._is_unauthorized_error(exc):
                    await self._trigger_unauthorized_shutdown(exc)
                    return
                if self._is_connection_error(exc):
                    self._request_restart("ConnectionError in equity loop")
                await self.ledger.log_event("ERROR", "grandmother", "equity_loop_failed", {"error": str(exc)})

            await asyncio.sleep(interval)

    async def _macro_loop(self) -> None:
        interval = max(1, self.settings.grandmother_macro_news_refresh_min) * 60
        while self.running:
            try:
                regime = await self.macro_analyzer.analyze()
                self.macro_regime = regime
                await self.ledger.set_state("macro_regime", regime.to_dict())
                await self.ledger.snapshot_macro(regime)
                await self.ledger.log_event("INFO", "grandmother", "macro_regime_updated", regime.to_dict())

                if self.last_account is not None:
                    await self._rebalance_allowances(self.last_account, reason="macro_update")
            except Exception as exc:  # noqa: BLE001
                if self._is_connection_error(exc):
                    self._request_restart("ConnectionError in macro loop")
                await self.ledger.log_event("ERROR", "grandmother", "macro_loop_failed", {"error": str(exc)})
            await asyncio.sleep(interval)

    async def _overseer_loop(self) -> None:
        review_h, review_m = self._parse_hhmm(self.settings.grandmother_overseer_review_time)
        while self.running:
            try:
                self._prune_overseer_expired()
                now = datetime.now(timezone.utc)
                today = now.date().isoformat()
                review_due = now.hour > review_h or (now.hour == review_h and now.minute >= review_m)
                should_bootstrap = self.overseer_last_review_date is None
                should_review = self.overseer_last_review_date != today and (review_due or should_bootstrap)
                if should_review and not self.unauthorized_halt:
                    await self._run_daily_overseer_review(now)
            except Exception as exc:  # noqa: BLE001
                await self.ledger.log_event("ERROR", "grandmother", "overseer_loop_failed", {"error": str(exc)})
            await asyncio.sleep(20)

    async def _run_daily_overseer_review(self, now: datetime) -> None:
        day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        rows = await self._load_fill_rows(day_start, now)

        symbol_stats, total_realized_pnl, total_slippage_cost, transaction_volume = self._build_symbol_review(rows)
        randomized_rows = [row for row in rows if str(row.get("reason", "")).startswith("explore_")]
        randomized_symbol_pnl = self._compute_symbol_realized(randomized_rows)

        churn_threshold = max(1, self.settings.grandmother_overseer_trade_churn_threshold)
        for symbol, stats in symbol_stats.items():
            if stats["trade_count"] > churn_threshold and stats["realized_pnl"] < 0:
                await self.ledger.log_event(
                    "WARNING",
                    "grandmother",
                    "overseer_churn_detected",
                    {
                        "symbol": symbol,
                        "trade_count": stats["trade_count"],
                        "realized_pnl": stats["realized_pnl"],
                        "advice": "Stop chasing micro-volatility. The child is spinning wheels.",
                    },
                )

        self.overseer_symbol_penalty_mult = self._derive_symbol_penalty_multipliers(symbol_stats)
        self._last_penalty_refresh = now

        base_cooldown = max(
            1,
            self.settings.child_signal_cooldown_sec,
            self.settings.grandmother_overseer_min_trade_cooldown_sec,
        )
        suggested_cooldown = base_cooldown
        if rows:
            churn_factor = min(3.0, float(len(rows)) / float(churn_threshold))
            suggested_cooldown += int(churn_factor * 30)
        if total_slippage_cost > abs(total_realized_pnl) and rows:
            suggested_cooldown = int(
                max(
                    suggested_cooldown,
                    self.settings.grandmother_overseer_cooldown_minutes * 60,
                )
                * max(1.0, self.settings.grandmother_overseer_penalty_buffer_mult)
            )
        self.overseer_trade_cooldown_sec = self._clamp_trade_cooldown(suggested_cooldown)

        vix_proxy = await self._estimate_vix_proxy()
        if vix_proxy > self.settings.grandmother_overseer_vix_threshold:
            self.overseer_min_confidence = self.settings.grandmother_overseer_high_vol_min_confidence
            self.overseer_risk_scale = self.settings.grandmother_overseer_high_vol_risk_scale
        else:
            self.overseer_min_confidence = self.settings.grandmother_overseer_low_vol_min_confidence
            self.overseer_risk_scale = self.settings.grandmother_overseer_low_vol_risk_scale

        daily_budget = self.initial_equity * max(0.0001, self.settings.grandmother_overseer_daily_budget_pct)
        lock_threshold = -(daily_budget * 0.20)
        lock_expiry = (now + timedelta(hours=self.settings.grandmother_overseer_symbol_lock_hours)).isoformat()
        for symbol, stats in symbol_stats.items():
            if stats["realized_pnl"] < lock_threshold:
                self.overseer_symbol_locks[symbol] = lock_expiry
                await self.ledger.log_event(
                    "WARNING",
                    "grandmother",
                    "overseer_symbol_locked",
                    {
                        "symbol": symbol,
                        "duration_hours": self.settings.grandmother_overseer_symbol_lock_hours,
                        "reason": "symbol_pnl_below_daily_budget_threshold",
                    },
                )

        guidance_expiry = (now + timedelta(hours=24)).isoformat()
        for symbol, pnl in randomized_symbol_pnl.items():
            if pnl > 0:
                self.overseer_aggressive_whitelist[symbol] = guidance_expiry
                self.overseer_aggressive_blacklist.pop(symbol, None)
            elif pnl < 0:
                self.overseer_aggressive_blacklist[symbol] = guidance_expiry
                self.overseer_aggressive_whitelist.pop(symbol, None)

        top_symbol, top_pnl = self._top_symbol(symbol_stats, reverse=True)
        bottom_symbol, bottom_pnl = self._top_symbol(symbol_stats, reverse=False)
        aggression_score = max(
            0.0,
            min(1.0, (1.0 - self.overseer_min_confidence) * 0.6 + self.overseer_risk_scale * 0.4),
        )
        system_adjustment = (
            f"Cooldown {self.overseer_trade_cooldown_sec}s + min_confidence {self.overseer_min_confidence:.2f}."
        )
        if vix_proxy > self.settings.grandmother_overseer_vix_threshold:
            system_adjustment += " Volatility high: reduced risk scale."
        if self.overseer_symbol_penalty_mult:
            hot_penalty = sorted(self.overseer_symbol_penalty_mult.items(), key=lambda item: item[1], reverse=True)[0]
            system_adjustment += f" Penalty lens active ({hot_penalty[0]} x{hot_penalty[1]:.2f})."

        self.overseer_morning_briefing = {
            "Top_Performer_Logic": f"{top_symbol} ({top_pnl:+.2f})",
            "Bottom_Performer_Logic": f"{bottom_symbol} ({bottom_pnl:+.2f})",
            "System_Adjustment": system_adjustment,
            "Aggression_Level": f"{aggression_score:.2f}",
        }
        self.overseer_last_review_date = now.date().isoformat()
        self._prune_overseer_expired()
        self._apply_overseer_controls_to_mothers()
        await self._persist_overseer_state()

        await self.ledger.log_event(
            "INFO",
            "grandmother",
            "overseer_daily_review_complete",
            {
                "review_date": self.overseer_last_review_date,
                "trade_count": len(rows),
                "total_realized_pnl": total_realized_pnl,
                "total_slippage_cost": total_slippage_cost,
                "transaction_volume": transaction_volume,
                "vix_proxy": vix_proxy,
                "min_confidence": self.overseer_min_confidence,
                "risk_scale": self.overseer_risk_scale,
                "trade_cooldown_sec": self.overseer_trade_cooldown_sec,
                "penalty_symbols": len(self.overseer_symbol_penalty_mult),
                "briefing": self.overseer_morning_briefing,
            },
        )

    async def _load_fill_rows(self, start_ts: datetime, end_ts: datetime) -> list[dict[str, object]]:
        query = """
            SELECT
                f.ts AS ts,
                f.intent_id AS intent_id,
                f.symbol AS symbol,
                f.side AS side,
                f.qty AS qty,
                f.observed_price AS observed_price,
                f.effective_price AS effective_price,
                f.commission AS commission,
                f.slippage AS slippage,
                i.mother_id AS mother_id,
                i.child_id AS child_id,
                i.reason AS reason,
                i.confidence AS confidence
            FROM fills f
            LEFT JOIN intents i ON i.intent_id = f.intent_id
            WHERE f.ts >= ? AND f.ts <= ?
            ORDER BY f.ts ASC
        """
        out: list[dict[str, object]] = []
        async with aiosqlite.connect(self.settings.hive_state_db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(query, (start_ts.isoformat(), end_ts.isoformat()))
            rows = await cursor.fetchall()
            for row in rows:
                out.append(dict(row))
        return out

    def _build_symbol_review(
        self, rows: list[dict[str, object]]
    ) -> tuple[dict[str, dict[str, float]], float, float, float]:
        lots: dict[str, deque[list[float]]] = defaultdict(deque)
        stats: dict[str, dict[str, float]] = defaultdict(
            lambda: {
                "trade_count": 0.0,
                "buy_count": 0.0,
                "sell_count": 0.0,
                "closed_trades": 0.0,
                "wins": 0.0,
                "realized_pnl": 0.0,
                "slippage_cost": 0.0,
                "commission_cost": 0.0,
                "notional": 0.0,
                "mother_accuracy": 0.0,
                "child_efficiency": 0.0,
            }
        )

        for row in rows:
            symbol = str(row.get("symbol") or "")
            side = str(row.get("side") or "")
            qty = float(row.get("qty") or 0.0)
            effective = float(row.get("effective_price") or 0.0)
            commission = float(row.get("commission") or 0.0)
            slippage = float(row.get("slippage") or 0.0)
            if qty <= 0 or not symbol:
                continue

            ref = stats[symbol]
            ref["trade_count"] += 1
            ref["notional"] += abs(effective * qty)
            ref["slippage_cost"] += abs(slippage) * qty
            ref["commission_cost"] += commission

            if side == "buy":
                ref["buy_count"] += 1
                lot_price = effective + (commission / qty)
                lots[symbol].append([qty, lot_price])
                continue

            ref["sell_count"] += 1
            remaining = qty
            realized = -commission
            while remaining > 1e-9 and lots[symbol]:
                lot_qty, lot_price = lots[symbol][0]
                matched = min(remaining, lot_qty)
                realized += (effective - lot_price) * matched
                lot_qty -= matched
                remaining -= matched
                if lot_qty <= 1e-9:
                    lots[symbol].popleft()
                else:
                    lots[symbol][0][0] = lot_qty

            ref["realized_pnl"] += realized
            ref["closed_trades"] += 1
            if realized >= 0:
                ref["wins"] += 1

        for symbol, ref in stats.items():
            closed = max(1.0, ref["closed_trades"])
            ref["mother_accuracy"] = ref["wins"] / closed
            cost_ratio = (ref["slippage_cost"] + ref["commission_cost"]) / max(ref["notional"], 1e-9)
            ref["child_efficiency"] = max(0.0, 1.0 - min(1.0, cost_ratio * 40.0))

        total_realized = sum(v["realized_pnl"] for v in stats.values())
        total_slippage = sum(v["slippage_cost"] for v in stats.values())
        total_volume = sum(v["notional"] for v in stats.values())
        return dict(stats), total_realized, total_slippage, total_volume

    def _compute_symbol_realized(self, rows: list[dict[str, object]]) -> dict[str, float]:
        lots: dict[str, deque[list[float]]] = defaultdict(deque)
        out: dict[str, float] = defaultdict(float)
        for row in rows:
            symbol = str(row.get("symbol") or "")
            side = str(row.get("side") or "")
            qty = float(row.get("qty") or 0.0)
            effective = float(row.get("effective_price") or 0.0)
            commission = float(row.get("commission") or 0.0)
            if qty <= 0 or not symbol:
                continue
            if side == "buy":
                lots[symbol].append([qty, effective + (commission / qty)])
                continue

            remaining = qty
            realized = -commission
            while remaining > 1e-9 and lots[symbol]:
                lot_qty, lot_price = lots[symbol][0]
                matched = min(remaining, lot_qty)
                realized += (effective - lot_price) * matched
                lot_qty -= matched
                remaining -= matched
                if lot_qty <= 1e-9:
                    lots[symbol].popleft()
                else:
                    lots[symbol][0][0] = lot_qty
            out[symbol] += realized
        return dict(out)

    async def _orderflow_ws_asset_loop(self, asset: str, symbols: list[str]) -> None:
        ws_url = self._orderflow_ws_url(asset)
        if not ws_url:
            await self.ledger.log_event(
                "WARNING",
                "grandmother",
                "orderflow_ws_invalid_asset",
                {"asset": asset},
            )
            return

        reconnect_sec = max(1, self.settings.grandmother_orderflow_ws_reconnect_sec)
        subscribe_payload = self._orderflow_ws_subscribe_payload(asset, symbols)

        while self.running:
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "action": "auth",
                                "key": self.settings.alpaca_api_key,
                                "secret": self.settings.alpaca_secret_key,
                            }
                        )
                    )
                    await ws.send(json.dumps(subscribe_payload))
                    await self.ledger.log_event(
                        "INFO",
                        "grandmother",
                        "orderflow_ws_connected",
                        {"asset": asset, "symbols": len(symbols)},
                    )

                    while self.running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                        self._ingest_orderflow_ws_payload(asset, raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await self.ledger.log_event(
                    "WARNING",
                    "grandmother",
                    "orderflow_ws_disconnected",
                    {"asset": asset, "error": str(exc), "retry_sec": reconnect_sec},
                )
                if self.running:
                    await asyncio.sleep(reconnect_sec)

    def _orderflow_ws_url(self, asset: str) -> str | None:
        # Prefer an explicit streaming URL when provided (avoids REST->WS mismatches).
        # Accepts full ws:// or wss:// values, or will derive from https/http.
        raw_stream = getattr(self.settings, "alpaca_data_stream_url", None) or ""
        if raw_stream:
            base = raw_stream.rstrip("/")
            if base.startswith("wss://") or base.startswith("ws://"):
                # already a websocket URL
                base = base
            elif base.startswith("https://"):
                base = "wss://" + base.removeprefix("https://")
            elif base.startswith("http://"):
                base = "ws://" + base.removeprefix("http://")
            else:
                # fallback, assume it's a host like stream.data.alpaca.markets
                base = "wss://" + base
        else:
            base = self.settings.alpaca_data_url.rstrip("/")
            if base.startswith("https://"):
                base = "wss://" + base.removeprefix("https://")
            elif base.startswith("http://"):
                base = "ws://" + base.removeprefix("http://")
        if asset == "crypto":
            return f"{base}/v1beta3/crypto/us"
        if asset == "stock":
            return f"{base}/v2/iex"
        return None

    def _orderflow_ws_subscribe_payload(self, asset: str, symbols: list[str]) -> dict[str, object]:
        payload: dict[str, object] = {
            "action": "subscribe",
            "trades": symbols,
            "quotes": symbols,
        }
        if asset == "crypto" and self.settings.grandmother_l2_tape_enabled:
            payload["orderbooks"] = symbols
        return payload

    def _ingest_orderflow_ws_payload(self, asset: str, raw: str | bytes) -> None:
        try:
            decoded = raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else raw
            message = json.loads(decoded)
        except Exception:
            return

        events = message if isinstance(message, list) else [message]
        now = datetime.now(timezone.utc)
        for event in events:
            if not isinstance(event, dict):
                continue
            event_type = str(event.get("T") or event.get("type") or "").strip().lower()
            if event_type in {"success", "subscription", "error", "authenticated", "connected"}:
                continue

            symbol_raw = str(event.get("S") or event.get("symbol") or "").strip()
            if not symbol_raw:
                continue
            symbol = self.gateway._canonical_symbol(symbol_raw)

            if event_type in {"o", "ob", "book", "orderbook"}:
                self._ingest_ws_orderbook(symbol, event, now)
                continue
            if event_type in {"q", "quote"}:
                self._ingest_ws_quote_as_book(symbol, event, now)
                continue
            if event_type in {"t", "trade"}:
                self._ingest_ws_trade(symbol, event, now)
                continue
            if asset == "crypto" and ("b" in event or "a" in event or "bids" in event or "asks" in event):
                self._ingest_ws_orderbook(symbol, event, now)

    def _ingest_ws_orderbook(self, symbol: str, event: dict[str, object], now: datetime) -> None:
        depth = max(1, self.settings.grandmother_l2_depth_levels)
        bids = event.get("b", event.get("bids", []))
        asks = event.get("a", event.get("asks", []))
        metrics = self._orderbook_depth_metrics(bids, asks, depth)
        if not metrics:
            return
        self._ws_orderbook_metrics[symbol] = metrics
        self._ws_last_update_by_symbol[symbol] = now

    def _ingest_ws_quote_as_book(self, symbol: str, event: dict[str, object], now: datetime) -> None:
        bp = self._safe_float(event.get("bp", event.get("bid_price")))
        ap = self._safe_float(event.get("ap", event.get("ask_price")))
        bs = self._safe_float(event.get("bs", event.get("bid_size")))
        ass = self._safe_float(event.get("as", event.get("ask_size")))
        bids = [{"p": bp, "s": bs}] if bp > 0 and bs > 0 else []
        asks = [{"p": ap, "s": ass}] if ap > 0 and ass > 0 else []
        metrics = self._orderbook_depth_metrics(bids, asks, max(1, self.settings.grandmother_l2_depth_levels))
        if not metrics:
            return
        self._ws_orderbook_metrics[symbol] = metrics
        self._ws_last_update_by_symbol[symbol] = now

    def _ingest_ws_trade(self, symbol: str, event: dict[str, object], now: datetime) -> None:
        side = self._normalize_trade_side(event)
        price = self._safe_float(event.get("p", event.get("price")))
        qty = self._safe_float(event.get("s", event.get("size")))
        if price <= 0 or qty <= 0:
            return
        if side == "unknown":
            prev_price = self._ws_last_trade_price_by_symbol.get(symbol)
            if prev_price is None:
                side = "buy"
            else:
                side = "buy" if price >= prev_price else "sell"
        self._ws_last_trade_price_by_symbol[symbol] = price

        ts = self._parse_event_ts(event.get("t")) or now
        events = self._ws_trade_events[symbol]
        events.append((ts, side, qty, qty * price))

        max_window = max(60, self.settings.grandmother_orderflow_delta_lookback_sec * 6)
        cutoff = now - timedelta(seconds=max_window)
        while events and events[0][0] < cutoff:
            events.popleft()
        self._ws_last_update_by_symbol[symbol] = now

    def _build_ws_trade_metrics(self, symbol: str, lookback_sec: int, now: datetime) -> dict[str, float]:
        events = self._ws_trade_events.get(symbol)
        if not events:
            return {}

        cutoff = now - timedelta(seconds=max(10, lookback_sec))
        large_threshold = max(0.0, self.settings.grandmother_tape_large_print_min_notional)
        delta_qty = 0.0
        delta_notional = 0.0
        buy_qty = 0.0
        sell_qty = 0.0
        buy_notional = 0.0
        sell_notional = 0.0
        largest_print_notional = 0.0
        largest_buy_print_notional = 0.0
        largest_sell_print_notional = 0.0
        large_print_count = 0.0
        large_buy_print_count = 0.0
        large_sell_print_count = 0.0

        for ts, side, qty, notional in events:
            if ts < cutoff:
                continue
            sign = 1.0 if side == "buy" else -1.0
            delta_qty += sign * qty
            delta_notional += sign * notional
            largest_print_notional = max(largest_print_notional, notional)
            if sign > 0:
                buy_qty += qty
                buy_notional += notional
                largest_buy_print_notional = max(largest_buy_print_notional, notional)
            else:
                sell_qty += qty
                sell_notional += notional
                largest_sell_print_notional = max(largest_sell_print_notional, notional)
            if large_threshold > 0 and notional >= large_threshold:
                large_print_count += 1.0
                if sign > 0:
                    large_buy_print_count += 1.0
                else:
                    large_sell_print_count += 1.0

        return {
            "delta_qty": delta_qty,
            "delta_notional": delta_notional,
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "largest_print_notional": largest_print_notional,
            "largest_buy_print_notional": largest_buy_print_notional,
            "largest_sell_print_notional": largest_sell_print_notional,
            "large_print_count": large_print_count,
            "large_buy_print_count": large_buy_print_count,
            "large_sell_print_count": large_sell_print_count,
        }

    def _ws_orderflow_fresh(self, symbol: str, now: datetime) -> bool:
        if not self.settings.grandmother_orderflow_ws_enabled:
            return False
        last = self._ws_last_update_by_symbol.get(symbol)
        if not last:
            return False
        max_staleness = max(2, self.settings.grandmother_orderflow_ws_max_staleness_sec)
        return (now - last).total_seconds() <= max_staleness

    @staticmethod
    def _orderbook_depth_metrics(
        bids_raw: object, asks_raw: object, depth: int
    ) -> dict[str, float]:
        bids = GrandmotherRuntime._normalize_orderbook_levels(bids_raw)
        asks = GrandmotherRuntime._normalize_orderbook_levels(asks_raw)
        if not bids and not asks:
            return {}

        depth = max(1, depth)
        bid_slice = bids[:depth]
        ask_slice = asks[:depth]
        bid_volume = sum(size for _, size in bid_slice)
        ask_volume = sum(size for _, size in ask_slice)
        bid_notional = sum(price * size for price, size in bid_slice)
        ask_notional = sum(price * size for price, size in ask_slice)
        total = bid_volume + ask_volume
        imbalance = (bid_volume - ask_volume) / total if total > 0 else 0.0
        total_notional = bid_notional + ask_notional
        imbalance_notional = (bid_notional - ask_notional) / total_notional if total_notional > 0 else 0.0

        whale_bid_price, whale_bid_size, whale_bid_notional = GrandmotherRuntime._largest_level(bid_slice)
        whale_ask_price, whale_ask_size, whale_ask_notional = GrandmotherRuntime._largest_level(ask_slice)
        best_bid = bid_slice[0][0] if bid_slice else 0.0
        best_ask = ask_slice[0][0] if ask_slice else 0.0
        spread = best_ask - best_bid if best_bid > 0 and best_ask >= best_bid else 0.0

        return {
            "bid_volume_10": max(0.0, bid_volume),
            "ask_volume_10": max(0.0, ask_volume),
            "bid_volume_depth": max(0.0, bid_volume),
            "ask_volume_depth": max(0.0, ask_volume),
            "bid_notional_depth": max(0.0, bid_notional),
            "ask_notional_depth": max(0.0, ask_notional),
            "imbalance_qty": imbalance,
            "imbalance_notional": imbalance_notional,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "whale_bid_price": whale_bid_price,
            "whale_bid_size": whale_bid_size,
            "whale_bid_notional": whale_bid_notional,
            "whale_ask_price": whale_ask_price,
            "whale_ask_size": whale_ask_size,
            "whale_ask_notional": whale_ask_notional,
        }

    @staticmethod
    def _normalize_orderbook_levels(levels_raw: object) -> list[tuple[float, float]]:
        out: list[tuple[float, float]] = []
        if not isinstance(levels_raw, list):
            return out
        for level in levels_raw:
            if not isinstance(level, dict):
                continue
            price = GrandmotherRuntime._safe_float(level.get("p", level.get("price")))
            size = GrandmotherRuntime._safe_float(level.get("s", level.get("size")))
            if price > 0 and size > 0:
                out.append((price, size))
        return out

    @staticmethod
    def _largest_level(levels: list[tuple[float, float]]) -> tuple[float, float, float]:
        if not levels:
            return (0.0, 0.0, 0.0)
        price, size = max(levels, key=lambda item: item[0] * item[1])
        return (price, size, price * size)

    @staticmethod
    def _safe_float(value: object) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _parse_event_ts(value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _normalize_trade_side(event: dict[str, object]) -> str:
        raw = str(
            event.get("tks")
            or event.get("taker_side")
            or event.get("side")
            or ""
        ).strip().lower()
        if raw in {"b", "buy", "bid", "buyer"}:
            return "buy"
        if raw in {"s", "sell", "ask", "seller"}:
            return "sell"
        return "unknown"

    async def _update_orderflow_cache(self, asset: str, payload: dict[str, Snapshot]) -> None:
        symbols = list(payload.keys())
        if not symbols:
            return

        lookback = max(10, self.settings.grandmother_orderflow_delta_lookback_sec)
        depth = max(1, self.settings.grandmother_l2_depth_levels)
        orderbooks_task = None
        trades_task = None
        if asset == "crypto":
            orderbooks_task = asyncio.to_thread(self.gateway.get_crypto_orderbook_liquidity, symbols, depth)
            trades_task = asyncio.to_thread(self.gateway.get_crypto_trade_delta, symbols, lookback)
        else:
            trades_task = asyncio.to_thread(self.gateway.get_stock_trade_delta, symbols, lookback)
        open_interest_task = asyncio.to_thread(self.gateway.get_open_interest, symbols)

        orderbooks_map: dict[str, dict[str, float]] = {}
        trade_delta_map: dict[str, dict[str, float]] = {}
        open_interest_map: dict[str, float] = {}

        tasks = [open_interest_task]
        if orderbooks_task:
            tasks.append(orderbooks_task)
        if trades_task:
            tasks.append(trades_task)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        idx = 0
        oi_result = results[idx]
        idx += 1
        if isinstance(oi_result, dict):
            open_interest_map = {symbol: float(v) for symbol, v in oi_result.items() if isinstance(v, (int, float))}

        if orderbooks_task:
            ob_result = results[idx]
            idx += 1
            if isinstance(ob_result, dict):
                orderbooks_map = {
                    symbol: value for symbol, value in ob_result.items() if isinstance(value, dict)
                }

        if trades_task:
            td_result = results[idx]
            if isinstance(td_result, dict):
                trade_delta_map = {
                    symbol: value for symbol, value in td_result.items() if isinstance(value, dict)
                }

        now = datetime.now(timezone.utc)
        spread_spike_detected = False
        spread_window_sec = max(60, self.settings.grandmother_orderflow_spread_avg_window_sec)
        price_window_sec = max(30, self.settings.grandmother_orderflow_price_window_sec)
        spread_spike_mult = max(1.1, self.settings.grandmother_orderflow_spread_spike_multiplier)
        spike_pct = max(0.0001, self.settings.grandmother_orderflow_price_spike_pct)
        wall_min_notional = max(0.0, self.settings.grandmother_l2_whale_wall_min_notional)
        wall_ratio = max(1.0, self.settings.grandmother_l2_whale_wall_ratio)

        for symbol, snapshot in payload.items():
            orderbook = dict(orderbooks_map.get(symbol, {}))
            trade_delta = dict(trade_delta_map.get(symbol, {}))

            ws_fresh = self._ws_orderflow_fresh(symbol, now)
            if ws_fresh:
                ws_book = self._ws_orderbook_metrics.get(symbol)
                if ws_book:
                    orderbook.update(ws_book)
                ws_trade = self._build_ws_trade_metrics(symbol, lookback, now)
                if ws_trade:
                    trade_delta.update(ws_trade)

            bid_volume_depth = max(0.0, float(orderbook.get("bid_volume_depth", orderbook.get("bid_volume_10", snapshot.bid_size or 0.0))))
            ask_volume_depth = max(0.0, float(orderbook.get("ask_volume_depth", orderbook.get("ask_volume_10", snapshot.ask_size or 0.0))))
            bid_notional_depth = max(0.0, float(orderbook.get("bid_notional_depth", 0.0)))
            ask_notional_depth = max(0.0, float(orderbook.get("ask_notional_depth", 0.0)))
            total_liquidity = bid_volume_depth + ask_volume_depth
            imbalance = (bid_volume_depth - ask_volume_depth) / total_liquidity if total_liquidity > 0 else 0.0
            total_notional = bid_notional_depth + ask_notional_depth
            imbalance_notional = (
                float(orderbook.get("imbalance_notional"))
                if "imbalance_notional" in orderbook
                else ((bid_notional_depth - ask_notional_depth) / total_notional if total_notional > 0 else 0.0)
            )

            delta_qty = float(trade_delta.get("delta_qty", 0.0))
            delta_notional = float(trade_delta.get("delta_notional", 0.0))
            buy_notional = float(trade_delta.get("buy_notional", max(0.0, delta_notional)))
            sell_notional = float(trade_delta.get("sell_notional", max(0.0, -delta_notional)))
            largest_print_notional = float(trade_delta.get("largest_print_notional", 0.0))
            large_buy_print_count = float(trade_delta.get("large_buy_print_count", 0.0))
            large_sell_print_count = float(trade_delta.get("large_sell_print_count", 0.0))
            tape_buy_sweep = large_buy_print_count >= max(1, self.settings.grandmother_tape_buy_sweep_min_count)
            tape_sell_sweep = large_sell_print_count >= max(1, self.settings.grandmother_tape_sell_sweep_min_count)

            whale_bid_notional = max(0.0, float(orderbook.get("whale_bid_notional", 0.0)))
            whale_ask_notional = max(0.0, float(orderbook.get("whale_ask_notional", 0.0)))
            whale_bid_wall = bool(
                whale_bid_notional >= wall_min_notional
                and whale_bid_notional >= max(1.0, ask_notional_depth) * wall_ratio
            )
            whale_ask_wall = bool(
                whale_ask_notional >= wall_min_notional
                and whale_ask_notional >= max(1.0, bid_notional_depth) * wall_ratio
            )

            spread = snapshot.spread
            if spread is None:
                spread = float(orderbook.get("spread", 0.0)) or None
            if spread is not None and spread > 0:
                history = self._orderflow_spread_history[symbol]
                history.append((now, spread))
                while history and (now - history[0][0]).total_seconds() > spread_window_sec:
                    history.popleft()

            spread_avg = 0.0
            spread_values = [value for _, value in self._orderflow_spread_history.get(symbol, deque())]
            if spread_values:
                spread_avg = statistics.fmean(spread_values)
            spread_spike = bool(
                spread is not None
                and spread_avg > 0
                and spread > (spread_avg * spread_spike_mult)
            )
            if spread_spike:
                spread_spike_detected = True

            price_history = self._orderflow_price_history[symbol]
            price_history.append((now, snapshot.price))
            while price_history and (now - price_history[0][0]).total_seconds() > price_window_sec:
                price_history.popleft()
            prices = [p for _, p in price_history]
            price_change = 0.0
            if len(prices) >= 2 and prices[0] > 0:
                price_change = (prices[-1] - prices[0]) / prices[0]
            stopped_falling = False
            if len(prices) >= 3:
                stopped_falling = prices[-2] < prices[-3] and prices[-1] >= prices[-2]
            accumulation = price_change < 0 and (delta_qty > 0 or tape_buy_sweep) and stopped_falling

            oi_current = open_interest_map.get(symbol)
            oi_prev = self._orderflow_last_open_interest.get(symbol)
            oi_change = None
            if oi_current is not None:
                if oi_prev is not None:
                    oi_change = oi_current - oi_prev
                self._orderflow_last_open_interest[symbol] = oi_current
            oi_available = oi_current is not None
            oi_required = self.settings.grandmother_orderflow_open_interest_required_for_exhaustion
            exhaustion = False
            if price_change >= spike_pct:
                if oi_available and oi_change is not None and oi_change < 0:
                    exhaustion = True
                elif oi_required:
                    exhaustion = True

            self.orderflow_metrics[symbol] = {
                "imbalance": imbalance,
                "imbalance_notional": imbalance_notional,
                "bid_volume_10": bid_volume_depth,
                "ask_volume_10": ask_volume_depth,
                "bid_volume_depth": bid_volume_depth,
                "ask_volume_depth": ask_volume_depth,
                "bid_notional_depth": bid_notional_depth,
                "ask_notional_depth": ask_notional_depth,
                "whale_bid_price": float(orderbook.get("whale_bid_price", 0.0)),
                "whale_bid_size": float(orderbook.get("whale_bid_size", 0.0)),
                "whale_bid_notional": whale_bid_notional,
                "whale_ask_price": float(orderbook.get("whale_ask_price", 0.0)),
                "whale_ask_size": float(orderbook.get("whale_ask_size", 0.0)),
                "whale_ask_notional": whale_ask_notional,
                "whale_bid_wall": whale_bid_wall,
                "whale_ask_wall": whale_ask_wall,
                "trade_delta_qty_60s": delta_qty,
                "trade_delta_notional_60s": delta_notional,
                "trade_buy_notional_60s": buy_notional,
                "trade_sell_notional_60s": sell_notional,
                "largest_print_notional_60s": largest_print_notional,
                "large_buy_print_count_60s": large_buy_print_count,
                "large_sell_print_count_60s": large_sell_print_count,
                "tape_buy_sweep": tape_buy_sweep,
                "tape_sell_sweep": tape_sell_sweep,
                "spread": float(spread or 0.0),
                "spread_avg_5m": spread_avg,
                "spread_spike": spread_spike,
                "price_change_window": price_change,
                "accumulation_signal": accumulation,
                "exhaustion_signal": exhaustion,
                "open_interest": oi_current,
                "open_interest_change": oi_change,
                "ws_orderflow_fresh": ws_fresh,
                "updated_ts_epoch": now.timestamp(),
            }

        if spread_spike_detected:
            halt_seconds = max(30, self.settings.grandmother_orderflow_spread_halt_sec)
            self.orderflow_spread_halt_until = (now + timedelta(seconds=halt_seconds)).isoformat()
            await self.ledger.log_event(
                "WARNING",
                "grandmother",
                "orderflow_spread_halt_triggered",
                {"halt_seconds": halt_seconds},
            )

    def _is_orderflow_spread_halt_active(self) -> bool:
        if not self.settings.grandmother_orderflow_enabled:
            return False
        if not self.orderflow_spread_halt_until:
            return False
        return self._parse_ts(self.orderflow_spread_halt_until) > datetime.now(timezone.utc)

    def _scanner_veto_status(self, intent: OrderIntent) -> str | None:
        if not self.settings.grandmother_scanner_enabled:
            return None
        if intent.side != "buy":
            return None
        if "/" in intent.symbol:
            return None
        symbol = intent.symbol.upper()
        if self.settings.grandmother_quality_filter_enabled and self.quality_watchlist and symbol not in self.quality_watchlist:
            return "dropped_quality_filter"
        if self.scanner_focus_symbols and symbol not in self.scanner_focus_symbols:
            return "dropped_scanner_not_focus"
        return None

    def _stock_sector_position_counts(self, positions_map: dict[str, float]) -> tuple[dict[str, int], set[str]]:
        counts: dict[str, int] = defaultdict(int)
        held_symbols: set[str] = set()
        for symbol, qty in positions_map.items():
            if qty <= 1e-9:
                continue
            if "/" in symbol:
                continue
            symbol_upper = symbol.upper()
            held_symbols.add(symbol_upper)
            counts[self._symbol_sector(symbol_upper)] += 1
        return dict(counts), held_symbols

    def _sector_diversity_veto_status(
        self,
        intent: OrderIntent,
        sector_counts: dict[str, int],
        held_symbols: set[str],
        pending_new_symbols: set[str],
    ) -> str | None:
        if not self.settings.grandmother_sector_diversity_enabled:
            return None
        if intent.side != "buy":
            return None
        if "/" in intent.symbol:
            return None

        symbol = intent.symbol.upper()
        if symbol in held_symbols or symbol in pending_new_symbols:
            return None

        sector = self._symbol_sector(symbol)
        max_per_sector = max(1, self.settings.grandmother_max_positions_per_sector)
        if sector_counts.get(sector, 0) >= max_per_sector:
            return "dropped_sector_concentration"

        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        pending_new_symbols.add(symbol)
        return None

    def _symbol_sector(self, symbol: str) -> str:
        return self.settings.grandmother_sector_map.get(symbol.upper(), f"symbol_{symbol.upper()}")

    def _orderflow_veto_status(self, intent: OrderIntent) -> str | None:
        if not self.settings.grandmother_orderflow_enabled:
            return None
        metrics = self.orderflow_metrics.get(intent.symbol)
        if not metrics:
            return None

        side = intent.side
        if side == "sell" and intent.reason in {
            "whale_ask_imbalance_exit",
            "atr_trailing_stop",
            "ladder_banker_33",
            "ladder_runner_33",
            "max_holding_age",
        }:
            return None

        imbalance = float(metrics.get("imbalance") or 0.0)
        delta_qty = float(metrics.get("trade_delta_qty_60s") or 0.0)
        delta_notional = float(metrics.get("trade_delta_notional_60s") or 0.0)
        accumulation = bool(metrics.get("accumulation_signal"))
        exhaustion = bool(metrics.get("exhaustion_signal"))
        whale_bid_wall = bool(metrics.get("whale_bid_wall"))
        whale_ask_wall = bool(metrics.get("whale_ask_wall"))
        tape_buy_sweep = bool(metrics.get("tape_buy_sweep"))
        tape_sell_sweep = bool(metrics.get("tape_sell_sweep"))
        l2_veto = self.settings.grandmother_l2_veto_enabled and self.settings.grandmother_l2_tape_enabled

        if side == "buy":
            allow_buy_override = l2_veto and whale_bid_wall and tape_buy_sweep
            if l2_veto and whale_ask_wall and not whale_bid_wall and not tape_buy_sweep and not accumulation:
                return "dropped_orderflow_whale_ask_wall"
            if imbalance < self.settings.grandmother_orderflow_imbalance_veto_threshold and not accumulation and not allow_buy_override:
                return "dropped_orderflow_liquidity_wall"
            if self.settings.grandmother_orderflow_min_delta_alignment and delta_qty <= 0 and not accumulation and not allow_buy_override:
                return "dropped_orderflow_delta_mismatch"
            if exhaustion:
                return "dropped_orderflow_exhaustion"

            required_pressure = (
                intent.qty
                * max(intent.observed_price, 0.0)
                * max(1.0, self.settings.grandmother_orderflow_buy_pressure_ratio)
            )
            if required_pressure > 0 and max(0.0, delta_notional) < required_pressure and not accumulation and not allow_buy_override:
                return "dropped_orderflow_buy_pressure"
        else:
            if l2_veto and whale_bid_wall and not tape_sell_sweep:
                return "dropped_orderflow_whale_bid_wall"
            if self.settings.grandmother_orderflow_min_delta_alignment and delta_qty >= 0:
                return "dropped_orderflow_delta_mismatch"
        return None

    async def _refresh_penalty_state(self, force: bool = False) -> None:
        now = datetime.now(timezone.utc)
        if (
            not force
            and self._last_penalty_refresh is not None
            and (now - self._last_penalty_refresh).total_seconds() < 60
        ):
            return
        lookback_hours = max(1, self.settings.grandmother_overseer_penalty_lookback_hours)
        start = now - timedelta(hours=lookback_hours)
        rows = await self._load_fill_rows(start, now)
        symbol_stats, _, _, _ = self._build_symbol_review(rows)
        self.overseer_symbol_penalty_mult = self._derive_symbol_penalty_multipliers(symbol_stats)
        self._last_penalty_refresh = now

    def _derive_symbol_penalty_multipliers(self, symbol_stats: dict[str, dict[str, float]]) -> dict[str, float]:
        multiplier_by_symbol: dict[str, float] = {}
        buffer_mult = max(1.0, self.settings.grandmother_overseer_penalty_buffer_mult)

        for symbol, stats in symbol_stats.items():
            trade_count = int(stats.get("trade_count", 0.0))
            if trade_count < 4:
                continue

            notional = max(1e-9, float(stats.get("notional", 0.0)))
            slippage_cost = max(0.0, float(stats.get("slippage_cost", 0.0)))
            commission_cost = max(0.0, float(stats.get("commission_cost", 0.0)))
            realized_pnl = float(stats.get("realized_pnl", 0.0))

            friction_ratio = (slippage_cost + commission_cost) / notional
            pnl_drag = 0.0
            if realized_pnl < 0:
                pnl_drag = min(2.0, abs(realized_pnl) / max(1e-6, notional * 0.01))

            penalty_units = (friction_ratio * 100.0) + pnl_drag
            multiplier = 1.0 + (penalty_units * buffer_mult)
            multiplier_by_symbol[symbol] = max(1.0, min(5.0, multiplier))

        return multiplier_by_symbol

    async def _estimate_vix_proxy(self) -> float:
        try:
            bars_map = await asyncio.to_thread(self.gateway.get_stock_bars, ["SPY"], 120)
            bars = bars_map.get("SPY", [])
            closes = [snap.price for snap in bars if snap.price > 0]
            if len(closes) < 10:
                return 0.0
            returns = []
            for idx in range(1, len(closes)):
                prev = closes[idx - 1]
                if prev <= 0:
                    continue
                returns.append((closes[idx] - prev) / prev)
            if len(returns) < 5:
                return 0.0
            minute_std = statistics.pstdev(returns)
            annualized = minute_std * math.sqrt(252 * 390) * 100.0
            return float(max(0.0, annualized))
        except Exception:
            return 0.0

    @staticmethod
    def _top_symbol(symbol_stats: dict[str, dict[str, float]], reverse: bool) -> tuple[str, float]:
        if not symbol_stats:
            return "n/a", 0.0
        symbol, stats = sorted(
            symbol_stats.items(),
            key=lambda item: item[1].get("realized_pnl", 0.0),
            reverse=reverse,
        )[0]
        return symbol, float(stats.get("realized_pnl", 0.0))

    async def _heartbeat_loop(self) -> None:
        interval = max(10, self.settings.grandmother_heartbeat_interval_sec)
        while self.running:
            await asyncio.sleep(interval)
            if self.unauthorized_halt:
                continue
            for mother_id, mother in self.mothers.items():
                ok = mother.heartbeat()
                await self.ledger.save_heartbeat(mother_id, "ok" if ok else "failed")
                if not ok and self.settings.grandmother_restart_mother_on_heartbeat_fail:
                    await self._restart_mother(mother_id)

    async def _restart_mother(self, mother_id: str) -> None:
        mother = self.mothers[mother_id]
        await mother.stop()
        restored = await self._restore_children(mother_id)
        await mother.start(restored_children=restored)
        await self.ledger.log_event("WARNING", "grandmother", "mother_restarted", {"mother_id": mother_id})

    async def _schedule_loop(self) -> None:
        last_nightly_date: str | None = None
        last_weekly_date: str | None = None
        nightly_h, nightly_m = self._parse_hhmm(self.settings.mother_nightly_selection_time)
        weekly_h, weekly_m = self._parse_hhmm(self.settings.mother_weekly_crossover_time)
        weekly_day = self.settings.mother_weekly_crossover_day.upper()

        while self.running:
            now = datetime.now(timezone.utc)
            now_date = now.date().isoformat()
            if self.unauthorized_halt:
                await asyncio.sleep(20)
                continue

            if now.hour == nightly_h and now.minute == nightly_m and last_nightly_date != now_date:
                for mother in self.mothers.values():
                    await mother.evolve()
                await self.ledger.log_event("INFO", "grandmother", "nightly_evolution_done", {"date": now_date})
                last_nightly_date = now_date

            if (
                now.strftime("%A").upper() == weekly_day
                and now.hour == weekly_h
                and now.minute == weekly_m
                and last_weekly_date != now_date
                and not (now.hour == nightly_h and now.minute == nightly_m and last_nightly_date == now_date)
            ):
                for mother in self.mothers.values():
                    await mother.evolve()
                await self.ledger.log_event("INFO", "grandmother", "weekly_evolution_done", {"date": now_date})
                last_weekly_date = now_date

            await asyncio.sleep(20)

    async def _check_profit_milestones(self, account: AccountView) -> None:
        if self.initial_equity <= 0:
            return
        pnl_pct = (account.equity - self.initial_equity) / self.initial_equity
        milestone = int(pnl_pct // 0.05)
        if milestone <= self.last_profit_milestone_sent:
            return
        for level in range(self.last_profit_milestone_sent + 1, milestone + 1):
            pct = level * 5
            await notify_mac(
                title="TradingAI Profit Milestone",
                subtitle="Grandmother Report",
                message=f"Portfolio reached +{pct}% profit. Equity: {account.equity:,.2f}",
            )
            await self.ledger.log_event(
                "INFO",
                "grandmother",
                "profit_milestone_hit",
                {"milestone_pct": pct, "equity": account.equity},
            )
        self.last_profit_milestone_sent = milestone
        await self.ledger.set_state(
            "profit_milestone_state",
            {"last_milestone": self.last_profit_milestone_sent, "updated_at": datetime.now(timezone.utc).isoformat()},
        )

    def _is_symbol_locked(self, symbol: str) -> bool:
        expiry = self.overseer_symbol_locks.get(symbol)
        if not expiry:
            return False
        return self._parse_ts(expiry) > datetime.now(timezone.utc)

    def _is_cooldown_active(self, symbol: str) -> bool:
        if self.overseer_trade_cooldown_sec <= 0:
            return False
        ts = self.overseer_last_trade_ts_by_symbol.get(symbol)
        if not ts:
            return False
        last_trade = self._parse_ts(ts)
        delta = (datetime.now(timezone.utc) - last_trade).total_seconds()
        return delta < self.overseer_trade_cooldown_sec

    def _is_global_cooldown_active(self) -> bool:
        cooldown = max(0, self.settings.grandmother_overseer_global_trade_cooldown_sec)
        if cooldown <= 0 or not self.overseer_last_global_trade_ts:
            return False
        last_trade = self._parse_ts(self.overseer_last_global_trade_ts)
        delta = (datetime.now(timezone.utc) - last_trade).total_seconds()
        return delta < cooldown

    def _is_aggressive_blacklisted(self, intent: OrderIntent) -> bool:
        if intent.side != "buy" or intent.mother_id not in {"speedy", "chaos"}:
            return False
        expiry = self.overseer_aggressive_blacklist.get(intent.symbol)
        if not expiry:
            return False
        return self._parse_ts(expiry) > datetime.now(timezone.utc)

    def _passes_edge_filter(self, intent: OrderIntent) -> bool:
        if intent.side != "buy":
            return True
        mother = self.mothers.get(intent.mother_id)
        if mother is None:
            return True
        child = mother.children.get(intent.child_id)
        if child is None:
            return True
        px = max(0.0, intent.observed_price)
        if px <= 0:
            return False

        expected_reward_per_unit = px * max(0.0, child.dna.take_profit)
        round_trip_cost_per_unit = px * (
            (self.settings.paper_slippage_pct * 2) + (self.settings.paper_virtual_commission_pct * 2)
        )
        base_ratio = max(1.0, self.settings.grandmother_overseer_spread_reward_ratio)
        penalty_mult = max(1.0, self.overseer_symbol_penalty_mult.get(intent.symbol, 1.0))
        required_reward = round_trip_cost_per_unit * (base_ratio * penalty_mult)
        return expected_reward_per_unit >= required_reward

    async def _count_orders_since(self, start_ts: datetime) -> int:
        query = "SELECT COUNT(*) FROM orders WHERE ts >= ?"
        async with aiosqlite.connect(self.settings.hive_state_db_path) as db:
            cursor = await db.execute(query, (start_ts.isoformat(),))
            row = await cursor.fetchone()
            return int(row[0] if row else 0)

    def _clamp_trade_cooldown(self, value: int) -> int:
        floor = max(0, self.settings.grandmother_overseer_min_trade_cooldown_sec)
        ceil = max(floor, self.settings.grandmother_overseer_max_trade_cooldown_sec)
        return max(floor, min(ceil, int(value)))

    @staticmethod
    def _parse_ts(value: str | None) -> datetime:
        if not value:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return datetime.fromtimestamp(0, tz=timezone.utc)

    def _request_restart(self, reason: str) -> None:
        if self.unauthorized_halt:
            return
        self.restart_requested = True
        self.restart_reason = reason

    async def _trigger_unauthorized_shutdown(self, exc: Exception) -> None:
        if self.unauthorized_halt:
            return
        self.unauthorized_halt = True
        self.trading_frozen = True
        self.restart_requested = False
        self.restart_reason = ""

        for mother in self.mothers.values():
            await mother.stop()

        await self.ledger.log_event(
            "CRITICAL",
            "grandmother",
            "alpaca_unauthorized_stop",
            {"error": str(exc)},
        )
        await notify_mac(
            title="TradingAI Safety Halt",
            subtitle="Alpaca Unauthorized (401)",
            message="Children stopped. Update API keys and restart runtime.",
        )

    @staticmethod
    def _is_unauthorized_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return "401" in text or "unauthorized" in text

    @staticmethod
    def _is_connection_error(exc: Exception) -> bool:
        text = str(exc).lower()
        needles = (
            "connectionerror",
            "name resolution",
            "temporarily unavailable",
            "max retries exceeded",
            "timed out",
            "network is unreachable",
            "connection reset",
            "remoteprotocolerror",
        )
        return any(token in text for token in needles)

    async def _get_account(self) -> AccountView:
        return await asyncio.to_thread(self.gateway.get_account)

    @staticmethod
    def _parse_hhmm(value: str) -> tuple[int, int]:
        h, m = value.split(":", maxsplit=1)
        return int(h), int(m)
