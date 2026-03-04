from __future__ import annotations

from collections import deque
from time import monotonic
from typing import Callable
from uuid import uuid4

from .config import Settings
from .data_bus import MemoryDataBus
from .indicators import atr, ema, macd, momentum, rolling_high, rsi, sma, stddev, zscore
from .models import ChildDNA, ChildPerformance, OrderIntent, PositionState, Snapshot, VirtualFill


class ChildRuntime:
    def __init__(
        self,
        settings: Settings,
        mother_id: str,
        dna: ChildDNA,
        submit_intent: Callable[[OrderIntent], object],
        allowance_getter: Callable[[], float],
        child_id: str | None = None,
    ) -> None:
        self.settings = settings
        self.id = child_id or f"child-{mother_id}-{uuid4().hex[:8]}"
        self.mother_id = mother_id
        self.dna = dna
        self._submit_intent = submit_intent
        self._allowance_getter = allowance_getter

        self.performance = ChildPerformance()
        self.position = PositionState()
        self.prices: deque[float] = deque(maxlen=600)
        self.volumes: deque[float] = deque(maxlen=600)
        self.last_signal_monotonic = 0.0
        self.last_version = 0
        self.active = True
        self.awaiting_fill = False
        self.awaiting_fill_since = 0.0
        self.position_age_bars = 0
        self.position_highest_price = 0.0
        self.position_initial_qty = 0.0
        self.ladder_stage = 0
        self._ask_imbalance_since_monotonic: float | None = None
        self._last_l2_ask_bid_ratio = 0.0

    async def run(self, bus: MemoryDataBus, frozen_getter: Callable[[], bool]) -> None:
        while self.active:
            try:
                envelope = await bus.wait_for_update(self.last_version, timeout=5.0)
                self.last_version = envelope.version
            except TimeoutError:
                continue

            snapshot = envelope.snapshots.get(self.dna.ticker)
            if snapshot is None:
                continue

            self._push_snapshot(snapshot)
            if frozen_getter():
                continue
            if self.awaiting_fill and monotonic() - self.awaiting_fill_since > max(5, self.settings.child_intent_timeout_sec):
                self.awaiting_fill = False
                self.awaiting_fill_since = 0.0
            if self.awaiting_fill:
                continue

            intent = self._evaluate(snapshot)
            if intent is None:
                continue

            accepted = await self._submit_intent(intent)
            if accepted:
                self.awaiting_fill = True
                self.awaiting_fill_since = monotonic()

    def stop(self) -> None:
        self.active = False

    def on_virtual_fill(self, fill: VirtualFill) -> float:
        self.awaiting_fill = False
        self.awaiting_fill_since = 0.0

        if fill.side == "buy":
            prev_qty = self.position.qty
            total_cost = (self.position.entry_price * self.position.qty) + (fill.effective_price * fill.qty) + fill.commission
            self.position.qty += fill.qty
            if self.position.qty > 0:
                self.position.entry_price = total_cost / self.position.qty
            else:
                self.position.entry_price = 0.0
            if prev_qty <= 1e-9 and self.position.qty > 0:
                self.position_highest_price = fill.effective_price
                self.position_initial_qty = self.position.qty
                self.ladder_stage = 0
            else:
                self.position_highest_price = max(self.position_highest_price, fill.effective_price)
                self.position_initial_qty = max(self.position_initial_qty, self.position.qty)
            self.performance.fee_paid += fill.commission
            return 0.0

        # Sell path: realize pnl on closed quantity.
        close_qty = min(self.position.qty, fill.qty)
        if close_qty <= 0:
            self.performance.register_return(-fill.commission, fee=fill.commission)
            return -fill.commission

        realized = (fill.effective_price - self.position.entry_price) * close_qty - fill.commission
        self.position.qty -= close_qty
        if self.position.qty <= 1e-9:
            self.position.qty = 0.0
            self.position.entry_price = 0.0
            self.position_highest_price = 0.0
            self.position_initial_qty = 0.0
            self.ladder_stage = 0
            self._ask_imbalance_since_monotonic = None
            self._last_l2_ask_bid_ratio = 0.0
        else:
            self._refresh_ladder_stage()
        self.performance.register_return(realized, fee=fill.commission)
        return realized

    def _push_snapshot(self, snapshot: Snapshot) -> None:
        now = monotonic()
        self.prices.append(snapshot.price)
        self.volumes.append(snapshot.volume)
        if self.position.qty > 0:
            self.position_age_bars += 1
            self.position_highest_price = max(self.position_highest_price, snapshot.price)
            bid_size = max(0.0, float(snapshot.bid_size))
            ask_size = max(0.0, float(snapshot.ask_size))
            if ask_size <= 0:
                self._last_l2_ask_bid_ratio = 0.0
                self._ask_imbalance_since_monotonic = None
            else:
                self._last_l2_ask_bid_ratio = ask_size / max(bid_size, 1e-9)
                threshold = max(1.0, self.settings.child_l2_ask_bid_ratio_threshold)
                if self._last_l2_ask_bid_ratio >= threshold:
                    if self._ask_imbalance_since_monotonic is None:
                        self._ask_imbalance_since_monotonic = now
                else:
                    self._ask_imbalance_since_monotonic = None
        else:
            self.position_age_bars = 0
            self.position_highest_price = 0.0
            self.position_initial_qty = 0.0
            self.ladder_stage = 0
            self._ask_imbalance_since_monotonic = None
            self._last_l2_ask_bid_ratio = 0.0

    def _evaluate(self, snapshot: Snapshot) -> OrderIntent | None:
        now = monotonic()
        if now - self.last_signal_monotonic < self.settings.child_signal_cooldown_sec:
            return None

        side, confidence, reason, indicators = self._signal(snapshot)
        if side is None:
            return None

        # Position-aware routing.
        if side == "sell" and self.position.qty <= 0:
            return None

        qty = self._position_size(snapshot.price, confidence, side, indicators)
        if qty <= 0:
            return None

        self.last_signal_monotonic = now
        return OrderIntent(
            child_id=self.id,
            mother_id=self.mother_id,
            symbol=self.dna.ticker,
            side=side,
            qty=qty,
            observed_price=snapshot.price,
            confidence=confidence,
            reason=reason,
            indicators=indicators,
        )

    def _signal(self, snapshot: Snapshot) -> tuple[str | None, float, str, dict[str, float]]:
        data = list(self.prices)
        signal = self.dna.entry_signal.upper()
        indicators: dict[str, float] = {}

        if len(data) < 40:
            return None, 0.0, "insufficient_history", indicators

        risk_side, risk_conf, risk_reason, risk_indicators = self._risk_exit_signal(snapshot, data)
        if risk_side is not None:
            return risk_side, risk_conf, risk_reason, risk_indicators

        if signal == "EMA_CROSS":
            return self._signal_ema_cross(data)
        if signal == "RSI":
            return self._signal_rsi(data)
        if signal == "MACD":
            return self._signal_macd(data)
        if signal == "VOLUME_SPIKE":
            return self._signal_volume_breakout(data)
        if signal == "HYBRID":
            return self._signal_hybrid(data)
        if signal == "ZSCORE_REVERT":
            return self._signal_zscore(data)
        if signal == "BREAKOUT":
            side, conf, reason, indicators = self._signal_breakout(data)
        else:
            side, conf, reason, indicators = self._signal_ema_cross(data)

        if side is None:
            return self._signal_exploration(data)
        return side, conf, reason, indicators

    def _risk_exit_signal(self, snapshot: Snapshot, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        if self.position.qty <= 0 or self.position.entry_price <= 0:
            return None, 0.0, "no_position", {}

        now = monotonic()
        entry_price = max(self.position.entry_price, 1e-9)
        profit_pct = (snapshot.price - entry_price) / entry_price

        imbalance_hold = max(1, self.settings.child_l2_ask_bid_hold_sec)
        imbalance_seconds = 0.0
        if self._ask_imbalance_since_monotonic is not None:
            imbalance_seconds = max(0.0, now - self._ask_imbalance_since_monotonic)
        if (
            self._last_l2_ask_bid_ratio >= max(1.0, self.settings.child_l2_ask_bid_ratio_threshold)
            and imbalance_seconds >= imbalance_hold
        ):
            return "sell", 1.0, "whale_ask_imbalance_exit", {
                "ask_bid_ratio": self._last_l2_ask_bid_ratio,
                "imbalance_seconds": imbalance_seconds,
                "sell_qty": self.position.qty,
                "sell_fraction": 1.0,
            }

        atr_val = self._current_atr(prices, snapshot.price)
        stop_mult = max(0.1, self.settings.child_atr_stop_multiplier)
        self.position_highest_price = max(self.position_highest_price, snapshot.price, self.position.entry_price)
        floor_stop = self.position.entry_price - (stop_mult * atr_val)
        trail_stop = self.position_highest_price - (stop_mult * atr_val)
        stop_price = max(floor_stop, trail_stop)
        if self.ladder_stage >= 1:
            stop_price = max(stop_price, self.position.entry_price)

        if snapshot.price <= stop_price:
            return "sell", 1.0, "atr_trailing_stop", {
                "atr": atr_val,
                "stop_price": stop_price,
                "highest_price": self.position_highest_price,
                "profit_pct": profit_pct,
                "sell_qty": self.position.qty,
                "sell_fraction": 1.0,
            }

        first_target = max(0.0, self.settings.child_ladder_first_target_pct)
        second_target = max(first_target, self.settings.child_ladder_second_target_pct)

        if self.ladder_stage < 1 and profit_pct >= first_target:
            sell_qty = self._ladder_sell_qty(self.settings.child_ladder_first_fraction)
            if sell_qty > 1e-9:
                return "sell", 1.0, "ladder_banker_33", {
                    "profit_pct": profit_pct,
                    "target_pct": first_target,
                    "sell_qty": sell_qty,
                    "sell_fraction": min(1.0, max(0.0, self.settings.child_ladder_first_fraction)),
                    "atr": atr_val,
                    "break_even_armed": 1.0,
                }

        if self.ladder_stage < 2 and profit_pct >= second_target:
            sell_qty = self._ladder_sell_qty(self.settings.child_ladder_second_fraction)
            if sell_qty > 1e-9:
                return "sell", 1.0, "ladder_runner_33", {
                    "profit_pct": profit_pct,
                    "target_pct": second_target,
                    "sell_qty": sell_qty,
                    "sell_fraction": min(1.0, max(0.0, self.settings.child_ladder_second_fraction)),
                    "atr": atr_val,
                }

        if self.position_age_bars >= max(20, self.settings.child_max_holding_bars):
            return "sell", 0.65, "max_holding_age", {
                "bars": float(self.position_age_bars),
                "sell_qty": self.position.qty,
                "sell_fraction": 1.0,
            }

        return None, 0.0, "risk_hold", {}

    def _current_atr(self, prices: list[float], fallback_price: float) -> float:
        period = max(2, self.settings.child_atr_period)
        atr_val = atr(prices, period)
        floor_pct = max(0.0001, self.settings.child_atr_floor_pct)
        floor_abs = max(1e-9, fallback_price * floor_pct)
        if atr_val is None:
            return floor_abs
        return max(floor_abs, atr_val)

    def _ladder_sell_qty(self, fraction: float) -> float:
        frac = min(1.0, max(0.0, fraction))
        if frac <= 0:
            return 0.0
        base_qty = self.position_initial_qty if self.position_initial_qty > 1e-9 else self.position.qty
        return min(self.position.qty, base_qty * frac)

    def _refresh_ladder_stage(self) -> None:
        if self.position_initial_qty <= 1e-9:
            self.ladder_stage = 0
            return
        sold_qty = max(0.0, self.position_initial_qty - self.position.qty)
        first_qty = self.position_initial_qty * min(1.0, max(0.0, self.settings.child_ladder_first_fraction))
        second_qty = self.position_initial_qty * min(
            1.0,
            max(0.0, self.settings.child_ladder_first_fraction + self.settings.child_ladder_second_fraction),
        )
        tolerance = max(1e-6, self.position_initial_qty * 0.005)
        if sold_qty + tolerance >= second_qty:
            self.ladder_stage = 2
        elif sold_qty + tolerance >= first_qty:
            self.ladder_stage = 1
        else:
            self.ladder_stage = 0

    def _signal_ema_cross(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        fast = int(self.dna.params[0]) if self.dna.params else 9
        slow = int(self.dna.params[1]) if len(self.dna.params) > 1 else 26
        fast_ema = ema(prices, fast)
        slow_ema = ema(prices, slow)
        if fast_ema is None or slow_ema is None:
            return None, 0.0, "ema_unavailable", {}

        spread = (fast_ema - slow_ema) / max(abs(slow_ema), 1e-9)
        indicators = {"ema_fast": fast_ema, "ema_slow": slow_ema, "spread": spread}
        conf = min(1.0, abs(spread) * 50)

        if spread > 0 and self.position.qty <= 0:
            return "buy", conf, "ema_bull_cross", indicators
        if spread < 0 and self.position.qty > 0:
            return "sell", conf, "ema_bear_cross", indicators
        return None, 0.0, "ema_no_cross", indicators

    def _signal_rsi(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        period = int(self.dna.params[0]) if self.dna.params else 14
        oversold = float(self.dna.params[1]) if len(self.dna.params) > 1 else 30.0
        overbought = float(self.dna.params[2]) if len(self.dna.params) > 2 else 70.0
        rsi_val = rsi(prices, period)
        if rsi_val is None:
            return None, 0.0, "rsi_unavailable", {}

        indicators = {"rsi": rsi_val}
        if rsi_val <= oversold and self.position.qty <= 0:
            conf = min(1.0, (oversold - rsi_val) / max(oversold, 1))
            return "buy", max(0.2, conf), "rsi_oversold", indicators
        if rsi_val >= overbought and self.position.qty > 0:
            conf = min(1.0, (rsi_val - overbought) / max(100 - overbought, 1))
            return "sell", max(0.2, conf), "rsi_overbought", indicators
        return None, 0.0, "rsi_hold", indicators

    def _signal_macd(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        macd_line, signal_line = macd(prices)
        if macd_line is None or signal_line is None:
            return None, 0.0, "macd_unavailable", {}
        diff = macd_line - signal_line
        indicators = {"macd": macd_line, "signal": signal_line, "diff": diff}
        conf = min(1.0, abs(diff) * 30)
        if diff > 0 and self.position.qty <= 0:
            return "buy", max(0.2, conf), "macd_bullish", indicators
        if diff < 0 and self.position.qty > 0:
            return "sell", max(0.2, conf), "macd_bearish", indicators
        return None, 0.0, "macd_hold", indicators

    def _signal_volume_breakout(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        if len(self.volumes) < 30:
            return None, 0.0, "volume_unavailable", {}
        current_volume = self.volumes[-1]
        baseline = sum(list(self.volumes)[-21:-1]) / 20
        lookback = int(self.dna.params[0]) if self.dna.params else 20
        breakout = rolling_high(prices[:-1], lookback)
        if breakout is None:
            return None, 0.0, "breakout_unavailable", {}

        spike_ratio = current_volume / max(baseline, 1e-9)
        breakout_strength = (prices[-1] - breakout) / max(breakout, 1e-9)
        indicators = {
            "volume_spike": spike_ratio,
            "breakout_strength": breakout_strength,
        }
        if spike_ratio > 1.8 and breakout_strength > 0 and self.position.qty <= 0:
            conf = min(1.0, (spike_ratio - 1.0) * 0.35 + breakout_strength * 20)
            return "buy", max(0.2, conf), "volume_breakout", indicators

        if self.position.qty > 0 and breakout_strength < -0.01:
            conf = min(1.0, abs(breakout_strength) * 20)
            return "sell", max(0.2, conf), "breakout_failure", indicators

        return None, 0.0, "volume_breakout_hold", indicators

    def _signal_hybrid(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        fast = ema(prices, 12)
        slow = ema(prices, 26)
        rsi_val = rsi(prices, 14)
        mom = momentum(prices, 8)
        if fast is None or slow is None or rsi_val is None or mom is None:
            return None, 0.0, "hybrid_unavailable", {}

        trend = (fast - slow) / max(abs(slow), 1e-9)
        indicators = {"trend": trend, "rsi": rsi_val, "momentum": mom}

        if trend > 0 and rsi_val < 45 and mom > 0 and self.position.qty <= 0:
            conf = min(1.0, trend * 80 + (45 - rsi_val) / 45 + mom * 50)
            return "buy", max(0.25, conf), "hybrid_entry", indicators

        if self.position.qty > 0 and (trend < 0 or rsi_val > 65 or mom < -0.005):
            conf = min(1.0, abs(trend) * 80 + max(0.0, (rsi_val - 65) / 35) + abs(min(mom, 0.0)) * 100)
            return "sell", max(0.25, conf), "hybrid_exit", indicators

        return None, 0.0, "hybrid_hold", indicators

    def _signal_zscore(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        period = int(self.dna.params[0]) if self.dna.params else 20
        threshold = float(self.dna.params[1]) if len(self.dna.params) > 1 else 1.5
        z_val = zscore(prices, period)
        mean_val = sma(prices, period)
        if z_val is None or mean_val is None:
            return None, 0.0, "zscore_unavailable", {}

        indicators = {"zscore": z_val, "mean": mean_val}
        if z_val <= -threshold and self.position.qty <= 0:
            conf = min(1.0, abs(z_val) / max(threshold, 0.01))
            return "buy", max(0.2, conf), "zscore_revert_buy", indicators
        if self.position.qty > 0 and z_val >= 0:
            conf = min(1.0, abs(z_val))
            return "sell", max(0.2, conf), "zscore_revert_exit", indicators
        return None, 0.0, "zscore_hold", indicators

    def _signal_breakout(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        lookback = int(self.dna.params[0]) if self.dna.params else 20
        breakout_high = rolling_high(prices[:-1], lookback)
        trailing_sma = sma(prices, min(lookback, 50))
        if breakout_high is None or trailing_sma is None:
            return None, 0.0, "breakout_unavailable", {}

        breakout_strength = (prices[-1] - breakout_high) / max(breakout_high, 1e-9)
        below_trend = prices[-1] < trailing_sma
        indicators = {"breakout_strength": breakout_strength, "sma": trailing_sma}

        if breakout_strength > 0 and self.position.qty <= 0:
            conf = min(1.0, breakout_strength * 50)
            return "buy", max(0.2, conf), "breakout_up", indicators

        if self.position.qty > 0 and below_trend:
            conf = min(1.0, abs((prices[-1] - trailing_sma) / max(trailing_sma, 1e-9)) * 20)
            return "sell", max(0.2, conf), "trend_break", indicators

        return None, 0.0, "breakout_hold", indicators

    def _signal_exploration(self, prices: list[float]) -> tuple[str | None, float, str, dict[str, float]]:
        mom = momentum(prices, 5)
        mean_20 = sma(prices, 20)
        z_val = zscore(prices, 20)
        if mom is None or mean_20 is None or z_val is None:
            return None, 0.0, "explore_unavailable", {}

        px = prices[-1]
        indicators = {"mom5": mom, "mean20": mean_20, "zscore": z_val}
        if self.position.qty <= 0 and mom > 0.0015 and px > mean_20:
            return "buy", 0.22, "explore_trend_probe", indicators
        if self.position.qty <= 0 and z_val < -1.8:
            return "buy", 0.24, "explore_revert_probe", indicators
        if self.position.qty > 0 and mom < -0.0025:
            return "sell", 0.3, "explore_momentum_exit", indicators
        return None, 0.0, "explore_hold", indicators

    def _position_size(self, price: float, confidence: float, side: str, indicators: dict[str, float]) -> float:
        if side == "sell":
            explicit_qty = max(0.0, float(indicators.get("sell_qty", 0.0)))
            if explicit_qty > 0:
                return min(self.position.qty, explicit_qty)
            fraction = min(1.0, max(0.0, float(indicators.get("sell_fraction", 1.0))))
            return min(self.position.qty, self.position.qty * fraction)

        allowance = max(float(self._allowance_getter()), 0.0)
        if allowance <= 0 or price <= 0:
            return 0.0

        returns = self._recent_returns(30)
        vol = stddev(returns)
        target_vol = 0.01
        vol_scale = min(1.5, max(0.35, target_vol / max(vol, 0.0025)))

        notional_cap = allowance * min(self.dna.max_position_pct, self.settings.child_max_position_pct)
        risk_adjusted = notional_cap * max(0.1, confidence) * max(0.25, self.dna.risk_bias) * vol_scale
        qty = risk_adjusted / price
        return max(0.0, qty)

    def _recent_returns(self, n: int) -> list[float]:
        prices = list(self.prices)
        if len(prices) < 3:
            return []
        out: list[float] = []
        start = max(1, len(prices) - n)
        for i in range(start, len(prices)):
            prev = prices[i - 1]
            if prev == 0:
                continue
            out.append((prices[i] - prev) / prev)
        return out
