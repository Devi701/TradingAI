from __future__ import annotations

from math import sqrt



def sma(series: list[float], period: int) -> float | None:
    if period <= 0 or len(series) < period:
        return None
    return sum(series[-period:]) / period



def ema(series: list[float], period: int) -> float | None:
    if period <= 0 or len(series) < period:
        return None
    k = 2 / (period + 1)
    current = sum(series[:period]) / period
    for value in series[period:]:
        current = value * k + current * (1 - k)
    return current



def rsi(series: list[float], period: int = 14) -> float | None:
    if len(series) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        delta = series[i] - series[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses += abs(delta)
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100 - (100 / (1 + rs))



def macd(series: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float | None, float | None]:
    if len(series) < slow + signal:
        return None, None
    fast_ema = ema(series, fast)
    slow_ema = ema(series, slow)
    if fast_ema is None or slow_ema is None:
        return None, None
    macd_line = fast_ema - slow_ema

    # Approximate signal line from history of MACD values.
    macd_values: list[float] = []
    for i in range(slow + signal, len(series) + 1):
        f = ema(series[:i], fast)
        s = ema(series[:i], slow)
        if f is None or s is None:
            continue
        macd_values.append(f - s)
    if len(macd_values) < signal:
        return macd_line, None
    signal_line = ema(macd_values, signal)
    return macd_line, signal_line



def stddev(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return sqrt(variance)



def zscore(values: list[float], period: int = 20) -> float | None:
    if len(values) < period:
        return None
    window = values[-period:]
    mean = sum(window) / period
    sd = stddev(window)
    if sd == 0:
        return 0.0
    return (values[-1] - mean) / sd



def momentum(values: list[float], period: int = 10) -> float | None:
    if len(values) < period + 1:
        return None
    prior = values[-(period + 1)]
    if prior == 0:
        return None
    return (values[-1] - prior) / prior



def atr(values: list[float], period: int = 14) -> float | None:
    # Close-to-close ATR approximation (high/low is not available in live snapshots).
    if period <= 0 or len(values) < period + 1:
        return None
    true_ranges = [abs(values[i] - values[i - 1]) for i in range(len(values) - period, len(values))]
    if not true_ranges:
        return None
    return sum(true_ranges) / len(true_ranges)


def rolling_high(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return max(values[-period:])



def rolling_low(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return min(values[-period:])



def sharpe_ratio(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    avg = sum(returns) / len(returns)
    vol = stddev(returns)
    if vol == 0:
        return 0.0
    return avg / vol



def max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    worst = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak <= 0:
            continue
        dd = (peak - value) / abs(peak)
        worst = max(worst, dd)
    return worst
