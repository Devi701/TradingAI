from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4


Side = Literal["buy", "sell"]
AssetClass = Literal["stock", "crypto"]
RiskMode = Literal["risk_on", "risk_off", "neutral"]



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Snapshot:
    symbol: str
    price: float
    timestamp: datetime
    volume: float = 0.0
    bid_price: float | None = None
    ask_price: float | None = None
    bid_size: float = 0.0
    ask_size: float = 0.0
    spread: float | None = None
    asset_class: AssetClass = "stock"
    delayed_label: str | None = None


@dataclass
class ChildDNA:
    ticker: str
    entry_signal: str
    params: list[float]
    stop_loss: float
    take_profit: float
    max_position_pct: float
    risk_bias: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "entry_signal": self.entry_signal,
            "params": self.params,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "max_position_pct": self.max_position_pct,
            "risk_bias": self.risk_bias,
        }


@dataclass
class OrderIntent:
    child_id: str
    mother_id: str
    symbol: str
    side: Side
    qty: float
    observed_price: float
    confidence: float
    reason: str = ""
    indicators: dict[str, float] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=utc_now)


@dataclass
class VirtualFill:
    intent_id: str
    symbol: str
    side: Side
    qty: float
    observed_price: float
    effective_price: float
    commission: float
    slippage: float
    latency_ms: int
    filled_at: datetime = field(default_factory=utc_now)


@dataclass
class ChildPerformance:
    pnl: float = 0.0
    trade_count: int = 0
    wins: int = 0
    losses: int = 0
    gross_pnl: float = 0.0
    fee_paid: float = 0.0
    returns: list[float] = field(default_factory=list)
    equity_curve: list[float] = field(default_factory=list)

    def register_return(self, value: float, fee: float = 0.0) -> None:
        self.returns.append(value)
        self.trade_count += 1
        self.pnl += value
        self.gross_pnl += value + fee
        self.fee_paid += fee
        if value >= 0:
            self.wins += 1
        else:
            self.losses += 1
        current_equity = (self.equity_curve[-1] if self.equity_curve else 0.0) + value
        self.equity_curve.append(current_equity)


@dataclass
class PositionState:
    qty: float = 0.0
    entry_price: float = 0.0


@dataclass
class MacroRegime:
    score: float
    mode: RiskMode
    summary: str
    headline_count: int
    fetched_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "mode": self.mode,
            "summary": self.summary,
            "headline_count": self.headline_count,
            "fetched_at": self.fetched_at.isoformat(),
        }
