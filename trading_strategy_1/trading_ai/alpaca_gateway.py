from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import httpx
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

from .config import Settings
from .models import Snapshot


@dataclass
class AccountView:
    account_number: str
    equity: float
    cash: float
    buying_power: float


class AlpacaGateway:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._compact_crypto_map = {symbol.replace("/", ""): symbol for symbol in settings.all_crypto_symbols}
        self._crypto_orderbook_supported: bool | None = None
        self.trading_client = TradingClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key,
            paper=True,
            url_override=settings.alpaca_base_url,
        )

    def get_account(self) -> AccountView:
        account = self.trading_client.get_account()
        return AccountView(
            account_number=account.account_number,
            equity=float(account.equity),
            cash=float(account.cash),
            buying_power=float(account.buying_power),
        )

    def close_all_positions(self) -> Any:
        return self.trading_client.close_all_positions(cancel_orders=True)

    def get_positions_map(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for pos in self.trading_client.get_all_positions():
            symbol = str(pos.symbol)
            qty = abs(float(pos.qty))
            canonical = self._canonical_symbol(symbol)
            out[canonical] = qty
        return out

    def submit_market_order(self, symbol: str, side: str, qty: float) -> dict[str, Any]:
        trade_symbol = symbol
        tif = TimeInForce.GTC if "/" in symbol else TimeInForce.DAY
        req = MarketOrderRequest(
            symbol=trade_symbol,
            qty=round(qty, 6),
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=tif,
            client_order_id=f"hive-{uuid4()}",
        )
        order = self.trading_client.submit_order(order_data=req)
        if hasattr(order, "model_dump"):
            return order.model_dump()
        return order.__dict__

    def get_stock_snapshots(self, symbols: list[str]) -> dict[str, Snapshot]:
        if not symbols:
            return {}
        url = f"{self.settings.alpaca_data_url.rstrip('/')}/v2/stocks/snapshots"
        out: dict[str, Snapshot] = {}
        for chunk in _chunked(symbols, 50):
            response = self._get(url, chunk)
            snapshots = response.get("snapshots", response)
            for symbol, payload in snapshots.items():
                if not isinstance(payload, dict):
                    continue
                trade = payload.get("latestTrade", {})
                quote = payload.get("latestQuote", {})
                bar = payload.get("dailyBar", {})
                price = float(trade.get("p") or 0.0)
                if price <= 0:
                    continue
                bid_price = _to_float(quote.get("bp"))
                ask_price = _to_float(quote.get("ap"))
                bid_size = _to_float(quote.get("bs"))
                ask_size = _to_float(quote.get("as"))
                out[symbol] = Snapshot(
                    symbol=symbol,
                    price=price,
                    volume=float(bar.get("v") or 0.0),
                    timestamp=_to_dt(trade.get("t")),
                    bid_price=bid_price if bid_price > 0 else None,
                    ask_price=ask_price if ask_price > 0 else None,
                    bid_size=max(0.0, bid_size),
                    ask_size=max(0.0, ask_size),
                    spread=_calc_spread(bid_price, ask_price),
                    asset_class="stock",
                    delayed_label=self.settings.stock_data_delay_label,
                )
        return out

    def get_crypto_snapshots(self, symbols: list[str]) -> dict[str, Snapshot]:
        if not symbols:
            return {}
        url = f"{self.settings.alpaca_data_url.rstrip('/')}/v1beta3/crypto/us/snapshots"
        response = self._get(url, symbols)
        snapshots = response.get("snapshots", {})
        out: dict[str, Snapshot] = {}
        for symbol, payload in snapshots.items():
            trade = payload.get("latestTrade", {})
            quote = payload.get("latestQuote", {})
            bar = payload.get("dailyBar", {})
            price = float(trade.get("p") or 0.0)
            if price <= 0:
                continue
            bid_price = _to_float(quote.get("bp"))
            ask_price = _to_float(quote.get("ap"))
            bid_size = _to_float(quote.get("bs"))
            ask_size = _to_float(quote.get("as"))
            out[symbol] = Snapshot(
                symbol=symbol,
                price=price,
                volume=float(bar.get("v") or 0.0),
                timestamp=_to_dt(trade.get("t")),
                bid_price=bid_price if bid_price > 0 else None,
                ask_price=ask_price if ask_price > 0 else None,
                bid_size=max(0.0, bid_size),
                ask_size=max(0.0, ask_size),
                spread=_calc_spread(bid_price, ask_price),
                asset_class="crypto",
            )
        return out

    def get_stock_bars(self, symbols: list[str], limit: int = 120) -> dict[str, list[Snapshot]]:
        if not symbols or limit <= 0:
            return {}
        url = f"{self.settings.alpaca_data_url.rstrip('/')}/v2/stocks/bars"
        response = self._get(url, symbols, extra_params={"timeframe": "1Min", "limit": str(limit), "feed": "iex"})
        bars_map = response.get("bars", response)
        return _parse_bars_map(bars_map, asset_class="stock", delayed_label=self.settings.stock_data_delay_label)

    def get_crypto_bars(self, symbols: list[str], limit: int = 120) -> dict[str, list[Snapshot]]:
        if not symbols or limit <= 0:
            return {}
        url = f"{self.settings.alpaca_data_url.rstrip('/')}/v1beta3/crypto/us/bars"
        response = self._get(url, symbols, extra_params={"timeframe": "1Min", "limit": str(limit)})
        bars_map = response.get("bars", response)
        return _parse_bars_map(bars_map, asset_class="crypto", delayed_label=None)

    def get_crypto_orderbook_liquidity(self, symbols: list[str], depth: int = 10) -> dict[str, dict[str, float]]:
        if not symbols:
            return {}
        if self._crypto_orderbook_supported is False:
            return {}
        url = f"{self.settings.alpaca_data_url.rstrip('/')}/v1beta3/crypto/us/orderbooks"
        params = {"limit": str(max(1, depth))}
        try:
            response = self._get(url, symbols, extra_params=params)
            self._crypto_orderbook_supported = True
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code in {401, 403, 404}:
                self._crypto_orderbook_supported = False
            return {}
        except Exception:
            return {}

        books_map = response.get("orderbooks", response.get("books", response))
        out: dict[str, dict[str, float]] = {}
        if not isinstance(books_map, dict):
            return out
        for symbol, payload in books_map.items():
            if not isinstance(payload, dict):
                continue
            bids = payload.get("bids", payload.get("b", []))
            asks = payload.get("asks", payload.get("a", []))
            out[symbol] = _orderbook_depth_metrics(bids, asks, depth)
        return out

    def get_stock_trade_delta(self, symbols: list[str], lookback_sec: int = 60) -> dict[str, dict[str, float]]:
        if not symbols:
            return {}
        url = f"{self.settings.alpaca_data_url.rstrip('/')}/v2/stocks/trades"
        now = datetime.now(timezone.utc)
        start = (now - timedelta(seconds=max(10, lookback_sec))).isoformat().replace("+00:00", "Z")
        end = now.isoformat().replace("+00:00", "Z")
        params = {"limit": "1000", "start": start, "end": end, "sort": "asc"}
        out: dict[str, dict[str, float]] = {}
        for chunk in _chunked(symbols, 30):
            try:
                response = self._get(url, chunk, extra_params=params)
            except Exception:
                continue
            out.update(
                _parse_trade_delta_map(
                    response.get("trades", response),
                    large_print_min_notional=max(0.0, self.settings.grandmother_tape_large_print_min_notional),
                )
            )
        return out

    def get_crypto_trade_delta(self, symbols: list[str], lookback_sec: int = 60) -> dict[str, dict[str, float]]:
        if not symbols:
            return {}
        url = f"{self.settings.alpaca_data_url.rstrip('/')}/v1beta3/crypto/us/trades"
        now = datetime.now(timezone.utc)
        start = (now - timedelta(seconds=max(10, lookback_sec))).isoformat().replace("+00:00", "Z")
        end = now.isoformat().replace("+00:00", "Z")
        params = {"limit": "1000", "start": start, "end": end, "sort": "asc"}
        try:
            response = self._get(url, symbols, extra_params=params)
        except Exception:
            return {}
        return _parse_trade_delta_map(
            response.get("trades", response),
            large_print_min_notional=max(0.0, self.settings.grandmother_tape_large_print_min_notional),
        )

    def get_open_interest(self, symbols: list[str]) -> dict[str, float]:
        # Alpaca spot feeds do not publish open interest for these assets.
        return {}

    def _get(self, url: str, symbols: list[str], extra_params: dict[str, str] | None = None) -> dict[str, Any]:
        headers = {
            "APCA-API-KEY-ID": self.settings.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.settings.alpaca_secret_key,
        }
        params = {"symbols": ",".join(symbols)}
        if "/stocks/" in url:
            params["feed"] = "iex"
        if extra_params:
            params.update(extra_params)
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            return resp.json()

    def _canonical_symbol(self, symbol: str) -> str:
        if symbol in self._compact_crypto_map:
            return self._compact_crypto_map[symbol]
        return symbol



def _to_dt(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _calc_spread(bid: float, ask: float) -> float | None:
    if bid > 0 and ask > 0 and ask >= bid:
        return ask - bid
    return None


def _parse_bars_map(bars_map: dict[str, Any], asset_class: str, delayed_label: str | None) -> dict[str, list[Snapshot]]:
    out: dict[str, list[Snapshot]] = {}
    if not isinstance(bars_map, dict):
        return out
    for symbol, bars in bars_map.items():
        if not isinstance(bars, list):
            continue
        parsed: list[Snapshot] = []
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            price = float(bar.get("c") or 0.0)
            if price <= 0:
                continue
            parsed.append(
                Snapshot(
                    symbol=symbol,
                    price=price,
                    volume=float(bar.get("v") or 0.0),
                    timestamp=_to_dt(bar.get("t")),
                    asset_class=asset_class,  # type: ignore[arg-type]
                    delayed_label=delayed_label,
                )
            )
        if parsed:
            out[symbol] = parsed
    return out


def _parse_trade_delta_map(
    trades_map: dict[str, Any], large_print_min_notional: float
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    if not isinstance(trades_map, dict):
        return out

    for symbol, rows in trades_map.items():
        if isinstance(rows, dict):
            rows = rows.get("trades", [])
        if not isinstance(rows, list):
            continue
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
        prev_price: float | None = None
        for trade in rows:
            if not isinstance(trade, dict):
                continue
            price = _to_float(trade.get("p", trade.get("price")))
            qty = _to_float(trade.get("s", trade.get("size")))
            if qty <= 0 or price <= 0:
                continue

            side = _normalize_trade_side(trade)
            if side == "unknown":
                if prev_price is None:
                    side = "buy"
                else:
                    side = "buy" if price >= prev_price else "sell"
            prev_price = price

            sign = 1.0 if side == "buy" else -1.0
            notional = qty * price
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

            if large_print_min_notional > 0 and notional >= large_print_min_notional:
                large_print_count += 1.0
                if sign > 0:
                    large_buy_print_count += 1.0
                else:
                    large_sell_print_count += 1.0

        out[symbol] = {
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
    return out


def _orderbook_depth_metrics(
    bids: list[dict[str, Any]] | list[Any], asks: list[dict[str, Any]] | list[Any], depth: int
) -> dict[str, float]:
    depth = max(1, depth)
    bid_levels = _normalize_orderbook_levels(bids)
    ask_levels = _normalize_orderbook_levels(asks)
    bid_slice = bid_levels[:depth]
    ask_slice = ask_levels[:depth]

    bid_volume = sum(size for _, size in bid_slice)
    ask_volume = sum(size for _, size in ask_slice)
    bid_notional = sum(price * size for price, size in bid_slice)
    ask_notional = sum(price * size for price, size in ask_slice)

    best_bid = bid_levels[0][0] if bid_levels else 0.0
    best_ask = ask_levels[0][0] if ask_levels else 0.0
    whale_bid_price, whale_bid_size, whale_bid_notional = _largest_orderbook_level(bid_slice)
    whale_ask_price, whale_ask_size, whale_ask_notional = _largest_orderbook_level(ask_slice)

    total_volume = bid_volume + ask_volume
    imbalance = (bid_volume - ask_volume) / total_volume if total_volume > 0 else 0.0

    total_notional = bid_notional + ask_notional
    imbalance_notional = (bid_notional - ask_notional) / total_notional if total_notional > 0 else 0.0

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
        "spread": _calc_spread(best_bid, best_ask) or 0.0,
        "whale_bid_price": whale_bid_price,
        "whale_bid_size": whale_bid_size,
        "whale_bid_notional": whale_bid_notional,
        "whale_ask_price": whale_ask_price,
        "whale_ask_size": whale_ask_size,
        "whale_ask_notional": whale_ask_notional,
    }


def _normalize_orderbook_levels(levels: list[dict[str, Any]] | list[Any]) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for level in levels or []:
        if not isinstance(level, dict):
            continue
        price = _to_float(level.get("p", level.get("price")))
        size = _to_float(level.get("s", level.get("size")))
        if price > 0 and size > 0:
            out.append((price, size))
    return out


def _largest_orderbook_level(levels: list[tuple[float, float]]) -> tuple[float, float, float]:
    if not levels:
        return (0.0, 0.0, 0.0)
    price, size = max(levels, key=lambda item: item[0] * item[1])
    return (price, size, price * size)


def _chunked(items: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size))
    return [items[i : i + size] for i in range(0, len(items), size)]


def _normalize_trade_side(trade: dict[str, Any]) -> str:
    raw = str(
        trade.get("tks")
        or trade.get("taker_side")
        or trade.get("side")
        or trade.get("S")
        or ""
    ).strip().lower()
    if raw in {"b", "buy", "bid", "buyer"}:
        return "buy"
    if raw in {"s", "sell", "ask", "seller"}:
        return "sell"
    return "unknown"
