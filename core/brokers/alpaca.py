"""
Alpaca adapter (alpaca-py SDK, >= 0.40).

    pip install alpaca-py

Environment:
    ALPACA_API_KEY, ALPACA_SECRET_KEY
    ALPACA_PAPER=1        (default; set 0 for live — deliberately explicit)
    ALPACA_STOCK_FEED=iex (free) | sip (paid, consolidated tape)

Capability notes (verified against Alpaca docs, Sep 2026):
    * Crypto: bars, L1 quotes, trades, and L2 order book (REST snapshot +
      websocket deltas) for a limited set of pairs. Symbols are "BTC/USD".
    * Equities: bars, L1 quotes, trades. NO order book depth of any kind.
    * Paper and live share the same API; only the base URL differs.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, AsyncIterator, Callable, Optional, Sequence

from alpaca.data.enums import DataFeed
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.live import CryptoDataStream, StockDataStream
from alpaca.data.requests import (
    CryptoBarsRequest,
    CryptoLatestOrderbookRequest,
    CryptoLatestQuoteRequest,
    StockBarsRequest,
    StockLatestQuoteRequest,
)
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide as _Side
from alpaca.trading.enums import OrderStatus as _Status
from alpaca.trading.enums import OrderType as _Type
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.enums import TimeInForce as _TIF
from alpaca.trading.requests import (
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    StopLimitOrderRequest,
    StopOrderRequest,
)
from alpaca.trading.stream import TradingStream

from .base import (
    Account,
    AssetClass,
    Bar,
    BookLevel,
    Broker,
    BrokerError,
    Capability,
    Order,
    OrderBookEvent,
    OrderRejected,
    OrderRequest,
    OrderStatus,
    OrderType,
    Position,
    Quote,
    Side,
    TimeInForce,
    Trade,
    UnsupportedCapability,
    utcnow,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dec(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    return Decimal(str(x))


def _utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


_TIMEFRAMES = {
    "1Min": TimeFrame(1, TimeFrameUnit.Minute),
    "5Min": TimeFrame(5, TimeFrameUnit.Minute),
    "15Min": TimeFrame(15, TimeFrameUnit.Minute),
    "1Hour": TimeFrame(1, TimeFrameUnit.Hour),
    "1Day": TimeFrame(1, TimeFrameUnit.Day),
}

_STATUS_MAP = {
    _Status.NEW: OrderStatus.NEW,
    _Status.ACCEPTED: OrderStatus.ACCEPTED,
    _Status.PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED,
    _Status.FILLED: OrderStatus.FILLED,
    _Status.CANCELED: OrderStatus.CANCELED,
    _Status.REJECTED: OrderStatus.REJECTED,
    _Status.EXPIRED: OrderStatus.EXPIRED,
    _Status.PENDING_NEW: OrderStatus.PENDING,
    _Status.PENDING_CANCEL: OrderStatus.PENDING,
    _Status.PENDING_REPLACE: OrderStatus.PENDING,
    _Status.ACCEPTED_FOR_BIDDING: OrderStatus.ACCEPTED,
    _Status.HELD: OrderStatus.PENDING,
}

_TYPE_MAP = {
    _Type.MARKET: OrderType.MARKET,
    _Type.LIMIT: OrderType.LIMIT,
    _Type.STOP: OrderType.STOP,
    _Type.STOP_LIMIT: OrderType.STOP_LIMIT,
}


def _to_order(o: Any) -> Order:
    return Order(
        order_id=str(o.id),
        client_order_id=o.client_order_id,
        symbol=o.symbol,
        side=Side(o.side.value),
        order_type=_TYPE_MAP.get(o.order_type, OrderType.MARKET),
        status=_STATUS_MAP.get(o.status, OrderStatus.UNKNOWN),
        qty=_dec(o.qty),
        notional=_dec(o.notional),
        filled_qty=_dec(o.filled_qty) or Decimal(0),
        filled_avg_price=_dec(o.filled_avg_price),
        limit_price=_dec(o.limit_price),
        stop_price=_dec(o.stop_price),
        submitted_at=_utc(o.submitted_at),
        filled_at=_utc(o.filled_at),
        raw=o,
    )


def _to_bar(b: Any, received_at: datetime) -> Bar:
    return Bar(
        symbol=b.symbol, timestamp=_utc(b.timestamp),
        open=_dec(b.open), high=_dec(b.high), low=_dec(b.low), close=_dec(b.close),
        volume=_dec(b.volume), vwap=_dec(b.vwap), trade_count=b.trade_count,
        received_at=received_at,
    )


def _to_quote(q: Any, received_at: Optional[datetime] = None) -> Quote:
    return Quote(
        symbol=q.symbol, timestamp=_utc(q.timestamp),
        bid_price=_dec(q.bid_price) or Decimal(0), bid_size=_dec(q.bid_size) or Decimal(0),
        ask_price=_dec(q.ask_price) or Decimal(0), ask_size=_dec(q.ask_size) or Decimal(0),
        received_at=received_at or utcnow(),
    )


def _to_trade(t: Any, received_at: Optional[datetime] = None) -> Trade:
    side = None
    ts = getattr(t, "taker_side", None)
    if ts:
        side = Side.BUY if str(ts).upper().startswith("B") else Side.SELL
    return Trade(
        symbol=t.symbol, timestamp=_utc(t.timestamp),
        price=_dec(t.price), size=_dec(t.size),
        trade_id=str(t.id) if getattr(t, "id", None) is not None else None,
        taker_side=side, received_at=received_at or utcnow(),
    )


def _to_book(ob: Any, received_at: Optional[datetime] = None) -> OrderBookEvent:
    # alpaca-py Orderbook: bids/asks are lists of {price, size}; `reset` marks
    # a full snapshot. Only the stream sets reset; REST "latest" is always a
    # snapshot, which we normalise below.
    return OrderBookEvent(
        symbol=ob.symbol, timestamp=_utc(ob.timestamp),
        bids=tuple(BookLevel(_dec(l.price), _dec(l.size)) for l in ob.bids),
        asks=tuple(BookLevel(_dec(l.price), _dec(l.size)) for l in ob.asks),
        is_snapshot=bool(getattr(ob, "reset", True)),
        received_at=received_at or utcnow(),
    )


def _to_position(p: Any) -> Position:
    qty = _dec(p.qty) or Decimal(0)
    if str(p.side).lower().endswith("short"):
        qty = -abs(qty)
    return Position(
        symbol=p.symbol,
        asset_class=AssetClass.CRYPTO if "crypto" in str(p.asset_class).lower() else AssetClass.EQUITY,
        qty=qty,
        avg_entry_price=_dec(p.avg_entry_price) or Decimal(0),
        market_value=_dec(p.market_value) or Decimal(0),
        unrealized_pl=_dec(p.unrealized_pl) or Decimal(0),
        current_price=_dec(p.current_price),
    )


def _build_request(req: OrderRequest) -> Any:
    common = dict(
        symbol=req.symbol,
        side=_Side(req.side.value),
        time_in_force=_TIF(req.time_in_force.value),
        client_order_id=req.client_order_id,
        extended_hours=req.extended_hours or None,
        qty=float(req.qty) if req.qty is not None else None,
        notional=float(req.notional) if req.notional is not None else None,
    )
    if req.order_type is OrderType.MARKET:
        return MarketOrderRequest(**common)
    if req.order_type is OrderType.LIMIT:
        return LimitOrderRequest(limit_price=float(req.limit_price), **common)
    if req.order_type is OrderType.STOP:
        return StopOrderRequest(stop_price=float(req.stop_price), **common)
    if req.order_type is OrderType.STOP_LIMIT:
        return StopLimitOrderRequest(
            limit_price=float(req.limit_price), stop_price=float(req.stop_price), **common)
    raise ValueError(req.order_type)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class AlpacaBroker(Broker):
    name = "alpaca"

    _CAPS = {
        AssetClass.EQUITY: {Capability.TRADE, Capability.BARS, Capability.QUOTES_L1,
                            Capability.TRADES_TAPE, Capability.EXTENDED_HOURS,
                            Capability.FRACTIONAL},
        AssetClass.CRYPTO: {Capability.TRADE, Capability.BARS, Capability.QUOTES_L1,
                            Capability.TRADES_TAPE, Capability.ORDERBOOK_L2,
                            Capability.FRACTIONAL},
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        paper: Optional[bool] = None,
        stock_feed: Optional[str] = None,
        # injection points for tests
        trading_client: Any = None,
        stock_data: Any = None,
        crypto_data: Any = None,
    ) -> None:
        self._key = api_key or os.environ.get("ALPACA_API_KEY", "")
        self._secret = secret_key or os.environ.get("ALPACA_SECRET_KEY", "")
        if paper is None:
            paper = os.environ.get("ALPACA_PAPER", "1") != "0"
        self.is_paper = paper
        self._stock_feed = DataFeed((stock_feed or os.environ.get("ALPACA_STOCK_FEED", "iex")).lower())

        if not (self._key and self._secret) and trading_client is None:
            raise BrokerError("ALPACA_API_KEY / ALPACA_SECRET_KEY not set")

        self._trading = trading_client or TradingClient(self._key, self._secret, paper=paper)
        self._stock = stock_data or StockHistoricalDataClient(self._key, self._secret)
        self._crypto = crypto_data or CryptoHistoricalDataClient(self._key, self._secret)

        if not paper:
            log.warning("AlpacaBroker initialised in LIVE mode")

    # --- capabilities ------------------------------------------------------
    def supports(self, capability: Capability, asset_class: AssetClass) -> bool:
        return capability in self._CAPS[asset_class]

    # --- account -----------------------------------------------------------
    def get_account(self) -> Account:
        a = self._trading.get_account()
        return Account(
            account_id=str(a.id), equity=_dec(a.equity) or Decimal(0),
            cash=_dec(a.cash) or Decimal(0), buying_power=_dec(a.buying_power) or Decimal(0),
            is_paper=self.is_paper, currency=str(a.currency or "USD"),
        )

    def get_positions(self) -> list[Position]:
        return [_to_position(p) for p in self._trading.get_all_positions()]

    # --- orders ------------------------------------------------------------
    def submit_order(self, req: OrderRequest) -> Order:
        # Idempotency: if we already have an order with this client id, return it
        # rather than submitting a duplicate (retries after network errors).
        try:
            existing = self._trading.get_order_by_client_id(req.client_order_id)
            log.info("submit_order: client_order_id %s already exists, returning it",
                     req.client_order_id)
            return _to_order(existing)
        except Exception:
            pass
        try:
            o = self._trading.submit_order(_build_request(req))
        except Exception as e:  # alpaca-py raises APIError; keep adapter SDK-agnostic
            raise OrderRejected(str(e)) from e
        order = _to_order(o)
        if order.status is OrderStatus.REJECTED:
            raise OrderRejected(f"{req.symbol} {req.side.value}: rejected by broker")
        return order

    def get_order(self, order_id: str) -> Order:
        return _to_order(self._trading.get_order_by_id(order_id))

    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN,
                               symbols=[symbol] if symbol else None)
        return [_to_order(o) for o in self._trading.get_orders(req)]

    def cancel_order(self, order_id: str) -> None:
        self._trading.cancel_order_by_id(order_id)

    def cancel_all_orders(self) -> None:
        self._trading.cancel_orders()

    # --- historical data ---------------------------------------------------
    def get_bars(self, symbols: Sequence[str], asset_class: AssetClass,
                 start: datetime, end: Optional[datetime], timeframe: str) -> dict[str, list[Bar]]:
        self.require(Capability.BARS, asset_class)
        tf = _TIMEFRAMES.get(timeframe)
        if tf is None:
            raise ValueError(f"timeframe must be one of {list(_TIMEFRAMES)}")
        syms = list(symbols)
        if asset_class is AssetClass.CRYPTO:
            resp = self._crypto.get_crypto_bars(
                CryptoBarsRequest(symbol_or_symbols=syms, timeframe=tf, start=start, end=end))
        else:
            resp = self._stock.get_stock_bars(
                StockBarsRequest(symbol_or_symbols=syms, timeframe=tf, start=start, end=end,
                                 feed=self._stock_feed))
        received = utcnow()
        out: dict[str, list[Bar]] = {s: [] for s in syms}
        for sym, bars in resp.data.items():
            out[sym] = sorted((_to_bar(b, received) for b in bars), key=lambda b: b.timestamp)
        return out

    def get_latest_quote(self, symbols: Sequence[str], asset_class: AssetClass) -> dict[str, Quote]:
        self.require(Capability.QUOTES_L1, asset_class)
        syms = list(symbols)
        if asset_class is AssetClass.CRYPTO:
            resp = self._crypto.get_crypto_latest_quote(CryptoLatestQuoteRequest(symbol_or_symbols=syms))
        else:
            resp = self._stock.get_stock_latest_quote(
                StockLatestQuoteRequest(symbol_or_symbols=syms, feed=self._stock_feed))
        received = utcnow()
        return {s: _to_quote(q, received) for s, q in resp.items()}

    def get_latest_orderbook(self, symbols: Sequence[str], asset_class: AssetClass) -> dict[str, OrderBookEvent]:
        self.require(Capability.ORDERBOOK_L2, asset_class)   # equities raise here
        resp = self._crypto.get_crypto_latest_orderbook(
            CryptoLatestOrderbookRequest(symbol_or_symbols=list(symbols)))
        received = utcnow()
        out = {}
        for s, ob in resp.items():
            ev = _to_book(ob, received)
            # REST latest is always a full book regardless of the reset flag
            out[s] = OrderBookEvent(ev.symbol, ev.timestamp, ev.bids, ev.asks, True, ev.received_at)
        return out

    # --- streaming ---------------------------------------------------------
    def _data_stream(self, asset_class: AssetClass):
        if asset_class is AssetClass.CRYPTO:
            return CryptoDataStream(self._key, self._secret)
        return StockDataStream(self._key, self._secret, feed=self._stock_feed)

    async def _pump(self, stream: Any, subscribe: Callable[[Callable], None],
                    convert: Callable[[Any, datetime], Any]) -> AsyncIterator[Any]:
        """
        Bridge alpaca-py's callback API to an async generator.

        alpaca-py's `run()` calls asyncio.run() internally, which breaks inside an
        existing loop, so we drive `_run_forever()` as a task. Stamp `received_at`
        in the callback — before the queue — so it reflects arrival, not consumption.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)

        async def handler(msg: Any) -> None:
            item = convert(msg, utcnow())
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                log.warning("stream queue full; dropping %s", getattr(item, "symbol", "?"))

        subscribe(handler)
        task = asyncio.create_task(stream._run_forever())
        try:
            while True:
                if task.done() and queue.empty():
                    exc = task.exception()
                    raise BrokerError(f"stream ended: {exc!r}")
                try:
                    yield await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
        finally:
            task.cancel()
            try:
                await stream.close()
            except Exception:
                pass

    def stream_quotes(self, symbols: Sequence[str], asset_class: AssetClass) -> AsyncIterator[Quote]:
        self.require(Capability.QUOTES_L1, asset_class)
        s = self._data_stream(asset_class)
        return self._pump(s, lambda h: s.subscribe_quotes(h, *symbols), _to_quote)

    def stream_trades(self, symbols: Sequence[str], asset_class: AssetClass) -> AsyncIterator[Trade]:
        self.require(Capability.TRADES_TAPE, asset_class)
        s = self._data_stream(asset_class)
        return self._pump(s, lambda h: s.subscribe_trades(h, *symbols), _to_trade)

    def stream_orderbook(self, symbols: Sequence[str], asset_class: AssetClass) -> AsyncIterator[OrderBookEvent]:
        self.require(Capability.ORDERBOOK_L2, asset_class)
        s = self._data_stream(asset_class)
        return self._pump(s, lambda h: s.subscribe_orderbooks(h, *symbols), _to_book)

    def stream_order_updates(self) -> AsyncIterator[Order]:
        s = TradingStream(self._key, self._secret, paper=self.is_paper)
        return self._pump(s, lambda h: s.subscribe_trade_updates(h),
                          lambda upd, _received: _to_order(upd.order))
