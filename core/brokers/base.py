"""
Broker-agnostic interface.

Strategies depend on `Broker` only — never on a concrete SDK. Every concrete
adapter (Alpaca, IBKR, ...) maps its own models into the dataclasses here.

Design rules:
  * All timestamps are timezone-aware UTC.
  * Every inbound market-data event carries `received_at` (our wall clock) in
    addition to the venue's `timestamp`, so PITSession can reason about what
    was knowable when.
  * Capabilities are explicit. A strategy asks `broker.supports(Capability.L2)`
    rather than discovering at 3am that equities have no depth.
  * Orders are idempotent via `client_order_id`. Retrying a submit with the
    same id must not create a second order.
"""
from __future__ import annotations

import abc
import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import AsyncIterator, Callable, Optional, Sequence


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class AssetClass(str, enum.Enum):
    EQUITY = "equity"
    CRYPTO = "crypto"


class Side(str, enum.Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, enum.Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class TimeInForce(str, enum.Enum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"


class OrderStatus(str, enum.Enum):
    NEW = "new"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    PENDING = "pending"          # anything in-flight we can't classify finer
    UNKNOWN = "unknown"

    @property
    def is_terminal(self) -> bool:
        return self in {OrderStatus.FILLED, OrderStatus.CANCELED,
                        OrderStatus.REJECTED, OrderStatus.EXPIRED}


class Capability(str, enum.Enum):
    """What a broker can do for a given asset class. Queried, never assumed."""
    TRADE = "trade"
    BARS = "bars"
    QUOTES_L1 = "quotes_l1"
    TRADES_TAPE = "trades_tape"
    ORDERBOOK_L2 = "orderbook_l2"
    EXTENDED_HOURS = "extended_hours"
    FRACTIONAL = "fractional"


# ---------------------------------------------------------------------------
# Market data models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Bar:
    symbol: str
    timestamp: datetime          # bar open time, UTC
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vwap: Optional[Decimal] = None
    trade_count: Optional[int] = None
    received_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class Quote:
    """Top of book (L1)."""
    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal
    received_at: datetime = field(default_factory=utcnow)

    @property
    def mid(self) -> Decimal:
        return (self.bid_price + self.ask_price) / 2

    @property
    def spread(self) -> Decimal:
        return self.ask_price - self.bid_price


@dataclass(frozen=True)
class Trade:
    """Time & sales print."""
    symbol: str
    timestamp: datetime
    price: Decimal
    size: Decimal
    trade_id: Optional[str] = None
    taker_side: Optional[Side] = None   # populated only when the venue reports it
    received_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class OrderBookEvent:
    """
    An L2 order book message.

    `is_snapshot=True`  -> bids/asks are the full book; replace local state.
    `is_snapshot=False` -> bids/asks are level updates; a level with size 0
                           means "remove this price level".

    This is the raw wire semantics. Feed it to the order book reconstructor
    in core/data; do not maintain book state inside broker adapters.
    """
    symbol: str
    timestamp: datetime
    bids: Sequence[BookLevel]
    asks: Sequence[BookLevel]
    is_snapshot: bool
    received_at: datetime = field(default_factory=utcnow)


# ---------------------------------------------------------------------------
# Trading models
# ---------------------------------------------------------------------------

@dataclass
class OrderRequest:
    symbol: str
    side: Side
    qty: Optional[Decimal] = None          # exactly one of qty / notional
    notional: Optional[Decimal] = None
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    extended_hours: bool = False
    client_order_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    tag: Optional[str] = None              # strategy name, for attribution

    def __post_init__(self) -> None:
        if (self.qty is None) == (self.notional is None):
            raise ValueError("Specify exactly one of qty or notional")
        if self.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and self.limit_price is None:
            raise ValueError(f"{self.order_type.value} order requires limit_price")
        if self.order_type in (OrderType.STOP, OrderType.STOP_LIMIT) and self.stop_price is None:
            raise ValueError(f"{self.order_type.value} order requires stop_price")


@dataclass(frozen=True)
class Order:
    order_id: str
    client_order_id: str
    symbol: str
    side: Side
    order_type: OrderType
    status: OrderStatus
    qty: Optional[Decimal]
    notional: Optional[Decimal]
    filled_qty: Decimal
    filled_avg_price: Optional[Decimal]
    limit_price: Optional[Decimal]
    stop_price: Optional[Decimal]
    submitted_at: Optional[datetime]
    filled_at: Optional[datetime]
    raw: object = field(default=None, repr=False, compare=False)  # SDK object, for debugging


@dataclass(frozen=True)
class Position:
    symbol: str
    asset_class: AssetClass
    qty: Decimal                 # signed: negative = short
    avg_entry_price: Decimal
    market_value: Decimal
    unrealized_pl: Decimal
    current_price: Optional[Decimal] = None


@dataclass(frozen=True)
class Account:
    account_id: str
    equity: Decimal
    cash: Decimal
    buying_power: Decimal
    is_paper: bool
    currency: str = "USD"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class BrokerError(Exception):
    """Base class for adapter errors."""


class UnsupportedCapability(BrokerError):
    pass


class OrderRejected(BrokerError):
    pass


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------

class Broker(abc.ABC):
    """
    Everything a strategy or the execution router is allowed to call.

    Sync methods are request/response REST. Streaming methods are async
    generators so the caller controls backpressure and lifetime.
    """

    name: str
    is_paper: bool

    # --- capabilities ------------------------------------------------------
    @abc.abstractmethod
    def supports(self, capability: Capability, asset_class: AssetClass) -> bool: ...

    def require(self, capability: Capability, asset_class: AssetClass) -> None:
        if not self.supports(capability, asset_class):
            raise UnsupportedCapability(
                f"{self.name} does not support {capability.value} for {asset_class.value}")

    # --- account -----------------------------------------------------------
    @abc.abstractmethod
    def get_account(self) -> Account: ...

    @abc.abstractmethod
    def get_positions(self) -> list[Position]: ...

    # --- orders ------------------------------------------------------------
    @abc.abstractmethod
    def submit_order(self, req: OrderRequest) -> Order: ...

    @abc.abstractmethod
    def get_order(self, order_id: str) -> Order: ...

    @abc.abstractmethod
    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]: ...

    @abc.abstractmethod
    def cancel_order(self, order_id: str) -> None: ...

    @abc.abstractmethod
    def cancel_all_orders(self) -> None: ...

    # --- historical data ---------------------------------------------------
    @abc.abstractmethod
    def get_bars(self, symbols: Sequence[str], asset_class: AssetClass,
                 start: datetime, end: Optional[datetime],
                 timeframe: str) -> dict[str, list[Bar]]:
        """
        timeframe: "1Min", "5Min", "15Min", "1Hour", "1Day".
        Returns bars keyed by symbol, ascending by timestamp.
        """

    @abc.abstractmethod
    def get_latest_quote(self, symbols: Sequence[str], asset_class: AssetClass) -> dict[str, Quote]: ...

    @abc.abstractmethod
    def get_latest_orderbook(self, symbols: Sequence[str], asset_class: AssetClass) -> dict[str, OrderBookEvent]:
        """Full snapshot per symbol. Raises UnsupportedCapability where no L2."""

    # --- streaming ---------------------------------------------------------
    @abc.abstractmethod
    def stream_quotes(self, symbols: Sequence[str], asset_class: AssetClass) -> AsyncIterator[Quote]: ...

    @abc.abstractmethod
    def stream_trades(self, symbols: Sequence[str], asset_class: AssetClass) -> AsyncIterator[Trade]: ...

    @abc.abstractmethod
    def stream_orderbook(self, symbols: Sequence[str], asset_class: AssetClass) -> AsyncIterator[OrderBookEvent]: ...

    @abc.abstractmethod
    def stream_order_updates(self) -> AsyncIterator[Order]:
        """Fills / cancels / rejects pushed by the broker."""
