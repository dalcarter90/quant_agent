"""Offline tests for AlpacaBroker: model mapping, capabilities, idempotency."""
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace as NS

import pytest
from alpaca.trading.enums import OrderSide, OrderStatus as AStatus, OrderType as AType

from core.brokers.alpaca import AlpacaBroker
from core.brokers.base import (
    AssetClass, Capability, OrderRequest, OrderStatus, OrderType, Side,
    UnsupportedCapability, OrderRejected,
)

T0 = datetime(2026, 9, 3, 14, 30, tzinfo=timezone.utc)


def sdk_order(**kw):
    base = dict(id="o1", client_order_id="c1", symbol="AAPL", side=OrderSide.BUY,
                order_type=AType.MARKET, status=AStatus.ACCEPTED, qty="10", notional=None,
                filled_qty="0", filled_avg_price=None, limit_price=None, stop_price=None,
                submitted_at=T0, filled_at=None)
    base.update(kw)
    return NS(**base)


class FakeTrading:
    def __init__(self):
        self.orders = {}
        self.submitted = []

    def get_order_by_client_id(self, cid):
        if cid in self.orders:
            return self.orders[cid]
        raise Exception("not found")

    def submit_order(self, req):
        self.submitted.append(req)
        o = sdk_order(client_order_id=req.client_order_id, symbol=req.symbol,
                      side=req.side, order_type=req.type)
        self.orders[req.client_order_id] = o
        return o

    def get_account(self):
        return NS(id="acct", equity="100000", cash="50000", buying_power="200000", currency="USD")

    def get_all_positions(self):
        return [NS(symbol="BTC/USD", asset_class="crypto", qty="0.5", side="long",
                   avg_entry_price="60000", market_value="31000", unrealized_pl="1000",
                   current_price="62000"),
                NS(symbol="TSLA", asset_class="us_equity", qty="10", side="short",
                   avg_entry_price="250", market_value="-2400", unrealized_pl="100",
                   current_price="240")]


class FakeCrypto:
    def get_crypto_latest_orderbook(self, req):
        return {"BTC/USD": NS(symbol="BTC/USD", timestamp=T0, reset=False,
                              bids=[NS(price=61999.5, size=0.3)],
                              asks=[NS(price=62000.5, size=0.1)])}


@pytest.fixture
def broker():
    return AlpacaBroker(trading_client=FakeTrading(), stock_data=object(),
                        crypto_data=FakeCrypto(), paper=True)


def test_capabilities(broker):
    assert broker.supports(Capability.ORDERBOOK_L2, AssetClass.CRYPTO)
    assert not broker.supports(Capability.ORDERBOOK_L2, AssetClass.EQUITY)


def test_equity_orderbook_raises(broker):
    with pytest.raises(UnsupportedCapability):
        broker.get_latest_orderbook(["AAPL"], AssetClass.EQUITY)


def test_rest_orderbook_is_snapshot_with_arrival_time(broker):
    ev = broker.get_latest_orderbook(["BTC/USD"], AssetClass.CRYPTO)["BTC/USD"]
    assert ev.is_snapshot is True            # REST latest forced to snapshot
    assert ev.bids[0].price == Decimal("61999.5")
    assert ev.received_at >= ev.timestamp


def test_submit_is_idempotent(broker):
    req = OrderRequest(symbol="AAPL", side=Side.BUY, qty=Decimal("10"))
    o1 = broker.submit_order(req)
    o2 = broker.submit_order(req)              # simulated retry
    assert o1.client_order_id == o2.client_order_id
    assert len(broker._trading.submitted) == 1


def test_limit_order_maps_and_requires_price(broker):
    with pytest.raises(ValueError):
        OrderRequest(symbol="AAPL", side=Side.SELL, qty=Decimal(1), order_type=OrderType.LIMIT)
    req = OrderRequest(symbol="AAPL", side=Side.SELL, qty=Decimal(1),
                       order_type=OrderType.LIMIT, limit_price=Decimal("230.10"))
    broker.submit_order(req)
    sent = broker._trading.submitted[-1]
    assert sent.limit_price == 230.10 and sent.side == OrderSide.SELL


def test_qty_xor_notional():
    with pytest.raises(ValueError):
        OrderRequest(symbol="AAPL", side=Side.BUY)
    with pytest.raises(ValueError):
        OrderRequest(symbol="AAPL", side=Side.BUY, qty=Decimal(1), notional=Decimal(100))


def test_positions_signed(broker):
    pos = {p.symbol: p for p in broker.get_positions()}
    assert pos["BTC/USD"].qty == Decimal("0.5") and pos["BTC/USD"].asset_class is AssetClass.CRYPTO
    assert pos["TSLA"].qty == Decimal("-10") and pos["TSLA"].asset_class is AssetClass.EQUITY


def test_account(broker):
    a = broker.get_account()
    assert a.equity == Decimal("100000") and a.is_paper is True


def test_rejected_status_raises(broker):
    broker._trading.submit_order = lambda req: sdk_order(status=AStatus.REJECTED,
                                                          client_order_id=req.client_order_id)
    with pytest.raises(OrderRejected):
        broker.submit_order(OrderRequest(symbol="AAPL", side=Side.BUY, qty=Decimal(1)))
