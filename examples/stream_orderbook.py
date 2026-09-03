"""
Stream the BTC/USD L2 book from Alpaca and print snapshot/delta semantics.
Needs ALPACA_API_KEY / ALPACA_SECRET_KEY. Ctrl-C to stop.
"""
import asyncio
from core.brokers.alpaca import AlpacaBroker
from core.brokers.base import AssetClass


async def main():
    broker = AlpacaBroker()
    n = 0
    async for ev in broker.stream_orderbook(["BTC/USD"], AssetClass.CRYPTO):
        kind = "SNAPSHOT" if ev.is_snapshot else "delta   "
        lag_ms = (ev.received_at - ev.timestamp).total_seconds() * 1000
        print(f"{kind} {ev.symbol} bids={len(ev.bids):3d} asks={len(ev.asks):3d} lag={lag_ms:7.1f}ms")
        n += 1
        if n >= 50:
            break


if __name__ == "__main__":
    asyncio.run(main())
