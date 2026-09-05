# Bridging the Alpaca crypto L2 stream to the order book reconstructor

**Status: proposal. Nothing here is implemented.**

## Scope note

Step 8 of this work asked for a comparison of `OrderBookEvent` against the
existing reconstructor's input interface. There is no reconstructor in this
repo yet — `core/data/` does not exist, and `core/brokers/base.py` refers to it
only as a forward reference ("Feed it to the order book reconstructor in
core/data"). So this document runs the other way: it specifies the input
contract the reconstructor should accept, derived from what the Alpaca adapter
actually emits. When the reconstructor is written, this is the interface to
build against; if it already exists elsewhere, this is the checklist to
reconcile against it.

## What the adapter emits

`core/brokers/base.py`:

```python
@dataclass(frozen=True)
class OrderBookEvent:
    symbol: str
    timestamp: datetime          # venue time, UTC
    bids: Sequence[BookLevel]    # BookLevel(price: Decimal, size: Decimal)
    asks: Sequence[BookLevel]
    is_snapshot: bool
    received_at: datetime        # our arrival clock, UTC
```

Two producers, and they differ:

| Source | Method | `is_snapshot` |
|---|---|---|
| REST latest | `get_latest_orderbook()` | always forced `True` — a REST "latest" response is a full book regardless of what the payload's `reset` flag says |
| Websocket | `stream_orderbook()` | mirrors alpaca-py's `reset` flag: `True` on the initial book, `False` for subsequent updates |

Crypto only. `Capability.ORDERBOOK_L2` is absent for equities, and
`get_latest_orderbook`/`stream_orderbook` raise `UnsupportedCapability` there.
Symbols are slashed pairs, `"BTC/USD"`.

## Proposed reconstructor interface

Keep it a pure fold over events. No I/O, no clock reads, no knowledge of
brokers — that keeps it replayable from the bitemporal store byte for byte.

```python
class OrderBookReconstructor:
    def __init__(self, symbol: str, depth: int | None = None) -> None: ...
    def apply(self, ev: OrderBookEvent) -> BookState | None: ...
    def reset(self) -> None: ...
```

`apply` returns the post-event book, or `None` while the book is not yet
usable (see gap handling). `BookState` carries sorted bid/ask level lists plus
both `timestamp` and `received_at` copied through from the event that produced
it — the reconstructed book must stay PIT-attributable, and that means keeping
arrival time, not just venue time.

## Snapshot vs delta

    if ev.is_snapshot:
        replace the entire book with ev.bids / ev.asks
    else:
        merge ev.bids / ev.asks into the existing side, per price level

Rules:

1. **A delta arriving before any snapshot is not applicable.** Buffer it or
   drop it, but do not treat it as a book. The reconstructor starts in a
   `NEEDS_SNAPSHOT` state and `apply` returns `None` until the first
   `is_snapshot=True` event. Recommendation: buffer, so that the common
   startup ordering (subscribe → deltas begin → snapshot arrives) does not
   silently lose the first few updates. Cap the buffer and drop oldest.
2. **A mid-stream snapshot is authoritative.** Replace, never merge. Alpaca
   re-sends a full book after a reconnect; merging would leave stale levels
   that the venue has already removed.
3. **Deltas are per-price-level absolute, not deltas of size.** A level in the
   message replaces the size at that price; it does not add to it. Levels not
   mentioned are unchanged.

## Size-0 level removal

A level with `size == 0` in a delta means *remove this price level*, not "a
level with zero quantity". Concretely:

```python
for lvl in ev.bids:
    if lvl.size == 0:
        book.bids.pop(lvl.price, None)
    else:
        book.bids[lvl.price] = lvl.size
```

Three things to get right:

- **Only in deltas.** A `size == 0` level inside a snapshot should not happen;
  if one appears, drop the level rather than recording a zero — and log it,
  because it means an assumption here is wrong.
- **Removing an absent price is not an error.** It happens routinely when the
  level fell outside the depth window we were sent. Make it a no-op, not an
  exception.
- **Compare against `Decimal(0)`, and key the book by `Decimal`.** Prices
  arrive as `Decimal` from `_dec()`. Do not round-trip through `float` for the
  dict key: `61999.50` and `61999.5` must be the same level, and float
  comparison will eventually disagree.

## The two real risks

Both come out of reading the adapter, and both need a decision before this is
implemented.

**1. `OrderBookEvent` has no sequence number, so gaps are undetectable.**
There is no field carrying a venue sequence or update id, which means a
dropped delta corrupts the book silently and permanently — the reconstructor
cannot tell a missed message from a quiet market. Options, in preference
order:

  a. Add an optional `sequence: int | None` to `OrderBookEvent` and populate it
     in `_to_book` if alpaca-py exposes one on the orderbook model. Verify
     against the SDK before assuming it does.
  b. If the venue exposes no sequence, force periodic resynchronisation: call
     `get_latest_orderbook()` on an interval and feed the result in as a
     snapshot, bounding how long a corrupted book can persist.
  c. At minimum, treat any stream restart as `NEEDS_SNAPSHOT`.

**2. The adapter drops messages under backpressure.** `AlpacaBroker._pump`
uses a bounded queue and, on overflow, logs a warning and discards the message:

```python
except asyncio.QueueFull:
    log.warning("stream queue full; dropping %s", ...)
```

For quotes and trades that is a survivable sampling loss. For a *stateful*
book it is silent corruption, and combined with risk 1 it is undetectable. The
bridge must not consume the stream through a lossy path. Either the
reconstructor consumes `stream_orderbook()` directly and fast enough to keep
the queue drained, or `_pump` grows a per-stream policy so order book streams
fail loudly (raise/reconnect-and-resnapshot) instead of dropping. **Recommend
the latter**: dropping an order book delta should never be a warning-level
event.

## Suggested shape of the bridge

Thin. The adapter already produces the right event type, so the bridge is a
loop, not a translation layer:

```python
async for ev in broker.stream_orderbook(["BTC/USD"], AssetClass.CRYPTO):
    state = reconstructor.apply(ev)
    if state is not None:
        sink.write(state)     # -> bitemporal store, keyed by both timestamps
```

Keep book state out of `core/brokers/` entirely, as `base.py` already
instructs. The adapter's job ends at emitting well-formed wire events.

## Open questions

- Does alpaca-py's `Orderbook` model expose a sequence/update id? Decides
  risk 1.
- Is the websocket book depth-capped, and at what depth? Determines whether
  "remove an absent price" is routine or a symptom.
- Should the reconstructor emit on every event, or coalesce to a tick
  interval? Affects the write volume into the bitemporal store.
