# quant_agent — working notes

Conventions this repo holds to. Read before adding a layer or a strategy.

## Planned monorepo layout

    core/                shared machinery; knows nothing about any one strategy
      data/              bitemporal store, PIT sessions, order book reconstruction
      brokers/           broker-agnostic interface + concrete adapters
      features/          feature computation
      altdata/           non-price data sources
      models/            model definitions and training
      execution/         order routing, sizing, risk
      research/          backtests, studies, evaluation
    strategies/          built only on core; never on a vendor SDK
      intraday_crypto/
      swing_equities/

Only `core/brokers/` exists today. The rest is the target shape — put new code
where it will eventually live rather than at the root.

## Broker layer

`core/brokers/base.py` is the whole contract. `core/brokers/alpaca.py` is one
implementation of it.

**Strategies import only from `core.brokers.base`, never a concrete SDK.**
No `import alpaca` anywhere outside `core/brokers/alpaca.py`. Strategies take a
`Broker` and talk to it through `Order`, `OrderRequest`, `Quote`, `Bar`,
`Trade`, `OrderBookEvent`, `Position`, `Account` — dataclasses the adapter maps
the vendor's models into. Swapping Alpaca for IBKR should touch one file.

**All timestamps are timezone-aware UTC.** Adapters normalise on the way in
(`_utc()` in the Alpaca adapter attaches UTC to any naive datetime the SDK
hands back). A naive datetime reaching `core/` is a bug.

**Every market-data event carries `received_at` alongside the venue
timestamp.** `timestamp` is when the venue says it happened; `received_at` is
our wall clock when the message arrived. Both are needed: PIT sessions reason
about what was *knowable* when, and that is arrival time, not event time. The
Alpaca adapter stamps `received_at` inside the websocket callback, before the
message is queued, so it reflects arrival rather than consumption — keep that
property in any new adapter.

**Capabilities are queried, never assumed.** Ask
`broker.supports(Capability.ORDERBOOK_L2, AssetClass.CRYPTO)`, or call
`broker.require(...)` to raise `UnsupportedCapability` up front. Concretely:
Alpaca crypto has L2 depth, Alpaca equities have none at all. Discovering that
from an empty book at 3am is not the plan.

**Paper is the default; live is opt-in.** `ALPACA_PAPER` defaults to `1`. Live
trading requires setting `ALPACA_PAPER=0` explicitly — there is no other way to
reach it, and the adapter logs a warning when it initialises in live mode.
Credentials come from `.env` (gitignored); `.env.example` is the template.

**Orders are idempotent via `client_order_id`.** `submit_order` looks the id up
before submitting, so retrying after a network error returns the existing order
instead of doubling the position.

## Tests

`pytest` from the repo root. Broker tests are offline: they inject fake
trading/data clients through `AlpacaBroker(trading_client=..., crypto_data=...)`
rather than touching the network. Keep it that way — no test should need
credentials.
