# quant_agent

Point-in-time quantitative research and execution platform.

## Layout

    core/         shared, strategy-agnostic machinery
      brokers/    broker-agnostic interface + concrete adapters
    strategies/   trading strategies, built only on core

See [CLAUDE.md](CLAUDE.md) for the conventions every layer is expected to hold to.

## Development

    python -m venv .venv
    .venv/Scripts/activate      # Windows;  source .venv/bin/activate on POSIX
    pip install -e ".[dev]"
    pytest
