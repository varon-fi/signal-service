# Signal Service

Real-time trading signal generation service for varon.fi.

## Responsibilities
- Consume OHLC market data via gRPC DataService streaming
- Load active strategies from Postgres
- Generate trading signals and emit `TradeSignal` events via gRPC
- Persist signals to Postgres
- Remain mode-agnostic for signal generation (paper/live routing is handled downstream)

## Active Strategy Surface

`signal-service` now supports only:
- `range_mean_reversion`

Legacy strategy families were removed from this service to keep runtime parity with shared strategy logic and reduce drift.

## Data Source

Signal history and warmup reads use canonical `ohlcs` only.
Legacy `ohlc_imports` fallback paths were removed.

## Architecture

```text
[DataService] -> gRPC OHLC stream -> [Signal Service] -> gRPC TradeSignal -> [Orders Service]
                     ^                                              |
                     |                                              v
                Postgres (ohlcs)                             Postgres (signals)
```

## Quick Start

```bash
pip install -e .
pytest -q
python -m signal_service.main
```

## Environment Variables
- `DATABASE_URL`: Postgres connection string
- `DATASERVICE_GRPC_ADDR`: DataService gRPC endpoint (default `localhost:50051`)
- `SIGNALSERVICE_GRPC_PORT`: SignalService gRPC port (default `50052`)

## Strategy Loading
- Strategies are loaded from Postgres at startup (`status='active'`).
- Unsupported strategy names are skipped.
- Restart service to apply strategy config changes.
- Strategy configs are loaded from the canonical `(strategy_id, symbol, timeframe)` scope.
