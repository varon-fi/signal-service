# Signal Service

Real-time trading signal generation service for varon.fi platform.

## Responsibilities
- Consume OHLC market data via gRPC DataService streaming
- Load live strategies from database
- Generate buy/sell signals using TA-Lib indicators
- Emit TradeSignal events via gRPC SignalService
- Persist signal history to Postgres

## Architecture

```
[DataService] → gRPC OHLC stream → [Signal Service] → gRPC TradeSignal → [Orders Service]
                     ↑                                              ↓
              Postgres (market data)                          Postgres (signals)
                     ↑
            Config from DB (strategies table)
```

## Tech Stack
- Python 3.12+
- gRPC + protobuf
- PostgreSQL (asyncpg)
- TA-Lib (via python wrapper)
- pytest for testing

## Quick Start

```bash
# Install dependencies
pip install -e .

# Run tests
pytest -q

# Start service
python -m signal_service.main
```

## Environment Variables
- `DATABASE_URL` — Postgres connection string
- `DATASERVICE_GRPC_ADDR` — DataService gRPC endpoint (default: localhost:50051)
- `SIGNALSERVICE_GRPC_PORT` — Port to expose SignalService (default: 50052)
- `SIGNALSERVICE_TRADING_MODE` — Strategy mode filter (`live` or `paper`, default: `live`)

## Strategy Loading
- Strategies are loaded from Postgres on startup (filtered by mode + active flags).
- To apply strategy config changes, restart the service (no hot-reload wired yet).
