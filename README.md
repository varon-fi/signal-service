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
- `EXECUTIONSERVICE_GRPC_ADDR` — ExecutionService gRPC endpoint (default: localhost:50053)

## E2E Testing

Local end-to-end smoke tests using mock gRPC services:

```bash
# Terminal 1: Mock DataService
python scripts/mock_dataservice.py 50051

# Terminal 2: Mock ExecutionService
python scripts/mock_executionservice.py 50053

# Terminal 3: Signal Service E2E runner
python scripts/signal_service_e2e.py
```

See `scripts/RUNBOOK.md` for detailed instructions, troubleshooting, and proto alignment notes.
