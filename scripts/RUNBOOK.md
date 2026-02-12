# Local E2E Smoke Test Runbook

## Overview

End-to-end smoke test for Signal Service using mock gRPC services.
Tests the full data flow: DataService → Signal Service → ExecutionService.

**Services in the smoke path:**
1. **Mock DataService** (port 50051) → emits synthetic OHLC market data
2. **Signal Service** (port 50052) → consumes data, generates trading signals
3. **Mock ExecutionService** (port 50053) → receives OrderRequests, logs them

## Prerequisites

- Python 3.12+
- `signal-service` installed: `pip install -e .`
- `varon-fi` package with current proto definitions

## Quick Start

### 1. Start Mock DataService

```bash
python scripts/mock_dataservice.py 50051
```

Expected output:
```
Mock DataService running on port 50051
Emitting OHLC for: ['BTC', 'ETH']
```

### 2. Start Mock ExecutionService

```bash
python scripts/mock_executionservice.py 50053
```

Expected output:
```
Mock ExecutionService running on port 50053
Waiting for OrderRequests from Signal Service...
(ExecuteSignal RPC expects OrderRequest per current proto)
```

### 3. Start Signal Service E2E Runner

#### Option A: Mock Mode (No Database Required - Recommended)

```bash
# Optional: set environment variables
export DATASERVICE_GRPC_ADDR="localhost:50051"
export EXECUTIONSERVICE_GRPC_ADDR="localhost:50053"
export SIGNALSERVICE_GRPC_PORT="50052"

# Run with mock engine (no database needed)
python scripts/signal_service_e2e.py --mock
```

#### Option B: With Database

```bash
# Ensure Postgres is running with seeded strategies table
export DATABASE_URL="postgresql://postgres@localhost/varon_fi"
export DATASERVICE_GRPC_ADDR="localhost:50051"
export EXECUTIONSERVICE_GRPC_ADDR="localhost:50053"
export SIGNALSERVICE_GRPC_PORT="50052"

# Run with database-backed engine
python scripts/signal_service_e2e.py
```

Expected output:
```
============================================================
Signal Service - Local E2E Test
============================================================
DataService:      localhost:50051
Signal Service:   port 50052
ExecutionService: localhost:50053
============================================================
[OK] Connected to ExecutionService at localhost:50053
[OK] Connected to DataService at localhost:50051
[OK] Signal Service gRPC server running on port 50052

Waiting for OHLC data and generating signals...
(Press Ctrl+C to stop)

[Signal Service] Generated signal: BTC long
                 Confidence: 0.85, Strategy: mtf_confluence
```

## Service Ports

| Service | Port | Environment Variable | Proto Alignment |
|---------|------|---------------------|-----------------|
| DataService | 50051 | `DATASERVICE_GRPC_ADDR` | OHLC with TraceContext |
| Signal Service | 50052 | `SIGNALSERVICE_GRPC_PORT` | TradeSignal emission |
| ExecutionService | 50053 | `EXECUTIONSERVICE_GRPC_ADDR` | OrderRequest/TradingMode |

## Verification Steps

### 1. Check Data Flow

In the Signal Service terminal, you should see:
- `[OK] Connected to ...` messages for both services
- `[Signal Service] Generated signal: ...` when signals are triggered

### 2. Check ExecutionService Reception

In the ExecutionService terminal, you should see:
```
[ExecutionService] Received OrderRequest:
  Signal: <signal_id>
  Strategy: mtf_confluence (v1.0.0)
  Symbol: BTC long @ <price>
  Size: 0.0, Type: market, Mode: PAPER
  Total orders received: 1
```

### 3. Verify OrderRequest Structure

The mock ExecutionService logs should show:
- `signal_id`: UUID linking back to the signal
- `strategy_id` / `strategy_version`: Strategy metadata
- `symbol`: Trading pair (e.g., "BTC")
- `side`: "long" or "short"
- `price`: Entry price (0.0 for market)
- `order_type`: "market" or "limit"
- `mode`: TradingMode enum (PAPER=0, LIVE=1)
- `correlation_id`: For distributed tracing

## Proto Alignment

These scripts align with the current Platform v1 proto:

- **ExecuteSignal RPC**: Takes `OrderRequest` (not TradeSignal)
- **TradingMode enum**: `PAPER = 0`, `LIVE = 1`
- **TraceContext**: Includes correlation_id, idempotency_key, source_service, latency_ms, timestamp
- **OrderRequest fields**: signal_id, strategy_id, strategy_version, symbol, side, size, price, order_type, mode, risk_checks, trace

## Troubleshooting

### "cannot import name 'X' from 'varon_fi'"

**Solution**: Install or update the varon-fi package:
```bash
pip install git+https://github.com/varon-fi/python.git@<latest-commit-sha>
```

### "Connection refused" errors

**Solution**: Start services in correct order:
1. DataService first (port 50051)
2. ExecutionService second (port 50053)
3. Signal Service last (port 50052)

### No signals generated

**Check**:
- Signal Service loaded strategies from database (or mock mode)
- Strategy's `on_candle()` method returns a Signal
- OHLC data format matches strategy expectations (needs sufficient history)

### OrderRequests not reaching ExecutionService

**Check**:
- `EXECUTIONSERVICE_GRPC_ADDR` env var is set correctly
- ExecutionService is running before Signal Service starts
- Network connectivity between services

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATASERVICE_GRPC_ADDR` | `localhost:50051` | DataService gRPC address |
| `EXECUTIONSERVICE_GRPC_ADDR` | `localhost:50053` | ExecutionService gRPC address |
| `SIGNALSERVICE_GRPC_PORT` | `50052` | Port for Signal Service gRPC server |
| `DATABASE_URL` | `postgresql://...` | PostgreSQL connection (optional for mock) |

### Mock Data Configuration

Edit `scripts/mock_dataservice.py` to change:
- `self.symbols`: List of trading pairs to emit
- `self.price_seeds`: Base prices for each symbol
- Sleep interval: Frequency of OHLC emission (default: 5 seconds)

## Extending the Smoke Test

### Add More Symbols

Edit `scripts/mock_dataservice.py`:
```python
self.symbols = ['BTC', 'ETH', 'SOL', 'XRP']
self.price_seeds = {
    'BTC': 50000.0,
    'ETH': 3000.0,
    'SOL': 100.0,
    'XRP': 0.50,
}
```

### Test with Real Strategy

Ensure your strategy is loaded from the database or configure mock strategies in `signal_service_e2e.py`.

### Test Error Handling

Stop one service and verify others handle disconnection gracefully:
- Stop DataService → Signal Service should handle disconnect
- Stop ExecutionService → Signal Service should retry or log

## Known Limitations

1. **Mock DataService** generates synthetic OHLC, not realistic market patterns
2. **No database required** for basic smoke test (strategies can be mocked)
3. **No persistence** in mock ExecutionService (orders logged only)
4. **Single instance** only - no clustering or load balancing

## Next Steps

After successful smoke test:
1. Deploy to staging environment with real DataService
2. Test with real ExecutionService in paper trading mode
3. Add monitoring and alerting
4. Consider adding automated assertions (e.g., "expect N signals in M minutes")

## References

- Proto definition: `varon-fi/python/varon_fi.proto`
- Execution client: `src/signal_service/grpc/execution_client.py`
- Strategy engine: `src/signal_service/strategy/engine.py`
