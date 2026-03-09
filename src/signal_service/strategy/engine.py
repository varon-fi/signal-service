"""Strategy engine - loads and executes strategies."""

from __future__ import annotations

import hashlib
import inspect
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import asyncpg
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp
from structlog import get_logger

import signal_service.strategy  # registers supported strategies
from varon_fi import BaseStrategy, Signal, StrategyConfig, create_strategy, list_strategies
from varon_fi.proto.varon_fi_pb2 import TraceContext, TradeSignal

logger = get_logger(__name__)

SUPPORTED_STRATEGY_NAMES = {"range_mean_reversion"}


class StrategyEngine:
    """Manages live strategies and generates signals."""

    def __init__(
        self,
        database_url: str,
    ):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
        self.strategies: dict[str, BaseStrategy] = {}
        # Track last processed candle per strategy/symbol/timeframe to prevent duplicate signals.
        self._last_candle_ts: dict[str, datetime] = {}
        # Track warmup requirements per strategy/symbol/timeframe.
        self._warmup_required: dict[str, int] = {}
        self._warmup_complete: dict[str, bool] = {}
        # Track latest candle timestamp seen at startup per symbol/timeframe.
        self._startup_latest_ts: dict[str, datetime] = {}
        # Lightweight instrumentation counters for per-candle evaluation health.
        self._metrics: dict[str, int] = {
            "candles_processed": 0,
            "strategies_evaluated": 0,
            "signals_emitted": 0,
            "signals_dropped": 0,
        }
        # Runtime artifact fingerprints keyed by strategy_id.
        self._strategy_fingerprints: dict[str, dict[str, str]] = {}
        # Track first-candle fingerprint log emission per strategy/symbol/timeframe.
        self._fingerprint_logged_combos: set[str] = set()

    async def initialize(self):
        """Initialize DB connection and load active strategies."""
        self.pool = await asyncpg.create_pool(self.database_url)
        await self._load_strategies()
        await self._initialize_startup_state()
        await self._initialize_positions_state()
        logger.info("StrategyEngine initialized", strategy_count=len(self.strategies))

    async def reload_strategies(self):
        """Reload strategies from the database."""
        self.strategies.clear()
        self._warmup_required.clear()
        self._warmup_complete.clear()
        self._strategy_fingerprints.clear()
        self._fingerprint_logged_combos.clear()
        await self._load_strategies()
        await self._initialize_startup_state()
        await self._initialize_positions_state()
        logger.info("Strategies reloaded", strategy_count=len(self.strategies))

    def get_required_subscriptions(self) -> dict[str, list[str]]:
        """Get all symbol/timeframe combinations required by active strategies."""
        subscriptions: dict[str, set[str]] = {}
        for strategy in self.strategies.values():
            for timeframe in strategy.timeframes:
                if timeframe not in subscriptions:
                    subscriptions[timeframe] = set()
                subscriptions[timeframe].update(strategy.symbols)
        return {tf: list(symbols) for tf, symbols in subscriptions.items()}

    async def _load_strategies(self):
        """Load all active strategies from database."""
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    s.id,
                    s.name,
                    s.version,
                    s.params,
                    s.init_periods,
                    s.status,
                    sc.symbol,
                    sc.timeframe,
                    sc.meta
                FROM strategies s
                JOIN strategy_configs sc
                  ON sc.strategy_id = s.id
                WHERE s.status = 'active'
                  AND sc.enabled = TRUE
                ORDER BY s.id, sc.symbol, sc.timeframe
                """
            )

        for row in rows:
            strategy = self._create_strategy(row)
            if strategy is None:
                continue

            strategy_id = str(row["id"])
            symbol = str(row["symbol"])
            timeframe = str(row["timeframe"])
            strategy_key = f"{strategy_id}:{symbol}:{timeframe}"
            self.strategies[strategy_key] = strategy

            fingerprint = self._build_strategy_fingerprint(strategy)
            self._strategy_fingerprints[strategy_key] = fingerprint
            logger.info(
                "Strategy runtime fingerprint",
                stage="startup",
                strategy_id=strategy_id,
                strategy_name=strategy.name,
                strategy_version=strategy.version,
                module_path=fingerprint["module_path"],
                module_sha256=fingerprint["module_sha256"],
                git_commit=fingerprint["git_commit"],
                params_hash=fingerprint["params_hash"],
            )

            init_periods = row.get("init_periods") if isinstance(row, dict) else row.get("init_periods")
            for symbol in strategy.symbols:
                for timeframe in strategy.timeframes:
                    warmup_key = f"{strategy_key}:{symbol}:{timeframe}"
                    required = int(init_periods) if init_periods else 0
                    self._warmup_required[warmup_key] = required
                    self._warmup_complete[warmup_key] = required == 0
                    logger.info(
                        "Strategy warmup initialized",
                        strategy=strategy.name,
                        symbol=symbol,
                        timeframe=timeframe,
                        min_bars=required,
                    )

    def _create_strategy(self, row: asyncpg.Record) -> Optional[BaseStrategy]:
        """Instantiate a strategy from DB row."""
        name = row.get("name") if isinstance(row, dict) else row["name"]
        if not name:
            logger.warning("Strategy missing name", row=row)
            return None

        if name not in SUPPORTED_STRATEGY_NAMES:
            logger.info("Skipping unsupported strategy", name=name)
            return None

        raw_params = row.get("params") if isinstance(row, dict) else row["params"]
        if raw_params is None:
            logger.warning("Strategy missing params; defaulting to empty", name=name)
            params: dict = {}
        else:
            parsed_params = json.loads(raw_params) if isinstance(raw_params, str) else raw_params
            if not isinstance(parsed_params, dict):
                logger.warning("Strategy params must be object; defaulting to empty", name=name)
                params = {}
            else:
                params = parsed_params

        raw_meta = row.get("meta") if isinstance(row, dict) else row["meta"]
        meta = json.loads(raw_meta) if isinstance(raw_meta, str) else (raw_meta or {})
        if not isinstance(meta, dict):
            logger.warning("Strategy config meta must be object; ignoring", name=name, meta_type=type(meta).__name__)
            meta = {}

        override_params = meta.get("strategy_params", {})
        if not isinstance(override_params, dict):
            logger.warning(
                "strategy_configs.meta.strategy_params must be object; ignoring override",
                name=name,
                override_type=type(override_params).__name__,
            )
            override_params = {}
        params = {**params, **override_params}

        version = row.get("version") if isinstance(row, dict) else row["version"]
        if not version:
            logger.warning("Strategy missing version; defaulting to 1.0.0", name=name)
            version = "1.0.0"

        config = StrategyConfig(name=name, params=params)
        symbol = row.get("symbol") if isinstance(row, dict) else row["symbol"]
        timeframe = row.get("timeframe") if isinstance(row, dict) else row["timeframe"]
        try:
            strategy = create_strategy(
                config,
                strategy_id=str(row["id"]),
                name=name,
                version=version,
                symbols=[str(symbol)],
                timeframes=[str(timeframe)],
            )
            return strategy
        except KeyError:
            logger.warning(
                "Unknown strategy type",
                name=name,
                available=list(list_strategies().keys()),
            )
            return None

    def _params_hash(self, params: dict | None) -> str:
        payload = json.dumps(params or {}, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _file_sha256(self, file_path: str) -> str:
        if not file_path:
            return ""
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            return ""
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _git_commit_for_path(self, file_path: str) -> str:
        if not file_path:
            return ""
        try:
            commit = subprocess.check_output(
                ["git", "-C", str(Path(file_path).parent), "rev-parse", "HEAD"],
                stderr=subprocess.DEVNULL,
                text=True,
            )
            return commit.strip()
        except Exception:
            return ""

    def _build_strategy_fingerprint(self, strategy: BaseStrategy) -> dict[str, str]:
        module_path = inspect.getsourcefile(strategy.__class__) or inspect.getfile(strategy.__class__)
        module_path = str(Path(module_path).resolve()) if module_path else ""
        params = getattr(strategy, "params", None) or {}
        return {
            "strategy_name": str(getattr(strategy, "name", "") or ""),
            "strategy_version": str(getattr(strategy, "version", "") or ""),
            "module_path": module_path,
            "module_sha256": self._file_sha256(module_path),
            "git_commit": self._git_commit_for_path(module_path),
            "params_hash": self._params_hash(params),
        }

    def _emit_first_candle_fingerprint(
        self,
        *,
        strategy_id: str,
        symbol: str,
        timeframe: str,
    ) -> None:
        combo_key = f"{strategy_id}:{symbol}:{timeframe}"
        if combo_key in self._fingerprint_logged_combos:
            return

        fp = self._strategy_fingerprints.get(strategy_id, {})
        logger.info(
            "Strategy runtime fingerprint",
            stage="first_candle",
            strategy_id=strategy_id,
            strategy_name=fp.get("strategy_name", ""),
            strategy_version=fp.get("strategy_version", ""),
            symbol=symbol,
            timeframe=timeframe,
            module_path=fp.get("module_path", ""),
            module_sha256=fp.get("module_sha256", ""),
            git_commit=fp.get("git_commit", ""),
            params_hash=fp.get("params_hash", ""),
        )
        self._fingerprint_logged_combos.add(combo_key)

    def _attach_strategy_fingerprint_meta(self, signal: Signal, strategy_id: str) -> None:
        fp = self._strategy_fingerprints.get(strategy_id, {})
        if signal.meta is None:
            signal.meta = {}
        signal.meta.update(
            {
                "strategy_runtime_name": fp.get("strategy_name", ""),
                "strategy_runtime_version": fp.get("strategy_version", ""),
                "strategy_artifact_path": fp.get("module_path", ""),
                "strategy_artifact_hash": fp.get("module_sha256", ""),
                "strategy_artifact_git_commit": fp.get("git_commit", ""),
                "strategy_params_hash": fp.get("params_hash", ""),
            }
        )

    async def _initialize_startup_state(self):
        """Initialize startup gating and warmup state."""
        if not self.strategies or self.pool is None:
            return

        combos: set[tuple[str, str]] = set()
        for strategy in self.strategies.values():
            for symbol in strategy.symbols:
                for timeframe in strategy.timeframes:
                    combos.add((symbol, timeframe))

        async with self.pool.acquire() as conn:
            for symbol, timeframe in combos:
                row = await conn.fetchrow(
                    """
                    SELECT MAX(ts) as ts
                    FROM ohlcs o
                    JOIN instruments i ON o.instrument_id = i.id
                    WHERE i.symbol = $1 AND o.timeframe = $2
                    """,
                    symbol,
                    timeframe,
                )
                if row and row["ts"]:
                    self._startup_latest_ts[f"{symbol}:{timeframe}"] = row["ts"]

        for strategy_id, strategy in self.strategies.items():
            lookback_days = (strategy.params or {}).get("lookback_days")
            for symbol in strategy.symbols:
                for timeframe in strategy.timeframes:
                    warmup_key = f"{strategy_id}:{symbol}:{timeframe}"
                    required = self._warmup_required.get(warmup_key, 0)
                    lookback_bars = self._calc_lookback_bars(timeframe, lookback_days)
                    bars_needed = max(required, lookback_bars) if lookback_bars else required
                    if bars_needed <= 0:
                        continue
                    history = await self._fetch_history(symbol, timeframe, bars=bars_needed)
                    if len(history) >= required:
                        self._warmup_complete[warmup_key] = True
                        logger.info(
                            "Strategy warmup complete",
                            strategy=strategy.name,
                            symbol=symbol,
                            timeframe=timeframe,
                            history_bars=len(history),
                        )
                    else:
                        self._warmup_complete[warmup_key] = False
                        logger.info(
                            "Strategy warmup pending",
                            strategy=strategy.name,
                            symbol=symbol,
                            timeframe=timeframe,
                            history_bars=len(history),
                            required_bars=required,
                        )

    async def _initialize_positions_state(self):
        """Hydrate strategy position state from latest canonical signal history."""
        if not self.strategies or not self.pool:
            return

        async with self.pool.acquire() as conn:
            for strategy_key, strategy in self.strategies.items():
                if not hasattr(strategy, "_positions"):
                    continue

                strategy_id = str(getattr(strategy, "strategy_id", "") or strategy_key)
                for symbol in strategy.symbols:
                    row = await conn.fetchrow(
                        """
                        SELECT s.signal_type, s.signal_value, s.ts, s.payload
                        FROM signals s
                        JOIN instruments i ON s.instrument_id = i.id
                        WHERE s.strategy_id = $1
                          AND i.symbol = $2
                        ORDER BY s.ts DESC
                        LIMIT 1
                        """,
                        strategy_id,
                        symbol,
                    )
                    if not row:
                        continue

                    signal_type = str(row["signal_type"] or "").upper()
                    if signal_type in {"FLAT", "NONE", "UNKNOWN", ""}:
                        continue

                    if signal_type in {"LONG", "BUY"}:
                        side = "long"
                    elif signal_type in {"SHORT", "SELL"}:
                        side = "short"
                    else:
                        continue

                    signal_value = row["signal_value"]
                    entry_price = float(signal_value) if signal_value is not None else None
                    entry_ts_raw = row["ts"]
                    entry_ts = (
                        self._normalize_candle_ts(entry_ts_raw)
                        if entry_ts_raw is not None
                        else None
                    )

                    payload = row["payload"]
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload) if payload else {}
                        except json.JSONDecodeError:
                            payload = {}
                    payload = payload or {}
                    try:
                        entry_deviation = float(payload.get("deviation_pct", 0.0))
                    except (TypeError, ValueError):
                        entry_deviation = 0.0

                    strategy._positions[symbol] = {
                        "side": side,
                        "entry_price": entry_price,
                        "entry_ts": entry_ts,
                        "entry_deviation": entry_deviation,
                    }
                    logger.info(
                        "Hydrated position state",
                        source="signals_history",
                        strategy=strategy.name,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        signal_type=signal_type,
                        side=side,
                        entry_price=entry_price,
                        entry_ts=entry_ts,
                    )

    def _calc_lookback_bars(self, timeframe: str, lookback_days: Optional[int]) -> int:
        """Convert lookback_days into number of bars for a timeframe."""
        if not lookback_days or not timeframe:
            return 0
        tf = str(timeframe).strip().lower()
        try:
            if tf.endswith("m"):
                minutes = int(tf[:-1])
                if minutes <= 0:
                    return 0
                bars_per_day = int((24 * 60) / minutes)
                return int(lookback_days) * bars_per_day
            if tf.endswith("h"):
                hours = int(tf[:-1])
                if hours <= 0:
                    return 0
                bars_per_day = int(24 / hours)
                return int(lookback_days) * bars_per_day
            if tf.endswith("d"):
                days = int(tf[:-1]) if tf[:-1] else 1
                if days <= 0:
                    return 0
                return int(lookback_days / days)
        except Exception:
            return 0
        return 0

    def _in_strategy_session(self, strategy: BaseStrategy, candle_ts: Optional[datetime]) -> bool:
        """Check per-strategy session window if available."""
        if candle_ts is None:
            return True

        if hasattr(strategy, "_in_session") and callable(getattr(strategy, "_in_session")):
            try:
                return bool(strategy._in_session(candle_ts))
            except Exception:
                return True

        params = getattr(strategy, "params", None) or {}
        session_start = params.get("session_start") or getattr(strategy, "session_start", None)
        session_end = params.get("session_end") or getattr(strategy, "session_end", None)
        if not session_start or not session_end:
            return True

        if isinstance(session_start, str):
            session_start = datetime.strptime(session_start, "%H:%M").time()
        if isinstance(session_end, str):
            session_end = datetime.strptime(session_end, "%H:%M").time()

        current_time = candle_ts.time()
        return session_start <= current_time <= session_end

    async def process_candle_signals(self, ohlc: dict) -> list[Signal]:
        """Process an OHLC candle and return all generated signals."""
        symbol = ohlc.get("symbol")
        timeframe = ohlc.get("timeframe")

        signals: list[Signal] = []
        strategies_evaluated = 0
        signals_emitted = 0

        for strategy_key, strategy in self.strategies.items():
            if symbol not in strategy.symbols or timeframe not in strategy.timeframes:
                continue

            strategies_evaluated += 1
            strategy_id = str(getattr(strategy, "strategy_id", "") or strategy_key.split(":", 1)[0])

            warmup_key = f"{strategy_key}:{symbol}:{timeframe}"
            required = self._warmup_required.get(warmup_key, 0)
            lookback_days = (strategy.params or {}).get("lookback_days")
            lookback_bars = self._calc_lookback_bars(timeframe, lookback_days)
            bars_needed = max(200, required, lookback_bars) if lookback_bars else max(200, required)
            history = await self._fetch_history(symbol, timeframe, bars=bars_needed)
            confirmed_ohlc, confirmed_history = self._confirmed_candle_view(
                symbol=symbol,
                timeframe=timeframe,
                incoming_ohlc=ohlc,
                history=history,
            )
            if confirmed_ohlc is None or confirmed_history.empty:
                logger.debug(
                    "Skipping signal - no confirmed candle available",
                    strategy=strategy.name,
                    symbol=symbol,
                    timeframe=timeframe,
                )
                continue
            confirmed_ts = self._normalize_candle_ts(
                confirmed_ohlc.get("timestamp") or confirmed_ohlc.get("ts")
            )

            if not self._in_strategy_session(strategy, confirmed_ts):
                continue

            if confirmed_ts is not None:
                startup_key = f"{symbol}:{timeframe}"
                startup_ts = self._startup_latest_ts.get(startup_key)
                if startup_ts is not None and confirmed_ts <= startup_ts:
                    continue

            if confirmed_ts is not None:
                dedupe_key = f"{strategy_key}:{symbol}:{timeframe}"
                last_ts = self._last_candle_ts.get(dedupe_key)
                if last_ts is not None and confirmed_ts <= last_ts:
                    continue
                self._last_candle_ts[dedupe_key] = confirmed_ts

            if required > 0 and not self._warmup_complete.get(warmup_key, True):
                if len(confirmed_history) < required:
                    logger.debug(
                        "Skipping signal - warmup in progress",
                        strategy=strategy.name,
                        symbol=symbol,
                        timeframe=timeframe,
                        history_bars=len(confirmed_history),
                        required_bars=required,
                    )
                    continue
                self._warmup_complete[warmup_key] = True
                logger.info(
                    "Strategy warmup complete",
                    strategy=strategy.name,
                    symbol=symbol,
                    timeframe=timeframe,
                    history_bars=len(confirmed_history),
                )

            self._emit_first_candle_fingerprint(
                strategy_id=strategy_key,
                symbol=symbol,
                timeframe=timeframe,
            )

            signal = strategy.on_candle(confirmed_ohlc, confirmed_history)
            if not signal:
                continue

            signal.strategy_id = strategy_id
            signal.strategy_version = strategy.version
            signal.symbol = symbol
            signal.timeframe = timeframe

            if signal.meta is None:
                signal.meta = {}

            self._attach_strategy_fingerprint_meta(signal, strategy_key)

            signal_db_id = await self._persist_signal(signal)
            if signal_db_id:
                signal.signal_db_id = signal_db_id

            logger.info(
                "Signal generated",
                strategy=strategy.name,
                symbol=symbol,
                side=signal.side,
                correlation_id=signal.correlation_id,
            )
            signals.append(signal)
            signals_emitted += 1

        self._metrics["candles_processed"] += 1
        self._metrics["strategies_evaluated"] += strategies_evaluated
        self._metrics["signals_emitted"] += signals_emitted

        if strategies_evaluated > 0:
            logger.debug(
                "Candle strategy evaluation summary",
                symbol=symbol,
                timeframe=timeframe,
                strategies_evaluated=strategies_evaluated,
                signals_emitted=signals_emitted,
                signals_dropped=0,
            )

        return signals

    def _confirmed_candle_view(
        self,
        *,
        symbol: str,
        timeframe: str,
        incoming_ohlc: dict,
        history: pd.DataFrame,
    ) -> tuple[Optional[dict], pd.DataFrame]:
        """Evaluate strategies on the latest confirmed candle, not the just-opened bar."""
        if history.empty:
            return None, history

        history_df = history.copy()
        ts_col = "timestamp" if "timestamp" in history_df.columns else "ts" if "ts" in history_df.columns else None
        if ts_col is None:
            return None, history_df.iloc[0:0]

        history_df = history_df.sort_values(ts_col).reset_index(drop=True)
        incoming_ts = self._normalize_candle_ts(incoming_ohlc.get("timestamp") or incoming_ohlc.get("ts"))

        # When a new bar first appears in `ohlcs`, it is still forming and may be
        # reconciled later. Evaluate the previous completed candle instead.
        if incoming_ts is not None:
            ts_series = history_df[ts_col].apply(self._normalize_candle_ts)
            history_df = history_df.loc[ts_series < incoming_ts].reset_index(drop=True)

        if history_df.empty:
            return None, history_df

        confirmed = history_df.iloc[-1]
        confirmed_ohlc = {
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp": confirmed[ts_col],
            "open": float(confirmed["open"]),
            "high": float(confirmed["high"]),
            "low": float(confirmed["low"]),
            "close": float(confirmed["close"]),
            "volume": float(confirmed["volume"]),
        }
        if "count" in confirmed.index and confirmed["count"] is not None and not pd.isna(confirmed["count"]):
            confirmed_ohlc["count"] = int(confirmed["count"])

        return confirmed_ohlc, history_df

    async def process_candle(self, ohlc: dict) -> Optional[Signal]:
        """Backward-compatible wrapper returning the first generated signal."""
        signals = await self.process_candle_signals(ohlc)
        return signals[0] if signals else None

    def get_metrics_snapshot(self) -> dict[str, int]:
        """Return a copy of lightweight processing counters."""
        return dict(self._metrics)

    def _normalize_candle_ts(self, ts) -> Optional[datetime]:
        """Normalize candle timestamp to timezone-aware UTC datetime."""
        if ts is None:
            return None

        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts, timezone.utc)
        elif isinstance(ts, datetime):
            dt = ts
        elif hasattr(ts, "seconds") and hasattr(ts, "nanos"):
            dt = datetime.fromtimestamp(ts.seconds, timezone.utc)
        elif isinstance(ts, str):
            dt = pd.to_datetime(ts)
        elif hasattr(ts, "ToDatetime"):
            dt = ts.ToDatetime(tzinfo=timezone.utc)
        else:
            return None

        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt

    def _to_trade_signal(self, signal: Signal, signal_db_id: Optional[str] = None) -> TradeSignal:
        """Convert internal Signal to TradeSignal protobuf."""
        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)

        meta = self._normalize_meta(signal.meta)

        trace = TraceContext(
            correlation_id=signal.correlation_id if signal.correlation_id else str(uuid.uuid4()),
            idempotency_key=signal.idempotency_key if signal.idempotency_key else str(uuid.uuid4()),
            source_service="signal-service",
            latency_ms=0,
            timestamp=timestamp,
        )

        return TradeSignal(
            signal_id=signal_db_id or signal.idempotency_key or str(uuid.uuid4()),
            strategy_id=signal.strategy_id or "",
            strategy_version=signal.strategy_version or "",
            symbol=signal.symbol or "",
            timeframe=signal.timeframe or "5m",
            side=signal.side,
            price=signal.price or 0.0,
            confidence=signal.confidence,
            meta={str(k): "" if v is None else str(v) for k, v in meta.items()},
            trace=trace,
        )

    def _normalize_meta(self, meta_value) -> dict:
        """Normalize meta field to Python dict."""
        if meta_value is None:
            return {}
        if isinstance(meta_value, dict):
            return meta_value
        if isinstance(meta_value, str):
            try:
                return json.loads(meta_value) if meta_value else {}
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse meta as JSON: {meta_value[:100]}...")
                return {}
        logger.warning(f"Unexpected meta type {type(meta_value)}, using empty dict")
        return {}

    async def _fetch_history(self, symbol: str, timeframe: str, bars: int = 200) -> pd.DataFrame:
        """Fetch recent OHLC history from canonical ohlcs table."""
        bars = int(bars or 0)
        if bars <= 0:
            bars = 1

        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ts as timestamp, open, high, low, close, volume
                FROM ohlcs o
                JOIN instruments i ON o.instrument_id = i.id
                WHERE i.symbol = $1 AND o.timeframe = $2
                ORDER BY ts DESC
                LIMIT $3
                """,
                symbol,
                timeframe,
                bars,
            )

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        if df.empty:
            return df
        return df.sort_values("timestamp").reset_index(drop=True)

    async def _persist_signal(self, signal: Signal) -> Optional[str]:
        """Persist signal to database. Returns signal UUID (id)."""
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            instrument_id = await conn.fetchval(
                """
                SELECT id FROM instruments WHERE symbol = $1
                """,
                signal.symbol,
            )

            if instrument_id is None:
                logger.warning("Unknown instrument for signal", symbol=signal.symbol)
                return None

            signal_type = signal.side.upper() if signal.side else "UNKNOWN"
            signal_value = float(signal.price) if signal.price else 0.0

            row = await conn.fetchrow(
                """
                INSERT INTO signals
                (exchange_id, instrument_id, strategy_id, strategy_version,
                 signal_type, signal_value, confidence, payload,
                 idempotency_key, correlation_id)
                VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id
                """,
                instrument_id,
                signal.strategy_id,
                signal.strategy_version,
                signal_type,
                signal_value,
                signal.confidence,
                json.dumps(signal.meta),
                signal.idempotency_key,
                signal.correlation_id,
            )
            return str(row["id"]) if row else None

    async def shutdown(self):
        """Cleanup resources."""
        if self.pool:
            await self.pool.close()
        logger.info("StrategyEngine shutdown")
