"""gRPC client for ExecutionService."""

from typing import Optional
import uuid

import grpc
from structlog import get_logger
from google.protobuf.timestamp_pb2 import Timestamp

# Import from varon-fi package
from varon_fi.proto.varon_fi_pb2 import TradeSignal, OrderAck
from varon_fi.proto.varon_fi_pb2_grpc import ExecutionServiceStub

logger = get_logger(__name__)


class ExecutionServiceClient:
    """Client for sending signals to ExecutionService (Orders Service).
    
    Handles connection management, retry logic, timeout handling,
    and correlation ID chaining for observability.
    """
    
    def __init__(
        self, 
        addr: str,
        timeout: float = 5.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        self.addr = addr
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.channel: Optional[grpc.aio.Channel] = None
        self.stub: Optional[ExecutionServiceStub] = None
        
    async def connect(self):
        """Connect to ExecutionService."""
        self.channel = grpc.aio.insecure_channel(self.addr)
        self.stub = ExecutionServiceStub(self.channel)
        logger.info("Connected to ExecutionService", addr=self.addr)
        
    async def execute_signal(
        self, 
        signal: TradeSignal,
        correlation_id: Optional[str] = None,
    ) -> OrderAck:
        """Send a signal to ExecutionService with retry logic.
        
        Args:
            signal: The TradeSignal to execute
            correlation_id: Optional correlation ID for tracing
            
        Returns:
            OrderAck response from ExecutionService
            
        Raises:
            grpc.RpcError: If all retries exhausted
        """
        # Generate correlation ID if not provided, chain with signal_id
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        
        # Ensure signal has correlation_id set
        signal.correlation_id = correlation_id
        
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(
                    "Sending signal to ExecutionService",
                    attempt=attempt,
                    signal_id=signal.signal_id,
                    correlation_id=correlation_id,
                    strategy_id=signal.strategy_id,
                    symbol=signal.symbol,
                    side=signal.side,
                )
                
                response = await self.stub.ExecuteSignal(
                    signal, 
                    timeout=self.timeout,
                )
                
                logger.info(
                    "Signal accepted by ExecutionService",
                    signal_id=signal.signal_id,
                    correlation_id=correlation_id,
                    order_id=response.order_id if response.success else None,
                    success=response.success,
                )
                return response
                
            except grpc.RpcError as e:
                last_error = e
                logger.warning(
                    "ExecutionService call failed",
                    attempt=attempt,
                    max_retries=self.max_retries,
                    signal_id=signal.signal_id,
                    correlation_id=correlation_id,
                    error_code=e.code(),
                    error_details=e.details(),
                )
                
                if attempt < self.max_retries:
                    import asyncio
                    await asyncio.sleep(self.retry_delay * attempt)  # Exponential backoff
                    
        # All retries exhausted
        logger.error(
            "ExecutionService call failed after all retries",
            signal_id=signal.signal_id,
            correlation_id=correlation_id,
            max_retries=self.max_retries,
            error_code=last_error.code() if last_error else None,
            error_details=last_error.details() if last_error else None,
        )
        raise last_error
        
    async def disconnect(self):
        """Close connection to ExecutionService."""
        if self.channel:
            await self.channel.close()
            self.channel = None
            self.stub = None
        logger.info("Disconnected from ExecutionService")
