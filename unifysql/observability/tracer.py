import time
from types import TracebackType
from typing import Literal, Optional, Type

from unifysql.observability.logger import get_logger

# Instantiate logger
logger = get_logger()

class Span:
    """Lightweight Span context manager to calculate latency of pipeline operations."""
    def __init__(self, stage: str):
        """Initialize Span for stage."""
        self.stage = stage
        self.start_time = time.time()
        self.latency_ms = 0.0

    def __enter__(self) -> "Span":
        """Record Span instance start time."""
        self.start_time = time.time()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> Literal[False]:
        """Record Span instance end time."""
        self.latency_ms = (time.time() - self.start_time) * 1000.0
        logger.info("span_completed", stage=self.stage, latency_ms=self.latency_ms)
        return False
