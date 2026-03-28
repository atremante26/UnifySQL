import time

import pytest

from unifysql.observability.tracer import Span


def test_span() -> None:
    """Pytest unit test for Span."""
    with Span("test_span") as span:
        assert isinstance(span, Span)
        time.sleep(0.1) # ~100ms

    assert span.latency_ms
    assert span.latency_ms >= 90
    assert span.latency_ms < 200

def test_span_exception_propagates() -> None:
    """Pytest unit test for Span errors."""
    with pytest.raises(ValueError):
        with Span("test"):
            raise ValueError("test error")
