from typing import cast
from uuid import UUID

import structlog


def bind_query_id(query_id: UUID) -> None:
    """Bind a query_id to structlog logger."""
    # Clear existing context variables
    structlog.contextvars.clear_contextvars()

    # Bind query_id to logger as context variable
    structlog.contextvars.bind_contextvars(query_id=query_id)


def configure_logging() -> None:
    """Configure struclog logger."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )


def get_logger() -> structlog.stdlib.BoundLogger:
    """Return structlog logger."""
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger())
