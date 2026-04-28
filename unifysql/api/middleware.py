import signal
import time
from typing import Tuple
from uuid import uuid4

from flask import Flask, Response, g, jsonify, request

from unifysql.config import settings
from unifysql.observability.logger import bind_query_id, get_logger

# Instantiate logger
logger = get_logger()


def _timeout_handler(signum: int, frame: object) -> None:
    """Raises TimeoutError when SIGALRM fires after E2E budget exceeded."""
    raise TimeoutError("Request exceeded E2E timeout budget")


# Register SIGALRM handler — fires when signal.alarm() countdown expires
# Note: SIGALRM is Unix-only
signal.signal(signal.SIGALRM, _timeout_handler)


def register_middleware(app: Flask) -> None:
    """
    Registers before/after request hooks and error handlers on the Flask app.
    """

    @app.before_request
    def before() -> None:
        # Generate a unique query_id for this request
        # All downstream log records will include this automatically
        query_id_uuid = uuid4()
        g.query_id = str(query_id_uuid)

        # Bind query_id to structlog context — threads through all log records
        bind_query_id(query_id_uuid)

        # Record start time for latency calculation in after_request
        g.start_time = time.time()

        # Set hard E2E timeout — raises TimeoutError if request exceeds budget
        signal.alarm(settings.e2e_timeout_s)

        logger.info(
            "request_received",
            path=request.path,
            method=request.method,
            query_id=g.query_id,
        )

    @app.after_request
    def after(response: Response) -> Response:
        # Cancel the E2E timeout since request completed within budget
        signal.alarm(0)

        # Compute total request latency
        latency_ms = (time.time() - g.start_time) * 1000

        logger.info(
            "request_completed",
            path=request.path,
            status=response.status_code,
            latency_ms=latency_ms,
            query_id=g.query_id,
        )
        return response

    @app.errorhandler(Exception)
    def handle_error(e: Exception) -> Tuple[Response, int]:
        # Catch all unhandled exceptions — log and return 500
        logger.error("unhandled_error", error=str(e), query_id=g.get("query_id"))
        return jsonify({"error": "internal server error"}), 500
