import asyncio
from typing import Any, Dict, List, Tuple
from uuid import uuid4

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from unifysql.config import settings
from unifysql.execution.executor import BaseExecutor
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.models import QueryResult, WarehouseType

# Instantiate logger
logger = get_logger()


class SnowflakeExecutor(BaseExecutor):
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def _run_query(
        self, connection: SnowflakeConnection, sql: str
    ) -> Tuple[List[str], List[Any]]:
        """Executes SQL synchronously in a thread pool."""
        cursor = connection.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return columns, rows

    async def execute(self, sql: str) -> QueryResult:
        """
        Executes a validated SQL query against Snowflake asynchronously.
        """
        # Initialize connection
        connection: SnowflakeConnection | None = None

        # snowflake-connector-python has incomplete type stubs — ignore mypy here
        def _connect() -> SnowflakeConnection:
            return snowflake.connector.connect(self.connection_string)  # type: ignore[call-arg]

        connection = await asyncio.to_thread(_connect)

        # Initialize result set
        result_set: Dict[str, List[Any]] = {}

        try:
            with Span("db_execution") as span:
                records = await asyncio.wait_for(
                    asyncio.to_thread(self._run_query, connection, sql),
                    timeout=settings.db_execution_timeout_s,
                )
            logger.info("snowflake_query_executed", sql=sql, latency_ms=span.latency_ms)

        except asyncio.TimeoutError:
            logger.error("snowflake_execution_timeout", sql=sql)
            raise

        finally:
            if connection:
                connection.close()

        # Format records
        if records:
            columns, rows = records
            result_set = {
                col: [row[i] for row in rows] for i, col in enumerate(columns)
            }

        # Return QueryResult
        return QueryResult(
            query_id=uuid4(),
            sql=sql,
            result_set=result_set,
            row_count=len(records[1]) if records else 0,
            execution_ms=span.latency_ms,
            warehouse=WarehouseType.snowflake,
        )
