import asyncio
from typing import Any, Dict, List
from uuid import uuid4

import asyncpg

from unifysql.config import settings
from unifysql.execution.executor import BaseExecutor
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.models import QueryResult, WarehouseType

# Instantiate logger
logger = get_logger()


class PostgresExecutor(BaseExecutor):
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    async def execute(self, sql: str) -> QueryResult:
        """
        Executes a validated SQL query against Postgres asynchronously.
        """
        # Connect to Postgres
        connection = await asyncpg.connect(self.connection_string)

        # Initialize result set
        result_set: Dict[str, List[Any]] = {}

        try:
            with Span("db_execution") as span:
                records = await asyncio.wait_for(
                    connection.fetch(sql),
                    timeout=settings.db_execution_timeout_s,
                )
            logger.info("postgres_query_executed", sql=sql, latency_ms=span.latency_ms)

        except asyncio.TimeoutError:
            logger.error("postgres_execution_timeout", sql=sql)
            raise

        finally:
            await connection.close()

        # Format records
        if records:
            columns = records[0].keys()
            result_set = {col: [dict(r)[col] for r in records] for col in columns}

        # Return QueryResult
        return QueryResult(
            query_id=uuid4(),
            sql=sql,
            result_set=result_set,
            row_count=len(records) if records else 0,
            execution_ms=span.latency_ms,
            warehouse=WarehouseType.postgres,
        )
