import asyncio
from typing import Any, Dict, List
from uuid import uuid4

from google.cloud import bigquery

from unifysql.config import settings
from unifysql.execution.executor import BaseExecutor
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.models import QueryResult, WarehouseType

# Instantiate logger
logger = get_logger()


class BigQueryExecutor(BaseExecutor):
    def __init__(self, connection_string: str) -> None:
        self.bq_project = connection_string

    async def execute(self, sql: str) -> QueryResult:
        """Executes a validated SQL query against BigQuery asynchronously."""
        # Initialize client
        client = bigquery.Client(project=self.bq_project)

        # Initialize result set
        result_set: Dict[str, List[Any]] = {}

        # Execute query
        try:
            with Span("db_execution") as span:

                def _run_query() -> Any:
                    return client.query(sql).result()

                records = await asyncio.wait_for(
                    asyncio.to_thread(_run_query),
                    timeout=settings.db_execution_timeout_s,
                )
            logger.info("bigquery_query_executed", sql=sql, latency_ms=span.latency_ms)

        except asyncio.TimeoutError:
            logger.error("bigquery_execution_timeout", sql=sql)
            raise
        finally:
            client.close() # type: ignore[no-untyped-call]

        # Format records
        rows = list(records)
        columns = [field.name for field in records.schema]
        result_set = {col: [row[col] for row in rows] for col in columns}

        # Return QueryResult
        return QueryResult(
            query_id=uuid4(),
            sql=sql,
            result_set=result_set,
            row_count=len(rows) if records else 0,
            execution_ms=span.latency_ms,
            warehouse=WarehouseType.big_query,
        )
