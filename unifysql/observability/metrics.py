from typing import Optional

from unifysql.observability.logger import get_logger
from unifysql.semantic.models import ErrorDetail, QueryResult, TranslationResult

# Instantiate logger
logger = get_logger()

def log_translation_result(translation_result: TranslationResult) -> None:
    """Write a structured log record at the end of every translation request."""
    logger.info(
        "translation_request_completed",
        query_id=translation_result.query_id,
        sql=translation_result.sql,
        dialect=translation_result.dialect,
        confidence=translation_result.confidence,
        semantic_layer_version=translation_result.semantic_layer_version,
        model_used=translation_result.model_used,
        tables_used=translation_result.tables_used,
        joins_used=translation_result.joins_used,
        few_shot_count=translation_result.few_shot_count,
        latency_ms=translation_result.latency_ms,
        token_count=translation_result.token_count,
        selection_rationale=translation_result.selection_rationale
    )

def log_execution_result(
        query_result: QueryResult,
        error_detail: Optional[ErrorDetail]
) -> None:
    """Write a structured log record at the end of every execution request."""
    logger.info(
        "execution_request_completed",
        query_id=query_result.query_id,
        sql=query_result.sql,
        row_count=query_result.row_count,
        execution_ms=query_result.execution_ms,
        warehouse=query_result.warehouse.value,
        error_detail=error_detail.model_dump() if error_detail else None
    )
