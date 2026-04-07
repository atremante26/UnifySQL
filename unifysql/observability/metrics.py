from typing import List, Optional

from unifysql.observability.logger import get_logger
from unifysql.semantic.models import (
    CorrectionRecord,
    ErrorDetail,
    QueryResult,
    TranslationResult,
    ValidationResult,
)

# Instantiate logger
logger = get_logger()


def log_translation_result(
    translation_result: TranslationResult, validation_result: ValidationResult
) -> None:
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
        selection_rationale=translation_result.selection_rationale,
        validation_valid=validation_result.valid,
        validation_error_type=validation_result.error_type,
        validation_error_detail=validation_result.error_detail,
    )


def log_execution_result(
    query_result: QueryResult, error_detail: Optional[ErrorDetail]
) -> None:
    """Write a structured log record at the end of every execution request."""
    logger.info(
        "execution_request_completed",
        query_id=query_result.query_id,
        sql=query_result.sql,
        row_count=query_result.row_count,
        execution_ms=query_result.execution_ms,
        warehouse=query_result.warehouse.value,
        error_detail=error_detail.model_dump() if error_detail else None,
    )


def log_correction_stored(correction: CorrectionRecord) -> None:
    """Write a structured log record when a new correction is saved."""
    logger.info(
        "correction_record_saved",
        correction=correction.model_dump(),
        retrieval_count=correction.retrieval_count,
        schema_hash=correction.schema_hash,
        semantic_layer_version=correction.semantic_layer_version,
    )


def log_correction_retrieved(
    corrections: List[CorrectionRecord], similarity_scores: List[float]
) -> None:
    """Write a structured log record when corrections retrieved as few-shot examples."""
    logger.info(
        "correction_records_retrieved",
        n_corrections=len(corrections),
        corrected_query_ids=[c.correction.query_id for c in corrections],
        retrievals=[
            {"query_id": str(c.correction.query_id), "similarity": s}
            for c, s in zip(corrections, similarity_scores)
        ],
    )
