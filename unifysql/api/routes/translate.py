import asyncio
from typing import Tuple
from uuid import uuid4

from flask import Blueprint, Response, jsonify, request

from unifysql.api.models import TranslateRequest, TranslateResponse
from unifysql.config import settings
from unifysql.execution.bigquery_executor import BigQueryExecutor
from unifysql.execution.executor import BaseExecutor
from unifysql.execution.postgres_executor import PostgresExecutor
from unifysql.execution.snowflake_executor import SnowflakeExecutor
from unifysql.observability.logger import get_logger
from unifysql.observability.metrics import log_translation_result
from unifysql.semantic.models import JoinSource, TranslationRequest, TranslationResult
from unifysql.semantic.models import ValidationResult as PipelineValidationResult
from unifysql.semantic.store import SemanticLayerStore
from unifysql.translation.compiler import Compiler
from unifysql.translation.context_builder import ContextBuilder
from unifysql.translation.translator import Translator
from unifysql.translation.validator import Validator

# Instantiate logger
logger = get_logger()

translate_bp = Blueprint("translate", __name__)


@translate_bp.route("/translate", methods=["POST"])
def translate_question() -> Tuple[Response, int]:
    """Runs the full online translation pipeline for a natural language question."""
    # Parse and validate request
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    try:
        req = TranslateRequest.model_validate(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    try:
        # Load semantic layer
        semantic_store = SemanticLayerStore()
        semantic_layer = semantic_store.load_by_schema_id(schema_id=req.schema_id)

        # Build context
        context_builder = ContextBuilder(model_name=req.model_preference)
        context_result = context_builder.build_context(
            question=req.question, schema_id=req.schema_id
        )
        logger.info(
            "context_built",
            schema_id=str(req.schema_id),
            n_tables=len(context_result.relevant_tables),
            n_corrections=len(context_result.few_shot_corrections),
        )

        # Translate question (Text-to-SQL)
        translator = Translator(model_name=req.model_preference)
        translation_request = TranslationRequest(
            question=req.question,
            schema_id=req.schema_id,
            dialect=req.dialect,
            model_preference=req.model_preference,
            execute=req.execute,
            preview=req.preview,
        )
        gen_sql = translator.translate(
            context=context_result, request=translation_request
        )
        logger.info("sql_generated", model=translator.model_name)

        # Compile generated result into dialect
        compiler = Compiler()
        compiler_result = compiler.compile(
            sql=gen_sql, dialect=req.dialect, preview=req.preview
        )

        if not compiler_result.validation.valid:
            logger.warning(
                "compilation_failed", error_type=compiler_result.validation.error_type
            )
            return (
                jsonify(
                    {
                        "error_type": str(compiler_result.validation.error_type),
                        "error_detail": str(compiler_result.validation.error_detail),
                    }
                ),
                400,
            )
        logger.info("sql_compiled", dialect=req.dialect, preview=req.preview)

        # Validate compiled SQL against semantic layer
        validator = Validator()
        validation_result = validator.validate(
            sql=compiler_result.sql, semantic_layer=semantic_layer
        )

        if not validation_result.valid:
            logger.warning("validation_failed", error_type=validation_result.error_type)
            return (
                jsonify(
                    {
                        "error_type": str(validation_result.error_type),
                        "error_detail": str(validation_result.error_detail),
                    }
                ),
                400,
            )
        logger.info("sql_validated", valid=validation_result.valid)

        # Compute confidence score and warning
        confidence = 1.0
        warnings = []

        # Deduct for no corrections retrieved
        if not context_result.few_shot_corrections:
            confidence -= 0.10

        # Deduct for fallback model used
        # Check if model_preference differs from used model
        if translator.model_name == settings.fallback_model:
            confidence -= 0.20

        for table_entry in context_result.relevant_tables.values():
            for join in table_entry.joins:
                if join.join_source == JoinSource.inferred:
                    warnings.append(f"inferred join used: {join.on_clause}")
                if (
                    join.join_source == JoinSource.llm_inferred
                    and join.join_confidence
                    < settings.join_confidence_execution_threshold
                ):
                    warnings.append(f"low-confidence join excluded: {join.on_clause}")

        if confidence < 0.7:
            warnings.append("low confidence score; results may be unreliable")

        # Optional execution
        sql_result = None
        if req.execute:
            executor: BaseExecutor
            if req.dialect.lower() == "postgres":
                executor = PostgresExecutor(connection_string=str(settings.postgres_url))
            elif req.dialect.lower() == "snowflake":
                executor = SnowflakeExecutor(connection_string=str(settings.snowflake_dsn))
            else:
                executor = BigQueryExecutor(connection_string=str(settings.bq_project))

            sql_result = asyncio.run(executor.execute(compiler_result.sql))
            logger.info(
                "sql_executed", row_count=sql_result.row_count if sql_result else 0
            )

        # Build response model
        response = TranslateResponse(
            query_id=uuid4(),
            sql=compiler_result.sql,
            confidence=confidence,
            warnings=warnings,
            selection_rationale=context_result.selection_rationale,
            model_used=translator.model_name,
            semantic_layer_version=semantic_layer.version,
            result=sql_result,
        )

        # Log translation result
        log_translation_result(
            translation_result=TranslationResult(
                query_id=response.query_id,
                sql=compiler_result.sql,
                dialect=req.dialect,
                confidence=confidence,
                semantic_layer_version=semantic_layer.version,
                model_used=translator.model_name,
                tables_used=list(context_result.relevant_tables.keys()),
                joins_used=[
                    j.on_clause
                    for t in context_result.relevant_tables.values()
                    for j in t.joins
                ],
                few_shot_count=len(context_result.few_shot_corrections),
                latency_ms=0.0,
                token_count=0,
                selection_rationale=context_result.selection_rationale,
            ),
            validation_result=PipelineValidationResult(
                valid=validation_result.valid,
                error_type=validation_result.error_type,
                error_detail=validation_result.error_detail,
            ),
        )
        # Serialize to JSON and return
        return jsonify(response.model_dump(mode="json")), 200
    except FileNotFoundError:
        return jsonify({"error": "Schema not found"}), 404
    except Exception as e:
        logger.error("translate_failed", error=str(e))
        return jsonify({"error": str(e)}), 500
