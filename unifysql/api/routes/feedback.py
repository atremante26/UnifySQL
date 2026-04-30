from datetime import datetime
from typing import Tuple
from uuid import UUID

from flask import Blueprint, Response, jsonify, request

from unifysql.api.models import FeedbackRequest, FeedbackResponse
from unifysql.feedback.retriever import FeedbackRetriever
from unifysql.feedback.store import FeedbackStore
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import Correction, CorrectionRecord
from unifysql.semantic.store import SemanticLayerStore
from unifysql.translation.validator import Validator

# Instantiate logger
logger = get_logger()

feedback_bp = Blueprint("feedback", __name__)


@feedback_bp.route("/feedback", methods=["POST"])
def add_correction() -> Tuple[Response, int]:
    """Stores a SQL correction and embeds it for future few-shot retrieval."""
    # Parse and validate request
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    try:
        req = FeedbackRequest.model_validate(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    try:
        # Load semantic layer
        semantic_store = SemanticLayerStore()
        semantic_layer = semantic_store.load_by_schema_id(schema_id=req.schema_id)

        # Validate compiled SQL against semantic layer
        validator = Validator()
        validation_result = validator.validate(
            sql=req.corrected_sql, semantic_layer=semantic_layer
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
        logger.info("correction_sql_validated", schema_id=str(req.schema_id))

        # Embed question
        feedback_store = FeedbackStore()
        retriever = FeedbackRetriever(feedback_store=feedback_store)
        correction = Correction(
            query_id=req.query_id,
            question=req.question,
            bad_sql=req.bad_sql,
            corrected_sql=req.corrected_sql,
            schema_id=req.schema_id,
            created_at=datetime.now(),
        )
        embedding = retriever.embed_correction(correction=correction)
        logger.info("correction_embedded", schema_id=str(req.schema_id))

        # Construct CorrectionRecord
        correction_record = CorrectionRecord(
            correction=correction,
            embedding_vector=embedding,
            retrieval_count=0,
            schema_hash=semantic_layer.schema_hash,
            semantic_layer_version=semantic_layer.version,
        )

        # Store CorrectionRecord
        correction_id = feedback_store.insert(correction_record=correction_record)
        logger.info(
            "correction_stored",
            correction_id=correction_id,
            schema_id=str(req.schema_id),
        )

        # Build response model
        response = FeedbackResponse(
            correction_id=UUID(correction_id),
            retrieval_count=0,
            validation="passed" if validation_result.valid else "failed",
        )

        # Serialize to JSON and return
        return jsonify(response.model_dump(mode="json")), 200

    except FileNotFoundError:
        return jsonify({"error": "Schema not found"}), 404
    except Exception as e:
        logger.error("feedback_failed", error=str(e))
        return jsonify({"error": str(e)}), 500
