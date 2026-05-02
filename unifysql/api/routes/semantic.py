import threading
from datetime import datetime
from typing import Tuple
from uuid import UUID, uuid4

from flask import Blueprint, Response, g, jsonify, request
from sqlalchemy import create_engine

from unifysql.api.models import (
    DiffResponse,
    SchemaRegistrationRequest,
    SchemaRegistrationResponse,
    SemanticLayerResponse,
)
from unifysql.config import settings
from unifysql.ingestion.adaptor import BaseAdaptor
from unifysql.ingestion.bigquery_adaptor import BigQueryAdaptor
from unifysql.ingestion.enricher import MetadataEnricher
from unifysql.ingestion.extractor import SchemaExtractor
from unifysql.ingestion.postgres_adaptor import PostgresAdaptor
from unifysql.ingestion.snowflake_adaptor import SnowflakeAdaptor
from unifysql.observability.logger import get_logger
from unifysql.semantic.annotator import Annotator
from unifysql.semantic.embedder import SemanticEmbedder
from unifysql.semantic.mapper import RelationshipMapper
from unifysql.semantic.models import SemanticLayer
from unifysql.semantic.store import SemanticLayerStore

logger = get_logger()
semantic_bp = Blueprint("semantic", __name__)


@semantic_bp.route("/schemas", methods=["POST"])
def register_schema() -> Tuple[Response, int]:
    """
    Registers a new warehouse schema and triggers async semantic layer construction.
    """
    data = request.get_json()
    if not data:
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "bad_request",
                    "error_detail": "Request body is required",
                }
            ),
            400,
        )

    try:
        req = SchemaRegistrationRequest.model_validate(data)
    except Exception as e:
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "bad_request",
                    "error_detail": str(e),
                }
            ),
            400,
        )

    schema_id = uuid4()

    def _run_offline_pipeline() -> None:
        """Runs the full offline pipeline in a background thread."""
        try:
            adaptor: BaseAdaptor
            if req.dialect.lower() == "postgres":
                adaptor = PostgresAdaptor(connection_string=req.connection_string)
            elif req.dialect.lower() == "snowflake":
                adaptor = SnowflakeAdaptor(connection_string=req.connection_string)
            else:
                adaptor = BigQueryAdaptor(connection_string=req.connection_string)

            extractor = SchemaExtractor(adaptor=adaptor, dialect=req.dialect)
            schema = extractor.extract()

            engine = create_engine(req.connection_string)
            enricher = MetadataEnricher(schema=schema, engine=engine)
            enriched_schema = enricher.enrich()

            annotator = Annotator(model_name=req.model_preference)
            annotated_tables = {
                t.name: annotator.annotate(table=t) for t in enriched_schema
            }

            mapper = RelationshipMapper(model_name=req.model_preference)
            mapped_tables = mapper.map(tables=annotated_tables, schemas=enriched_schema)

            semantic_store = SemanticLayerStore()
            schema_hash = enriched_schema[0].schema_hash
            try:
                existing = semantic_store.load_by_schema_hash(schema_hash=schema_hash)
                major, minor = existing.version.split(".")
                version = f"{major}.{int(minor) + 1}"
            except FileNotFoundError:
                version = "1.0"

            semantic_layer = SemanticLayer(
                version=version,
                schema_hash=schema_hash,
                schema_id=schema_id,
                dialect=req.dialect,
                generated_by=req.model_preference or settings.default_model,
                tables=mapped_tables,
                created_at=datetime.now(),
            )
            semantic_store.save(layer=semantic_layer)

            embedder = SemanticEmbedder()
            embedder.embed_tables(schema_id=schema_id, tables=mapped_tables)

            logger.info(
                "offline_pipeline_completed",
                schema_id=str(schema_id),
                n_tables=len(mapped_tables),
                version=version,
            )

        except Exception as e:
            logger.error(
                "offline_pipeline_failed", schema_id=str(schema_id), error=str(e)
            )

    thread = threading.Thread(target=_run_offline_pipeline)
    thread.daemon = True
    thread.start()

    response = SchemaRegistrationResponse(
        schema_id=schema_id,
        status="constructing",
        semantic_layer_version=None,
    )
    return jsonify(response.model_dump(mode="json")), 202


@semantic_bp.route("/semantic-layer/<schema_id>", methods=["GET"])
def get_semantic_layer(schema_id: str) -> Tuple[Response, int]:
    """Returns the most recent SemanticLayer for a given schema_id."""
    try:
        parsed_schema_id = UUID(schema_id)
    except (TypeError, ValueError) as e:
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "bad_request",
                    "error_detail": f"Invalid schema_id: {str(e)}",
                }
            ),
            400,
        )

    try:
        semantic_store = SemanticLayerStore()
        semantic_layer = semantic_store.load_by_schema_id(schema_id=parsed_schema_id)
    except TimeoutError:
        logger.error("request_timeout", path=request.path)
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "timeout",
                    "error_detail": "Request exceeded E2E timeout budget",
                }
            ),
            504,
        )
    except FileNotFoundError as e:
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "not_found",
                    "error_detail": str(e),
                }
            ),
            404,
        )

    response = SemanticLayerResponse(
        schema_id=parsed_schema_id, semantic_layer=semantic_layer
    )
    return jsonify(response.model_dump(mode="json")), 200


@semantic_bp.route("/semantic-layer/<schema_id>/diff", methods=["GET"])
def diff_semantic_layer(schema_id: str) -> Tuple[Response, int]:
    """Returns the diff between two SemanticLayer versions."""
    try:
        parsed_schema_id = UUID(schema_id)
    except (TypeError, ValueError) as e:
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "bad_request",
                    "error_detail": f"Invalid schema_id: {str(e)}",
                }
            ),
            400,
        )

    compare_to = request.args.get("compare_to")
    if not compare_to:
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "bad_request",
                    "error_detail": "compare_to query parameter is required",
                }
            ),
            400,
        )

    try:
        semantic_store = SemanticLayerStore()
        current_layer = semantic_store.load_by_schema_id(schema_id=parsed_schema_id)
        stored_layer = semantic_store.load_by_schema_id_and_version(
            schema_id=parsed_schema_id, version=compare_to
        )
        diff = semantic_store.diff(
            stored_layer=stored_layer, current_layer=current_layer
        )
    except TimeoutError:
        logger.error("request_timeout", path=request.path)
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "timeout",
                    "error_detail": "Request exceeded E2E timeout budget",
                }
            ),
            504,
        )
    except FileNotFoundError as e:
        return (
            jsonify(
                {
                    "query_id": g.get("query_id"),
                    "error_type": "not_found",
                    "error_detail": str(e),
                }
            ),
            404,
        )

    response = DiffResponse(
        schema_id=parsed_schema_id,
        added_tables=diff["added_tables"],
        removed_tables=diff["removed_tables"],
        column_changes=diff["column_changes"],
    )
    return jsonify(response.model_dump(mode="json")), 200
