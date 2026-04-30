from typing import Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

from unifysql.semantic.models import QueryResult, SemanticLayer


# Schema Models
class SchemaRegistrationRequest(BaseModel):
    """Request body for POST /schemas — registers a new warehouse schema."""

    connection_string: str
    dialect: str
    model_preference: Optional[str] = None


class SchemaRegistrationResponse(BaseModel):
    """Response for POST /schemas - returns schema_id immediately, processing async."""

    schema_id: UUID
    status: str = "constructing"
    semantic_layer_version: Optional[str] = None


# Translation Models
class TranslateRequest(BaseModel):
    """Request body for POST /translate — translates a NL question to SQL."""

    question: str
    schema_id: UUID
    dialect: str
    model_preference: Optional[str] = None
    execute: bool = False
    preview: bool = True


class TranslateResponse(BaseModel):
    """Response for POST /translate — compiled SQL w/ confidence & optional results."""

    query_id: UUID
    sql: str
    confidence: float
    warnings: List[str]
    selection_rationale: str
    model_used: str
    semantic_layer_version: str
    result: Optional[QueryResult] = None


# Feedback Models
class FeedbackRequest(BaseModel):
    """Request body for POST /feedback — submits a SQL correction."""

    query_id: UUID
    question: str
    bad_sql: str
    corrected_sql: str
    schema_id: UUID


class FeedbackResponse(BaseModel):
    """Response for POST /feedback — confirms correction stored."""

    correction_id: UUID
    retrieval_count: int
    validation: str


# Semantic Layer Models
class SemanticLayerResponse(BaseModel):
    """Response for GET /semantic-layer/{schema_id}."""

    schema_id: UUID
    semantic_layer: SemanticLayer


class DiffResponse(BaseModel):
    """Response for GET /semantic-layer/{schema_id}/diff."""

    schema_id: UUID
    added_tables: List[str]
    removed_tables: List[str]
    column_changes: Dict[str, Dict[str, List[str]]]
