from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel


# Ingestion Models
class ColumnSchema(BaseModel):
    """A single column schema."""
    name: str
    type: str
    nullable: bool
    is_pk: bool
    is_fk: bool
    sample_values: List[str]
    null_rate: float

class TableSchema(BaseModel):
    """A single table schema."""
    name: str
    columns: List[ColumnSchema]
    row_count: int
    schema_hash: str
    dialect: str
    raw_ddl: str

class WarehouseType(Enum):
    """Warehouse type enum."""
    postgres = "POSTGRES"
    snowflake = "SNOWFLAKE"
    big_query = "BIG_QUERY"

class SchemaMetadata(BaseModel):
    """Metadata for a single schema."""
    tables: List[TableSchema]
    database: str
    warehouse_type: WarehouseType
    extracted_at: datetime

# Semantic Layer Models
class ColumnRole(Enum):
    """Column role enum."""
    metric = "metric"
    dimension = "dimension"
    filter = "filter"
    identifier = "identifier"

class ColumnEntry(BaseModel):
    """A single column entry."""
    name: str
    description: str
    alias: List[str]
    role: ColumnRole
    aggregation: Optional[str] = None

class JoinCardinality(Enum):
    """Join cardinality enum."""
    one_to_one = "one_to_one"
    one_to_many = "one_to_many"
    many_to_many = "many_to_many"

class JoinSource(Enum):
    """Join source enum."""
    declared = "declared"
    inferred = "inferred"
    llm_inferred = "llm_inferred"

class JoinPath(BaseModel):
    """A join path between two columns."""
    target_table: str
    on_clause: str
    cardinality: JoinCardinality
    confidence: float
    join_confidence: float
    join_source: JoinSource

class DialectHint(BaseModel):
    """A hint for SQL dialect."""
    function_name: str
    template: str

class TableEntry(BaseModel):
    """A single table entry."""
    description: str
    columns: List[ColumnEntry]
    joins: List[JoinPath]
    filters: List[str]
    dialect_hints: List[DialectHint]

class AnnotatorOutput(BaseModel):
    """The output of the LangChain annotator."""
    description: str
    columns: List[ColumnEntry]
    filters: List[str]
    dialect_hints: List[DialectHint]

class SemanticLayer(BaseModel):
    """Data schema for semantic layer"""
    version: str
    schema_hash: str
    dialect: str
    generated_by: str
    tables: Dict[str, TableEntry]
    created_at: datetime

# Translation Model
class TranslationRequest(BaseModel):
    """A text-to-SQL translation request."""
    question: str
    schema_id: UUID
    dialect: str
    model_preference: Optional[str] = None
    execute: bool = False
    preview: bool = True

class TranslationResult(BaseModel):
    """The result of a text-to-SQL translation request."""
    query_id: UUID
    sql: str
    dialect: str
    confidence: float
    semantic_layer_version: str
    model_used: str
    tables_used: List[str]
    joins_used: List[str]
    few_shot_count: int
    latency_ms: float
    token_count: int
    selection_rationale: str

class ValidationResult(BaseModel):
    """The result of validating a generated SQL query."""
    valid: bool
    error_type: Optional[str] = None
    error_detail: Optional[str] = None

# Execution and Response Models
class QueryResult(BaseModel):
    """The result of executing a generated SQL query."""
    query_id: UUID
    sql: str
    result_set: Dict[str, List[Any]] # {column_name: [val1, val2, ...]}
    row_count: int
    execution_ms: float
    warehouse: WarehouseType

class ErrorType(Enum):
    """Enum of error types."""
    syntax = "syntax"
    schema = "schema"
    join = "join"
    dialect = "dialect"
    LLM = "LLM"
    execution = "execution"

class ErrorDetail(BaseModel):
    """Structured error detail for a failed pipeline operation."""
    query_id: UUID
    error_type: ErrorType
    error_detail: str
    sql: Optional[str] = None

class APIResponse(BaseModel):
    """The response of a Flask API call."""
    query_id: UUID
    sql: Optional[str] = None
    result: Optional[QueryResult] = None
    confidence: Optional[float] = None
    warnings: List[str]
    error: Optional[ErrorDetail] = None

# Feedback Models
class Correction(BaseModel):
    """A corrected SQL query."""
    query_id: UUID
    question: str
    bad_sql: str
    corrected_sql: str
    schema_id: UUID
    created_at: datetime

class CorrectionRecord(BaseModel):
    """A record of a corrected SQL query."""
    correction: Correction
    embedding_vector: List[float]
    retrieval_count: int
    schema_hash: str
    semantic_layer_version: str
