# Changelog

## [Unreleased]

## [0.1.0] - 2026-04-25

### Phase 0 — Project skeleton and tooling
- Poetry project initialized with Python 3.11
- Native ARM64 Python via pyenv
- Full directory skeleton with `__init__.py` files
- Pydantic BaseSettings config module
- Makefile with install, test, lint, format, fix, typecheck targets
- `.env.example` with all environment variables

### Phase 1 — Pydantic data models
- All pipeline models defined in `unifysql/semantic/models.py`
- Added FKSource enum, AnnotatorOutput, MapperOutput, ContextResult, CompilerResult
- Added name field to ColumnEntry, source_table to JoinPath, schema_id to SemanticLayer

### Phase 2 — Observability scaffold
- structlog JSON configuration with query_id context binding
- Span context manager for per-stage latency tracking
- Structured metrics logging for translation, execution, and correction events
- PII scrubber with substring pattern matching

### Phase 3 — Schema ingestion
- BaseAdaptor abstract interface
- PostgresAdaptor, SnowflakeAdaptor, BigQueryAdaptor via SQLAlchemy
- SchemaExtractor with SHA-256 DDL fingerprinting
- MetadataEnricher with row counts, sample values, null rates, FK inference
- FKSource tracking (declared vs inferred) on ColumnSchema

### Phase 4 — Semantic layer construction
- LLM annotator with PydanticOutputParser, retry, fallback routing, span timing
- Three-stage relationship mapper (declared → inferred → llm_inferred)
- Versioned YAML store with save, load_by_schema_hash, load_by_schema_id, diff
- ChromaDB embedder for table description similarity search
- Annotator and mapper prompt templates in prompts.py
- SchemaStaleError custom exception

### Phase 5 — Translation pipeline
- ContextBuilder with ChromaDB similarity retrieval and LLM rationale generation
- FeedbackRetriever integration for few-shot corrections
- LLM translator with retry and fallback routing
- SQLGlot compiler with dialect transpilation and preview mode
- Validator with SELECT guard and schema-grounded column validation

### Phase 6 — DB execution
- BaseExecutor abstract async interface
- PostgresExecutor using asyncpg
- SnowflakeExecutor using asyncio.to_thread
- BigQueryExecutor using asyncio.to_thread
- Timeout enforcement via asyncio.wait_for

### Phase 7 — Feedback loop
- FeedbackStore with SQLAlchemy ORM to SQLite/Postgres and ChromaDB
- FeedbackRetriever with similarity threshold filtering and stale version down-ranking

### Phase 8 — Eval harness
- GoldenEntry, EvalResult, RegressionReport models
- 26-entry Spider dev golden set across 14 databases
- run_single, run_eval with EX/EM scoring
- Click CLI with --dataset, --n, --model, --execute, --compare
- Regression detection via compare_runs()