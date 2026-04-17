# UnifySQL

Model-agnostic, warehouse-aware Text-to-SQL pipeline with an automatically constructed and maintained semantic layer.

## Setup

### Prerequisites
- [pyenv](https://github.com/pyenv/pyenv) — Python version management
- [Poetry](https://python-poetry.org/docs/#installation) — dependency management

### Installation

1. Clone the repo
   ```
   git clone https://github.com/atremante26/UnifySQL.git
   cd unifysql
   ```

2. Install Python 3.11
   ```
   pyenv install 3.11.13
   pyenv local 3.11.13
   ```

3. Install dependencies
   ```
   poetry install
   ```

4. **Apple Silicon only** — install torch and sentence-transformers manually
   ```
   poetry run pip install --no-cache-dir "torch>=2.4"
   poetry run pip install --no-cache-dir "numpy<2"
   poetry run pip install --no-cache-dir sentence-transformers
   ```

5. Configure secrets
   ```
   cp .env.example .env
   ```
   Fill in your API keys and connection strings in `.env`.

6. Verify installation
   ```
   poetry run python -c "from sentence_transformers import SentenceTransformer; print('ok')"
   ```

## Development

```
make install     # install dependencies
make test        # run test suite
make lint        # run ruff linter
make fix         # auto-fix ruff lint errors
make format      # run black formatter
make typecheck   # run mypy type checker
```

## Architecture

UnifySQL is split into two paths:

**Offline (once per schema)** — schema ingestion → LLM annotation → semantic layer construction and storage

**Online (every query)** — NL question → context builder → LLM translation → SQLGlot compilation → validation → DB execution → response

## Status

- Phase 0 complete — project skeleton, dependencies, and environment configured
- Phase 1 complete — Pydantic data models for all pipeline stages
- Phase 2 complete — observability scaffold (structured logging, span timing, metrics, PII scrubber)
- Phase 3 complete — schema ingestion (Postgres, Snowflake, BigQuery adaptors, extractor, metadata enricher)
- Phase 4 complete — semantic layer construction (LLM annotator, relationship mapper, versioned YAML store, ChromaDB embedder)
- Phase 5 complete — translation pipeline (context builder, LLM translator, SQLGlot compiler, validator)
- Phase 6 complete — DB execution (async Postgres, Snowflake, BigQuery executors with timeout enforcement)
- Phase 7 complete — feedback loop (correction store with SQLite/Postgres + ChromaDB, similarity retriever with threshold filtering)

## Project Structure

```
unifysql/
├── ingestion/              # Offline path — schema extraction and enrichment
│   ├── adaptor.py          # BaseAdaptor abstract interface
│   ├── postgres_adaptor.py # PostgresAdaptor implementation
│   ├── snowflake_adaptor.py # SnowflakeAdaptor implementation
│   ├── bigquery_adaptor.py  # BigQueryAdaptor implementation
│   ├── extractor.py        # SchemaExtractor with SHA-256 fingerprinting
│   └── enricher.py         # MetadataEnricher with row counts, samples, FK inference
├── semantic/               # Semantic layer construction and storage
│   ├── annotator.py        # LLM annotator with retry and fallback routing
│   ├── mapper.py           # Relationship mapper with three-stage join inference
│   ├── store.py            # Versioned YAML persistence with drift detection
│   ├── embedder.py         # ChromaDB table embedding and similarity search
│   ├── prompts.py          # Annotator and mapper prompt templates
│   └── models.py           # All Pydantic data models
├── translation/            # Online path — NL to SQL translation
│   ├── context_builder.py  # Embedding similarity retrieval and rationale generation
│   ├── translator.py       # LLM SQL generation with retry and fallback routing
│   ├── compiler.py         # SQLGlot dialect transpilation with preview mode
│   ├── validator.py        # SELECT guard and schema-grounded validation
│   └── prompts.py          # Rationale and translator prompt templates
├── execution/              # DB execution and result handling
│   ├── executor.py         # BaseExecutor abstract async interface
│   ├── postgres_executor.py # PostgresExecutor using asyncpg
│   ├── snowflake_executor.py # SnowflakeExecutor using asyncio.to_thread
│   └── bigquery_executor.py  # BigQueryExecutor using asyncio.to_thread
├── feedback/               # Correction loop and retrieval
│   ├── store.py            # FeedbackStore with SQLite/Postgres and ChromaDB
│   └── retriever.py        # FeedbackRetriever with similarity filtering
├── observability/          # Logging, tracing, metrics, PII scrubbing
├── eval/                   # Evaluation harness and golden set
├── api/                    # Flask API and middleware
├── exceptions.py           # Custom exceptions
└── config.py               # Pydantic settings
```
