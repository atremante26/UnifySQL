# UnifySQL

Model-agnostic, warehouse-aware NL → SQL pipeline with an automatically constructed and maintained semantic layer.

## Setup

### Prerequisites
- [pyenv](https://github.com/pyenv/pyenv) — Python version management
- [Poetry](https://python-poetry.org/docs/#installation) — dependency management

### Installation

1. Clone the repo
```
   git clone <repo-url>
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