FROM python:3.11-slim

WORKDIR /app

# Install system dependencies needed for torch/sentence-transformers
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

# Copy dependency files first
COPY pyproject.toml poetry.lock ./

# Install dependencies only (no project package)
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-root

# Install sentence-transformers and torch manually (ARM64 compatibility)
RUN pip install --no-cache-dir torch
RUN pip install --no-cache-dir sentence-transformers

# Copy full source after dependencies installed
COPY . .

# Run with Gunicorn
CMD ["poetry", "run", "gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "unifysql.api.app:create_app()"]