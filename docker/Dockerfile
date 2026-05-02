FROM python:3.11-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

# Copy source
COPY . .

# Run Flask
CMD ["poetry", "run", "gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "unifysql.api.app:create_app()"]