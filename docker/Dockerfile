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
CMD ["poetry", "run", "flask", "--app", "unifysql.api.app:create_app", "run", "--host", "0.0.0.0", "--port", "5000"]