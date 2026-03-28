install: 
	poetry install

test:
	poetry run pytest

lint:
	poetry run ruff check .

format:
	poetry run black .

typecheck:
	poetry run mypy --show-error-codes --pretty .