install: 
	poetry install

test:
	poetry run pytest

lint:
	poetry run ruff check .

fix:
	poetry run ruff check --fix .

format:
	poetry run black .

typecheck:
	poetry run mypy --show-error-codes --pretty .