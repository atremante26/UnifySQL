from datetime import datetime

import pytest

from unifysql.semantic.models import ColumnEntry, ColumnRole, SemanticLayer, TableEntry
from unifysql.translation.validator import Validator


@pytest.fixture
def semantic_layer() -> SemanticLayer:
    return SemanticLayer(
        version="1.0",
        schema_hash="abc123",
        dialect="postgres",
        generated_by="gpt-4o",
        created_at=datetime.now(),
        tables={
            "users": TableEntry(
                description="User accounts",
                columns=[
                    ColumnEntry(
                        name="id",
                        description="User ID",
                        alias=[],
                        role=ColumnRole.identifier,
                    ),
                    ColumnEntry(
                        name="email",
                        description="Email",
                        alias=[],
                        role=ColumnRole.dimension,
                    ),
                    ColumnEntry(
                        name="revenue",
                        description="Revenue",
                        alias=[],
                        role=ColumnRole.metric,
                    ),
                ],
                joins=[],
                filters=[],
                dialect_hints=[],
            ),
            "orders": TableEntry(
                description="Customer orders",
                columns=[
                    ColumnEntry(
                        name="id",
                        description="Order ID",
                        alias=[],
                        role=ColumnRole.identifier,
                    ),
                    ColumnEntry(
                        name="user_id",
                        description="User FK",
                        alias=[],
                        role=ColumnRole.identifier,
                    ),
                    ColumnEntry(
                        name="total",
                        description="Order total",
                        alias=[],
                        role=ColumnRole.metric,
                    ),
                ],
                joins=[],
                filters=[],
                dialect_hints=[],
            ),
        },
    )


@pytest.fixture
def validator() -> Validator:
    return Validator()


def test_valid_select(validator: Validator, semantic_layer: SemanticLayer) -> None:
    """Pytest unit test for valid SELECT statement."""
    result = validator.validate(
        sql="SELECT id, email FROM users", semantic_layer=semantic_layer
    )
    assert result.valid is True
    assert result.error_type is None


def test_valid_join(validator: Validator, semantic_layer: SemanticLayer) -> None:
    """Pytest unit test for valid JOIN statement."""
    result = validator.validate(
        sql="SELECT users.id, orders.total "
        "FROM users "
        "JOIN orders ON users.id = orders.user_id",
        semantic_layer=semantic_layer,
    )
    assert result.valid is True
    assert result.error_type is None


def test_invalid_not_select(
    validator: Validator, semantic_layer: SemanticLayer
) -> None:
    """Pytest unit test for non-SELECT statement."""
    result = validator.validate(sql="DROP TABLE users", semantic_layer=semantic_layer)
    assert result.valid is False
    assert result.error_type == "syntax"
    assert result.error_detail == "Only SELECT statements are permitted"


def test_invalid_unknown_table(
    validator: Validator, semantic_layer: SemanticLayer
) -> None:
    """Pytest unit test for unknown table reference."""
    result = validator.validate(
        sql="SELECT id FROM unknown_table", semantic_layer=semantic_layer
    )
    assert result.valid is False
    assert result.error_type == "schema"


def test_invalid_unknown_column(
    validator: Validator, semantic_layer: SemanticLayer
) -> None:
    """Pytest unit test for unknown column reference."""
    result = validator.validate(
        sql="SELECT users.fake_col FROM users", semantic_layer=semantic_layer
    )
    assert result.valid is False
    assert result.error_type == "schema"
