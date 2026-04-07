from unifysql.observability.scrubber import is_pii_column, scrub_columns
from unifysql.semantic.models import ColumnSchema


def test_is_pii_column() -> None:
    """Pytest unit tests for the function is_pii_column()."""
    assert is_pii_column("email")
    assert is_pii_column("user_email")
    assert not is_pii_column("product_name")
    assert is_pii_column("EMAIL")


def test_scrub_columns() -> None:
    """Pytest unit tests for the function scrub_columns()."""
    column1 = ColumnSchema(
        name="user_email",
        type="varchar",
        nullable=True,
        is_pk=False,
        is_fk=False,
        sample_values=["test@example.com", "user@gmail.com"],
        null_rate=0.0,
    )

    column2 = ColumnSchema(
        name="user_phone",
        type="int",
        nullable=True,
        is_pk=False,
        is_fk=False,
        sample_values=["1234567890", "0987654321"],
        null_rate=0.0,
    )

    column3 = ColumnSchema(
        name="churn_rate",
        type="float",
        nullable=True,
        is_pk=False,
        is_fk=False,
        sample_values=["0.5", "0.25"],
        null_rate=0.0,
    )

    result = scrub_columns([column1, column2, column3])
    assert result[0].sample_values == ["[REDACTED]", "[REDACTED]"]
    assert result[1].sample_values == ["[REDACTED]", "[REDACTED]"]
    assert result[2].sample_values == ["0.5", "0.25"]


def test_scrub_columns_empty_list() -> None:
    """Pytest unit test for empty columns list in the function scrub_columns()."""
    assert scrub_columns([]) == []


def test_scrub_columns_empty_sample_values() -> None:
    """Pytest unit test for empty sample values in the function scrub_columns()."""
    column = ColumnSchema(
        name="churn_rate",
        type="float",
        nullable=True,
        is_pk=False,
        is_fk=False,
        sample_values=[],
        null_rate=0.0,
    )
    result = scrub_columns([column])
    assert result[0].sample_values == []


def test_scrub_columns_case_insensitive() -> None:
    """Pytest unit test for case insensitive columns in the function scrub_columns()."""
    column = ColumnSchema(
        name="USER_EMAIL",
        type="varchar",
        nullable=True,
        is_pk=False,
        is_fk=False,
        sample_values=["test@example.com", "user@gmail.com"],
        null_rate=0.0,
    )
    result = scrub_columns([column])
    assert result[0].sample_values == ["[REDACTED]", "[REDACTED]"]
