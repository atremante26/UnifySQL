import pytest

from unifysql.translation.compiler import Compiler


@pytest.fixture
def compiler() -> Compiler:
    return Compiler()


def test_compile_valid_sql(compiler: Compiler) -> None:
    """Pytest unit test for valid SQL compilation."""
    result = compiler.compile(
        sql="SELECT * FROM users", dialect="postgres", preview=False
    )
    assert result.validation.valid is True
    assert result.sql != ""


def test_compile_preview_appends_limit(compiler: Compiler) -> None:
    """Pytest unit test for preview mode appending LIMIT clause."""
    result = compiler.compile(
        sql="SELECT * FROM users", dialect="postgres", preview=True
    )
    assert result.validation.valid is True
    assert "LIMIT" in result.sql


def test_compile_no_preview(compiler: Compiler) -> None:
    """Pytest unit test for no LIMIT clause when preview is False."""
    result = compiler.compile(
        sql="SELECT * FROM users", dialect="postgres", preview=False
    )
    assert result.validation.valid is True
    assert "LIMIT" not in result.sql


def test_compile_invalid_sql(compiler: Compiler) -> None:
    """Pytest unit test for invalid SQL returning syntax error."""
    result = compiler.compile(sql="NOT VALID SQL ##", dialect="postgres", preview=False)
    assert result.validation.valid is False
    assert result.validation.error_type == "syntax"
    assert result.sql == ""
