import re
from typing import Optional

import sqlglot

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import CompilerResult, ValidationResult

# Instantiate logger
logger = get_logger()

_FENCE_RE = re.compile(r"^```(?:sql)?\s*\n?(.*?)\n?```\s*$", re.DOTALL | re.IGNORECASE)


def _strip_markdown_fences(sql: str) -> str:
    """Strips leading/trailing markdown code fences from LLM SQL output."""
    match = _FENCE_RE.match(sql.strip())
    return match.group(1).strip() if match else sql.strip()


class Compiler:

    def compile(self, sql: str, dialect: str, preview: bool) -> CompilerResult:
        """Transpiles raw SQL to the target dialect using SQLGlot."""

        # Strip markdown fences that LLMs occasionally emit
        sql = _strip_markdown_fences(sql)

        sql = sql.strip().rstrip(";")
        if preview:
            sql = re.sub(
                r"\s+LIMIT\s+\d+\s*$", "", sql, flags=re.IGNORECASE
            ).strip()
            sql = f"{sql} LIMIT {settings.preview_default_limit}"

        # Initialize return values
        result: str = ""
        error_type: Optional[str] = None
        error_detail: Optional[str] = None

        # Transpile SQL
        try:
            result = sqlglot.transpile(
                sql=sql, write=dialect, identify=True, pretty=True
            )[0]
            logger.info("query_transpiled", sql=result)

        except sqlglot.errors.ParseError as e:
            error_type = "syntax"
            error_detail = str(e)
            logger.error(
                "transpilation_failed", error_type=error_type, error_detail=error_detail
            )

        # Return CompilerResult
        return CompilerResult(
            sql=result,
            validation=ValidationResult(
                valid=error_type is None,
                error_type=error_type,
                error_detail=error_detail,
            ),
        )
