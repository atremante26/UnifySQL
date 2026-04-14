from typing import Optional

import sqlglot

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import CompilerResult, ValidationResult

# Instantiate logger
logger = get_logger()


class Compiler:

    def compile(self, sql: str, dialect: str, preview: bool) -> CompilerResult:
        """Transpiles raw SQL to the target dialect using SQLGlot."""

        # Update query if preview is True
        if preview:
            sql += f" LIMIT {settings.preview_default_limit}"

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
