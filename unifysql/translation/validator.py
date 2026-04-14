from typing import Optional

import sqlglot

from unifysql.observability.logger import get_logger
from unifysql.semantic.models import SemanticLayer, ValidationResult

# Instantiate logger
logger = get_logger()


class Validator:
    def validate(self, sql: str, semantic_layer: SemanticLayer) -> ValidationResult:
        """
        Validates compiled SQL against the semantic layer.

        Checks in order: (1) statement is `SELECT` only, (2) all referenced
        tables exist in the semantic layer, (3) all referenced columns
        exist in their respective tables.
        """
        # Define globals
        valid = True
        error_type: Optional[str] = None
        error_detail: Optional[str] = None

        # Parse query with SQLGlot
        ast = sqlglot.parse_one(sql)

        # Check if SELECT
        if not isinstance(ast, sqlglot.exp.Select):
            valid = False
            error_type = "syntax"
            error_detail = "Only SELECT statements are permitted"
            logger.error("validation_failed", error_type=error_type)
            return ValidationResult(
                valid=valid, error_type=error_type, error_detail=error_detail
            )

        # Check table references
        referenced_tables = {t.name for t in ast.find_all(sqlglot.exp.Table)}
        known_tables = set(semantic_layer.tables.keys())
        missing_tables = referenced_tables - known_tables
        if missing_tables:
            valid = False
            error_type = "schema"
            error_detail = f"Unknown tables: {missing_tables}"
            logger.error(
                "validation_failed", error_type=error_type, error_detail=error_detail
            )

        # Check column references
        for col in ast.find_all(sqlglot.exp.Column):
            col_name = col.name
            table_name = col.table  # which table this column belongs to
            if table_name and table_name in semantic_layer.tables:
                known_columns = {
                    c.name for c in semantic_layer.tables[table_name].columns
                }
                if col_name not in known_columns:
                    valid = False
                    error_type = "schema"
                    error_detail = f"Unknown column: {col_name} in {table_name}"
                    logger.error(
                        "validation_failed",
                        error_type=error_type,
                        error_detail=error_detail,
                    )

        if valid:
            logger.info("validation_successful", sql=sql)

        return ValidationResult(
            valid=valid, error_type=error_type, error_detail=error_detail
        )
