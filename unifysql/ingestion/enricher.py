from typing import List

from sqlalchemy import text
from sqlalchemy.engine import Engine

from unifysql.observability.logger import get_logger
from unifysql.semantic.models import TableSchema

# Instantiate logger
logger = get_logger()

class MetadataEnricher():
    def __init__(self, schema: List[TableSchema], engine: Engine):
        self.schema = schema
        self.engine = engine

    def enrich(self) -> List[TableSchema]:
        """
        Enriches a `TableSchema` with metadata (sample values,
        null rate, row counts, foreign keys).
        """
        try:
            with self.engine.connect() as connection:
                for i, table in enumerate(self.schema):
                    updated_columns = []
                    for col in table.columns:
                        # Sample values
                        result = connection.execute(
                            text(f"SELECT {col.name} FROM {table.name} LIMIT 10")
                        )
                        sample_values = [str(row[0]) for row in result.fetchall()]

                        # Null rate
                        null_count = connection.execute(
                            text(
                                f"SELECT COUNT(*) FROM {table.name} "
                                f"WHERE {col.name} IS NULL"
                            )
                        ).scalar()
                        total_count = connection.execute(
                            text(f"SELECT COUNT(*) FROM {table.name}")
                        ).scalar()
                        null_rate = (
                            float(null_count or 0) / float(total_count)
                            if total_count else 0.0
                        )

                        # Update columns
                        updated_columns.append(col.model_copy(update={
                            "sample_values": sample_values,
                            "null_rate": null_rate,
                            "is_fk": col.is_fk or self._infer_fk(col.name)
                        }))

                    # Row count
                        row_count = connection.execute(
                            text(f"SELECT COUNT(*) FROM {table.name}")
                        ).scalar()

                    # Update TableSchema
                    self.schema[i] = self.schema[i].model_copy(update={
                        "row_count": row_count or 0,
                        "columns": updated_columns
                    })

            logger.info("schema_enriched_with_metadata", n_tables=len(self.schema))

            return self.schema

        except Exception as e:
            logger.error("schema_enrichment_failed", error=str(e))
            raise

    def _infer_fk(self, column_name: str) -> bool:
        """
        Returns `True` if `column_name` ends with '_id', indicating a likely
        foreign key relationship by naming convention.
        """
        return column_name.endswith("_id")
