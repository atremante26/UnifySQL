import hashlib
from typing import List

from unifysql.ingestion.adaptor import BaseAdaptor
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import TableSchema

# Instantiate logger
logger = get_logger()

class SchemaExtractor():
    def __init__(self, adaptor: BaseAdaptor, dialect: str):
        self.adaptor = adaptor
        self.dialect = dialect

    def extract(self) -> List[TableSchema]:
        """
        Connects to the database via the adaptor, extracts DDL and
        column metadata for all tables, computes `SHA-256` fingerprint
        of the combined DDL, and returns a `List[TableSchema]`.
        """
        # Connect to database
        try:
            self.adaptor.connect()
        except Exception as e:
            logger.error("schema_extraction_failed", error=str(e))
            raise

        # Get tables
        tables = self.adaptor.get_tables()

        # Get DDL for each table
        ddl_map = {}
        for table_name in tables:
            ddl_map[table_name] = self.adaptor.get_ddl(table_name)

        # Compute hash for all DDL combined
        all_ddl = "".join(ddl_map[t] for t in sorted(ddl_map.keys()))
        schema_hash = hashlib.sha256(all_ddl.encode("utf-8")).hexdigest()

        final_tables = []
        for table_name in tables:

            # Get columns
            columns = self.adaptor.get_columns(table_name)

            final_tables.append(TableSchema(
                name=table_name,
                columns=columns,
                row_count=0, # default value
                schema_hash=schema_hash,
                dialect=self.dialect,
                raw_ddl=ddl_map[table_name]
            ))

        logger.info("schema_extraction_completed", n_tables=len(final_tables))

        return final_tables
