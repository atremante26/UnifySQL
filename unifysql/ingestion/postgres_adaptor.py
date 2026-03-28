from typing import List

from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.schema import CreateTable

from unifysql.ingestion.adaptor import BaseAdaptor
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import ColumnSchema

# Instantiate logger
logger = get_logger()

class PostgresAdaptor(BaseAdaptor):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

        # Define engine
        self.engine = create_engine(self.connection_string)

        # Inspect engine
        self.insp = inspect(self.engine)

    def connect(self) -> None:
        try:
            self.engine.connect()
            logger.info("postgres_connection_succeeded.")
        except Exception as e:
            logger.error("postgres_connection_failed", error=str(e))


    def get_tables(self) -> List[str]:
        return self.insp.get_table_names()

    def get_ddl(self, table_name: str) -> str:
        # Get Postgres metadata
        meta = MetaData()

        # Reflect database
        meta.reflect(bind=self.engine)

        # Extract table
        table = meta.tables[table_name]

        return str(CreateTable(table).compile(self.engine))

    def get_columns(self, table_name: str) -> List[ColumnSchema]:
        # Get all columns
        columns = self.insp.get_columns(table_name)

        # Primary Key info
        pk_info = self.insp.get_pk_constraint(table_name)
        pk_columns = pk_info["constrained_columns"]

        # Foreign Key info
        fk_info = self.insp.get_foreign_keys(table_name)
        fk_columns = [fk["constrained_columns"][0] for fk in fk_info]

        column_schemas = []
        with self.engine.connect() as connection:
            for c in columns:
                # Sample values
                result = connection.execute(
                    text(f"SELECT {c['name']} FROM {table_name} LIMIT 10")
                )
                sample_values = [str(row[0]) for row in result.fetchall()]

                # Null rate
                null_count = connection.execute(
                    text(f"SELECT COUNT(*) FROM {table_name} WHERE {c['name']} IS NULL")
                ).scalar()
                total_count = connection.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar()
                null_rate = (
                    float(null_count or 0) / float(total_count) if total_count else 0.0
                )

            column_schemas.append(ColumnSchema(
                name=c["name"],
                type=str(c["type"]),
                nullable=c["nullable"],
                is_pk=True if c["name"] in pk_columns else False,
                is_fk=True if c["name"] in fk_columns else False,
                sample_values=sample_values,
                null_rate=null_rate
            ))

        return column_schemas
