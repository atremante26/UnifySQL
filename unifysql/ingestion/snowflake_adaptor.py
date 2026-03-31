from typing import List

from sqlalchemy import MetaData, create_engine, inspect
from sqlalchemy.schema import CreateTable

from unifysql.ingestion.adaptor import BaseAdaptor
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import ColumnSchema

# Instantiate logger
logger = get_logger()

class SnowflakeAdaptor(BaseAdaptor):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

        # Define engine
        self.engine = create_engine(self.connection_string)

        # Inspect engine
        self.insp = inspect(self.engine)

    def connect(self) -> None:
        """
        Connects to a Snowflake database with `connection_string`
        using SQLAlchemy.
        """
        try:
            self.engine.connect()
            logger.info("snowflake_connection_succeeded.")
        except Exception as e:
            logger.error("snowflake_connection_failed", error=str(e))

    def get_tables(self) -> List[str]:
        """Returns a list of tables in a Snowflake database."""
        return self.insp.get_table_names()

    def get_ddl(self, table_name: str) -> str:
        """Returns the `CREATE TABLE DDL` statement for a specified table."""
        # Get Snowflake metadata
        meta = MetaData()

        # Reflect database
        meta.reflect(bind=self.engine)

        # Extract table
        table = meta.tables[table_name]

        return str(CreateTable(table).compile(self.engine))

    def get_columns(self, table_name: str) -> List[ColumnSchema]:
        """
        Returns structural column metadata for a specified table,
        including name, type, nullability and PK/FK status.
        """
        # Get all columns
        columns = self.insp.get_columns(table_name)

        # Primary Key info
        pk_info = self.insp.get_pk_constraint(table_name)
        pk_columns = pk_info["constrained_columns"]

        # Foreign Key info
        fk_info = self.insp.get_foreign_keys(table_name)
        fk_columns = [fk["constrained_columns"][0] for fk in fk_info]

        column_schemas = []
        for c in columns:
            column_schemas.append(ColumnSchema(
                name=c["name"],
                type=str(c["type"]),
                nullable=c["nullable"],
                is_pk=True if c["name"] in pk_columns else False,
                is_fk=True if c["name"] in fk_columns else False,
                sample_values=[], # default value
                null_rate=0.0 # default value
            ))

        return column_schemas
