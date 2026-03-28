from abc import ABC, abstractmethod
from typing import List

from unifysql.semantic.models import ColumnSchema


class BaseAdaptor(ABC):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    @abstractmethod
    def connect(self) -> None:
        """Connects to database using connection_string."""
        pass

    @abstractmethod
    def get_tables(self) -> List[str]:
        """Returns a list of all tables in database."""
        pass

    @abstractmethod
    def get_ddl(self, table_name: str) -> str:
        """Returns the DDL (`CREATE TABLE...` statement) for a specified table."""
        pass

    @abstractmethod
    def get_columns(self, table_name: str) -> List[ColumnSchema]:
        """Returns all of the columns and types for a specified table."""
        pass
