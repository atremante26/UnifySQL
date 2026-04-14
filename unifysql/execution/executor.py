from abc import ABC, abstractmethod

from unifysql.semantic.models import QueryResult


class BaseExecutor(ABC):
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    @abstractmethod
    async def execute(self, sql: str) -> QueryResult:
        """Executes a validated SQL query on database."""
        pass
