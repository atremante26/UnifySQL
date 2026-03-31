from typing import List

from unifysql.ingestion.adaptor import BaseAdaptor
from unifysql.ingestion.extractor import SchemaExtractor
from unifysql.semantic.models import ColumnSchema


class MockAdaptor1(BaseAdaptor):
    def connect(self) -> None:
        pass
    def get_tables(self) -> List[str]:
        return ["users", "orders"]
    def get_ddl(self, table_name: str) -> str:
        return f"CREATE TABLE {table_name} (id INT);"
    def get_columns(self, table_name: str) -> List[ColumnSchema]:
        return []

class MockAdaptor2(BaseAdaptor):
    def connect(self) -> None:
        pass
    def get_tables(self) -> List[str]:
        return ["users"]
    def get_ddl(self, table_name: str) -> str:
        return f"CREATE TABLE {table_name} (id INT, name VARCHAR);"
    def get_columns(self, table_name: str) -> List[ColumnSchema]:
        return []

class MockAdaptor3(BaseAdaptor):
    def connect(self) -> None:
        pass
    def get_tables(self) -> List[str]:
        return ["orders", "users"]
    def get_ddl(self, table_name: str) -> str:
        return f"CREATE TABLE {table_name} (id INT);"
    def get_columns(self, table_name: str) -> List[ColumnSchema]:
        return []

def test_schema_hash() -> None:
    """Pytest unit test for schema hash consistency."""
    # Initialize first mock adaptor
    mock_adaptor1 = MockAdaptor1(connection_string="mock://localhost")

    # Initialize schema extractor
    extractor1 = SchemaExtractor(adaptor=mock_adaptor1, dialect='MySQL')

    # Extract multiple schema using mock adaptor
    schema1 = extractor1.extract()
    schema2 = extractor1.extract()

    assert schema1[0].schema_hash == schema2[0].schema_hash

    # Initialize second mock adaptor
    mock_adaptor2 = MockAdaptor2(connection_string="mock://localhost")

    # Initialize schema extractor
    extractor2 = SchemaExtractor(adaptor=mock_adaptor2, dialect='MySQL')

    # Extract schema
    schema3 = extractor2.extract()

    assert not schema1[0].schema_hash == schema3[0].schema_hash

    # Initialize third mock adaptor
    mock_adaptor3 = MockAdaptor3(connection_string="mock://localhost")

    # Initialize schema extractor
    extractor3 = SchemaExtractor(adaptor=mock_adaptor3, dialect='MySQL')

    # Extract schema
    schema4 = extractor3.extract()

    assert schema1[0].schema_hash == schema4[0].schema_hash
