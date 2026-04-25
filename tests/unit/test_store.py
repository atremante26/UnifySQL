from datetime import datetime
from pathlib import Path
from typing import Dict
from uuid import uuid4

import pytest

from unifysql.semantic.models import (
    ColumnEntry,
    ColumnRole,
    SemanticLayer,
    TableEntry,
)
from unifysql.semantic.store import SemanticLayerStore


def make_column_entry(name: str) -> ColumnEntry:
    return ColumnEntry(
        name=name, description="Test column", alias=[], role=ColumnRole.dimension
    )


def make_table_entry(columns: list[str]) -> TableEntry:
    return TableEntry(
        description="Test table",
        columns=[make_column_entry(c) for c in columns],
        joins=[],
        filters=[],
        dialect_hints=[],
    )


def make_semantic_layer(
    schema_hash: str, tables: Dict[str, TableEntry]
) -> SemanticLayer:
    return SemanticLayer(
        version="1.0",
        schema_hash=schema_hash,
        schema_id=uuid4(),
        dialect="postgres",
        generated_by="gpt-4o",
        tables=tables,
        created_at=datetime.now(),
    )


def test_save_and_load(tmp_path: Path) -> None:
    """Pytest unit test for saving and loading a `SemanticLayer`."""
    # Create test SemanticLayer
    sample_tables = {
        f"table_{t}": make_table_entry(columns=[f"col_{c}" for c in range(3)])
        for t in range(3)
    }
    sample_layer = make_semantic_layer(schema_hash="abcdefg", tables=sample_tables)

    # Create test SemanticLayerStore
    sample_store = SemanticLayerStore(storage_dir=tmp_path)

    # Test save()
    sample_store.save(layer=sample_layer)

    # Test load()
    loaded_layer = sample_store.load_by_schema_hash(schema_hash="abcdefg")

    # Pytest tests
    assert sample_layer.schema_hash == loaded_layer.schema_hash
    assert sample_layer.version == loaded_layer.version
    assert sample_layer.dialect == loaded_layer.dialect
    assert sample_layer.tables == loaded_layer.tables


def test_load_not_found(tmp_path: Path) -> None:
    """Pytest unit test for loading a non-existent `SemanticLayer`."""
    # Create test SemanticLayerStore
    sample_store = SemanticLayerStore(storage_dir=tmp_path)

    # Test load() for hash that doesn't exist
    with pytest.raises(FileNotFoundError) as excinfo:
        sample_store.load_by_schema_hash(schema_hash="1234567")

    assert excinfo.type is FileNotFoundError


def test_diff_changes(tmp_path: Path) -> None:
    """Pytest unit test for `diff()` with two different `SemanticLayers`."""
    # Stored layer has table_0, table_1, table_2 with col_0, col_1, col_2
    sample_tables1 = {
        "table_0": make_table_entry(columns=["col_0", "col_1", "col_2"]),
        "table_1": make_table_entry(columns=["col_0"]),
        "table_2": make_table_entry(columns=["col_0"]),
    }

    # Current layer — table_2 removed, table_3 added, table_1 has new column
    sample_tables2 = {
        "table_0": make_table_entry(columns=["col_0", "col_1", "col_2"]),
        "table_1": make_table_entry(columns=["col_0", "col_new"]),
        "table_3": make_table_entry(columns=["col_0"]),
    }

    sample_layer1 = make_semantic_layer(schema_hash="abc", tables=sample_tables1)
    sample_layer2 = make_semantic_layer(schema_hash="def", tables=sample_tables2)

    sample_store = SemanticLayerStore(storage_dir=tmp_path)
    result = sample_store.diff(stored_layer=sample_layer1, current_layer=sample_layer2)

    assert "table_3" in result["added_tables"]
    assert "table_2" in result["removed_tables"]
    assert "col_new" in result["column_changes"]["table_1"]["added"]


def test_diff_identical(tmp_path: Path) -> None:
    """Pytest unit test for diff() with two identical `SemanticLayers`."""
    # SemanticLayer has table_0, table_1, table_2 with col_0, col_1, col_2
    sample_tables = {
        "table_0": make_table_entry(columns=["col_0", "col_1", "col_2"]),
        "table_1": make_table_entry(columns=["col_0"]),
        "table_2": make_table_entry(columns=["col_0"]),
    }
    sample_layer = make_semantic_layer(schema_hash="abc", tables=sample_tables)

    # Create test SemanticLayerStore
    sample_store = SemanticLayerStore(storage_dir=tmp_path)

    result = sample_store.diff(stored_layer=sample_layer, current_layer=sample_layer)
    assert result == {"added_tables": [], "removed_tables": [], "column_changes": {}}


def test_load_by_schema_id(tmp_path: Path) -> None:
    """Pytest unit test for loading a SemanticLayer by schema_id."""
    # Create test SemanticLayer with known schema_id
    known_schema_id = uuid4()
    sample_tables = {
        "table_0": make_table_entry(columns=["col_0", "col_1"]),
    }
    sample_layer = make_semantic_layer(schema_hash="xyz789", tables=sample_tables)
    sample_layer = sample_layer.model_copy(update={"schema_id": known_schema_id})

    # Save and load by schema_id
    sample_store = SemanticLayerStore(storage_dir=str(tmp_path))
    sample_store.save(layer=sample_layer)
    loaded_layer = sample_store.load_by_schema_id(schema_id=known_schema_id)

    assert loaded_layer.schema_id == known_schema_id
    assert loaded_layer.schema_hash == "xyz789"


def test_load_by_schema_id_not_found(tmp_path: Path) -> None:
    """Pytest unit test for loading a non-existent schema_id."""
    sample_store = SemanticLayerStore(storage_dir=str(tmp_path))

    with pytest.raises(FileNotFoundError):
        sample_store.load_by_schema_id(schema_id=uuid4())
