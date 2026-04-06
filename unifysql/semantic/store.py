import glob
import os
import time
from typing import Any, Dict

import yaml

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import SemanticLayer

# Instantiate logger
logger = get_logger()

class SemanticLayerStore():
    def __init__(self, storage_dir: str = settings.semantic_layer_dir):
        self.storage_dir = storage_dir

    def save(self, layer: SemanticLayer) -> None:
        """Serializes a `SemanticLayer` to a versioned YAML file on disk."""
        # Format file name
        created_at = time.time()
        file_name = f"{layer.schema_hash}_{created_at}.yaml"
        file_path = os.path.join(self.storage_dir, file_name)

        # Save semantic layer
        try:
            with open(file_path, 'w') as file:
                yaml.dump(layer.model_dump(mode="json"), file, default_flow_style=False)
                logger.info("schema_layer_serialization_succeeded", file=file_name)
        except Exception as e:
            logger.error("schema_layer_serialization_failed", error=str(e))

    def load(self, schema_hash: str) -> SemanticLayer:
        """Loads the most recent `SemanticLayer` for a given schema hash."""
        # Get latest file matching schema_hash
        files = glob.glob(f"{self.storage_dir}/{schema_hash}*.yaml")
        if not files:
            raise FileNotFoundError(
                f"No semantic layer found for schema hash {schema_hash}"
            )
        latest_file = max(files, key=os.path.getmtime)

        # Load latest YAML file
        with open(latest_file, 'r') as file:
            loaded_file = yaml.safe_load(file)

        # Deserialize and return SemanticLayer
        return SemanticLayer.model_validate(loaded_file)

    def diff(
            self,
            stored_layer: SemanticLayer,
            current_layer: SemanticLayer
        ) -> Dict[str, Any]:
        """Compares two `SemanticLayer` versions and returns their differences."""
        # Define new tables
        added_tables = (
            set(current_layer.tables.keys())
            - set(stored_layer.tables.keys())
        )

        # Define old tables
        removed_tables = (
            set(stored_layer.tables.keys())
            - set(current_layer.tables.keys())
        )

        # Define columns changes
        column_changes = {}
        for table_name in stored_layer.tables.keys() & current_layer.tables.keys():
            old_cols = {c.name for c in stored_layer.tables[table_name].columns}
            new_cols = {c.name for c in current_layer.tables[table_name].columns}

            added = new_cols - old_cols
            removed = old_cols - new_cols

            if added or removed:
                column_changes[table_name] = {
                    "added": list(added),
                    "removed": list(removed)
                }

        return {
            "added_tables": list(added_tables),
            "removed_tables": list(removed_tables),
            "column_changes": column_changes
        }
