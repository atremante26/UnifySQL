class SchemaStaleError(Exception):
    """Raised when the stored schema hash does not match the current DDL hash."""

    def __init__(self, stored_hash: str, current_hash: str):
        self.stored_hash = stored_hash
        self.current_hash = current_hash
        super().__init__(
            f"Schema drift detected: stored={stored_hash}, current={current_hash}"
        )
