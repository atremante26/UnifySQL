from typing import Dict, List
from uuid import UUID

import chromadb
import numpy as np
from sentence_transformers import SentenceTransformer

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import TableEntry

# Instantiate logger
logger = get_logger()


class SemanticEmbedder:
    def __init__(self) -> None:
        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(path=settings.chroma_path)
        self.collection = self.client.get_or_create_collection("unifysql")

        # Initialize embedding model
        self.model = SentenceTransformer(settings.embedding_model)

    def embed_tables(self, schema_id: UUID, tables: Dict[str, TableEntry]) -> None:
        """
        Embeds table descriptions and stores them in ChromaDB.

        Deletes existing embeddings for the schema before adding new ones
        to ensure stale embeddings are never matched against.
        """
        # Delete old embeddings for same schema_id
        self.collection.delete(where={"schema_id": str(schema_id), "type": "table"})
        logger.info("old_table_embeddings_deleted")

        # Embed tables
        for table_name, table_entry in tables.items():
            # Create table description embedding
            embedding = self.model.encode(table_entry.description)
            embedding_array = np.array(embedding)

            # Add table to collection
            self.collection.add(
                ids=[f"{schema_id}_{table_name}"],
                embeddings=[embedding_array],
                metadatas=[
                    {
                        "schema_id": str(schema_id),
                        "table_name": table_name,
                        "type": "table",
                    }
                ],
                documents=[table_entry.description],
            )
        logger.info("table_embeddings_added", n_tables=len(tables))

    def query_table(self, schema_id: UUID, question: str) -> List[str]:
        """
        Returns the top-k most semantically similar table names for a question.

        Embeds the NL question and queries ChromaDB for the most similar
        table description embeddings within the specified schema.
        """
        # Embed question
        question_embedding = self.model.encode(question)
        question_array = np.array(question_embedding)

        # Query table embeddings
        results = self.collection.query(
            query_embeddings=[question_array],
            n_results=settings.context_top_k_tables,
            where={"schema_id": str(schema_id), "type": "table"},
        )

        metadatas = results["metadatas"]
        if not metadatas:
            return []
        return [str(meta["table_name"]) for meta in metadatas[0]]
