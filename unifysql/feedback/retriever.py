from typing import List
from uuid import UUID

import numpy as np
from sentence_transformers import SentenceTransformer

from unifysql.config import settings
from unifysql.feedback.store import CorrectionRecordORM, FeedbackStore
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import Correction, CorrectionRecord

# Instantiate logger
logger = get_logger()


class FeedbackRetriever:
    def __init__(self, feedback_store: FeedbackStore) -> None:
        self.feedback_store = feedback_store

        # Initialize embedding model
        self.model = SentenceTransformer(settings.embedding_model)

        # Obtain ChromaDB collection
        self.collection = self.feedback_store.collection

    def embed_correction(self, correction: Correction) -> List[float]:
        """
        Computes the embedding vector for a correction question.
        """
        return self.model.encode(correction.question).tolist()

    def retrieve(
        self,
        question: str,
        schema_id: UUID,
        semantic_layer_version: str,
    ) -> List[CorrectionRecord]:
        """
        Retrieves top-k similar corrections for a given question.

        Embeds the question, queries `ChromaDB` filtered by `schema_id`,
        applies `correction_min_similarity` threshold, down-ranks
        corrections from prior semantic layer versions by 0.5x,
        and loads full records from `SQLite`/`Postgres`.
        """
        # Embed question
        embedded_question = np.array(self.model.encode(question))

        # Query ChromaDB
        results = self.collection.query(
            query_embeddings=[embedded_question],
            n_results=settings.correction_top_k,
            where={"schema_id": str(schema_id), "type": "correction"},
            include=["metadatas", "distances"],
        )
        logger.info("correction_embeddings_queried", n_results=len(results["ids"][0]))

        # Filter and down-rank corrections
        filtered_ids = []
        distances = results["distances"] or []
        metadatas = results["metadatas"] or []

        for i, correction_id in enumerate(results["ids"][0]):
            distance = distances[0][i]
            metadata = metadatas[0][i]
            similarity = 1 - distance

            # Filter below similarity threshold
            if similarity < settings.correction_min_similarity:
                continue

            # Down-rank stale corrections
            if metadata["semantic_layer_version"] != semantic_layer_version:
                similarity *= 0.5
                if similarity < settings.correction_min_similarity:
                    continue

            filtered_ids.append(correction_id)

        # Load full records from SQLite/Postgres
        filtered_records: List[CorrectionRecordORM] = []
        for correction_id in filtered_ids:
            record = self.feedback_store.get_by_id(correction_id)
            if record is not None:
                filtered_records.append(record)
        logger.info("corrections_retrieved", n_corrections=len(filtered_records))

        # TODO: call log_correction_retrieved() with similarity scores once tracked

        # Convert CorrectionRecordORM to CorrectionRecord
        return [
            CorrectionRecord(
                correction=Correction(
                    query_id=UUID(c.query_id),
                    question=c.question,
                    bad_sql=c.bad_sql,
                    corrected_sql=c.corrected_sql,
                    schema_id=UUID(c.schema_id),
                    created_at=c.created_at,
                ),
                embedding_vector=embedded_question.tolist(),
                retrieval_count=c.retrieval_count,
                schema_hash=c.schema_hash,
                semantic_layer_version=c.semantic_layer_version,
            )
            for c in filtered_records
        ]
