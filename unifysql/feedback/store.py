from datetime import datetime
from typing import Optional
from uuid import uuid4

import chromadb
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.semantic.models import CorrectionRecord

# Instantiate logger
logger = get_logger()


class Base(DeclarativeBase):
    pass


class CorrectionRecordORM(Base):
    __tablename__ = "corrections"

    id: Mapped[str] = mapped_column(primary_key=True)
    query_id: Mapped[str]
    question: Mapped[str]
    bad_sql: Mapped[str]
    corrected_sql: Mapped[str]
    schema_id: Mapped[str]
    created_at: Mapped[datetime]
    schema_hash: Mapped[str]
    semantic_layer_version: Mapped[str]
    retrieval_count: Mapped[int] = mapped_column(default=0)


class FeedbackStore:
    def __init__(self) -> None:
        self.correction_db_url = settings.correction_db_url

        # Create SQLAlchemy Engine (SQLite or Postgres)
        self.engine = create_engine(self.correction_db_url)

        # Emit CREATE TABLE DDL
        Base.metadata.create_all(self.engine)

        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(path=settings.chroma_path)
        self.collection = self.client.get_or_create_collection("unifysql")

    def insert(self, correction_record: CorrectionRecord) -> str:
        """Persists a `CorrectionRecord` to SQLite/Postgres and ChromaDB.

        Saves the full correction to the relational store and adds
        the pre-computed question embedding to ChromaDB keyed by
        the new `correction_id`.
        """
        # Save to SQLite or Postgres
        with Session(self.engine) as session:
            record = CorrectionRecordORM(
                id=str(uuid4()),
                query_id=str(correction_record.correction.query_id),
                question=correction_record.correction.question,
                bad_sql=correction_record.correction.bad_sql,
                corrected_sql=correction_record.correction.corrected_sql,
                schema_id=str(correction_record.correction.schema_id),
                created_at=correction_record.correction.created_at,
                schema_hash=correction_record.schema_hash,
                semantic_layer_version=correction_record.semantic_layer_version,
                retrieval_count=0,
            )
            session.add(record)
            session.commit()
            logger.info(
                "correction_stored",
                correction_id=record.id,
                schema_id=str(correction_record.correction.schema_id),
            )

        # Save embedding to ChromaDB
        self.collection.add(
            ids=[record.id],
            embeddings=[np.array(correction_record.embedding_vector)],
            metadatas=[
                {
                    "type": "correction",
                    "schema_id": str(correction_record.correction.schema_id),
                    "schema_hash": correction_record.schema_hash,
                    "semantic_layer_version": correction_record.semantic_layer_version,
                }
            ],
            documents=[correction_record.correction.question],
        )
        logger.info(
            "embedding_stored",
            correction_id=record.id,
            schema_id=str(correction_record.correction.schema_id),
        )

        return record.id

    def get_by_id(self, correction_id: str) -> Optional[CorrectionRecordORM]:
        """Retrieves a `CorrectionRecordORM` by its primary key."""
        with Session(self.engine) as session:
            return session.get(CorrectionRecordORM, correction_id)
