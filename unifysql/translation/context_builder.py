from typing import Any, Dict, List, Optional
from uuid import UUID

from langchain.chat_models import init_chat_model
from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential

from unifysql.config import settings
from unifysql.feedback.retriever import FeedbackRetriever
from unifysql.feedback.store import FeedbackStore
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.embedder import SemanticEmbedder
from unifysql.semantic.models import ContextResult, CorrectionRecord, TableEntry
from unifysql.semantic.store import SemanticLayerStore
from unifysql.translation.prompts import get_rationale_prompt

# Instantiate logger
logger = get_logger()


class ContextBuilder:
    def __init__(self, model_name: Optional[str]) -> None:
        self.embedder = SemanticEmbedder()
        self.store = SemanticLayerStore()
        self.retriever = FeedbackRetriever(feedback_store=FeedbackStore())

        # Initalize model
        self.model_name = str(
            model_name or settings.default_model or settings.fallback_model
        )
        self.model = init_chat_model(
            model=self.model_name, max_tokens=settings.max_tokens_per_call
        )
        self.parser = StrOutputParser()
        self.prompt = ChatPromptTemplate.from_template(get_rationale_prompt())
        self.chain = self.prompt | self.model | self.parser

    @retry(
        stop=stop_after_attempt(settings.llm_max_retries)
        | stop_after_delay(settings.llm_timeout_s),
        wait=wait_exponential(multiplier=settings.llm_retry_base_delay_s),
        reraise=True,
    )
    def _invoke(self, chain: Any, inputs: dict[str, Any]) -> tuple[str, int]:
        """Invokes the LangChain chain with retry and timeout logic."""
        callback = UsageMetadataCallbackHandler()
        result = chain.invoke(inputs, config={"callbacks": [callback]})
        token_count = sum(
            getattr(v, "total_tokens", 0) or 0 for v in callback.usage_metadata.values()
        )
        return result, token_count

    def build_context(
        self, question: str, schema_id: UUID, schema_hash: str
    ) -> ContextResult:
        """
        Builds the full context for a translation request.

        Retrieves relevant tables via embedding similarity, fetches
        few-shot corrections from the feedback store, and generates
        a selection rationale via LLM.
        """
        relevant_tables, semantic_layer_version = self._get_relevant_tables(
            question, schema_id, schema_hash
        )
        few_shot_corrections = self._get_few_shot_corrections(
            question, schema_id, semantic_layer_version
        )
        rationale = self._generate_rationale(
            question=question, tables=list(relevant_tables.keys())
        )
        return ContextResult(
            relevant_tables=relevant_tables,
            few_shot_corrections=few_shot_corrections,
            selection_rationale=rationale,
        )

    def _get_relevant_tables(
        self, question: str, schema_id: UUID, schema_hash: str
    ) -> tuple[Dict[str, TableEntry], str]:
        """
        Retrieves the top-k most relevant `TableEntry` objects and
        semantic_layer_version for a question.

        Queries ChromaDB for semantically similar table descriptions, then
        loads the full `TableEntry` objects from the semantic layer YAML.
        """
        # Get table names from embeddings
        table_embeddings = self.embedder.query_table(
            schema_id=schema_id, question=question
        )

        # Get SemanticLayer using schema_hash
        semantic_layer = self.store.load(schema_hash=schema_hash)

        # Get relevant tables
        relevant_tables = {
            name: semantic_layer.tables[name]
            for name in table_embeddings
            if name in semantic_layer.tables
        }
        logger.info("relevant_tables_retrieved", n_tables=len(relevant_tables))

        return relevant_tables, semantic_layer.version

    def _get_few_shot_corrections(
        self, question: str, schema_id: UUID, semantic_layer_version: str
    ) -> List[CorrectionRecord]:
        """Retrieves top-k similar past corrections for a given question."""
        return self.retriever.retrieve(
            question=question,
            schema_id=schema_id,
            semantic_layer_version=semantic_layer_version,
        )

    def _generate_rationale(self, question: str, tables: List[str]) -> str:
        """Generates a one-sentence explanation of why tables were selected."""
        # Prepare inputs
        inputs = {"question": question, "table_names": tables}

        # Extract the result
        try:
            with Span("table_rationale") as span:
                result, token_count = self._invoke(chain=self.chain, inputs=inputs)
            logger.info(
                "rationale_generated",
                table_names=tables,
                model=self.model_name,
                token_count=token_count,
                latency_ms=span.latency_ms,
            )
        except Exception as e:
            logger.warning(
                "primary_model_failed_falling_back",
                primary=self.model_name,
                fallback=settings.fallback_model,
                error=str(e),
            )
            fallback_model = init_chat_model(
                model=settings.fallback_model, max_tokens=settings.max_tokens_per_call
            )
            fallback_chain = self.prompt | fallback_model | self.parser

            try:
                with Span("table_rationale_fallback") as span:
                    result, token_count = self._invoke(
                        chain=fallback_chain, inputs=inputs
                    )
                logger.info(
                    "rationale_generated",
                    table_names=tables,
                    model=self.model_name,
                    token_count=token_count,
                    latency_ms=span.latency_ms,
                )
            except Exception as fallback_e:
                logger.error("fallback_model_failed", error=str(fallback_e))
                raise

        return str(result)
