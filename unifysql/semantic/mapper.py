from typing import Any, Dict, List, Optional

from langchain.chat_models import init_chat_model
from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.models import (
    FKSource,
    JoinCardinality,
    JoinPath,
    JoinSource,
    MapperOutput,
    TableEntry,
    TableSchema,
)
from unifysql.semantic.prompts import get_mapper_prompt

# Instantiate logger
logger = get_logger()


class RelationshipMapper:
    def __init__(self, model_name: Optional[str]):
        self.model_name = str(
            model_name or settings.default_model or settings.fallback_model
        )
        self.model = init_chat_model(
            model=self.model_name, max_tokens=settings.max_tokens_per_call
        )
        self.parser = PydanticOutputParser(pydantic_object=MapperOutput)
        self.prompt = ChatPromptTemplate.from_template(get_mapper_prompt())
        self.chain = self.prompt | self.model | self.parser
        self.format_instructions = self.parser.get_format_instructions()

    @retry(
        stop=stop_after_attempt(settings.llm_max_retries)
        | stop_after_delay(settings.llm_timeout_s),
        wait=wait_exponential(multiplier=settings.llm_retry_base_delay_s),
        reraise=True,
    )
    def _invoke(self, chain: Any, inputs: dict[str, Any]) -> tuple[MapperOutput, int]:
        """Invokes the LangChain chain with retry and timeout logic."""
        callback = UsageMetadataCallbackHandler()
        result = chain.invoke(inputs, config={"callbacks": [callback]})
        token_count = sum(
            getattr(v, "total_tokens", 0) or 0 for v in callback.usage_metadata.values()
        )
        return result, token_count

    def _build_deterministic_joins(
        self, tables: Dict[str, TableEntry], schema_lookup: Dict[str, TableSchema]
    ) -> Dict[str, TableEntry]:
        """Builds join paths from declared FK constraints and inferred naming
        patterns before the LLM call.
        Declared joins (`fk_source=FKSource.declared`) get `confidence=1.0` and
        `JoinSource.declared`. Inferred joins (`fk_source=FKSource.inferred`) get
        `confidence=0.75` and `JoinSource.inferred`.
        """
        # Build deterministic joins using fk_source to assign confidence
        for table_name, entry in tables.items():
            schema = schema_lookup.get(table_name)
            if not schema:
                continue
            for col in schema.columns:
                if col.is_fk:
                    target = col.name.replace("_id", "") + "s"
                    if target in tables:
                        confidence = 1.0 if col.fk_source == FKSource.declared else 0.75
                        join_source = (
                            JoinSource.declared
                            if col.fk_source == FKSource.declared
                            else JoinSource.inferred
                        )
                        existing = tables[table_name].joins
                        tables[table_name] = tables[table_name].model_copy(
                            update={
                                "joins": existing
                                + [
                                    JoinPath(
                                        source_table=table_name,
                                        target_table=target,
                                        on_clause=(
                                            f"{table_name}.{col.name} = {target}.id"
                                        ),
                                        cardinality=JoinCardinality.one_to_many,
                                        confidence=confidence,
                                        join_confidence=confidence,
                                        join_source=join_source,
                                    )
                                ]
                            }
                        )
        return tables

    def map(
        self, tables: Dict[str, TableEntry], schemas: List[TableSchema]
    ) -> Dict[str, TableEntry]:
        """Infers join relationships across all tables in a single LLM call.
        Builds deterministic joins first, then sends the full table graph
        to the LLM for ambiguous relationship inference. Deduplicates LLM
        joins against existing deterministic ones before merging.
        """
        # Create schema lookup dict
        schema_lookup = {s.name: s for s in schemas}

        # Update tables with deterministic joins
        tables = self._build_deterministic_joins(
            tables=tables, schema_lookup=schema_lookup
        )

        # Serialize table graph
        table_context = {
            table_name: {
                "description": entry.description,
                "filters": entry.filters,
                "columns": [
                    {
                        "name": col.name,
                        "role": col.role.value,
                        "is_pk": (
                            next(
                                (
                                    c.is_pk
                                    for c in schema_lookup[table_name].columns
                                    if c.name == col.name
                                ),
                                False,
                            )
                            if table_name in schema_lookup
                            else False
                        ),
                        "is_fk": (
                            next(
                                (
                                    c.is_fk
                                    for c in schema_lookup[table_name].columns
                                    if c.name == col.name
                                ),
                                False,
                            )
                            if table_name in schema_lookup
                            else False
                        ),
                    }
                    for col in entry.columns
                ],
                "dialect_hints": [h.model_dump() for h in entry.dialect_hints],
            }
            for table_name, entry in tables.items()
        }

        # Define inputs
        inputs = {
            "table_graph": str(table_context),
            "format_instructions": self.format_instructions,
        }

        # Extract the result
        try:
            with Span("llm_mapping") as span:
                result, token_count = self._invoke(chain=self.chain, inputs=inputs)
            logger.info(
                "mapping_completed",
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
                with Span("llm_mapping_fallback") as span:
                    result, token_count = self._invoke(
                        chain=fallback_chain, inputs=inputs
                    )
                logger.info(
                    "mapping_completed",
                    model=settings.fallback_model,
                    token_count=token_count,
                    latency_ms=span.latency_ms,
                )
            except Exception as fallback_e:
                logger.error("fallback_model_failed", error=str(fallback_e))
                raise

        # Build list of deterministic joins for deduplication
        existing_joins = {
            (j.source_table, j.target_table)
            for entry in tables.values()
            for j in entry.joins
        }

        # Update JoinPaths
        for join in result.joins:
            if join.source_table in tables:
                if (join.source_table, join.target_table) not in existing_joins:
                    existing = tables[join.source_table].joins
                    tables[join.source_table] = tables[join.source_table].model_copy(
                        update={"joins": existing + [join]}
                    )

        return tables
