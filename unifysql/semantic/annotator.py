from typing import Any, Optional

from langchain.chat_models import init_chat_model
from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.models import AnnotatorOutput, TableEntry, TableSchema
from unifysql.semantic.prompts import get_annotator_prompt

# Instantiate logger
logger = get_logger()


class Annotator:
    def __init__(self, model_name: Optional[str]):
        self.model_name = str(
            model_name or settings.default_model or settings.fallback_model
        )
        self.model = init_chat_model(
            model=self.model_name, max_tokens=settings.max_tokens_per_call
        )
        self.parser = PydanticOutputParser(pydantic_object=AnnotatorOutput)
        self.prompt = ChatPromptTemplate.from_template(get_annotator_prompt())
        self.chain = self.prompt | self.model | self.parser
        self.format_instructions = self.parser.get_format_instructions()

    @retry(
        stop=stop_after_attempt(settings.llm_max_retries)
        | stop_after_delay(settings.llm_timeout_s),
        wait=wait_exponential(multiplier=settings.llm_retry_base_delay_s),
        reraise=True,
    )
    def _invoke(
        self, chain: Any, inputs: dict[str, Any]
    ) -> tuple[AnnotatorOutput, int]:
        """Invokes the LangChain chain with retry and timeout logic."""
        callback = UsageMetadataCallbackHandler()
        result = chain.invoke(inputs, config={"callbacks": [callback]})
        token_count = sum(
            getattr(v, "total_tokens", 0) or 0 for v in callback.usage_metadata.values()
        )
        return result, token_count

    def annotate(self, table: TableSchema) -> TableEntry:
        """Annotates a single `TableSchema` with business context."""
        # Define inputs
        inputs = {
            "table_name": table.name,
            "columns": str(
                [
                    {
                        "name": c.name,
                        "type": c.type,
                        "nullable": c.nullable,
                        "sample_values": c.sample_values,
                        "null_rate": c.null_rate,
                    }
                    for c in table.columns
                ]
            ),
            "row_count": table.row_count,
            "ddl": table.raw_ddl,
            "format_instructions": self.format_instructions,
        }

        # Extract the result
        try:
            with Span("llm_annotation") as span:
                result, token_count = self._invoke(chain=self.chain, inputs=inputs)
            logger.info(
                "annotation_completed",
                table=table.name,
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
                with Span("llm_annotation_fallback") as span:
                    result, token_count = self._invoke(
                        chain=fallback_chain, inputs=inputs
                    )
                logger.info(
                    "annotation_completed",
                    table=table.name,
                    model=settings.fallback_model,
                    token_count=token_count,
                    latency_ms=span.latency_ms,
                )
            except Exception as fallback_e:
                logger.error("fallback_model_failed", error=str(fallback_e))
                raise

        # Return TableEntry object
        return TableEntry(
            description=result.description,
            columns=result.columns,
            joins=[],
            filters=result.filters,
            dialect_hints=result.dialect_hints,
        )
