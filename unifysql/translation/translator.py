from typing import Any, Optional

from langchain.chat_models import init_chat_model
from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential

from unifysql.config import settings
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.models import ContextResult, TranslationRequest
from unifysql.translation.prompts import get_translator_prompt

# Instantiate logger
logger = get_logger()


class Translator:
    def __init__(self, model_name: Optional[str]) -> None:
        # Initalize model
        self.model_name = str(
            model_name or settings.default_model or settings.fallback_model
        )
        self.model = init_chat_model(
            model=self.model_name, max_tokens=settings.max_tokens_per_call
        )
        self.parser = StrOutputParser()
        self.prompt = ChatPromptTemplate.from_template(get_translator_prompt())
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

    def translate(self, context: ContextResult, request: TranslationRequest) -> str:
        """
        Generates a raw SQL query from a natural language question.
        """
        # Serialize table context
        table_context = str(
            {
                name: {
                    "description": entry.description,
                    "columns": [
                        {
                            "name": c.name,
                            "role": c.role.value,
                            "description": c.description,
                        }
                        for c in entry.columns
                    ],
                    "joins": [
                        {"target": j.target_table, "on": j.on_clause}
                        for j in entry.joins
                    ],
                    "filters": entry.filters,
                }
                for name, entry in context.relevant_tables.items()
            }
        )

        # Define inputs
        inputs = {
            "question": request.question,
            "dialect": request.dialect,
            "table_context": table_context,
            "few_shot_corrections": str(
                [c.model_dump() for c in context.few_shot_corrections]
            )
            or "None",
            "selection_rationale": context.selection_rationale,
        }

        # Extract the result
        try:
            with Span("translator") as span:
                result, token_count = self._invoke(chain=self.chain, inputs=inputs)
            logger.info(
                "query_translated",
                sql=result,
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
                with Span("translator_fallback") as span:
                    result, token_count = self._invoke(
                        chain=fallback_chain, inputs=inputs
                    )
                logger.info(
                    "query_translated",
                    sql=result,
                    model=self.model_name,
                    token_count=token_count,
                    latency_ms=span.latency_ms,
                )
            except Exception as fallback_e:
                logger.error("fallback_model_failed", error=str(fallback_e))
                raise

        return str(result)
