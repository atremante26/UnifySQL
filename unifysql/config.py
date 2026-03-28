from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    # Database
    postgres_url: Optional[str] = None
    snowflake_dsn: Optional[str] = None
    bq_project: Optional[str] = None

    # LLM
    openai_api_key: str 
    anthropic_api_key: Optional[str]
    default_model: str = "gpt-4o"
    fallback_model: str = "claude-sonnet-4-6"
    llm_timeout_s: int = 30
    llm_max_retries: int = 3
    llm_retry_base_delay_s: float = 1.0

    # Pipeline config
    e2e_timeout_s: int = 45
    context_top_k_tables: int = 5
    correction_top_k: int = 3
    correction_min_similarity: float = 0.85
    max_tokens_per_call: int = 4096
    semantic_layer_cache_ttl_s: int = 3600
    embedding_model: str = "all-MiniLM-L6-v2"
    preview_default_limit: int = 100
    join_confidence_execution_threshold: float = 0.85

    # Observability
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env")

# Singleton
settings = Settings()