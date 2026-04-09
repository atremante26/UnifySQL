def get_rationale_prompt() -> str:
    """
    Returns the prompt template for the selection rationale LLM call.

    Instructs the LLM to explain in one sentence why the selected tables
    are relevant to the given NL question. Output is injected into
    TranslationResult.selection_rationale.

    Placeholders: `{question}`, `{table_names}`
    """
    return """You are explaining why a set of database tables were selected
to answer a natural language question.

Question: {question}

Selected tables: {table_names}

In one sentence, explain why these tables are relevant to answering
the question. Be specific about what data each table contributes.
Do not mention SQL or technical implementation details.
"""
