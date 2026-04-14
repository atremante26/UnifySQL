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


def get_translator_prompt() -> str:
    """
    Returns the prompt template for the LLM SQL translator chain.

    Instructs the LLM to generate a single valid SQL query from a
    natural language question using the provided semantic layer context.
    Output is a raw SQL string with no explanation or markdown.

    Placeholders: `{question}`, `{dialect}`, `{table_context}`,
    `{few_shot_corrections}`, `{selection_rationale}`
    """
    return """You are an expert SQL engineer generating a single SQL query
from a natural language question.

Dialect: {dialect}

Question: {question}

Relevant tables and context:
{table_context}

Selection rationale: {selection_rationale}

Few-shot correction examples:
{few_shot_corrections}

Rules:
- Generate only a single SELECT statement — no DDL, no DML, no explanations
- Use only the tables and columns provided in the context above
- Respect the dialect specified — use dialect-appropriate functions and syntax
- If joining tables, use the join paths provided in the table context
- Do not wrap the SQL in markdown code blocks or backticks
- Return only the raw SQL query and nothing else

SQL:
"""
