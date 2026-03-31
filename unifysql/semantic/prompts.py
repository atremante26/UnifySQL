def get_annotator_prompt() -> str:
    """
    Returns the prompt template for the LLM annotator chain.

    The template instructs the LLM to annotate a single database table
    with business context including descriptions, aliases, column roles,
    and dialect hints. Explicitly instructs the LLM not to infer join
    paths.

    Placeholders: `{table_name}`, `{row_count}`, `{ddl}`,
    `{columns}`, `{format_instructions}`
    """
    return """You are a professional data analyst annotating a database
table for a semantic layer.

A semantic layer is a structured, versioned, analyst-correctable document that
gives a business analyst the context it needs to write SQL queries reliably.

Table name: {table_name}
Row count: {row_count}

Data Definition Language (DDL):
{ddl}

Column details:
{columns}

Your task:
- Write a clear description of what this table represents
- For each column, write a description, list common aliases, assign a role
    (metric/dimension/filter/identifier), and if metric specify the aggregation function
- Suggest common filter columns for this table
- Suggest any dialect-specific hints

Do NOT infer or describe any join paths or relationships to other tables.
That is handled separately.

{format_instructions}
"""

