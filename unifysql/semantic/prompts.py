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

def get_mapper_prompt() -> str:
    """
    Returns the prompt template for the LLM relationship mapper chain.

    The template instructs the LLM to infer join paths across all tables
    in the semantic layer given the full table graph. Each join must include
    source table, target table, join clause, cardinality, confidence score,
    and join source type.

    Placeholders: `{table_graph}`, `{format_instructions}`
    """
    return """You are a senior data engineer inferring join relationships
between database tables for a semantic layer.

You will be given a full graph of all tables in the database, including
their columns, roles, PK/FK flags, and descriptions.

Table graph:
{table_graph}

Your task:
- Infer all meaningful join paths between tables
- For each join, specify:
    - source_table: the table the join originates from
    - target_table: the table being joined to
    - on_clause: the SQL join condition e.g. "orders.user_id = users.id"
    - cardinality: one_to_one, one_to_many, or many_to_many
    - confidence: your confidence score between 0.0 and 1.0
    - join_confidence: same as confidence for llm_inferred joins
    - join_source: always "llm_inferred" for joins you infer here

Rules:
- Only infer joins where there is strong evidence from column names,
  PK/FK flags, or naming conventions
- Do not infer speculative joins with no supporting evidence
- Prefer declared FK relationships (is_fk=True) as highest confidence
- A column ending in _id strongly suggests a foreign key relationship

{format_instructions}
"""
