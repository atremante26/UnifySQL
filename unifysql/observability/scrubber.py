from typing import List

from unifysql.semantic.models import ColumnSchema

PII_PATTERNS = [
    "email",
    "phone",
    "ssn",
    "social_security",
    "password",
    "passwd",
    "credit_card",
    "card_number",
    "date_of_birth",
    "dob",
    "birth_date",
    "address",
    "zip_code",
    "postal_code",
    "first_name",
    "last_name",
    "full_name",
    "ip_address",
    "passport",
    "drivers_license",
    "bank_account",
    "routing_number",
    "tax_id",
    "national_id",
]

def is_pii_column(column_name: str) -> bool:
    """Return True if a column_name matches a PII pattern, otherwise return False."""
    return any(pattern in column_name.lower() for pattern in PII_PATTERNS)

def scrub_columns(columns: List[ColumnSchema]) -> List[ColumnSchema]:
    """Redact PII columns from a list of ColumnSchemas."""
    return [
        column.model_copy(
            update={"sample_values": ["[REDACTED]"] * len(column.sample_values)}
        ) if is_pii_column(column.name) else column for column in columns
    ]
