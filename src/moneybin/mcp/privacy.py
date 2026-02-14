"""Privacy controls and query validation for the MCP server.

This module enforces read-only access, result size limits, and provides
the not-implemented helper for features not yet available in DuckDB.
"""

import logging
import os
import re

logger = logging.getLogger(__name__)

# Configurable limits via environment variables
MAX_ROWS: int = int(os.getenv("MONEYBIN_MCP_MAX_ROWS", "1000"))
MAX_CHARS: int = int(os.getenv("MONEYBIN_MCP_MAX_CHARS", "50000"))

# Optional table allowlist (comma-separated)
_allowed_tables_env = os.getenv("MONEYBIN_MCP_ALLOWED_TABLES", "")
ALLOWED_TABLES: set[str] | None = (
    {t.strip().lower() for t in _allowed_tables_env.split(",") if t.strip()}
    if _allowed_tables_env
    else None
)

# Patterns that indicate read-only SQL statements
_READ_ONLY_PREFIXES = re.compile(
    r"^\s*(SELECT|WITH|DESCRIBE|SHOW|PRAGMA|EXPLAIN)\b",
    re.IGNORECASE,
)

# Patterns that indicate write operations (even inside CTEs)
_WRITE_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE|COPY|ATTACH|DETACH|EXPORT|IMPORT)\b",
    re.IGNORECASE,
)


def validate_read_only_query(sql: str) -> str | None:
    """Validate that a SQL query is read-only.

    Args:
        sql: The SQL query string to validate.

    Returns:
        None if the query is valid, or an error message string if rejected.
    """
    stripped = sql.strip()

    if not stripped:
        return "Empty query is not allowed."

    if not _READ_ONLY_PREFIXES.match(stripped):
        return (
            "Only read-only queries are allowed. "
            "Queries must start with SELECT, WITH, DESCRIBE, SHOW, PRAGMA, or EXPLAIN."
        )

    if _WRITE_PATTERNS.search(stripped):
        return (
            "Write operations (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.) "
            "are not allowed through the MCP server."
        )

    return None


def check_table_allowed(table_name: str) -> str | None:
    """Check if a table is in the allowlist (if configured).

    Args:
        table_name: The table name to check.

    Returns:
        None if allowed, or an error message string if blocked.
    """
    if ALLOWED_TABLES is None:
        return None

    if table_name.lower() not in ALLOWED_TABLES:
        return (
            f"Table '{table_name}' is not in the allowed tables list. "
            f"Allowed tables: {', '.join(sorted(ALLOWED_TABLES))}"
        )

    return None


def truncate_result(text: str) -> str:
    """Truncate result text to configured character limit.

    Args:
        text: The result text to potentially truncate.

    Returns:
        The original text or truncated version with a notice.
    """
    if len(text) <= MAX_CHARS:
        return text

    truncated = text[:MAX_CHARS]
    return (
        f"{truncated}\n\n"
        f"[Result truncated at {MAX_CHARS:,} characters. "
        f"Use more specific queries or filters to reduce result size.]"
    )


def not_implemented(feature: str, enable_hint: str) -> str:
    """Generate a helpful not-implemented message for unavailable features.

    Args:
        feature: Description of the feature that isn't available yet.
        enable_hint: Instructions on how to enable the feature.

    Returns:
        A formatted message explaining the feature isn't available.
    """
    return (
        f"[Not Yet Available] {feature} has not been loaded into MoneyBin.\n\n"
        f"To enable this feature:\n{enable_hint}\n\n"
        "See MoneyBin docs for setup instructions."
    )
