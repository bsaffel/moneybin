"""Privacy controls and query validation for the MCP server.

This module enforces query safety: read-only validation for the general
query tool, managed write validation for dedicated write tools, and
result size limits.
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

# DuckDB table-valued functions that read local files or make network requests.
# These pass the read-only prefix check (SELECT/WITH) but can exfiltrate data.
# Includes scan_* and legacy parquet_scan aliases (resolve identically to read_*).
# glob() is matched as a function call only — \bglob\b would false-positive on
# DuckDB's GLOB infix comparison operator (e.g. WHERE desc GLOB '*AMAZON*').
_FILE_ACCESS_FUNCTIONS = re.compile(
    r"\b(read_csv|read_csv_auto|read_parquet|read_json|read_json_auto|"
    r"read_ndjson|read_text|read_blob|read_delta|read_iceberg|"
    r"scan_parquet|scan_csv|scan_csv_auto|scan_json|scan_ndjson|parquet_scan|"
    r"glob)\s*\(",
    re.IGNORECASE,
)

# URL scheme literals used as path arguments to DuckDB table scans when httpfs
# is loaded. These bypass function-name matching because DuckDB accepts
# `SELECT * FROM 'https://evil.com/data.parquet'` with no function keyword.
_URL_SCHEME_PATTERNS = re.compile(
    r"(https?://|s3://|az://|gcs://)",
    re.IGNORECASE,
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

# Dangerous DDL operations never allowed through managed writes
_DANGEROUS_OPS = re.compile(
    r"\b(DROP|ALTER|TRUNCATE|ATTACH|DETACH|EXPORT|COPY)\b",
    re.IGNORECASE,
)

# Schemas allowed for managed writes (INSERT/UPDATE/DELETE)
_WRITABLE_SCHEMAS = {"app", "raw"}

# Pattern to extract target schema from INSERT/UPDATE/DELETE statements
_WRITE_TARGET = re.compile(
    r"^\s*(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE|DELETE\s+FROM)\s+"
    r"(?:\"?(\w+)\"?\.)",
    re.IGNORECASE,
)

# Pattern for CREATE OR REPLACE TABLE in core schema (transforms only)
_CORE_TRANSFORM = re.compile(
    r"^\s*CREATE\s+OR\s+REPLACE\s+TABLE\s+(?:\"?core\"?\.)",
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

    if _FILE_ACCESS_FUNCTIONS.search(stripped):
        return (
            "File-access functions (read_csv, read_parquet, read_json, glob, etc.) "
            "are not allowed through the MCP server."
        )

    if _URL_SCHEME_PATTERNS.search(stripped):
        return (
            "URL literals (https://, s3://, etc.) are not allowed. "
            "Queries must read from database tables only."
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


def validate_managed_write(
    sql: str, *, allow_core_transforms: bool = False
) -> str | None:
    """Validate that a write operation targets only safe schemas.

    Managed writes are limited to INSERT/UPDATE/DELETE on app.* and raw.*
    schemas. Dangerous DDL (DROP, ALTER, TRUNCATE) is always rejected.

    When allow_core_transforms is True, CREATE OR REPLACE TABLE core.* is
    also permitted (used by the import service to rebuild core tables).

    Args:
        sql: The SQL statement to validate.
        allow_core_transforms: Allow CREATE OR REPLACE on core schema.

    Returns:
        None if the write is valid, or an error message string if rejected.
    """
    stripped = sql.strip()

    if not stripped:
        return "Empty query is not allowed."

    # Allow core transforms (CREATE OR REPLACE TABLE core.*)
    if allow_core_transforms and _CORE_TRANSFORM.match(stripped):
        return None

    # Block dangerous operations unconditionally
    if _DANGEROUS_OPS.search(stripped):
        ops = _DANGEROUS_OPS.findall(stripped)
        return (
            f"Dangerous operations ({', '.join(ops)}) are not allowed. "
            f"Only INSERT, UPDATE, and DELETE on app.* and raw.* schemas are permitted."
        )

    # Extract target schema
    match = _WRITE_TARGET.match(stripped)
    if not match:
        return (
            "Could not determine target schema. Managed writes must target "
            "app.* or raw.* schemas with explicit schema qualification."
        )

    schema = match.group(1).lower()
    if schema not in _WRITABLE_SCHEMAS:
        return (
            f"Writes to '{schema}' schema are not allowed. "
            f"Only app.* and raw.* schemas can be written to through managed tools."
        )

    return None
