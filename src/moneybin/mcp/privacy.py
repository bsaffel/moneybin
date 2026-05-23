"""Privacy controls and query validation for the MCP server.

This module enforces query safety: managed write validation for dedicated
write tools, table allowlisting, and result size limits. Read-only query
validation lives in :mod:`moneybin.privacy.sql_query` (a SQL-safety
primitive shared with the CLI) and is re-exported here for callers that
import it from this module.
"""

import functools
import logging
import re
from enum import StrEnum

from moneybin.config import get_settings
from moneybin.privacy.sql_query import (
    validate_read_only_query as validate_read_only_query,  # re-export
)
from moneybin.privacy.taxonomy import Tier

logger = logging.getLogger(__name__)


class Sensitivity(StrEnum):
    """Data sensitivity tier for MCP tools.

    Every tool declares its maximum data sensitivity. The privacy
    middleware uses this to enforce consent gates and response filtering.

    See ``mcp-architecture.md`` section 5 for tier definitions.
    """

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


_TIER_TO_SENSITIVITY: dict[Tier, Sensitivity] = {
    Tier.LOW: Sensitivity.LOW,
    Tier.MEDIUM: Sensitivity.MEDIUM,
    Tier.HIGH: Sensitivity.HIGH,
    Tier.CRITICAL: Sensitivity.CRITICAL,
}


def tier_to_sensitivity(tier: Tier) -> Sensitivity:
    """Map a privacy ``Tier`` (numeric) to the MCP ``Sensitivity`` enum."""
    return _TIER_TO_SENSITIVITY[tier]


def log_tool_call(tool_name: str, sensitivity: Sensitivity) -> None:
    """Log an MCP tool invocation with its sensitivity tier.

    This is a privacy middleware stub. In v1, it only logs.
    When the consent management and audit log specs are implemented,
    this will check consent status, apply redaction, and write to
    the audit table.

    Args:
        tool_name: The registered tool name (underscore-joined).
        sensitivity: The tool's declared sensitivity tier.
    """
    logger.debug(f"MCP tool call: {tool_name} (sensitivity={sensitivity.value})")


def audit_log(
    *,
    tool: str,
    sensitivity: str,
    metadata: dict[str, object],
) -> None:
    """Record a privacy-relevant tool invocation with structured metadata.

    Stub implementation — writes a structured log entry at INFO level.
    When the audit log spec (privacy-data-protection.md) is implemented,
    this will persist to the audit table.

    Only counts, version strings, and filter parameters are allowed in
    metadata — never descriptions, IDs, or per-record content.

    Args:
        tool: Tool or command name (e.g. "transactions_categorize_assist").
        sensitivity: Sensitivity tier string ("low", "medium", "high").
        metadata: Count/parameter metadata. No PII or financial content.
    """
    logger.info(f"audit: tool={tool} sensitivity={sensitivity} metadata={metadata}")


@functools.lru_cache(maxsize=1)
def _get_mcp_limits() -> tuple[int, int, frozenset[str] | None]:
    """Load MCP limits from config (lazy, cached after first call).

    Returns:
        Tuple of (max_rows, max_chars, allowed_tables).
    """
    cfg = get_settings().mcp
    allowed = (
        frozenset(t.lower() for t in cfg.allowed_tables) if cfg.allowed_tables else None
    )
    return cfg.max_rows, cfg.max_chars, allowed


def get_max_rows() -> int:
    """Get the configured maximum rows for MCP query results."""
    max_rows, _, _ = _get_mcp_limits()
    return max_rows


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


def check_table_allowed(table_name: str) -> str | None:
    """Check if a table is in the allowlist (if configured).

    Args:
        table_name: The table name to check.

    Returns:
        None if allowed, or an error message string if blocked.
    """
    _, _, allowed_tables = _get_mcp_limits()
    if allowed_tables is None:
        return None

    if table_name.lower() not in allowed_tables:
        return (
            f"Table '{table_name}' is not in the allowed tables list. "
            f"Allowed tables: {', '.join(sorted(allowed_tables))}"
        )

    return None


def truncate_result(text: str) -> str:
    """Truncate result text to configured character limit.

    Args:
        text: The result text to potentially truncate.

    Returns:
        The original text or truncated version with a notice.
    """
    _, max_chars, _ = _get_mcp_limits()
    if len(text) <= max_chars:
        return text

    truncated = text[:max_chars]
    return (
        f"{truncated}\n\n"
        f"[Result truncated at {max_chars:,} characters. "
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
