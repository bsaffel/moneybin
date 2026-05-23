# src/moneybin/privacy/sql_query.py
"""Privacy-enforcing read-only SQL execution shared by the MCP and CLI surfaces.

This is the shared primitive behind the ``sql_query`` MCP tool and the
``moneybin sql query`` CLI command. Both surfaces call
:func:`execute_sql_query`, so privacy enforcement is
structural rather than per-surface: the read-only gate, the core/app schema
restriction, sqlglot column lineage, and CRITICAL masking all run here, below
the adapters. Neither surface can return rows that skipped redaction, and a
future third surface inherits the same guarantees by calling this primitive.

The read-only validation (``validate_read_only_query`` and its safety regexes)
lives here too — it is a SQL-safety primitive alongside ``sql_lineage`` and
``redaction``, not an MCP concern. ``mcp.privacy`` re-exports it for callers
that still import it from there.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.redaction import redact_records
from moneybin.privacy.sql_lineage import (
    SqlParseError,
    SqlSchemaError,
    derive_query_tier,
    expand_star,
    get_current_schema_snapshot,
    is_data_query,
    parse_cached,
    resolve_output_classes,
    tables_outside_schemas,
)
from moneybin.privacy.taxonomy import DataClass, Tier

logger = logging.getLogger(__name__)

# Data queries may reference only the schemas the privacy CLASSIFICATION
# registry covers, so every queryable column has a known data class and the
# masking guarantee is sound. reports.* is deferred until its views are
# classified (tracked as a follow-up); raw/prep/meta are internal schemas.
_ALLOWED_QUERY_SCHEMAS = frozenset({"core", "app"})

# --- Read-only / file-access safety gate -----------------------------------
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

# DuckDB replacement scans can read files with `FROM 'path/to/file.csv'`
# without using read_csv/read_parquet. A single-quoted table source is not a
# normal catalog table reference, so reject it before execution.
_QUOTED_TABLE_SCAN = re.compile(
    r"(?<!')\b(FROM|JOIN)\s*'[^']+'",
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
            "are not allowed."
        )

    if _URL_SCHEME_PATTERNS.search(stripped):
        return (
            "URL literals (https://, s3://, etc.) are not allowed. "
            "Queries must read from database tables only."
        )

    if _QUOTED_TABLE_SCAN.search(stripped):
        return (
            "Quoted file/table path scans are not allowed. "
            "Queries must read from database tables only."
        )

    if _WRITE_PATTERNS.search(stripped):
        return (
            "Write operations (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.) "
            "are not allowed."
        )

    return None


@dataclass(frozen=True)
class SqlQueryResult:
    """Outcome of a privacy-enforced SQL query, ready for either surface.

    ``records`` are already redacted (CRITICAL columns masked); both adapters
    consume them as-is. ``output_classes`` maps each result column to its
    resolved data class — empty for metadata (DESCRIBE/SHOW/PRAGMA/EXPLAIN)
    queries, which carry no row-data classification.
    """

    records: list[dict[str, Any]]
    columns: list[str]
    output_classes: dict[str, DataClass]
    tier: Tier
    total_count: int
    truncated: bool
    is_metadata: bool = field(default=False)

    @property
    def classes_returned(self) -> list[str]:
        """Sorted data-class values for the envelope/audit.

        ``["aggregate"]`` when no row-data classes apply (metadata or
        pure-aggregate queries).
        """
        if not self.output_classes:
            return ["aggregate"]
        return sorted({c.value for c in self.output_classes.values()})


def _fetch(
    db: Database, query: str, max_rows: int
) -> tuple[list[str], list[Any], bool]:
    """Execute ``query`` read-only and fetch up to ``max_rows`` (+1 to detect more)."""
    # Security: the caller validated the query is read-only and free of
    # file-access vectors; the entire string is intentionally user SQL and
    # cannot be parameterized.
    result = db.execute(query)  # noqa: S608 — read-only, validated by caller
    columns = [desc[0] for desc in result.description]
    rows = result.fetchmany(max_rows + 1)
    truncated = len(rows) > max_rows
    return columns, rows[:max_rows], truncated


def execute_sql_query(db: Database, query: str, *, max_rows: int) -> SqlQueryResult:
    """Run a read-only SQL query with full privacy enforcement.

    Pipeline: read-only gate → parse → metadata-or-data routing → (data:
    core/app schema gate → sqlglot lineage → execute → CRITICAL masking).
    Returns redacted rows plus the resolved tier and per-column classes.

    Args:
        db: An open (read-only) database connection.
        query: The SQL query to execute.
        max_rows: Row cap; one extra row is fetched to detect truncation.

    Raises:
        UserError: On a rejected, unparseable, out-of-scope, unknown-table, or
            failed query. The ``code`` is one of the ``error_codes.SQL_*``
            values so both surfaces classify failures identically.
    """
    error = validate_read_only_query(query)
    if error:
        raise UserError(error, code=error_codes.SQL_INVALID_QUERY)

    try:
        tree = parse_cached(query)
    except SqlParseError as e:
        raise UserError(
            "Could not parse SQL.",
            code=error_codes.SQL_INVALID_QUERY,
            details={"detail": str(e)},
        ) from e

    # DESCRIBE/SHOW/PRAGMA/EXPLAIN return schema/plan text, not row data — run
    # them directly at LOW; the lineage gate applies only to data queries.
    if not is_data_query(tree):
        columns, rows, truncated = _fetch_metadata(db, query, max_rows)
        records = [dict(zip(columns, row, strict=False)) for row in rows]
        return SqlQueryResult(
            records=records,
            columns=columns,
            output_classes={},
            tier=Tier.LOW,
            total_count=max_rows + 1 if truncated else len(records),
            truncated=truncated,
            is_metadata=True,
        )

    try:
        snapshot = get_current_schema_snapshot(db)
        qtree = expand_star(tree, snapshot)
        disallowed = tables_outside_schemas(qtree, snapshot, _ALLOWED_QUERY_SCHEMAS)
        if disallowed:
            raise UserError(
                "Queries are limited to the core and app schemas.",
                code=error_codes.SQL_SCHEMA_NOT_ALLOWED,
                hint="Use the curated report views; raw/prep are internal schemas.",
                details={"disallowed": sorted(set(disallowed))},
            )
        output_classes = resolve_output_classes(qtree, snapshot, query)
        columns, rows, truncated = _fetch(db, query, max_rows)
    except SqlParseError as e:
        raise UserError(
            "Could not parse SQL.",
            code=error_codes.SQL_INVALID_QUERY,
            details={"detail": str(e)},
        ) from e
    # SqlSchemaError comes from the lineage qualify step; CatalogException from
    # DuckDB at execute time. Both mean "table/column doesn't exist".
    except (SqlSchemaError, duckdb.CatalogException) as e:
        # Don't echo str(e) to the client: a DuckDB/lineage message can quote
        # the query verbatim (including literal values). Log it server-side
        # (SanitizedLogFormatter masks PII) — the code + message classify it.
        logger.warning(f"sql_query unknown table/column: {e}")
        raise UserError(
            "Unknown table or column.",
            code=error_codes.SQL_UNKNOWN_TABLE,
        ) from e
    except duckdb.Error as e:
        logger.warning(f"sql_query execution error: {e}")
        raise UserError(
            "Query execution failed.",
            code=error_codes.SQL_QUERY_ERROR,
        ) from e

    records = [dict(zip(columns, row, strict=False)) for row in rows]
    # Map each DuckDB result column to its DataClass BY NAME. This is robust to
    # any divergence between sqlglot's projection order and DuckDB's runtime
    # column order (the SELECT * case), which a positional join is not. Named
    # projections and expanded `*` columns match by name directly. Unaliased
    # expressions are the one mismatch — sqlglot names MIN(account_id) ''/'?_i'
    # while DuckDB names it 'min(account_id)' — so they FAIL CLOSED to the
    # query's max tier. An unmasked CRITICAL value therefore can never slip
    # through: a name we can't resolve is treated as the most sensitive class
    # present (over-redaction, not under-redaction).
    fallback = (
        max(output_classes.values(), key=lambda c: c.tier)
        if output_classes
        else DataClass.AGGREGATE
    )
    col_classes = {col: output_classes.get(col, fallback) for col in columns}
    redacted = redact_records(records, col_classes, consent=None)

    return SqlQueryResult(
        records=redacted,
        columns=columns,
        output_classes=output_classes,
        tier=derive_query_tier(output_classes),
        # total_count > returned makes has_more true downstream. We don't pay
        # for an exact COUNT(*); +1 signals "at least one more row".
        total_count=max_rows + 1 if truncated else len(records),
        truncated=truncated,
    )


def _fetch_metadata(
    db: Database, query: str, max_rows: int
) -> tuple[list[str], list[Any], bool]:
    """Execute a metadata statement (DESCRIBE/SHOW/PRAGMA/EXPLAIN) at LOW.

    Wraps DuckDB errors in a UserError so the metadata path classifies
    failures the same way the data path does.
    """
    try:
        return _fetch(db, query, max_rows)
    except duckdb.Error as e:
        # See execute_sql_query: keep str(e) out of the client envelope.
        logger.warning(f"sql_query metadata error: {e}")
        raise UserError(
            "Query execution failed.",
            code=error_codes.SQL_QUERY_ERROR,
        ) from e
