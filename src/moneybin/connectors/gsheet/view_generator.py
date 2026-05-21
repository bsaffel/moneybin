"""Generate CREATE OR REPLACE VIEW SQL for seed-adapter per-connection views."""

from __future__ import annotations

import re

from moneybin.tables import GSHEET_SEEDS

_SAFE_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SAFE_CONN_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_SAFE_SQL_TYPES = frozenset({
    "VARCHAR",
    "BIGINT",
    "DECIMAL(18,2)",
    "DATE",
    "TIMESTAMP",
    "BOOLEAN",
    "DOUBLE",
})


def generate_seed_view_sql(
    *,
    alias: str,
    connection_id: str,
    typed_columns: dict[str, str],
) -> str:
    """Return CREATE OR REPLACE VIEW SQL for a seed connection.

    Args:
        alias: User-supplied slug; becomes the view name (raw.gsheet_<alias>).
               Must match _SAFE_ALIAS_RE (lowercase letters/digits/underscores,
               starts with letter, max 63 chars).
        connection_id: The connection's UUID. Bound as a literal in WHERE clause.
        typed_columns: source header → DuckDB type. Each type must be in
            _SAFE_SQL_TYPES (allowlist enforces no SQL injection via type
            strings even if persisted column_mapping is tampered with).

    Raises:
        ValueError if alias, connection_id, or any sql_type is not safe.
    """
    if not _SAFE_ALIAS_RE.fullmatch(alias):
        raise ValueError(
            f"Alias {alias!r} must match {_SAFE_ALIAS_RE.pattern} "
            f"(lowercase letters/digits/underscores, start with letter, max 63 chars)"
        )
    if not _SAFE_CONN_ID_RE.fullmatch(connection_id):
        raise ValueError(f"Invalid connection_id: {connection_id!r}")
    for header, sql_type in typed_columns.items():
        if sql_type not in _SAFE_SQL_TYPES:
            raise ValueError(
                f"Unsafe SQL type for column {header!r}: {sql_type!r}. "
                f"Must be one of: {sorted(_SAFE_SQL_TYPES)}"
            )

    view_name = f"gsheet_{alias}"
    select_parts: list[str] = []
    seen_normalized: dict[str, str] = {}

    for header, sql_type in typed_columns.items():
        col_name = _normalize_col_name(header)
        # Catch collisions like "Amount USD" + "Amount_USD" → both become
        # "amount_usd". Without this guard, DuckDB raises an opaque duplicate-
        # alias error during CREATE VIEW; here we point at the conflicting
        # headers so the user knows what to rename in their sheet.
        if col_name in seen_normalized:
            raise ValueError(
                f"Headers {seen_normalized[col_name]!r} and {header!r} both "
                f"normalize to {col_name!r}. Rename one of the conflicting "
                "columns in the sheet before connecting."
            )
        seen_normalized[col_name] = header
        # data->>'<header>' extracts as text; CAST to the inferred type.
        # Escape single-quotes in header for the SQL string literal.
        header_lit = header.replace("'", "''")
        select_parts.append(f"CAST(data->>'{header_lit}' AS {sql_type}) AS {col_name}")

    # Carry through lifecycle columns from raw.gsheet_seeds.
    select_parts.append("row_number")
    select_parts.append("deleted_from_source_at")
    select_parts.append("loaded_at")
    select_clause = ",\n    ".join(select_parts)

    # VIEW bodies cannot use ? placeholders — connection_id must be a literal.
    # _SAFE_CONN_ID_RE validates it is UUID4-hex-shaped (alphanumeric / underscore
    # / dash, ≤64 chars), so inline interpolation is safe by the project's
    # allowlist-then-quote convention.
    return (
        f"CREATE OR REPLACE VIEW raw.{view_name} AS\n"
        f"SELECT\n    {select_clause}\n"
        f"FROM {GSHEET_SEEDS.full_name}\n"
        f"WHERE connection_id = '{connection_id}'\n"
        f"  AND deleted_from_source_at IS NULL;"
    )


def _normalize_col_name(header: str) -> str:
    """Convert a source header into a safe lowercase column name.

    - Lowercases and strips whitespace
    - Replaces non-alphanumeric (except underscore) with underscore
    - Removes leading/trailing underscores
    - Prefixes with underscore if starts with digit
    - Defaults to 'col' if result is empty
    """
    s = header.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = "_" + s
    return s
