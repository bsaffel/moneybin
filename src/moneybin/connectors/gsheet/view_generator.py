"""Generate CREATE OR REPLACE VIEW SQL for seed-adapter per-connection views."""

from __future__ import annotations

import re

_SAFE_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SAFE_CONN_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


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
        typed_columns: source header → DuckDB type. Each becomes a CAST projection.

    Raises:
        ValueError if alias or connection_id are not safe.
    """
    if not _SAFE_ALIAS_RE.fullmatch(alias):
        raise ValueError(
            f"Alias {alias!r} must match {_SAFE_ALIAS_RE.pattern} "
            f"(lowercase letters/digits/underscores, start with letter, max 63 chars)"
        )
    if not _SAFE_CONN_ID_RE.fullmatch(connection_id):
        raise ValueError(f"Invalid connection_id: {connection_id!r}")

    view_name = f"gsheet_{alias}"
    select_parts: list[str] = []

    for header, sql_type in typed_columns.items():
        col_name = _normalize_col_name(header)
        # data->>'<header>' extracts as text; CAST to the inferred type.
        # Escape single-quotes in header for the SQL string literal.
        header_lit = header.replace("'", "''")
        select_parts.append(f"CAST(data->>'{header_lit}' AS {sql_type}) AS {col_name}")

    # Carry through lifecycle columns from raw.gsheet_seeds.
    select_parts.append("row_number")
    select_parts.append("deleted_from_source_at")
    select_parts.append("loaded_at")
    select_clause = ",\n    ".join(select_parts)

    # connection_id is regex-validated above; safe to inline.
    return (
        f"CREATE OR REPLACE VIEW raw.{view_name} AS\n"
        f"SELECT\n    {select_clause}\n"
        f"FROM raw.gsheet_seeds\n"
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
