"""Shared CREATE OR REPLACE VIEW builder for JSON seed tables (gsheet, pdf)."""

from __future__ import annotations

import re

from sqlglot import exp

_SAFE_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SAFE_FILTER_VALUE_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
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
    source_table: str,
    view_name: str,
    filter_column: str,
    filter_value: str,
    typed_columns: dict[str, str],
    carry_columns: list[str],
) -> str:
    """Return CREATE OR REPLACE VIEW SQL projecting JSON seed rows into typed columns.

    Args:
        source_table: Qualified seed table (e.g. "raw.pdf_seeds"). Caller-trusted
            (a TableRef.full_name), never user input.
        view_name: Unqualified view name (raw.<view_name>); must match _SAFE_ALIAS_RE.
        filter_column: Column to filter on ("alias" for pdf, "connection_id" for
            gsheet). Caller-trusted literal.
        filter_value: Value bound as a SQL literal (VIEW bodies can't use ?); must
            match _SAFE_FILTER_VALUE_RE.
        typed_columns: source JSON key -> DuckDB type (each in _SAFE_SQL_TYPES).
        carry_columns: lifecycle columns passed through verbatim (caller-trusted).

    Raises:
        ValueError on any unsafe alias/value/type.
    """
    if not _SAFE_ALIAS_RE.fullmatch(view_name):
        raise ValueError(f"View name {view_name!r} must match {_SAFE_ALIAS_RE.pattern}")
    if not _SAFE_FILTER_VALUE_RE.fullmatch(filter_value):
        raise ValueError(f"Invalid filter value: {filter_value!r}")
    for header, sql_type in typed_columns.items():
        if sql_type not in _SAFE_SQL_TYPES:
            raise ValueError(
                f"Unsafe SQL type for {header!r}: {sql_type!r}. "
                f"Must be one of: {sorted(_SAFE_SQL_TYPES)}"
            )

    seen: dict[str, str] = dict.fromkeys(carry_columns, "<carry column>")
    select_parts: list[str] = []
    for header, sql_type in typed_columns.items():
        col_name = _normalize_col_name(header)
        if col_name in seen:
            prior = seen[col_name]
            if prior == "<carry column>":
                raise ValueError(
                    f"Header {header!r} normalizes to {col_name!r}, which collides "
                    f"with the reserved carry column. Rename the header before importing."
                )
            raise ValueError(
                f"Headers {prior!r} and {header!r} both normalize to {col_name!r}. "
                f"Rename one before importing."
            )
        seen[col_name] = header
        header_lit = header.replace("'", "''")
        safe_col = exp.to_identifier(col_name, quoted=True).sql("duckdb")
        select_parts.append(f"CAST(data->>'{header_lit}' AS {sql_type}) AS {safe_col}")
    select_parts.extend(
        exp.to_identifier(col, quoted=True).sql("duckdb") for col in carry_columns
    )
    select_clause = ",\n    ".join(select_parts)

    safe_view = exp.to_identifier(view_name, quoted=True).sql("duckdb")
    # All interpolations gated: source_table/filter_column caller-trusted (TableRef-derived);
    # view_name + filter_value regex-validated above; column names sqlglot-quoted; sql_type allowlisted.
    return (
        f"CREATE OR REPLACE VIEW raw.{safe_view} AS\n"
        f"SELECT\n    {select_clause}\n"
        f"FROM {source_table}\n"
        f"WHERE {filter_column} = '{filter_value}';"
    )


def _normalize_col_name(header: str) -> str:
    """Lowercase a source header into a safe [a-z0-9_] column name."""
    s = re.sub(r"[^a-z0-9]+", "_", header.lower().strip()).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = "_" + s
    return s
