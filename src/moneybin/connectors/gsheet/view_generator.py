"""Generate CREATE OR REPLACE VIEW SQL for seed-adapter per-connection views."""

from __future__ import annotations

import re

from moneybin.sql.seed_view import generate_seed_view_sql as _shared
from moneybin.tables import GSHEET_SEEDS

_SAFE_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SAFE_CONN_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


def generate_seed_view_sql(
    *,
    alias: str,
    connection_id: str,
    typed_columns: dict[str, str],
) -> str:
    """Return CREATE OR REPLACE VIEW SQL for a seed connection.

    gsheet adapter: filters by connection_id and carries gsheet lifecycle columns.
    Wraps the shared seed_view builder with gsheet-specific validation messages
    and the soft-delete predicate (AND deleted_from_source_at IS NULL).

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

    sql = _shared(
        source_table=GSHEET_SEEDS.full_name,
        view_name=f"gsheet_{alias}",
        filter_column="connection_id",
        filter_value=connection_id,
        typed_columns=typed_columns,
        carry_columns=["row_number", "deleted_from_source_at", "loaded_at"],
    )
    assert sql.endswith(";")  # noqa: S101 — builder format invariant; failure = bug, not user error
    # gsheet views are live mirrors: exclude soft-deleted rows.
    # The shared builder can't carry this predicate because it's gsheet-specific;
    # append it before the trailing semicolon.
    return sql[:-1] + "\n  AND deleted_from_source_at IS NULL;"
