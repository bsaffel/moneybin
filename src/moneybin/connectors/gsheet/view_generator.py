"""Generate CREATE OR REPLACE VIEW SQL for seed-adapter per-connection views."""

from __future__ import annotations

import re

from moneybin.sql.seed_view import generate_seed_view_sql as _shared
from moneybin.tables import GSHEET_SEEDS

# Max 56 chars: "gsheet_" prefix (7 chars) + 56 = 63, fitting the shared builder's limit.
_SAFE_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,55}$")
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
               starts with letter, max 56 chars — "gsheet_" prefix + 56 = 63,
               fitting the shared builder's 63-char view-name limit).
        connection_id: The connection's UUID. Bound as a literal in WHERE clause.
        typed_columns: source header → DuckDB type. Each type must be in
            _SAFE_SQL_TYPES (allowlist enforces no SQL injection via type
            strings even if persisted column_mapping is tampered with).

    Raises:
        ValueError if alias, connection_id, or any sql_type is not safe.
    """
    if not _SAFE_ALIAS_RE.fullmatch(alias):
        raise ValueError(
            f"Alias {alias!r} exceeds the 56-char limit or contains invalid "
            f"characters. Required: lowercase letters/digits/underscores, "
            f"start with a letter, ≤56 chars (was ≤63 before PR #228 — the "
            f"limit was tightened so the generated view name "
            f"'gsheet_<alias>' fits DuckDB's 63-char identifier limit). "
            f"Reconnect this gsheet with a shorter alias to continue syncing."
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
    if not sql.endswith(";"):
        raise RuntimeError(
            "seed_view builder format changed: expected trailing ';'. "
            "Update the gsheet wrapper to match."
        )
    # gsheet views are live mirrors: exclude soft-deleted rows.
    # The shared builder can't carry this predicate because it's gsheet-specific;
    # append it before the trailing semicolon.
    return sql[:-1] + "\n  AND deleted_from_source_at IS NULL;"
