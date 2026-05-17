"""Sync the DataClass registry into DuckDB column comments.

Each classified column's existing comment is suffixed with
``[class: <DataClass value>]``. Re-running with the same registry is a
no-op. Changing a column's class replaces the suffix rather than
appending a second one.

The sync runs after the two existing comment-writing paths:

- ``schema._apply_comments`` (per-startup DDL comments for raw/app)
- SQLMesh's ``register_comments`` (per-run comments for SQLMesh-managed
  core models)

so the human description is the prefix and the class sigil is the
suffix.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

import sqlglot.expressions as exp

from moneybin.database import escape_sql_literal
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)

# Matches ` [class: word_chars]` with optional leading whitespace so a
# previously-applied sigil can be stripped before reapplication.
_SIGIL_RE = re.compile(r"\s*\[class:\s*[a-z0-9_]+\s*\]\s*$")


def _quote(ident: str) -> str:
    return exp.to_identifier(ident, quoted=True).sql(dialect="duckdb")


def _desired_comment(human: str | None, cls: DataClass) -> str:
    base = _SIGIL_RE.sub("", human or "").rstrip()
    sigil = f"[class: {cls.value}]"
    return f"{base} {sigil}".strip() if base else sigil


def sync_classification_comments(db: Database) -> int:
    """Append the [class: ...] sigil to each classified column's comment.

    Args:
        db: An open read-write ``Database``.

    Returns:
        Number of ``COMMENT ON COLUMN`` statements actually executed.
        Zero on a no-op run (idempotent).
    """
    current: dict[tuple[str, str, str], str | None] = {}
    rows = db.execute(
        """
        SELECT schema_name, table_name, column_name, comment
        FROM duckdb_columns()
        WHERE schema_name IN ('core', 'app')
        """
    ).fetchall()
    for schema, table, col, comment in rows:
        current[(schema, table, col)] = comment

    updates = 0
    for (schema, table), cols in CLASSIFICATION.items():
        for col, cls in cols.items():
            key = (schema, table, col)
            if key not in current:
                continue
            desired = _desired_comment(current[key], cls)
            if current[key] == desired:
                continue
            # DuckDB's COMMENT ON COLUMN does not accept `?` placeholders —
            # it requires an inline string literal. Use the project's
            # escape_sql_literal helper to single-quote-escape the value.
            safe = escape_sql_literal(desired)
            db.execute(
                f"COMMENT ON COLUMN {_quote(schema)}.{_quote(table)}."
                f"{_quote(col)} IS '{safe}'"
            )
            updates += 1

    if updates:
        logger.info(f"Synced {updates} privacy classification comment(s)")
    return updates
