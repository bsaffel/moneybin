"""Tests for sync_classification_comments — sigil append and idempotency."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.comment_sync import (
    _SIGIL_RE,  # pyright: ignore[reportPrivateUsage]  # tested directly
    sync_classification_comments,
)
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass


def _get_comment(db: Database, schema: str, table: str, column: str) -> str | None:
    row = db.execute(
        """
        SELECT comment FROM duckdb_columns()
        WHERE schema_name = ? AND table_name = ? AND column_name = ?
        """,
        [schema, table, column],
    ).fetchone()
    return row[0] if row else None


def _set_comment(db: Database, schema: str, table: str, column: str, text: str) -> None:
    # DuckDB COMMENT ON COLUMN cannot be parameterized — inline the
    # literal. Test input only; not user-supplied SQL.
    safe = text.replace("'", "''")
    db.execute(
        f'COMMENT ON COLUMN "{schema}"."{table}"."{column}" IS \'{safe}\''  # noqa: S608  # test input, not executing SQL
    )


def _pick_classified_column(
    db: Database,
) -> tuple[str, str, str, DataClass]:
    """Return a (schema, table, column, expected_class) that exists live."""
    for (schema, table), cols in CLASSIFICATION.items():
        for col, cls in cols.items():
            row = db.execute(
                """
                SELECT 1 FROM duckdb_columns()
                WHERE schema_name = ? AND table_name = ?
                  AND column_name = ?
                """,
                [schema, table, col],
            ).fetchone()
            if row is not None:
                return schema, table, col, cls
    pytest.fail("No classified column exists live — registry is empty?")


def test_sigil_regex_strips_only_the_sigil() -> None:
    assert (
        _SIGIL_RE.sub("", "Account id [class: account_identifier]").rstrip()
        == "Account id"
    )
    assert _SIGIL_RE.sub("", "no sigil here") == "no sigil here"
    # Only the trailing sigil is stripped; an interior one is left as-is.
    assert (
        _SIGIL_RE.sub("", "Mixed [class: foo] middle [class: bar]")
        == "Mixed [class: foo] middle"
    )


def test_first_sync_appends_sigil(populated_db: Database) -> None:
    schema, table, col, cls = _pick_classified_column(populated_db)
    _set_comment(populated_db, schema, table, col, "Human description")

    sync_classification_comments(populated_db)

    assert _get_comment(populated_db, schema, table, col) == (
        f"Human description [class: {cls.value}]"
    )


def test_sync_is_idempotent(populated_db: Database) -> None:
    schema, table, col, _cls = _pick_classified_column(populated_db)
    _set_comment(populated_db, schema, table, col, "Human description")
    sync_classification_comments(populated_db)

    updated = sync_classification_comments(populated_db)

    assert updated == 0


def test_sync_preserves_human_description(populated_db: Database) -> None:
    schema, table, col, cls = _pick_classified_column(populated_db)
    _set_comment(populated_db, schema, table, col, "Account identifier")

    sync_classification_comments(populated_db)
    first = _get_comment(populated_db, schema, table, col)
    sync_classification_comments(populated_db)
    second = _get_comment(populated_db, schema, table, col)

    assert first == f"Account identifier [class: {cls.value}]"
    assert second == first


def test_sync_replaces_stale_sigil(populated_db: Database) -> None:
    """If a column's class changes, the suffix is replaced, not duplicated."""
    schema, table, col, cls = _pick_classified_column(populated_db)
    _set_comment(
        populated_db,
        schema,
        table,
        col,
        "Account identifier [class: stale_value]",
    )

    sync_classification_comments(populated_db)

    comment = _get_comment(populated_db, schema, table, col)
    assert comment == f"Account identifier [class: {cls.value}]"
    assert comment is not None
    assert comment.count("[class:") == 1
