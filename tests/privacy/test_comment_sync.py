"""Tests for sync_classification_comments — sigil append and idempotency."""

from __future__ import annotations

import logging

import pytest

from moneybin.database import Database
from moneybin.privacy.comment_sync import sync_classification_comments
from moneybin.privacy.taxonomy import CLASSIFICATION, SIGIL_RE, DataClass


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
        SIGIL_RE.sub("", "Account id [class: account_identifier]").rstrip()
        == "Account id"
    )
    assert SIGIL_RE.sub("", "no sigil here") == "no sigil here"
    # Only the trailing sigil is stripped; an interior one is left as-is.
    assert (
        SIGIL_RE.sub("", "Mixed [class: foo] middle [class: bar]")
        == "Mixed [class: foo] middle"
    )


def test_first_sync_appends_sigil(populated_db: Database) -> None:
    schema, table, col, cls = _pick_classified_column(populated_db)
    _set_comment(populated_db, schema, table, col, "Human description")

    sync_classification_comments(populated_db.conn)

    assert _get_comment(populated_db, schema, table, col) == (
        f"Human description [class: {cls.value}]"
    )


def test_sync_is_idempotent(populated_db: Database) -> None:
    schema, table, col, _cls = _pick_classified_column(populated_db)
    _set_comment(populated_db, schema, table, col, "Human description")
    sync_classification_comments(populated_db.conn)

    updated = sync_classification_comments(populated_db.conn)

    assert updated == 0


def test_sync_preserves_human_description(populated_db: Database) -> None:
    schema, table, col, cls = _pick_classified_column(populated_db)
    _set_comment(populated_db, schema, table, col, "Account identifier")

    sync_classification_comments(populated_db.conn)
    first = _get_comment(populated_db, schema, table, col)
    sync_classification_comments(populated_db.conn)
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

    sync_classification_comments(populated_db.conn)

    comment = _get_comment(populated_db, schema, table, col)
    assert comment == f"Account identifier [class: {cls.value}]"
    assert comment is not None
    assert comment.count("[class:") == 1


def test_sync_strips_sigil_for_unregistered_column(
    populated_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Removing a column from CLASSIFICATION strips its sigil on next sync."""
    from moneybin.privacy import comment_sync as cs

    schema, table, col, cls = _pick_classified_column(populated_db)
    _set_comment(
        populated_db,
        schema,
        table,
        col,
        f"Account identifier [class: {cls.value}]",
    )

    # Copy the registry with the chosen column removed.
    registry_copy = {
        k: {c: v for c, v in cols.items() if not (k == (schema, table) and c == col)}
        for k, cols in CLASSIFICATION.items()
    }
    monkeypatch.setattr(cs, "CLASSIFICATION", registry_copy)

    cs.sync_classification_comments(populated_db.conn)

    assert _get_comment(populated_db, schema, table, col) == "Account identifier"


def test_sync_count_logged_at_debug_not_info(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The 'Synced N …' housekeeping line must not surface at INFO.

    SQLMesh recreates VIEW models on every apply, wiping their column
    comments, so this sync re-applies sigils after every refresh/import.
    That is expected work, not user/agent-facing signal — it belongs at
    DEBUG so it stays off the default CLI/MCP output stream.
    """
    schema, table, col, _cls = _pick_classified_column(populated_db)
    _set_comment(populated_db, schema, table, col, "Human description")

    with caplog.at_level(logging.DEBUG, logger="moneybin.privacy.comment_sync"):
        updated = sync_classification_comments(populated_db.conn)

    assert updated >= 1, "fixture should produce at least one comment write"
    synced = [
        r for r in caplog.records if "privacy classification comment" in r.getMessage()
    ]
    assert synced, "expected the sync to log its count"
    offenders = [
        logging.getLevelName(r.levelno) for r in synced if r.levelno > logging.DEBUG
    ]
    assert not offenders, (
        f"'Synced N …' must be logged at DEBUG, but saw it at {offenders}"
    )
