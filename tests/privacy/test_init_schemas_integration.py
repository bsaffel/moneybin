"""Verify init_schemas runs the classification sync at startup."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database


@pytest.fixture()
def fresh_db(tmp_path: Path) -> Database:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    return Database(
        tmp_path / "fresh.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )


def test_init_schemas_applies_sigils_to_app_tables(fresh_db: Database) -> None:
    """Sigils land on app.* columns after Database init (which calls init_schemas)."""
    row = fresh_db.execute(
        """
        SELECT comment FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'transaction_notes'
          AND column_name = 'text'
        """
    ).fetchone()
    assert row is not None, "expected app.transaction_notes.text to exist"
    comment = row[0]
    assert comment is not None
    assert "[class: user_note]" in comment, (
        f"expected [class: user_note] sigil, got: {comment!r}"
    )
    fresh_db.close()


def test_init_schemas_sigil_is_idempotent_for_app_tables(
    fresh_db: Database,
) -> None:
    """The sync inside init_schemas should be stable for app.* on rerun.

    Database.__init__ runs init_schemas (which syncs app.*) and then
    refresh_views (which creates core.dim_* views). A second sync call
    here is therefore expected to update only the freshly-materialized
    core.* surface — the app.* rows must already match the registry
    and contribute zero updates. We verify by reading the catalog
    twice and asserting app.* comments are unchanged.
    """
    before = {
        (s, t, c): comment
        for s, t, c, comment in fresh_db.execute(
            """
            SELECT schema_name, table_name, column_name, comment
            FROM duckdb_columns()
            WHERE schema_name = 'app'
            """
        ).fetchall()
    }

    from moneybin.privacy.comment_sync import sync_classification_comments

    sync_classification_comments(fresh_db.conn)

    after = {
        (s, t, c): comment
        for s, t, c, comment in fresh_db.execute(
            """
            SELECT schema_name, table_name, column_name, comment
            FROM duckdb_columns()
            WHERE schema_name = 'app'
            """
        ).fetchall()
    }

    assert before == after, "app.* comments drifted on second sync"
    fresh_db.close()
