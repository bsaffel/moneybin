"""Tests for V007: transaction-curation schema migration.

V007 introduces multi-note transaction notes, transaction tags, splits, import
labels, and a unified app.audit_log (subsuming the previously planned
app.ai_audit_log). The migration must:

- Backfill any pre-existing single-note rows in app.transaction_notes into the
  new multi-note shape with author='legacy' and a 12-hex-char note_id.
- Re-route app.ai_audit_log rows (if the table exists) into app.audit_log with
  action='ai.external_call' and AI-specific fields packed into context_json,
  then drop the old table.
"""

from __future__ import annotations

import json

from moneybin.database import Database
from moneybin.sql.migrations.V007__transaction_curation import migrate


def _drop_and_recreate_legacy_notes(db: Database) -> None:
    """Replace the new-shape transaction_notes table with the legacy single-note shape."""
    db.execute("DROP TABLE IF EXISTS app.transaction_notes")
    db.execute(
        """
        CREATE TABLE app.transaction_notes (
            transaction_id VARCHAR PRIMARY KEY,
            note VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _create_legacy_ai_audit_log(db: Database) -> None:
    """Create the retired app.ai_audit_log table per privacy-and-ai-trust.md."""
    db.execute(
        """
        CREATE TABLE app.ai_audit_log (
            audit_id VARCHAR PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            flow_tier INTEGER NOT NULL,
            feature VARCHAR NOT NULL,
            backend VARCHAR NOT NULL,
            model VARCHAR NOT NULL,
            data_sent_summary VARCHAR,
            data_sent_hash VARCHAR,
            response_summary VARCHAR,
            consent_reference VARCHAR,
            user_initiated BOOLEAN NOT NULL
        )
        """
    )


class TestV007Migration:
    """V007 migration: notes backfill, ai_audit_log retirement, new tables created."""

    def test_v007_creates_new_curation_tables(self, db: Database) -> None:
        """All five new tables exist after migration runs (idempotent on fresh DB)."""
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        expected = {
            ("raw", "manual_transactions"),
            ("app", "transaction_tags"),
            ("app", "transaction_splits"),
            ("app", "imports"),
            ("app", "audit_log"),
        }
        rows = db.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE (table_schema, table_name) IN "
            "(('raw', 'manual_transactions'), ('app', 'transaction_tags'), "
            "('app', 'transaction_splits'), ('app', 'imports'), ('app', 'audit_log'))"
        ).fetchall()
        assert {(r[0], r[1]) for r in rows} == expected

    def test_v007_backfills_legacy_notes_with_note_id_and_author(
        self, db: Database
    ) -> None:
        """Single-note rows get note_id (12 hex), author='legacy', preserved created_at."""
        _drop_and_recreate_legacy_notes(db)
        db.execute(
            "INSERT INTO app.transaction_notes (transaction_id, note, created_at) "
            "VALUES (?, ?, ?)",
            ["txn_legacy_1", "old single note", "2025-01-15 10:00:00"],
        )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        row = db.execute(
            "SELECT note_id, transaction_id, text, author, created_at "
            "FROM app.transaction_notes WHERE transaction_id = 'txn_legacy_1'"
        ).fetchone()
        assert row is not None
        note_id, transaction_id, text, author, created_at = row
        assert isinstance(note_id, str)
        assert len(note_id) == 12
        assert all(c in "0123456789abcdef" for c in note_id)
        assert transaction_id == "txn_legacy_1"
        assert text == "old single note"
        assert author == "legacy"
        assert str(created_at).startswith("2025-01-15 10:00:00")

    def test_v007_retires_ai_audit_log_table(self, db: Database) -> None:
        """ai_audit_log rows move to audit_log with AI fields in context_json; old table dropped."""
        _create_legacy_ai_audit_log(db)
        db.execute(
            """
            INSERT INTO app.ai_audit_log (
                audit_id, timestamp, flow_tier, feature, backend, model,
                data_sent_summary, data_sent_hash, response_summary,
                consent_reference, user_initiated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "ai_audit_1",
                "2025-02-01 12:00:00",
                2,
                "smart_import_parse",
                "anthropic",
                "claude-sonnet-4-6",
                "5 rows, redacted amounts",
                "deadbeef" * 8,
                "category labels list",
                "consent_grant_xyz",
                False,
            ],
        )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        # Old table is gone.
        result = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'ai_audit_log'"
        ).fetchone()
        assert result is not None
        assert result[0] == 0

        # Row re-routed into app.audit_log.
        row = db.execute(
            "SELECT audit_id, action, actor, occurred_at, context_json "
            "FROM app.audit_log WHERE audit_id = 'ai_audit_1'"
        ).fetchone()
        assert row is not None
        audit_id, action, actor, occurred_at, context_json = row
        assert audit_id == "ai_audit_1"
        assert action == "ai.external_call"
        assert actor == "ai:anthropic:claude-sonnet-4-6"
        assert str(occurred_at).startswith("2025-02-01 12:00:00")
        ctx = (
            json.loads(context_json) if isinstance(context_json, str) else context_json
        )
        assert ctx["flow_tier"] == 2
        assert ctx["feature"] == "smart_import_parse"
        assert ctx["backend"] == "anthropic"
        assert ctx["model"] == "claude-sonnet-4-6"
        assert ctx["data_sent_hash"] == "deadbeef" * 8
        assert ctx["consent_reference"] == "consent_grant_xyz"
        assert ctx["user_initiated"] is False

    def test_v007_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate run must leave state byte-identical to first run."""
        # Seed both legacy structures so the migration has real work to do.
        _drop_and_recreate_legacy_notes(db)
        db.execute(
            "INSERT INTO app.transaction_notes (transaction_id, note, created_at) "
            "VALUES (?, ?, ?)",
            ["txn_idem_1", "first legacy note", "2025-03-01 09:00:00"],
        )
        db.execute(
            "INSERT INTO app.transaction_notes (transaction_id, note, created_at) "
            "VALUES (?, ?, ?)",
            ["txn_idem_2", "second legacy note", "2025-03-02 09:00:00"],
        )
        _create_legacy_ai_audit_log(db)
        db.execute(
            """
            INSERT INTO app.ai_audit_log (
                audit_id, timestamp, flow_tier, feature, backend, model,
                data_sent_summary, data_sent_hash, response_summary,
                consent_reference, user_initiated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "ai_idem_1",
                "2025-03-03 09:00:00",
                1,
                "categorize",
                "anthropic",
                "claude-sonnet-4-6",
                "summary",
                "hash",
                "response",
                "consent_xyz",
                True,
            ],
        )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        notes_after_first = db.execute(
            "SELECT note_id, transaction_id, text, author, created_at "
            "FROM app.transaction_notes ORDER BY transaction_id"
        ).fetchall()
        audit_after_first = db.execute(
            "SELECT audit_id, occurred_at, actor, action, context_json "
            "FROM app.audit_log ORDER BY audit_id"
        ).fetchall()

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        notes_after_second = db.execute(
            "SELECT note_id, transaction_id, text, author, created_at "
            "FROM app.transaction_notes ORDER BY transaction_id"
        ).fetchall()
        audit_after_second = db.execute(
            "SELECT audit_id, occurred_at, actor, action, context_json "
            "FROM app.audit_log ORDER BY audit_id"
        ).fetchall()

        # Same row counts, same identifiers, same content.
        assert len(notes_after_first) == 2
        assert len(audit_after_first) == 1
        assert notes_after_second == notes_after_first
        assert audit_after_second == audit_after_first
