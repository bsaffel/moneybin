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
        legacy_notes = [
            ("txn_legacy_1", "old single note", "2025-01-15 10:00:00"),
            ("txn_legacy_2", "grocery run on the road", "2025-01-16 09:30:00"),
            (
                "txn_legacy_3",
                "split rent w/ roommate — paid back via venmo",
                "2025-01-17 18:45:00",
            ),
        ]
        for transaction_id, note, created_at in legacy_notes:
            db.execute(
                "INSERT INTO app.transaction_notes "
                "(transaction_id, note, created_at) VALUES (?, ?, ?)",
                [transaction_id, note, created_at],
            )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        rows = db.execute(
            "SELECT note_id, transaction_id, text, author, created_at "
            "FROM app.transaction_notes ORDER BY transaction_id"
        ).fetchall()
        assert len(rows) == len(legacy_notes)
        for (note_id, transaction_id, text, author, created_at), (
            expected_txn,
            expected_text,
            expected_ts,
        ) in zip(rows, legacy_notes, strict=True):
            assert isinstance(note_id, str)
            assert len(note_id) == 12
            assert all(c in "0123456789abcdef" for c in note_id)
            assert transaction_id == expected_txn
            assert text == expected_text
            assert author == "legacy"
            assert str(created_at).startswith(expected_ts)

    def test_v007_retires_ai_audit_log_table(self, db: Database) -> None:
        """ai_audit_log rows move to audit_log with AI fields in context_json; old table dropped."""
        _create_legacy_ai_audit_log(db)
        legacy_ai_rows = [
            (
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
            ),
            (
                "ai_audit_2",
                "2025-02-02 14:30:00",
                1,
                "categorize",
                "anthropic",
                "claude-haiku-4-5",
                "12 txns, masked merchants",
                "cafef00d" * 8,
                "category assignments",
                "consent_grant_abc",
                True,
            ),
            (
                "ai_audit_3",
                "2025-02-03 09:15:00",
                3,
                "merchant_normalize",
                "openai",
                "gpt-4o-mini",
                "30 merchant strings",
                "feedface" * 8,
                "canonical names",
                "consent_grant_def",
                True,
            ),
        ]
        for row in legacy_ai_rows:
            db.execute(
                """
                INSERT INTO app.ai_audit_log (
                    audit_id, timestamp, flow_tier, feature, backend, model,
                    data_sent_summary, data_sent_hash, response_summary,
                    consent_reference, user_initiated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(row),
            )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        result = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'ai_audit_log'"
        ).fetchone()
        assert result is not None
        assert result[0] == 0

        rows = db.execute(
            "SELECT audit_id, action, actor, occurred_at, context_json "
            "FROM app.audit_log ORDER BY audit_id"
        ).fetchall()
        assert len(rows) == len(legacy_ai_rows)
        for (audit_id, action, actor, occurred_at, context_json), legacy in zip(
            rows, legacy_ai_rows, strict=True
        ):
            (
                expected_audit_id,
                expected_ts,
                expected_flow_tier,
                expected_feature,
                expected_backend,
                expected_model,
                _summary,
                expected_hash,
                _response,
                expected_consent,
                expected_user_initiated,
            ) = legacy
            assert audit_id == expected_audit_id
            assert action == "ai.external_call"
            assert actor == f"ai:{expected_backend}:{expected_model}"
            assert str(occurred_at).startswith(expected_ts)
            ctx = (
                json.loads(context_json)
                if isinstance(context_json, str)
                else context_json
            )
            assert ctx["flow_tier"] == expected_flow_tier
            assert ctx["feature"] == expected_feature
            assert ctx["backend"] == expected_backend
            assert ctx["model"] == expected_model
            assert ctx["data_sent_hash"] == expected_hash
            assert ctx["consent_reference"] == expected_consent
            assert ctx["user_initiated"] is expected_user_initiated

    def test_v007_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate run must leave state byte-identical to first run."""
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

        assert len(notes_after_first) == 2
        assert len(audit_after_first) == 1
        assert notes_after_second == notes_after_first
        assert audit_after_second == audit_after_first
