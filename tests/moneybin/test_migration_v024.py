"""V024: add is_undo + undoes_operation_id to app.audit_log.

REC-PR3 marks audit rows produced by ``system_audit_undo`` so the undo
consumer can (a) recognize an already-undone operation and (b) make the undo
itself undoable. Two columns, both additive:

- ``is_undo BOOLEAN NOT NULL DEFAULT FALSE`` — touches existing data (the
  DEFAULT backfills every pre-spec row to FALSE), so it gets a populated
  ≥3-row fixture per ``.claude/rules/database.md``.
- ``undoes_operation_id VARCHAR NULL`` — pure additive, NULL on every
  non-undo row.

Mirrors V023's dual-path structure: an existing DB lacks both columns (the
``pre_v024_db`` fixture reconstructs that shape with all seven indexes), while
a fresh install already ships them via ``app_audit_log.sql`` so the migration
must no-op cleanly.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V024__add_undo_columns_to_audit_log import migrate
from tests.moneybin.migration_helpers import column_info, run_migration

# Realistic pre-spec audit rows: full 32-hex audit_ids + their op group ids.
_LEGACY_ROWS: tuple[tuple[str, str, str, str], ...] = (
    ("a1b2c3d4e5f60718293a4b5c6d7e8f90", "cli", "note.add", "op_" + "1" * 32),
    ("00112233445566778899aabbccddeeff", "mcp", "tag.add", "op_" + "2" * 32),
    (
        "ffeeddccbbaa99887766554433221100",
        "ai:anthropic:claude",
        "category.set",
        "op_legacy_x",
    ),
)

# Frozen post-V023 / pre-V024 shape: the 12 columns and 7 indexes an existing
# database carried before V024. Reconstructed here (the live schema file now
# ships is_undo + undoes_operation_id) so the migration runs against a table
# that already carries indexes + rows — the realistic upgrade case.
_PRE_V024_DDL = """
DROP TABLE app.audit_log;
CREATE TABLE app.audit_log (
    audit_id        VARCHAR PRIMARY KEY,
    occurred_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actor           VARCHAR NOT NULL,
    action          VARCHAR NOT NULL,
    target_schema   VARCHAR,
    target_table    VARCHAR,
    target_id       VARCHAR,
    before_value    JSON,
    after_value     JSON,
    parent_audit_id VARCHAR,
    context_json    JSON,
    operation_id    VARCHAR NOT NULL
);
CREATE INDEX idx_audit_log_target ON app.audit_log(target_table, target_id);
CREATE INDEX idx_audit_log_occurred ON app.audit_log(occurred_at DESC);
CREATE INDEX idx_audit_log_action ON app.audit_log(action);
CREATE INDEX idx_audit_log_actor ON app.audit_log(actor);
CREATE INDEX idx_audit_log_parent ON app.audit_log(parent_audit_id);
CREATE INDEX idx_audit_log_operation_id ON app.audit_log(operation_id);
CREATE INDEX idx_audit_log_occurred_at_op ON app.audit_log(occurred_at DESC, operation_id);
"""


def _indexes(db: Database) -> set[str]:
    return {
        r[0]
        for r in db.execute(
            "SELECT index_name FROM duckdb_indexes() "
            "WHERE schema_name = 'app' AND table_name = 'audit_log'"
        ).fetchall()
    }


@pytest.fixture()
def pre_v024_db(db: Database) -> Database:
    """audit_log as an existing database had it before V024: no undo columns."""
    db.execute(_PRE_V024_DDL)
    for audit_id, actor, action, operation_id in _LEGACY_ROWS:
        db.execute(
            "INSERT INTO app.audit_log "
            "(audit_id, actor, action, target_schema, target_table, target_id, "
            " before_value, after_value, parent_audit_id, context_json, operation_id) "
            "VALUES (?, ?, ?, 'app', 'transaction_notes', 'txn_x', "
            " NULL, '{\"k\": 1}', NULL, NULL, ?)",
            [audit_id, actor, action, operation_id],
        )
    return db


class TestExistingDbUpgrade:
    """V024 against a populated pre-spec audit_log."""

    def test_is_undo_added_not_null(self, pre_v024_db: Database) -> None:
        run_migration(pre_v024_db, migrate)
        _data_type, is_nullable = column_info(
            pre_v024_db, "app", "audit_log", "is_undo"
        )
        assert is_nullable is False

    def test_existing_rows_backfilled_to_false(self, pre_v024_db: Database) -> None:
        run_migration(pre_v024_db, migrate)
        rows = pre_v024_db.execute("SELECT is_undo FROM app.audit_log").fetchall()
        assert len(rows) == len(_LEGACY_ROWS)
        assert all(is_undo is False for (is_undo,) in rows)

    def test_undoes_operation_id_added_nullable(self, pre_v024_db: Database) -> None:
        run_migration(pre_v024_db, migrate)
        _data_type, is_nullable = column_info(
            pre_v024_db, "app", "audit_log", "undoes_operation_id"
        )
        assert is_nullable is True
        rows = pre_v024_db.execute(
            "SELECT undoes_operation_id FROM app.audit_log"
        ).fetchall()
        assert all(undoes is None for (undoes,) in rows)

    def test_pre_existing_indexes_survive(self, pre_v024_db: Database) -> None:
        # ADD COLUMN must not disturb the seven indexes already on the table.
        run_migration(pre_v024_db, migrate)
        indexes = _indexes(pre_v024_db)
        for name in (
            "idx_audit_log_target",
            "idx_audit_log_occurred",
            "idx_audit_log_action",
            "idx_audit_log_actor",
            "idx_audit_log_parent",
            "idx_audit_log_operation_id",
            "idx_audit_log_occurred_at_op",
        ):
            assert name in indexes, f"index {name!r} did not survive V024"

    def test_idempotent_rerun(self, pre_v024_db: Database) -> None:
        run_migration(pre_v024_db, migrate)
        run_migration(pre_v024_db, migrate)
        _dt, is_undo_nullable = column_info(pre_v024_db, "app", "audit_log", "is_undo")
        assert is_undo_nullable is False
        rows = pre_v024_db.execute("SELECT is_undo FROM app.audit_log").fetchall()
        assert all(is_undo is False for (is_undo,) in rows)


class TestFreshInstall:
    """V024 against a schema-file end-state (undo columns already present)."""

    def test_noop_without_error(self, db: Database) -> None:
        # db fixture already has both columns from the schema file.
        run_migration(db, migrate)
        _dt, is_undo_nullable = column_info(db, "app", "audit_log", "is_undo")
        assert is_undo_nullable is False
        _dt2, undoes_nullable = column_info(
            db, "app", "audit_log", "undoes_operation_id"
        )
        assert undoes_nullable is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
