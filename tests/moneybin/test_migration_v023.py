"""V023: add operation_id to app.audit_log with legacy backfill.

Two paths are covered:

- **Existing DB** (``pre_v023_db``): the column is absent and rows predate
  the spec. The migration adds it nullable, backfills ``op_legacy_<audit_id>``,
  tightens to NOT NULL, and builds the two indexes. Populated-fixture pattern
  per ``.claude/rules/database.md`` (touches existing data via ADD COLUMN +
  UPDATE + SET NOT NULL).
- **Fresh install** (``db``): the schema file already shipped operation_id
  NOT NULL, so the migration must no-op the column work and still ensure the
  indexes — and stay error-free (idempotency on first-ever open, where the
  runner applies every migration).
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V023__add_operation_id_to_audit_log import migrate
from tests.moneybin.migration_helpers import column_info, run_migration

pytestmark = pytest.mark.fresh_db

# Realistic pre-spec audit rows: full 32-hex audit_ids, varied actors/targets.
_LEGACY_ROWS: tuple[tuple[str, str, str], ...] = (
    ("a1b2c3d4e5f60718293a4b5c6d7e8f90", "cli", "note.add"),
    ("00112233445566778899aabbccddeeff", "mcp", "tag.add"),
    ("ffeeddccbbaa99887766554433221100", "ai:anthropic:claude", "category.set"),
)

# Frozen pre-V023 shape of app.audit_log: the 10 original columns plus the 5
# base indexes, exactly as an existing database had it. Reconstructed here
# (the live schema file now ships operation_id) so the migration runs against
# a table that already carries indexes — the case DuckDB's column-level ALTER
# is fussy about.
_PRE_V023_DDL = """
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
    context_json    JSON
);
CREATE INDEX idx_audit_log_target ON app.audit_log(target_table, target_id);
CREATE INDEX idx_audit_log_occurred ON app.audit_log(occurred_at DESC);
CREATE INDEX idx_audit_log_action ON app.audit_log(action);
CREATE INDEX idx_audit_log_actor ON app.audit_log(actor);
CREATE INDEX idx_audit_log_parent ON app.audit_log(parent_audit_id);
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
def pre_v023_db(db: Database) -> Database:
    """audit_log as an existing database had it before V023: no operation_id.

    The schema file now ships operation_id (fresh-install end state), so we
    drop it to reconstruct the pre-migration shape, then seed three rows the
    backfill must group as op_legacy_<audit_id>.
    """
    db.execute(_PRE_V023_DDL)
    for audit_id, actor, action in _LEGACY_ROWS:
        db.execute(
            "INSERT INTO app.audit_log "
            "(audit_id, actor, action, target_schema, target_table, target_id, "
            " before_value, after_value, parent_audit_id, context_json) "
            "VALUES (?, ?, ?, 'app', 'transaction_notes', 'txn_x', "
            " NULL, '{\"k\": 1}', NULL, NULL)",
            [audit_id, actor, action],
        )
    return db


class TestExistingDbUpgrade:
    """V023 against a populated pre-spec audit_log."""

    def test_column_added_and_not_null(self, pre_v023_db: Database) -> None:
        run_migration(pre_v023_db, migrate)
        _data_type, is_nullable = column_info(
            pre_v023_db, "app", "audit_log", "operation_id"
        )
        assert is_nullable is False

    def test_existing_rows_backfilled_to_op_legacy(self, pre_v023_db: Database) -> None:
        run_migration(pre_v023_db, migrate)
        rows = pre_v023_db.execute(
            "SELECT audit_id, operation_id FROM app.audit_log"
        ).fetchall()
        assert len(rows) == len(_LEGACY_ROWS)
        for audit_id, operation_id in rows:
            assert operation_id == f"op_legacy_{audit_id}"

    def test_both_indexes_created(self, pre_v023_db: Database) -> None:
        run_migration(pre_v023_db, migrate)
        indexes = _indexes(pre_v023_db)
        assert "idx_audit_log_operation_id" in indexes
        assert "idx_audit_log_occurred_at_op" in indexes

    def test_pre_existing_indexes_survive_drop_restore(
        self, pre_v023_db: Database
    ) -> None:
        # The SET NOT NULL workaround drops every explicit index and restores
        # it from catalog DDL; assert the 5 base indexes seeded by _PRE_V023_DDL
        # actually came back, not just the 2 new ones.
        run_migration(pre_v023_db, migrate)
        indexes = _indexes(pre_v023_db)
        for name in (
            "idx_audit_log_target",
            "idx_audit_log_occurred",
            "idx_audit_log_action",
            "idx_audit_log_actor",
            "idx_audit_log_parent",
        ):
            assert name in indexes, f"pre-existing index {name!r} was not restored"

    def test_nullable_column_partial_rerun_backfills_and_tightens(
        self, pre_v023_db: Database
    ) -> None:
        # Simulate a crash between ADD COLUMN and SET NOT NULL: the column
        # exists but is still nullable with NULL values. The migration's
        # nullable branch must backfill the stragglers and tighten.
        pre_v023_db.execute("ALTER TABLE app.audit_log ADD COLUMN operation_id VARCHAR")
        run_migration(pre_v023_db, migrate)
        _data_type, is_nullable = column_info(
            pre_v023_db, "app", "audit_log", "operation_id"
        )
        assert is_nullable is False
        rows = pre_v023_db.execute(
            "SELECT audit_id, operation_id FROM app.audit_log"
        ).fetchall()
        for audit_id, operation_id in rows:
            assert operation_id == f"op_legacy_{audit_id}"

    def test_idempotent_rerun(self, pre_v023_db: Database) -> None:
        run_migration(pre_v023_db, migrate)
        run_migration(pre_v023_db, migrate)
        rows = pre_v023_db.execute(
            "SELECT audit_id, operation_id FROM app.audit_log"
        ).fetchall()
        for audit_id, operation_id in rows:
            assert operation_id == f"op_legacy_{audit_id}"


class TestFreshInstall:
    """V023 against a schema-file end-state (operation_id already NOT NULL)."""

    def test_noop_creates_indexes_without_error(self, db: Database) -> None:
        # db fixture already has operation_id NOT NULL from the schema file.
        run_migration(db, migrate)
        indexes = _indexes(db)
        assert "idx_audit_log_operation_id" in indexes
        assert "idx_audit_log_occurred_at_op" in indexes

    def test_column_stays_not_null(self, db: Database) -> None:
        run_migration(db, migrate)
        _data_type, is_nullable = column_info(db, "app", "audit_log", "operation_id")
        assert is_nullable is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
