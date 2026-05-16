"""Tests for MigrationRunner self-heal on stuck failure rows.

When a migration left a success=false row in app.schema_migrations, the
runner inspects the row's content_hash. If the hash differs from the current
migration body's hash, the maintainer has shipped a fix and the runner
auto-clears the failure row and retries once. If the hashes match (or the row
predates the content_hash backfill from V013), the existing guard reinstates.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from moneybin.database import Database
from moneybin.migrations import (
    Migration,
    MigrationError,
    MigrationRunner,
    short_hash,
)
from tests.moneybin.migration_helpers import insert_rows

_FAILURE_COLUMNS = (
    "version",
    "filename",
    "checksum",
    "success",
    "execution_ms",
    "content_hash",
)


def _insert_failure_row(
    db: Database,
    *,
    version: int,
    filename: str,
    content_hash: str | None,
) -> None:
    insert_rows(
        db,
        "app",
        "schema_migrations",
        _FAILURE_COLUMNS,
        [(version, filename, "ignored", False, 0, content_hash)],
    )


class TestSelfHealOnHashMismatch:
    """Failure row whose stored hash != current file body → auto-clear + retry."""

    def test_clears_stuck_row_and_applies_successfully(
        self, db: Database, tmp_path: Path
    ) -> None:
        """Stuck V001 with stale hash gets cleared, migration re-runs, table created."""
        sql_file = tmp_path / "V001__create_self_heal_demo.sql"
        sql_file.write_text(
            "CREATE TABLE IF NOT EXISTS app.self_heal_demo (id INTEGER PRIMARY KEY);"
        )
        migration = Migration.from_file(sql_file)

        _insert_failure_row(
            db,
            version=1,
            filename="V001__create_self_heal_demo.sql",
            content_hash="0000000000000000",  # deliberately not the real hash
        )

        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)

        # Failure row replaced with a single success row carrying the new hash.
        rows = db.execute(
            "SELECT success, content_hash FROM app.schema_migrations WHERE version = 1"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] is True
        assert rows[0][1] == short_hash(migration.content)

        # And the migration's effect is visible.
        table_count = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'self_heal_demo'"
        ).fetchone()
        assert table_count is not None
        assert table_count[0] == 1

    def test_apply_all_recovers_through_pending(
        self, db: Database, tmp_path: Path
    ) -> None:
        """apply_all() surfaces the stuck-but-fixable migration via pending()."""
        sql_file = tmp_path / "V001__recoverable.sql"
        sql_file.write_text(
            "CREATE TABLE IF NOT EXISTS app.recoverable (id INTEGER PRIMARY KEY);"
        )
        _insert_failure_row(
            db,
            version=1,
            filename="V001__recoverable.sql",
            content_hash="ffffffffffffffff",
        )

        runner = MigrationRunner(db, migrations_dir=tmp_path)
        result = runner.apply_all()

        assert result.failed is False
        assert result.applied_count == 1
        assert db.execute(
            "SELECT success FROM app.schema_migrations WHERE version = 1"
        ).fetchone() == (True,)


class TestNoSelfHealOnHashMatch:
    """Failure row whose stored hash == current body → loud guard, no retry."""

    def test_raises_and_does_not_re_execute_body(
        self, db: Database, tmp_path: Path
    ) -> None:
        """Hash match means the code is unchanged; retry would re-fail. Body must not run."""
        sql_file = tmp_path / "V001__same_code.sql"
        sql_file.write_text("CREATE TABLE app.same_code (id INTEGER);")
        migration = Migration.from_file(sql_file)

        _insert_failure_row(
            db,
            version=1,
            filename="V001__same_code.sql",
            content_hash=short_hash(migration.content),
        )

        runner = MigrationRunner(db, migrations_dir=tmp_path)
        with (
            patch.object(
                runner,
                "_execute_python_migration",
                side_effect=AssertionError("must not run"),
            ),
            pytest.raises(MigrationError, match="failed previously with the same code"),
        ):
            runner.apply_one(migration)

        # Body never ran — the would-be-created table does not exist.
        table_count = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'same_code'"
        ).fetchone()
        assert table_count is not None
        assert table_count[0] == 0

        # Failure row was not replaced.
        rows = db.execute(
            "SELECT success FROM app.schema_migrations WHERE version = 1"
        ).fetchall()
        assert rows == [(False,)]


class TestNoSelfHealOnLegacyNullHash:
    """Failure row with content_hash=NULL (pre-V013) → preserve original guard."""

    def test_raises_with_legacy_guidance(self, db: Database, tmp_path: Path) -> None:
        """Pre-self-heal rows have no hash; can't tell if code changed — require manual clear."""
        sql_file = tmp_path / "V001__legacy.sql"
        sql_file.write_text("SELECT 1;")
        migration = Migration.from_file(sql_file)

        _insert_failure_row(
            db,
            version=1,
            filename="V001__legacy.sql",
            content_hash=None,
        )

        runner = MigrationRunner(db, migrations_dir=tmp_path)
        with pytest.raises(MigrationError, match="pre-dates self-heal"):
            runner.apply_one(migration)


class TestCheckStuckBranching:
    """check_stuck() raises only for unrecoverable stuck rows."""

    def test_silent_when_only_self_heal_eligible(
        self, db: Database, tmp_path: Path
    ) -> None:
        """Hash-mismatch stuck rows do not raise — apply_one handles them."""
        sql_file = tmp_path / "V001__heal_me.sql"
        sql_file.write_text("SELECT 1;")
        _insert_failure_row(
            db,
            version=1,
            filename="V001__heal_me.sql",
            content_hash="deadbeefdeadbeef",
        )

        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.check_stuck()  # must not raise

    def test_raises_when_file_missing(self, db: Database, tmp_path: Path) -> None:
        """Stuck row whose migration file is gone is unrecoverable."""
        _insert_failure_row(
            db,
            version=1,
            filename="V001__gone.sql",
            content_hash="deadbeefdeadbeef",
        )

        runner = MigrationRunner(db, migrations_dir=tmp_path)
        with pytest.raises(MigrationError, match="no longer exists on disk"):
            runner.check_stuck()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
