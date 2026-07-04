"""SQLMesh state migration persists to the durable catalog, and drift is visible.

Regression coverage for the ephemeral-catalog bug: the in-process SQLMesh
migrate built its ``DuckDBEngineAdapter`` without the cursor pin that every
other SQLMesh path uses, so ``ctx.migrate()`` wrote its state (``_versions`` /
``_snapshots`` / ``_environments``) into the throwaway ``memory.sqlmesh.*``
catalog instead of the persistent ``moneybin.sqlmesh.*``. The state evaporated
at process exit while MoneyBin recorded the migrate as done — permanently
leaving ``refresh_run`` broken after a SQLMesh version bump, with no CLI signal
or recovery path.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _make_db(tmp_path: Path, secret_store: MagicMock) -> Database:
    return Database(
        tmp_path / "state.duckdb",
        secret_store=secret_store,
        read_only=False,
        no_auto_upgrade=True,
    )


def _persistent_schema_version(db: Database) -> int | None:
    """Read schema_version from the *durable* moneybin.sqlmesh._versions table."""
    exists = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'sqlmesh' AND table_name = '_versions'"
    ).fetchone()
    if not exists:
        return None
    row = db.execute("SELECT schema_version FROM sqlmesh._versions").fetchone()
    return None if row is None else row[0]


class TestMigratePersistsToDurableCatalog:
    """migrate_sqlmesh_state() must advance the persistent catalog, not a temp one."""

    def test_advances_rolled_back_persistent_state(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        db = _make_db(tmp_path, mock_secret_store)
        try:
            # Establish durable SQLMesh state via the correctly-pinned context.
            with sqlmesh_context(db) as ctx:
                ctx.migrate()
            assert _persistent_schema_version(db) == SCHEMA_VERSION

            # Simulate a prior under-migration leaving the durable state behind.
            db.execute(
                "UPDATE sqlmesh._versions SET schema_version = ?",
                [SCHEMA_VERSION - 1],
            )
            assert _persistent_schema_version(db) == SCHEMA_VERSION - 1

            # The migrate must repair the DURABLE catalog and confirm it.
            assert db.migrate_sqlmesh_state() is True
            assert _persistent_schema_version(db) == SCHEMA_VERSION
        finally:
            db.close()

    def test_returns_false_when_state_does_not_advance(
        self, tmp_path: Path, mock_secret_store: MagicMock, mocker: MockerFixture
    ) -> None:
        """A migrate that runs but leaves durable state behind must report False.

        Guards the honest-return contract independently of the pin: even if a
        future SQLMesh change silently under-migrates, the caller must not
        record success (and thus must retry on the next open).
        """
        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        db = _make_db(tmp_path, mock_secret_store)
        try:
            with sqlmesh_context(db) as ctx:
                ctx.migrate()
            db.execute(
                "UPDATE sqlmesh._versions SET schema_version = ?",
                [SCHEMA_VERSION - 1],
            )
            # Make ctx.migrate() a no-op so the state stays behind despite the call.
            import sqlmesh

            mocker.patch.object(sqlmesh.Context, "migrate", autospec=True)  # type: ignore[attr-defined]

            assert db.migrate_sqlmesh_state() is False
        finally:
            db.close()


class TestRepairSqlmeshState:
    """Database.repair_sqlmesh_state() — the recovery path behind db migrate apply."""

    def _sqlmesh_component_version(self, db: Database) -> str | None:
        row = db.execute(
            "SELECT version FROM app.versions WHERE component = 'sqlmesh'"
        ).fetchone()
        return None if row is None else row[0]

    def test_repair_advances_state_and_records_version(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        """Behind state → repair advances it, records the version proxy, True."""
        import importlib.metadata

        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        db = _make_db(tmp_path, mock_secret_store)
        try:
            with sqlmesh_context(db) as ctx:
                ctx.migrate()
            db.execute(
                "UPDATE sqlmesh._versions SET schema_version = ?",
                [SCHEMA_VERSION - 1],
            )

            assert db.repair_sqlmesh_state() is True
            assert _persistent_schema_version(db) == SCHEMA_VERSION
            # Proxy recorded so the next auto-open skips the check.
            assert self._sqlmesh_component_version(db) == importlib.metadata.version(
                "sqlmesh"
            )
        finally:
            db.close()

    def test_repair_returns_false_and_records_nothing_when_ahead(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        """Ahead state can't be migrated back — repair reports False, no record."""
        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        db = _make_db(tmp_path, mock_secret_store)
        try:
            with sqlmesh_context(db) as ctx:
                ctx.migrate()
            db.execute(
                "UPDATE sqlmesh._versions SET schema_version = ?",
                [SCHEMA_VERSION + 1],
            )
            # Clear the proxy the pinned migrate above recorded, so we can assert
            # repair does NOT record a false success.
            db.execute("DELETE FROM app.versions WHERE component = 'sqlmesh'")

            assert db.repair_sqlmesh_state() is False
            assert self._sqlmesh_component_version(db) is None
        finally:
            db.close()


class TestSqlmeshStateDrift:
    """sqlmesh_state_drift() surfaces a state-behind-package condition for status."""

    def test_reports_drift_when_state_behind_package(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        from moneybin.migrations import (
            sqlmesh_state_assessment,
            sqlmesh_state_drift,
        )

        db = _make_db(tmp_path, mock_secret_store)
        try:
            with sqlmesh_context(db) as ctx:
                ctx.migrate()
            assert sqlmesh_state_drift(db) is None  # current — no drift
            assert sqlmesh_state_assessment(db)[1] is False

            db.execute(
                "UPDATE sqlmesh._versions SET schema_version = ?",
                [SCHEMA_VERSION - 1],
            )
            drift = sqlmesh_state_drift(db)
            assert drift is not None
            assert "behind" in drift.lower()
            # Behind state is repairable by a migrate.
            assert sqlmesh_state_assessment(db)[1] is True
        finally:
            db.close()

    def test_ahead_state_drifts_but_is_not_migratable(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        """State AHEAD of the package drifts, but a migrate can't move it back."""
        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        from moneybin.migrations import (
            sqlmesh_state_assessment,
            sqlmesh_state_drift,
        )

        db = _make_db(tmp_path, mock_secret_store)
        try:
            with sqlmesh_context(db) as ctx:
                ctx.migrate()
            db.execute(
                "UPDATE sqlmesh._versions SET schema_version = ?",
                [SCHEMA_VERSION + 1],
            )
            drift = sqlmesh_state_drift(db)
            assert drift is not None
            assert "ahead" in drift.lower()
            # Ahead state must NOT trigger a (doomed) migrate.
            assert sqlmesh_state_assessment(db)[1] is False
        finally:
            db.close()

    def test_no_drift_when_state_absent(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        """A DB that has never run a SQLMesh plan has no state to be behind on."""
        from moneybin.migrations import (
            sqlmesh_state_assessment,
            sqlmesh_state_drift,
        )

        db = _make_db(tmp_path, mock_secret_store)
        try:
            assert sqlmesh_state_drift(db) is None
            assert sqlmesh_state_assessment(db)[1] is False
        finally:
            db.close()

    def test_reports_drift_through_read_only_open(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        """`db migrate status` opens read-only — drift detection must work there too."""
        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        from moneybin.migrations import sqlmesh_state_drift

        db = _make_db(tmp_path, mock_secret_store)
        with sqlmesh_context(db) as ctx:
            ctx.migrate()
        db.execute(
            "UPDATE sqlmesh._versions SET schema_version = ?", [SCHEMA_VERSION - 1]
        )
        db.checkpoint("post_migration")  # flush so a fresh read-only open sees it
        db.close()

        ro = Database(
            tmp_path / "state.duckdb",
            secret_store=mock_secret_store,
            read_only=True,
            no_auto_upgrade=True,
        )
        try:
            drift = sqlmesh_state_drift(ro)
            assert drift is not None
            assert "migrate" in drift.lower()
        finally:
            ro.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
