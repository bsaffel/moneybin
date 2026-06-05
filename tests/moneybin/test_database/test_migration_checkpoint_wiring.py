"""Verify Database wiring emits a checkpoint after migrations apply.

Pins the wiring between MigrationRunner.apply_all() and
Database.checkpoint("post_migration") on the write-mode init path. A
no-op open (already-migrated DB) must NOT increment the counter — that
would dilute the dashboard signal for "a real migration ran here."
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from moneybin.database import Database
from moneybin.metrics.registry import DB_CHECKPOINT_TOTAL


def test_checkpoint_fires_after_migrations_apply(tmp_path: Path) -> None:
    """The post_migration checkpoint fires after applying pending migrations.

    Verifies the counter increments at least once after a first-init write
    open that runs the migration runner.
    """
    before = DB_CHECKPOINT_TOTAL.labels(reason="post_migration")._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    mock_store = MagicMock()
    mock_store.get_key.return_value = "mig-checkpoint-key"
    # no_auto_upgrade=False -> migrations run on init
    database = Database(
        tmp_path / "mig.duckdb",
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=False,
    )
    try:
        after = DB_CHECKPOINT_TOTAL.labels(reason="post_migration")._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
        assert after >= before + 1, (
            "Post-migration checkpoint did not fire after Database init "
            "with migrations enabled."
        )
    finally:
        database.close()


def test_checkpoint_does_not_fire_when_no_migrations_pending(
    tmp_path: Path,
) -> None:
    """Re-opening a fully-migrated DB must not increment the counter."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "mig-checkpoint-key"
    # First open: runs migrations.
    db1 = Database(
        tmp_path / "mig.duckdb",
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=False,
    )
    db1.close()
    before = DB_CHECKPOINT_TOTAL.labels(reason="post_migration")._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    # Second open: nothing to migrate.
    db2 = Database(
        tmp_path / "mig.duckdb",
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=False,
    )
    try:
        after = DB_CHECKPOINT_TOTAL.labels(reason="post_migration")._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
        assert after == before
    finally:
        db2.close()
