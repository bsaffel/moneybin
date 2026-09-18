"""Tests for read-only opens escalating a behind-schedule migration ladder.

MB-255 PR review found the general defect behind this file: a read-only
open ATTACHes and returns immediately (``Database.__init__``), never
reaching ``init_schemas()`` or ``MigrationRunner`` at all -- so a read-only
caller could read a schema the running code had already moved past. Worse,
``init_schemas()`` runs unconditionally *before* the ``no_auto_upgrade``
check on a write open, so a write with auto-upgrade disabled could silently
create a freshly-relocated table EMPTY while the real rows stayed stranded
under the table's old name.

Brandon's decision: opening the database on a newer app version brings it
current as part of that open -- read-only opens included -- by escalating to
a write open (reusing the existing write-lock coordination), and refuses
with a classified error instead of ever serving a stale or empty read when
the upgrade genuinely cannot run. Nothing here is specific to any one
migration or table; every test below exercises the general mechanism via a
synthetic version mismatch, not a real schema change.
"""

from __future__ import annotations

import importlib.metadata
import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
import moneybin.db_lock as db_lock_module
from moneybin.database import (
    Database,
    DatabaseLockError,
    DatabaseUpgradeRequiredError,
    get_database,
)

_STALE_VERSION = "0.0.1"


@pytest.fixture
def configured_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[tuple[Path, MagicMock, MagicMock], None, None]:
    """Bootstrap a fully-migrated DB and point ``get_database()`` at it.

    Mirrors the ``configured_db`` fixture in
    ``test_get_database_lock_integration.py``.
    """
    db_path = tmp_path / "escalation.duckdb"
    mock_store = MagicMock()
    mock_store.get_key.return_value = "read-only-escalation-test-key"

    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.no_auto_upgrade = False

    monkeypatch.setattr(db_module, "get_settings", lambda: mock_settings)
    monkeypatch.setattr(db_module, "SecretStore", lambda: mock_store)
    monkeypatch.setattr(db_module, "_cached_encryption_key", None)
    monkeypatch.setattr(db_module, "_migration_check_done", set[Path]())
    monkeypatch.setattr(db_module, "_active_write_conn", None)

    with Database(db_path, read_only=False, secret_store=mock_store):
        pass  # first open: runs init_schemas() + the full migration ladder

    yield db_path, mock_settings, mock_store


def _mark_ladder_stale(db_path: Path, mock_store: MagicMock) -> None:
    """Stamp ``app.versions`` behind the installed package.

    Bypasses ``Database.__init__``'s own migration path (which would just
    re-stamp it back to current) by tampering directly via a plain
    ``execute()`` on an already-open, already-migrated connection --
    simulating "the app was upgraded past what this file remembers"
    without needing a real schema change.
    """
    with Database(db_path, read_only=False, secret_store=mock_store) as db:
        db.execute(
            "UPDATE app.versions SET version = ? WHERE component = 'moneybin'",
            [_STALE_VERSION],
        )


def _stored_version(db_path: Path, mock_store: MagicMock) -> str | None:
    with Database(db_path, read_only=True, secret_store=mock_store) as db:
        row = db.execute(
            "SELECT version FROM app.versions WHERE component = 'moneybin'"
        ).fetchone()
    return row[0] if row else None


def test_read_only_open_with_no_pending_work_never_touches_the_write_lock(
    configured_db: tuple[Path, MagicMock, MagicMock],
) -> None:
    """The common case (ladder already current) stays lock-free.

    Regression guard: escalation must not fire, and therefore must not pay
    the write-lock round trip, when there is nothing to reconcile.
    """
    db_path, _mock_settings, _mock_store = configured_db
    lock_path = db_path.parent / (db_path.name + ".write.lock")
    assert not lock_path.exists()

    with get_database(read_only=True) as db:
        assert db.execute("SELECT 1").fetchone() == (1,)

    assert not lock_path.exists()


def test_read_only_open_applies_a_behind_ladder_and_reads_current_data(
    configured_db: tuple[Path, MagicMock, MagicMock],
) -> None:
    """A read-only open whose stamped version is behind escalates and heals.

    Proves the read that follows escalation sees current, correct state --
    not the stale value a plain ATTACH-and-return would have served.
    """
    db_path, _mock_settings, mock_store = configured_db
    _mark_ladder_stale(db_path, mock_store)
    assert _stored_version(db_path, mock_store) == _STALE_VERSION

    with get_database(read_only=True) as db:
        version = db.execute(
            "SELECT version FROM app.versions WHERE component = 'moneybin'"
        ).fetchone()

    assert version == (importlib.metadata.version("moneybin"),)


def test_read_only_open_refuses_when_no_auto_upgrade_is_set(
    configured_db: tuple[Path, MagicMock, MagicMock],
) -> None:
    """``no_auto_upgrade`` refuses the read rather than serving it stale."""
    db_path, mock_settings, mock_store = configured_db
    _mark_ladder_stale(db_path, mock_store)
    mock_settings.database.no_auto_upgrade = True

    with pytest.raises(DatabaseUpgradeRequiredError, match="no_auto_upgrade"):
        get_database(read_only=True)

    # Refused, not silently served: the stale stamp is untouched.
    assert _stored_version(db_path, mock_store) == _STALE_VERSION


def test_write_open_refuses_instead_of_silently_stranding_data_when_no_auto_upgrade(
    configured_db: tuple[Path, MagicMock, MagicMock],
) -> None:
    """A write open with ``no_auto_upgrade`` set also refuses when behind.

    The item-3 fix: ``init_schemas()`` runs unconditionally before the
    ``no_auto_upgrade`` check, so a write open used to proceed silently --
    which is exactly how a relocated table could be created empty while the
    real rows stayed stranded under the table's old name. A genuinely fresh
    install (never versioned) must still be allowed through untouched.
    """
    db_path, _mock_settings, mock_store = configured_db
    _mark_ladder_stale(db_path, mock_store)

    with pytest.raises(DatabaseUpgradeRequiredError, match="no_auto_upgrade"):
        Database(
            db_path, read_only=False, secret_store=mock_store, no_auto_upgrade=True
        )

    assert _stored_version(db_path, mock_store) == _STALE_VERSION


def test_fresh_install_with_no_auto_upgrade_is_not_refused(
    tmp_path: Path,
) -> None:
    """A never-before-versioned database is a fresh install, not a stale one.

    ``init_schemas()`` already built it at its current shape from scratch,
    so ``MigrationRunner.pending()`` reporting every migration unapplied
    must not trip the item-3 refusal.
    """
    mock_store = MagicMock()
    mock_store.get_key.return_value = "fresh-install-key"

    with Database(
        tmp_path / "fresh.duckdb",
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=True,
    ) as db:
        assert db.execute("SELECT 1").fetchone() == (1,)


def test_read_only_open_refuses_when_the_write_lock_cannot_be_created(
    configured_db: tuple[Path, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unwritable database file/filesystem refuses cleanly.

    The write lock's own ``os.open(..., O_CREAT)`` is what actually fails
    in that case; simulated deterministically here rather than via a real
    chmod, which is unreliable across sandboxes and CI users.
    """
    db_path, _mock_settings, mock_store = configured_db
    _mark_ladder_stale(db_path, mock_store)

    @contextmanager
    def boom_write_lock(*_args: object, **_kwargs: object) -> Generator[None]:
        raise PermissionError(
            "Operation not permitted (simulated read-only filesystem)"
        )
        yield  # pragma: no cover — unreachable; keeps this a generator function

    monkeypatch.setattr(db_lock_module, "write_lock", boom_write_lock)

    with pytest.raises(DatabaseUpgradeRequiredError, match="not writable"):
        get_database(read_only=True)


def test_read_only_open_refuses_when_the_write_lock_times_out(
    configured_db: tuple[Path, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A write lock that cannot be acquired in time refuses cleanly too."""
    db_path, _mock_settings, mock_store = configured_db
    _mark_ladder_stale(db_path, mock_store)

    @contextmanager
    def boom_write_lock(*_args: object, **_kwargs: object) -> Generator[None]:
        raise DatabaseLockError("simulated write-lock timeout")
        yield  # pragma: no cover — unreachable; keeps this a generator function

    monkeypatch.setattr(db_lock_module, "write_lock", boom_write_lock)

    with pytest.raises(DatabaseUpgradeRequiredError, match="write lock"):
        get_database(read_only=True)


@pytest.mark.integration
def test_concurrent_read_only_opens_serialize_the_upgrade(
    configured_db: tuple[Path, MagicMock, MagicMock],
) -> None:
    """Two racing read-only opens against a stale ladder both succeed.

    The first to observe the ladder as behind acquires the write lock and
    applies it; the second blocks on that same lock and, once it acquires
    its own escalation attempt, finds nothing pending (its write-mode
    ``Database.__init__`` no-ops) rather than double-applying or racing the
    schema.
    """
    db_path, _mock_settings, mock_store = configured_db
    _mark_ladder_stale(db_path, mock_store)

    outcomes: dict[str, str] = {}
    lock = threading.Lock()

    def read_once(name: str) -> None:
        try:
            with get_database(read_only=True) as db:
                version = db.execute(
                    "SELECT version FROM app.versions WHERE component = 'moneybin'"
                ).fetchone()
            with lock:
                outcomes[name] = version[0] if version else "none"
        except Exception as exc:  # surface unexpected errors for diagnosis
            with lock:
                outcomes[name] = f"error:{type(exc).__name__}"

    threads = [
        threading.Thread(target=read_once, args=(f"reader-{i}",)) for i in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)

    assert not any(t.is_alive() for t in threads), "a reader did not finish in time"
    current = importlib.metadata.version("moneybin")
    assert outcomes == {"reader-0": current, "reader-1": current}, outcomes
    assert db_path.with_name(db_path.name + ".write.lock").exists()
