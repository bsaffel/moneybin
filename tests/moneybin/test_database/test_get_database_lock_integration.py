"""Integration tests for ``write_lock`` + ``get_database`` composition.

Asserts the lifetime contract added in PR B: the per-profile file lock is
held for the entire lifetime of a write-mode ``Database`` (not merely
during ATTACH), so a second writer cannot slip in between
``get_database()`` returning and ``Database.close()`` running and produce
a raw ``duckdb.IOException`` at the ATTACH layer.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database, get_database
from moneybin.db_lock.lock import (
    _LOCK_SUFFIX,  # type: ignore[reportPrivateUsage]  # test-only access to the canonical lock-file suffix
)


@pytest.fixture
def configured_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Path, None, None]:
    """Bootstrap an encrypted DB and configure ``get_database()`` to find it.

    Mirrors the ``mock_secret_store`` + settings-injection pattern used by
    other ``test_database/`` modules: a real Database is created once with
    ``no_auto_upgrade=True`` so that subsequent ``get_database()`` calls
    open against an initialized file without paying the migration cost.
    """
    db_path = tmp_path / "integration.duckdb"

    mock_store = MagicMock()
    mock_store.get_key.return_value = "integration-test-key-for-lock-integration"

    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.no_auto_upgrade = True

    monkeypatch.setattr(db_module, "get_settings", lambda: mock_settings)
    monkeypatch.setattr(db_module, "SecretStore", lambda: mock_store)
    # The autouse clean_profile_state fixture resets these between tests, but
    # set them explicitly so this fixture is self-contained.
    monkeypatch.setattr(db_module, "_cached_encryption_key", None)
    monkeypatch.setattr(db_module, "_migration_check_done", set[Path]())
    monkeypatch.setattr(db_module, "_active_write_conn", None)

    with Database(
        db_path,
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=True,
    ) as db:
        db.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")

    yield db_path


def test_write_open_acquires_file_lock_then_releases_on_close(
    configured_db: Path,
) -> None:
    """Verify the file lock is held for the Database's lifetime, not just ATTACH.

    Inspects ``_held_by`` between ``get_database()`` and the context exit to
    catch regressions where the lock would release after ATTACH instead of
    after ``Database.close()``.
    """
    import moneybin.db_lock.lock as lock_module

    lock_path = configured_db.parent / (configured_db.name + _LOCK_SUFFIX)
    resolved_path = configured_db.resolve()

    with get_database(read_only=False) as db:
        # Lock still held — metadata present, holder registered.
        metadata = json.loads(lock_path.read_text())
        assert metadata["pid"] == os.getpid()
        assert metadata["operation_type"] == "interactive"
        assert resolved_path in lock_module._held_by  # pyright: ignore[reportPrivateUsage]
        db.execute("INSERT INTO t VALUES (1)")

    # After close, holder is cleared.
    assert resolved_path not in lock_module._held_by  # pyright: ignore[reportPrivateUsage]

    # A second write open must succeed immediately — the first release
    # actually freed the lock.
    with get_database(read_only=False) as db:
        db.execute("INSERT INTO t VALUES (2)")


def test_read_open_does_not_touch_file_lock(configured_db: Path) -> None:
    """Read-mode opens bypass ``write_lock`` entirely.

    No lock file is created and no holder is registered for read-only opens.
    """
    import moneybin.db_lock.lock as lock_module

    lock_path = configured_db.parent / (configured_db.name + _LOCK_SUFFIX)
    resolved_path = configured_db.resolve()
    assert not lock_path.exists()  # baseline

    with get_database(read_only=True) as db:
        assert not lock_path.exists()  # still untouched
        assert resolved_path not in lock_module._held_by  # pyright: ignore[reportPrivateUsage]
        db.execute("SELECT COUNT(*) FROM t").fetchone()

    assert not lock_path.exists()


def test_operation_type_kwarg_recorded_in_lock_metadata(
    configured_db: Path,
) -> None:
    """The ``operation_type`` kwarg is recorded in the lock-file metadata.

    Verifies the value flows from ``get_database()`` through ``write_lock``
    into the holder JSON payload.
    """
    lock_path = configured_db.parent / (configured_db.name + _LOCK_SUFFIX)
    with get_database(read_only=False, operation_type="migration") as db:
        metadata = json.loads(lock_path.read_text())
        assert metadata["operation_type"] == "migration"
        db.execute("INSERT INTO t VALUES (3)")


def test_shared_deadline_caps_total_writer_wait(configured_db: Path) -> None:
    """A single shared deadline drives both file-lock acquire and ATTACH retry.

    Happy-path open finishes well under ``max_wait``. Cross-process contention
    is exercised in Task 11's scenario coverage; this case guards against a
    regression that pushed the wait past the policy ceiling on the
    uncontested path.
    """
    start = time.monotonic()
    with get_database(read_only=False, max_wait=10.0) as db:
        elapsed = time.monotonic() - start
        assert elapsed < 10.0
        db.execute("INSERT INTO t VALUES (4)")
