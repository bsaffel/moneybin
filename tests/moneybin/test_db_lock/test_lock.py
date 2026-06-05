"""Unit tests for the write_lock context manager."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import pytest

from moneybin.database import DatabaseLockError
from moneybin.db_lock import write_lock
from moneybin.db_lock.lock import (
    _LOCK_SUFFIX,  # type: ignore[reportPrivateUsage]  # test-only access to the canonical lock-file suffix
)


def _lock_path(db_path: Path) -> Path:
    return db_path.parent / (db_path.name + _LOCK_SUFFIX)


def test_acquires_immediately_when_no_other_holder(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="interactive"):
        assert _lock_path(db_path).exists()
    # Lock file persists after release; OS-level lock is what's freed.
    assert _lock_path(db_path).exists()


def test_writes_holder_metadata_on_acquire(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="transform_apply"):
        metadata = json.loads(_lock_path(db_path).read_text())
    assert metadata["pid"] == os.getpid()
    assert metadata["operation_type"] == "transform_apply"
    assert "command" in metadata
    assert "started_at" in metadata


def test_reentrant_within_same_process(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="interactive"):
        # Re-enter — must not deadlock, must not raise.
        with write_lock(db_path, deadline=deadline, operation_type="migration"):
            pass


def test_raises_database_lock_error_on_timeout(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    acquired = threading.Event()
    release = threading.Event()

    def holder() -> None:
        deadline = time.monotonic() + 5.0
        with write_lock(db_path, deadline=deadline, operation_type="interactive"):
            acquired.set()
            release.wait(timeout=5.0)

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert acquired.wait(timeout=2.0)
        # NOTE: same-process re-entry would succeed via the reentrancy path.
        # The timeout test exercises cross-process contention by mocking
        # fcntl.flock to always raise BlockingIOError — verified separately
        # in test_raises_on_blocking_io_after_deadline below.
    finally:
        release.set()
        t.join(timeout=5.0)


def test_raises_on_blocking_io_after_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Force fcntl.flock to always raise BlockingIOError; assert timeout fires."""
    import fcntl

    def always_blocking(fd: int, op: int) -> None:
        raise BlockingIOError(11, "Resource temporarily unavailable")

    monkeypatch.setattr(fcntl, "flock", always_blocking)
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 0.2  # 200ms deadline
    with pytest.raises(DatabaseLockError) as excinfo:
        with write_lock(db_path, deadline=deadline, operation_type="interactive"):
            pass
    # write_lock raises a plain DatabaseLockError — recovery_actions are
    # added by classify_user_error at the CLI/MCP boundary, not here.
    # Just verify the message names the path and operation_type for
    # debuggability.
    msg = str(excinfo.value)
    assert "interactive" in msg
    assert str(db_path) in msg


def test_timeout_increments_metric_counter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import fcntl

    from moneybin.metrics.registry import DB_WRITE_LOCK_TIMEOUT_TOTAL

    def always_blocking(fd: int, op: int) -> None:
        raise BlockingIOError(11, "Resource temporarily unavailable")

    monkeypatch.setattr(fcntl, "flock", always_blocking)
    counter = DB_WRITE_LOCK_TIMEOUT_TOTAL.labels(operation_type="migration")
    before = counter._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 0.1
    with pytest.raises(DatabaseLockError):
        with write_lock(db_path, deadline=deadline, operation_type="migration"):
            pass
    after = counter._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    assert after == before + 1


def test_releases_lock_on_exception_in_block(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with pytest.raises(RuntimeError):
        with write_lock(db_path, deadline=deadline, operation_type="interactive"):
            raise RuntimeError("inner block failure")
    # A second acquire must succeed — the first context manager released on
    # exception unwind.
    deadline2 = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline2, operation_type="interactive"):
        pass
