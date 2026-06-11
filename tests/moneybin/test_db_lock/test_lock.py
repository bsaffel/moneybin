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
    # added by classify_user_error at the CLI/MCP boundary, not here. The
    # message names operation_type and the elapsed wait duration (an actionable
    # benchmark, not a vague "after the deadline") but NOT the db path: it
    # surfaces to users via classify_user_error and a profile path embeds the OS
    # username (no-PII-in-output rule).
    import re

    msg = str(excinfo.value)
    assert "interactive" in msg
    assert str(db_path) not in msg  # no PII (path/username) in user-facing error
    assert re.search(r"after \d+s", msg), msg
    assert "deadline" not in msg


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


# ---------------------------------------------------------------------------
# Regression tests for code-review findings F1, F2, F7, F8, F13.
# ---------------------------------------------------------------------------


def test_f1_metadata_write_preserves_fd_inode(tmp_path: Path) -> None:
    """F1 regression: lock_path inode after metadata write equals held fd inode.

    The original `_write_holder_metadata` used `os.replace` on the lock
    file, which swapped the inode and left the held fcntl on an unlinked
    old inode — letting a second writer open the path on a fresh inode
    and acquire its own flock. The in-place writer preserves the
    fd-to-inode binding.
    """
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="interactive"):
        lock_path = _lock_path(db_path)
        path_ino = os.stat(lock_path).st_ino
        # Find the held fd by re-opening + stat — the held fd is private
        # to write_lock, but inode equality is observable via path stat
        # vs. a fresh open. If `os.replace` were still in use, the path
        # would resolve to a NEW inode unrelated to the held one.
        fresh_fd = os.open(lock_path, os.O_RDONLY)
        try:
            fresh_ino = os.fstat(fresh_fd).st_ino
        finally:
            os.close(fresh_fd)
        assert path_ino == fresh_ino


def test_f2_different_thread_contends_at_fcntl_not_reentrancy(tmp_path: Path) -> None:
    """F2 regression: second thread must NOT bypass via reentrancy key match.

    Original reentrancy keyed on pid alone — a second thread in the same
    process matched the holder and silently fast-pathed past fcntl,
    breaking serialization. Fix keys reentrancy on (pid, thread_id);
    a different thread now opens its own fd and contends at fcntl.

    Asserts: when thread A holds and thread B's deadline expires before
    A releases, B raises DatabaseLockError. Pre-fix B would have entered
    via reentrancy and succeeded immediately — no timeout.
    """
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    acquired_by_a = threading.Event()
    release_a = threading.Event()
    b_outcome: dict[str, BaseException | str] = {}

    def thread_a() -> None:
        deadline = time.monotonic() + 5.0
        with write_lock(db_path, deadline=deadline, operation_type="interactive"):
            acquired_by_a.set()
            release_a.wait(timeout=5.0)

    def thread_b() -> None:
        # Short deadline — fcntl contention must win before deadline expires.
        deadline = time.monotonic() + 0.3
        try:
            with write_lock(db_path, deadline=deadline, operation_type="migration"):
                b_outcome["result"] = "acquired"
        except BaseException as exc:  # noqa: BLE001  # capture any exit
            b_outcome["result"] = exc

    a = threading.Thread(target=thread_a)
    a.start()
    try:
        assert acquired_by_a.wait(timeout=2.0)
        b = threading.Thread(target=thread_b)
        b.start()
        b.join(timeout=5.0)
        assert not b.is_alive()
    finally:
        release_a.set()
        a.join(timeout=5.0)

    outcome = b_outcome["result"]
    assert isinstance(outcome, DatabaseLockError), (
        f"thread B must timeout via fcntl contention, got: {outcome!r}"
    )


def test_f7_reentrant_does_not_overwrite_outer_metadata(tmp_path: Path) -> None:
    """F7 regression: inner reentry preserves outer's holder metadata.

    Pre-fix, inner reentry called `_write_holder_metadata` with the inner's
    operation_type, clobbering the outer's payload. After the inner exits,
    the file still reads the inner's operation_type — a confusing diagnostic
    that misrepresents who's holding the lock. Fix: don't write metadata on
    reentrant entry; outer's payload stays put for the whole nested scope.
    """
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="interactive"):
        with write_lock(db_path, deadline=deadline, operation_type="migration"):
            # Inside inner reentry — metadata should still be outer's.
            metadata = json.loads(_lock_path(db_path).read_text())
            assert metadata["operation_type"] == "interactive"
        # Outside inner; outer still holds.
        metadata = json.loads(_lock_path(db_path).read_text())
        assert metadata["operation_type"] == "interactive"


def test_f8_reentrant_inner_raise_still_releases_outer(tmp_path: Path) -> None:
    """F8 regression: inner reentry that raises must restore outer's depth.

    Pre-fix, the reentrant branch bumped depth BEFORE calling
    `_write_holder_metadata` (a fallible op). If write_holder_metadata
    raised, depth stayed bumped and the outer's `finally` decremented
    back to 1 instead of 0 — fd never released, lock leaked. Fix: no
    fallible call between depth bump and the yield/finally. After
    inner raises and full unwind, a fresh acquire from a different
    thread must succeed.
    """
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0

    with pytest.raises(RuntimeError, match="inner explosion"):
        with write_lock(db_path, deadline=deadline, operation_type="interactive"):
            with write_lock(db_path, deadline=deadline, operation_type="migration"):
                raise RuntimeError("inner explosion")

    # If F8 regressed (lock leaked), a different thread cannot acquire
    # within 0.5s. The current-thread re-entry would succeed via the
    # reentrancy fast-path even with a leak, which is why this test
    # uses a different thread.
    acquired_by_other = threading.Event()

    def other_thread() -> None:
        d = time.monotonic() + 0.5
        with write_lock(db_path, deadline=d, operation_type="interactive"):
            acquired_by_other.set()

    t = threading.Thread(target=other_thread)
    t.start()
    t.join(timeout=2.0)
    assert acquired_by_other.is_set(), (
        "different thread could not acquire — outer's lock leaked"
    )


def test_f13_lock_file_mode_is_0o600(tmp_path: Path) -> None:
    """F13 regression: lock file mode stays 0o600 after metadata write.

    Pre-fix, `_write_holder_metadata` wrote a tmpfile via
    `Path.write_text` (which uses prevailing umask, typically yielding
    0o644) then `os.replace`d over the original 0o600 file — downgrading
    the mode. Process command-lines in the metadata payload were then
    readable by other local users. Fix: in-place write through the held
    fd; the file is created once at 0o600 and never replaced.
    """
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="interactive"):
        mode = os.stat(_lock_path(db_path)).st_mode & 0o777
        assert mode == 0o600, f"lock file mode is 0o{mode:o}, expected 0o600"


def test_process_command_runs_before_lock_acquired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ps subprocess runs BEFORE the exclusive flock, not under LOCK_EX.

    Resolving the holder's argv via _process_command shells out to ps (up to a
    3 s timeout). Running that while holding LOCK_EX would stall every competing
    writer, so the command is resolved outside the critical section.
    """
    import fcntl

    import moneybin.db_lock.lock as lock_module

    events: list[str] = []
    real_process_command = lock_module._process_command  # type: ignore[reportPrivateUsage]

    def spy_process_command(pid: int) -> str:
        events.append("process_command")
        return real_process_command(pid)

    real_flock = fcntl.flock

    def spy_flock(fd: int, op: int) -> None:
        if op & fcntl.LOCK_EX:
            events.append("flock_ex")
        real_flock(fd, op)

    monkeypatch.setattr(lock_module, "_process_command", spy_process_command)
    monkeypatch.setattr(fcntl, "flock", spy_flock)

    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="interactive"):
        pass

    assert events.index("process_command") < events.index("flock_ex"), events


def test_out_of_lifo_reentrant_close_releases_lock(tmp_path: Path) -> None:
    """Closing reentrant write handles out of LIFO order still releases the lock.

    Regression: the lock fd was released by whichever write_lock frame's
    closure owned it, which assumed LIFO close order. A caller holding two
    write handles on one thread and closing the outer (first-acquired) before
    the inner stranded the fd and the OS lock — every later writer then timed
    out until the process exited. Release is now keyed to depth reaching 0 via
    the holder's fd, independent of close order.
    """
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0

    # Drive two reentrant contexts manually and close out of LIFO order.
    outer = write_lock(db_path, deadline=deadline, operation_type="interactive")
    inner = write_lock(db_path, deadline=deadline, operation_type="migration")
    outer.__enter__()
    inner.__enter__()
    outer.__exit__(None, None, None)  # close the first-acquired handle first
    inner.__exit__(None, None, None)

    # A different thread must now acquire quickly — it would time out (and
    # leave acquired unset) if the fd / OS lock had leaked.
    acquired = threading.Event()

    def other() -> None:
        d = time.monotonic() + 1.0
        try:
            with write_lock(db_path, deadline=d, operation_type="interactive"):
                acquired.set()
        except BaseException:  # noqa: BLE001, S110 — a leak surfaces via the assert below
            pass

    t = threading.Thread(target=other)
    t.start()
    t.join(timeout=3.0)
    assert acquired.is_set(), "lock leaked after out-of-LIFO reentrant close"


def test_write_lock_sanitizes_command_stored_on_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The on-disk lock metadata stores a path-free friendly name, not raw argv.

    The .write.lock file persists at 0o600 next to the encrypted database, so a
    raw command line (local paths, statement filenames, usernames) must never
    land in it. write_lock runs the argv through describe_process before storing.
    """
    import moneybin.db_lock.lock as lock_module

    def fake_process_command(_pid: int) -> str:
        return "/Users/bob/.venv/bin/moneybin transform apply /Users/bob/db.duckdb"

    monkeypatch.setattr(lock_module, "_process_command", fake_process_command)
    db_path = tmp_path / "test.duckdb"
    db_path.touch()
    deadline = time.monotonic() + 1.0
    with write_lock(db_path, deadline=deadline, operation_type="transform_apply"):
        metadata = json.loads(_lock_path(db_path).read_text())
    assert metadata["command"] == "transform pipeline"
    assert "/Users/bob" not in metadata["command"]
