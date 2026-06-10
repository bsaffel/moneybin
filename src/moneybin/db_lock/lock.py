"""Process file lock around write-mode DuckDB opens and multi-step ops.

The lock is a per-profile fcntl(LOCK_EX) on ``<db_path>.write.lock``. It does
NOT replace DuckDB's own ATTACH-layer arbitration — it sits in front of
writer-vs-writer contention only. Read-mode opens never call this. See
``docs/specs/database-writer-coordination.md`` § "PR B hardening pass" and
the design doc at
``private/plans/2026-06-04-database-writer-coordination-pr-b-design.md``.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import subprocess  # noqa: S404  # ps invoked with static args only
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from moneybin.db_lock._types import OperationType
from moneybin.metrics.registry import DB_WRITE_LOCK_TIMEOUT_TOTAL

if TYPE_CHECKING:
    from moneybin.database import DatabaseLockError

logger = logging.getLogger(__name__)

_LOCK_SUFFIX = ".write.lock"

_BACKOFF_INITIAL_SECONDS = 0.05
_BACKOFF_MULTIPLIER = 1.5
_BACKOFF_CAP_SECONDS = 0.5


def lock_path_for(db_path: Path) -> Path:
    """Return the write-lock metadata path for ``db_path``.

    Resolves ``db_path`` first, mirroring ``write_lock``, so a symlinked or
    relative path maps to the same lock file the primitive keys on. This is
    the public contract for locating the lock file; the ``_LOCK_SUFFIX``
    naming detail stays private to this module.
    """
    resolved = db_path.resolve()
    return resolved.parent / (resolved.name + _LOCK_SUFFIX)


@dataclass
class _Holder:
    """Reentrancy tracking entry."""

    pid: int
    thread_id: int
    depth: int
    # The single lock fd, owned by the holder (not by any one write_lock
    # frame's closure). Whichever frame drives depth to 0 releases it via the
    # holder, so reentrant release is order-independent — a caller closing two
    # write handles out of LIFO order no longer strands the fd and OS lock.
    fd: int


# Guards _held_by mutation. Each Database file_path resolves to a single
# entry; the holder owns the lock fd for the lifetime of the outermost acquire
# so OS-level fcntl semantics serialize cross-process attempts. Reentrancy is
# keyed on (pid, thread_id) — different threads in the same process each open
# their own fd and contend at fcntl (POSIX flock contends per
# open-file-description on Linux and macOS).
_held_by: dict[Path, _Holder] = {}
_held_by_lock = threading.Lock()


def _release_one(key: Path, pid: int, thread_id: int) -> None:
    """Decrement the holder's depth; release the OS lock + fd when it hits 0.

    Called from BOTH the reentrant and non-reentrant exit paths so that
    whichever write_lock frame drives depth to 0 — regardless of the order the
    caller closes its handles — releases the single fd stored on the holder.
    This is what makes reentrant release order-independent.
    """
    with _held_by_lock:
        holder = _held_by.get(key)
        if holder is None or holder.pid != pid or holder.thread_id != thread_id:
            return
        holder.depth -= 1
        if holder.depth <= 0:
            _held_by.pop(key, None)
            try:
                fcntl.flock(holder.fd, fcntl.LOCK_UN)
            finally:
                os.close(holder.fd)


def _process_command(pid: int) -> str:
    """Return the full argv for ``pid`` or a fallback string."""
    try:
        result = subprocess.run(  # noqa: S603  # ps with static args only
            ["ps", "-p", str(pid), "-o", "args="],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return f"pid {pid}"
    except Exception:  # noqa: BLE001  # ps failures are non-fatal — fall back
        return f"pid {pid}"
    return result.stdout.strip() or f"pid {pid}"


def _write_holder_metadata(
    fd: int, operation_type: OperationType, command: str
) -> None:
    """Write the lock holder metadata in-place via the held fd.

    Writes through the SAME fd that holds the fcntl lock — never
    replaces the file. A tmpfile + os.replace would swap the inode and
    leave the fcntl on the unlinked old inode; a second writer opening
    lock_path would then bind to the new inode and acquire its own
    flock. Diagnostic readers (system_status) may observe a partial
    JSON write window; this is acceptable because the lock authority
    is the held fcntl, not the file contents.

    ``command`` is resolved by the caller BEFORE the lock is held (see
    write_lock) so the ``ps`` subprocess never runs under LOCK_EX. This
    function does only fast in-place I/O.
    """
    payload = {
        "pid": os.getpid(),
        "command": command,
        "started_at": datetime.now(UTC).isoformat(),
        "operation_type": operation_type,
    }
    encoded = json.dumps(payload).encode()
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, encoded)


def _build_timeout_error(
    db_path: Path, operation_type: OperationType, waited_seconds: float
) -> DatabaseLockError:
    """Construct a plain DatabaseLockError carrying the diagnostic message.

    The message names the elapsed wait (``waited_seconds``) so the user has a
    concrete benchmark, matching the ATTACH-retry timeout message in
    ``database.py`` ("after Ns") rather than a vague "after the deadline".

    Recovery actions are NOT attached here. ``classify_user_error`` in
    ``src/moneybin/errors.py`` injects the structured ``RecoveryAction`` when
    it wraps ``DatabaseLockError`` into ``UserError`` at the CLI/MCP boundary,
    matching the pattern used by every other error in that classifier.
    """
    # Local import to avoid the circular dep between database.py (which imports
    # from db_lock) and this module (which raises database.py's error type).
    from moneybin.database import DatabaseLockError

    message = (
        f"Could not acquire write lock for {db_path} after {waited_seconds:.0f}s "
        f"(operation_type={operation_type})."
    )
    return DatabaseLockError(message)


@contextmanager
def write_lock(
    db_path: Path,
    *,
    deadline: float,
    operation_type: OperationType,
) -> Generator[None, None, None]:
    """Acquire the per-profile write critical-section lock.

    Polls ``fcntl.flock(LOCK_EX | LOCK_NB)`` with exponential backoff
    (50 ms → cap 500 ms) until ``deadline`` (a ``time.monotonic()`` value).
    Writes holder metadata to ``<db_path>.write.lock`` on acquire. Reentrant
    within the same process. Released on crash via ``fcntl`` OS semantics.

    Args:
        db_path: Path to the DuckDB file. Lock lives at
            ``<db_path>.write.lock`` in the same directory.
        deadline: Absolute ``time.monotonic()`` value past which the
            acquire times out. Shared with the caller's ATTACH-retry loop
            so the end-to-end writer wait stays under the policy ceiling.
        operation_type: Closed-set label classifying the write. Pyright
            checks the value against the ``OperationType`` Literal.

    Raises:
        DatabaseLockError: When ``deadline`` is reached with the lock
            still held by another process. The CLI/MCP boundary's
            ``classify_user_error`` enriches it with a structured
            ``system_status`` recovery action.
    """
    key = db_path.resolve()
    lock_path = key.parent / (key.name + _LOCK_SUFFIX)
    pid = os.getpid()
    thread_id = threading.get_ident()

    # Reentrancy: only same-pid AND same-thread bumps depth. A different
    # thread in this process opens its own fd and contends at fcntl —
    # POSIX flock contends per open-file-description on Linux and macOS,
    # so cross-thread serialization is correct via the backoff loop below.
    with _held_by_lock:
        existing = _held_by.get(key)
        if (
            existing is not None
            and existing.pid == pid
            and existing.thread_id == thread_id
        ):
            existing.depth += 1
            reentered = True
        else:
            reentered = False

    if reentered:
        # Outer holder owns the metadata payload. Inner reentry leaves it
        # untouched — "process P is in operation X" stays true for the
        # whole nested scope. No fallible operation between the depth
        # bump (above) and yield, so finally always runs the release. If this
        # frame happens to drive depth to 0 (the caller closed the outer
        # handle first), _release_one releases the holder's fd here.
        try:
            yield
        finally:
            _release_one(key, pid, thread_id)
        return

    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    registered = False
    delay = _BACKOFF_INITIAL_SECONDS
    # Resolve this process's own argv BEFORE acquiring the lock. _process_command
    # shells out to `ps` (up to a 3 s timeout); running it under LOCK_EX would
    # stall every competing writer for that duration. The command is fixed for
    # this process, so compute it outside the critical section and write only
    # the fast in-place metadata under the lock.
    command = _process_command(pid)
    wait_start = time.monotonic()
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    DB_WRITE_LOCK_TIMEOUT_TOTAL.labels(
                        operation_type=operation_type
                    ).inc()
                    logger.warning(
                        f"write_lock timeout: db_path={db_path} "
                        f"operation_type={operation_type}"
                    )
                    raise _build_timeout_error(
                        db_path, operation_type, time.monotonic() - wait_start
                    ) from None
                time.sleep(delay)
                delay = min(delay * _BACKOFF_MULTIPLIER, _BACKOFF_CAP_SECONDS)
        _write_holder_metadata(fd, operation_type, command)
        with _held_by_lock:
            _held_by[key] = _Holder(pid=pid, thread_id=thread_id, depth=1, fd=fd)
            registered = True
        try:
            yield
        finally:
            # Release via the holder (not the closure fd) so the frame that
            # drives depth to 0 owns the release regardless of close order.
            _release_one(key, pid, thread_id)
    except BaseException:
        # Error path before the outer holder was registered: release the OS
        # lock and close fd. If we already registered, the finally block
        # above owns release — we leave the fd to it.
        if not registered:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)
        raise
