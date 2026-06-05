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

from moneybin.db_lock._types import OperationType
from moneybin.metrics.registry import DB_WRITE_LOCK_TIMEOUT_TOTAL

logger = logging.getLogger(__name__)

_LOCK_SUFFIX = ".write.lock"

_BACKOFF_INITIAL_SECONDS = 0.05
_BACKOFF_MULTIPLIER = 1.5
_BACKOFF_CAP_SECONDS = 0.5


@dataclass
class _Holder:
    """Reentrancy tracking entry."""

    pid: int
    depth: int
    fd: int


# Guards _held_by mutation. Each Database file_path resolves to a single
# entry; the fd is held open for the duration of the outermost acquire so
# OS-level fcntl semantics serialize cross-process attempts.
_held_by: dict[Path, _Holder] = {}
_held_by_lock = threading.Lock()


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


def _write_holder_metadata(lock_path: Path, operation_type: OperationType) -> None:
    """Atomically write the lock-file metadata payload."""
    pid = os.getpid()
    payload = {
        "pid": pid,
        "command": _process_command(pid),
        "started_at": datetime.now(UTC).isoformat(),
        "operation_type": operation_type,
    }
    tmp = lock_path.with_suffix(lock_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, lock_path)


def _build_timeout_error(db_path: Path, operation_type: OperationType) -> Exception:
    """Construct a plain DatabaseLockError carrying the diagnostic message.

    Recovery actions are NOT attached here. ``classify_user_error`` in
    ``src/moneybin/errors.py`` injects the structured ``RecoveryAction`` when
    it wraps ``DatabaseLockError`` into ``UserError`` at the CLI/MCP boundary,
    matching the pattern used by every other error in that classifier.
    """
    # Local import to avoid the circular dep between database.py (which will
    # import from db_lock once get_database integrates the primitive in Task 4)
    # and this module (which raises database.py's error type).
    from moneybin.database import DatabaseLockError

    message = (
        f"Could not acquire write lock for {db_path} after the deadline "
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

    # Reentrancy path: same-pid holder bumps depth and exits without touching
    # fcntl, then decrements on unwind. No fd is opened here, so the outer
    # BaseException handler below is unreachable from this branch.
    with _held_by_lock:
        existing = _held_by.get(key)
        if existing is not None and existing.pid == pid:
            existing.depth += 1
            reentered = True
        else:
            reentered = False

    if reentered:
        _write_holder_metadata(lock_path, operation_type)
        try:
            yield
        finally:
            with _held_by_lock:
                holder = _held_by.get(key)
                if holder is not None and holder.pid == pid:
                    holder.depth -= 1
        return

    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    registered = False
    delay = _BACKOFF_INITIAL_SECONDS
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
                    raise _build_timeout_error(db_path, operation_type) from None
                time.sleep(delay)
                delay = min(delay * _BACKOFF_MULTIPLIER, _BACKOFF_CAP_SECONDS)
        _write_holder_metadata(lock_path, operation_type)
        with _held_by_lock:
            _held_by[key] = _Holder(pid=pid, depth=1, fd=fd)
            registered = True
        try:
            yield
        finally:
            with _held_by_lock:
                holder = _held_by.get(key)
                if holder is not None and holder.pid == pid:
                    holder.depth -= 1
                    if holder.depth <= 0:
                        _held_by.pop(key, None)
                        try:
                            fcntl.flock(fd, fcntl.LOCK_UN)
                        finally:
                            os.close(fd)
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
