"""Per-workbook lease for exclusive inbound or output role decisions."""

from __future__ import annotations

import fcntl
import hashlib
import os
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from moneybin.services.request_lifetime import RequestLifetime

_POLL_SECONDS = 0.05


@dataclass
class _Holder:
    pid: int
    thread_id: int
    depth: int
    fd: int


@dataclass
class _PermitState:
    active: bool = True


@dataclass(frozen=True, slots=True)
class WorkbookRolePermit:
    """Proof that one caller currently owns a workbook's role decision."""

    _workbook_digest: str
    _state: _PermitState

    def assert_for(self, spreadsheet_id: str) -> None:
        """Reject expired permits and permits for a different workbook."""
        if not self._state.active:
            raise RuntimeError("Workbook role permit is no longer active")
        if self._workbook_digest != _workbook_digest(spreadsheet_id):
            raise ValueError("Workbook role permit does not match destination")


_holders: dict[Path, _Holder] = {}
_holders_lock = threading.Lock()


def _workbook_digest(spreadsheet_id: str) -> str:
    return hashlib.sha256(spreadsheet_id.encode()).hexdigest()


def _lock_path(database_path: Path, spreadsheet_id: str) -> Path:
    digest = _workbook_digest(spreadsheet_id)
    return database_path.resolve().parent / f".workbook-role-{digest[:32]}.lock"


def _release(key: Path, pid: int, thread_id: int) -> None:
    with _holders_lock:
        holder = _holders.get(key)
        if holder is None or (holder.pid, holder.thread_id) != (pid, thread_id):
            return
        holder.depth -= 1
        if holder.depth:
            return
        _holders.pop(key)
        try:
            fcntl.flock(holder.fd, fcntl.LOCK_UN)
        finally:
            os.close(holder.fd)


@contextmanager
def workbook_role_lease(
    database_path: Path,
    spreadsheet_id: str,
    *,
    lifetime: RequestLifetime | None = None,
) -> Generator[WorkbookRolePermit]:
    """Serialize role rechecks and mutations for one private workbook ID."""
    if not spreadsheet_id:
        raise ValueError("spreadsheet_id is required for a workbook role lease")
    key = _lock_path(database_path, spreadsheet_id)
    pid = os.getpid()
    thread_id = threading.get_ident()
    state = _PermitState()

    with _holders_lock:
        holder = _holders.get(key)
        if holder is not None and (holder.pid, holder.thread_id) == (pid, thread_id):
            holder.depth += 1
            reentrant = True
        else:
            reentrant = False

    if reentrant:
        try:
            yield WorkbookRolePermit(_workbook_digest(spreadsheet_id), state)
        finally:
            state.active = False
            _release(key, pid, thread_id)
        return

    fd = os.open(key, os.O_CREAT | os.O_RDWR, 0o600)
    os.fchmod(fd, 0o600)
    try:
        while True:
            if lifetime is not None:
                lifetime.raise_if_cancelled()
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(_POLL_SECONDS)
        with _holders_lock:
            _holders[key] = _Holder(pid=pid, thread_id=thread_id, depth=1, fd=fd)
        fd = -1
        try:
            yield WorkbookRolePermit(_workbook_digest(spreadsheet_id), state)
        finally:
            state.active = False
            _release(key, pid, thread_id)
    finally:
        if fd >= 0:
            os.close(fd)
