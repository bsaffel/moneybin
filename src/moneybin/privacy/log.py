"""Profile-scoped privacy event log (JSONL with daily rotation).

Every classified MCP tool call and CLI ``--output json`` invocation
writes one JSON-per-line event here. Schema is deliberately fixed and
minimal — the audit trail must remain machine-grepable across years of
appends and the rotation cycle must not depend on a logging library
(stdlib only, profile-local, fail-soft).

Failure mode: a full disk or permission error during append is logged
at WARNING via the standard logger but never raised. Privacy
accounting is essential, but if the log can't be written, the tool
call must still succeed — refusing service over a full disk is worse
than skipping one audit entry.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_LOG_FILE = "privacy.log.jsonl"
_ROTATED_PREFIX = "privacy.log."
_ROTATED_SUFFIX = ".jsonl"
_LOCK = threading.Lock()


def _resolve_privacy_log_dir() -> Path:
    """Return the directory the privacy log lives in.

    Indirected via this helper so tests can monkey-patch the location
    without touching the real profile dir.
    """
    from moneybin.config import get_base_dir, get_current_profile

    base = get_base_dir()
    try:
        profile = get_current_profile()
    except RuntimeError:
        # No profile resolved — fall back to base dir. Single-user
        # bootstrap path before a profile is set.
        return base
    return base / "profiles" / profile


def _today_utc() -> str:
    """Return today's date in UTC as YYYY-MM-DD. Indirected for testing."""
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _file_date_utc(path: Path) -> str:
    """Return ``path``'s mtime as YYYY-MM-DD in UTC."""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).strftime("%Y-%m-%d")


def _rotate_if_new_day(path: Path) -> None:
    """Rotate yesterday's log to a dated file if ``path`` is from a prior day.

    No-op when the file is absent (a fresh current-day file is created on
    append) or already today's. The current-day file is created with
    restrictive perms by the caller's ``os.open``, so there's nothing for
    the caller to do with a return value.
    """
    if not path.exists():
        return
    file_day = _file_date_utc(path)
    if file_day == _today_utc():
        return
    rotated = path.parent / f"{_ROTATED_PREFIX}{file_day}{_ROTATED_SUFFIX}"
    path.rename(rotated)


def build_tool_call_event(
    *,
    actor: str,
    sensitivity: str,
    classes_returned: list[str],
    row_count: int,
) -> dict[str, Any]:
    """Construct the standard ``action="tool_call"`` event dict.

    Shared by the MCP decorator and CLI render path so the event schema
    is locked in one place — future additions (e.g. ``consent_mode``,
    ``profile``) propagate to both surfaces automatically.
    """
    return {
        "ts": datetime.now(UTC).isoformat(),
        "actor": actor,
        "action": "tool_call",
        "sensitivity": sensitivity,
        "classes_returned": classes_returned,
        "row_count": row_count,
    }


def write_privacy_event(event: dict[str, Any]) -> None:
    """Append a single event to ``privacy.log.jsonl`` (creating + rotating as needed).

    Fail-soft: file errors are logged at WARNING but never raised.
    """
    try:
        with _LOCK:
            log_dir = _resolve_privacy_log_dir()
            # The privacy log dir may be created here on the bootstrap /
            # no-profile path before any profile dir exists. mkdir(mode=0o700)
            # only applies the mode to the LEAF — intermediate parents created
            # by parents=True inherit the umask (typically 0o755, world-
            # readable). Don't toggle the process-wide umask to force 0o700:
            # umask is process-global, not thread-local, so a concurrent thread
            # creating files (e.g. MCP tool work via asyncio.to_thread) could
            # inherit it and get unintended permissions. Instead, snapshot which
            # ancestors are missing, create the tree, then chmod exactly the dirs
            # WE created to 0o700 — never touching pre-existing shared dirs.
            missing_dirs = [p for p in (log_dir, *log_dir.parents) if not p.exists()]
            log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            for created in missing_dirs:
                try:
                    created.chmod(0o700)
                except OSError:
                    # Best-effort: a TOCTOU race or platform quirk on one dir
                    # must not break the audit write (fail-soft contract).
                    pass
            log_path = log_dir / _LOG_FILE
            _rotate_if_new_day(log_path)
            line = json.dumps(event, sort_keys=True, separators=(",", ":"))
            # os.open with O_CREAT|O_APPEND|mode=0o600 sets restrictive perms
            # atomically at file creation. open("a") + chmod afterwards leaves
            # a sub-millisecond window where the file exists at the umask
            # default — narrow but real for a security-sensitive audit log.
            fd = os.open(
                log_path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o600,
            )
            # os.open applies `mode & ~umask`, so a umask that strips owner-write
            # (e.g. 0o200) would create the file read-only and the very next
            # append would raise PermissionError — silently killing the audit
            # log. fchmod the open fd to force 0o600 regardless of umask.
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except BaseException:
                # fdopen takes ownership of fd on success; if fchmod (before
                # fdopen) raised, close the raw fd ourselves to avoid a leak.
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise
    except Exception as exc:  # noqa: BLE001 — fail-soft: a missing audit row must
        # never break a tool call. json.dumps can raise TypeError/ValueError on a
        # non-serializable event; file I/O raises OSError/PermissionError; the
        # cross-process rotation race raises FileNotFoundError. All are logged
        # at WARNING and swallowed per the module's never-raise contract.
        logger.warning(f"privacy log write failed: {type(exc).__name__}: {exc}")


def read_privacy_events(
    filters: dict[str, Any],
    max_rows: int,
) -> list[dict[str, Any]]:
    """Return recent events (newest first), optionally filtered by ``filters``.

    Reads ``privacy.log.jsonl`` plus any rotated files in reverse date
    order. ``filters`` is a flat dict of ``field -> exact-match value``.
    Returns up to ``max_rows`` events.
    """
    # The per-row cap is checked AFTER append below, so max_rows=0 would
    # otherwise return the first matching row (1 >= 0). Short-circuit here.
    if max_rows <= 0:
        return []
    log_dir = _resolve_privacy_log_dir()
    if not log_dir.exists():
        return []
    files = sorted(
        (
            p
            for p in log_dir.iterdir()
            if p.name == _LOG_FILE
            or (p.name.startswith(_ROTATED_PREFIX) and p.name.endswith(_ROTATED_SUFFIX))
        ),
        key=lambda p: p.name,
        reverse=True,
    )
    # Ensure the current-day file is read first regardless of sort key. With
    # reverse=True it already sorts first ("privacy.log.jsonl" — 'j' > any date
    # digit), so this is defensive: it pins the invariant without relying on the
    # ASCII ordering of the rotated-file naming scheme.
    current = log_dir / _LOG_FILE
    if current in files:
        files = [current] + [f for f in files if f != current]

    out: list[dict[str, Any]] = []
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            # iterdir() ran outside _LOCK; a concurrent midnight rotation can
            # rename privacy.log.jsonl between listing and this read. Skip the
            # vanished file rather than propagate an unclassified error.
            continue
        for line in reversed(text.splitlines()):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if all(event.get(k) == v for k, v in filters.items()):
                out.append(event)
                if len(out) >= max_rows:
                    return out
    return out
