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

import fcntl  # POSIX-only: project targets macOS/Linux
import hashlib
import json
import logging
import os
import re
import secrets
import threading
import time
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

logger = logging.getLogger(__name__)

_LOG_FILE = "privacy.log.jsonl"
_LOCK_FILE = ".privacy.log.lock"
_ROTATED_PREFIX = "privacy.log."
_ROTATED_SUFFIX = ".jsonl"
_LOCK = threading.Lock()
_EVENT_ID_RE = re.compile(r"^[0-9a-f]{32}$")

# Upper bound on a single read_privacy_events call. A read scans JSONL files
# line-by-line into memory; cap here (one enforcement point for both the CLI
# --last flag and the MCP last param) so an unbounded request can't pull every
# rotated log into a Python list.
MAX_LOG_ROWS = 1000


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
    try:
        path.rename(rotated)
    except FileNotFoundError:
        # Another process (e.g. a concurrent MCP server + CLI run) rotated the
        # same file first. Each holds only its own in-process _LOCK, so the
        # rename can race. The current-day file is already gone; the caller's
        # os.open recreates it. Swallow rather than let the outer handler drop
        # this process's event as a generic failure.
        pass


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


def build_consent_event(
    *,
    actor: str,
    action: str,
    feature_category: str,
    backend: str,
    consent_mode: str | None = None,
) -> dict[str, Any]:
    """Construct a consent grant/revoke event for the privacy log.

    ``action`` is ``"consent.grant"`` or ``"consent.revoke"``. Carries
    only metadata (category, backend, mode) — never the grant prompt text
    or any financial data. ``consent_mode`` is None for revoke events (the
    mode belonged to the grant being removed); it serializes to JSON null.
    """
    return {
        "ts": datetime.now(UTC).isoformat(),
        "actor": actor,
        "action": action,
        "feature_category": feature_category,
        "backend": backend,
        "consent_mode": consent_mode,
    }


def write_privacy_event(event: dict[str, Any]) -> None:
    """Append a single event to ``privacy.log.jsonl`` (creating + rotating as needed).

    Fail-soft: file errors are logged at WARNING but never raised.
    """
    try:
        with _LOCK:
            log_dir = _resolve_privacy_log_dir()
            with _privacy_log_lock(log_dir) as lock_fd:
                log_path = log_dir / _LOG_FILE
                _rotate_if_new_day(log_path)
                persisted = dict(event)
                persisted["event_id"] = _new_event_id(lock_fd, log_dir)
                line = json.dumps(persisted, sort_keys=True, separators=(",", ":"))
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
                    # Two raise paths reach here: (a) fchmod raised before fdopen —
                    # fd is still open and ownership never transferred, so we must
                    # close it; (b) the write raised inside the `with`, whose context
                    # manager already closed fd — our close then no-ops (EBADF →
                    # OSError, caught). Either way fd ends up closed exactly once
                    # logically, with no leak.
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
    max_rows = min(max_rows, MAX_LOG_ROWS)
    out: list[dict[str, Any]] = []
    for event in _iter_privacy_events(filters):
        out.append(event)
        if len(out) >= max_rows:
            break
    return out


@dataclass(frozen=True)
class PrivacyEventPage:
    """One exact-total privacy-log keyset page."""

    events: list[dict[str, Any]]
    total_count: int
    snapshot_event_id: str | None
    legacy_digest: str
    has_more: bool


def read_privacy_events_page(
    filters: dict[str, Any],
    *,
    limit: int,
    snapshot_event_id: str | None = None,
    after_event_id: str | None = None,
    snapshot_total: int | None = None,
    legacy_digest: str | None = None,
) -> PrivacyEventPage:
    """Return one removal-safe page ordered by persisted append identity.

    Supported writers persist monotonic identities. Legacy rows receive stable
    derived identities at read time, so pagination never rewrites an audit log
    that an older process could still be appending. Any legacy-row change
    invalidates the cursor instead of silently renumbering duplicate rows.
    """
    if limit < 0 or (snapshot_total is not None and snapshot_total < 0):
        raise ValueError("limit and snapshot_total must be non-negative")
    if (snapshot_event_id is None) != (after_event_id is None):
        raise ValueError("snapshot and after event IDs must be supplied together")
    if (snapshot_event_id is None) != (legacy_digest is None):
        raise ValueError("legacy digest must accompany continuation IDs")
    if snapshot_event_id is not None and (
        not _valid_event_id(snapshot_event_id)
        or not _valid_event_id(cast(str, after_event_id))
        or cast(str, after_event_id) > snapshot_event_id
        or not isinstance(legacy_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", legacy_digest) is None
    ):
        raise ValueError("invalid privacy keyset")

    with _LOCK:
        log_dir = _resolve_privacy_log_dir()
        with _privacy_log_lock(log_dir):
            events, resolved_legacy_digest = _privacy_events_with_ids(filters)
            if legacy_digest is not None and legacy_digest != resolved_legacy_digest:
                raise ValueError("legacy privacy log changed during pagination")
            matching = sorted(
                events,
                key=lambda event: cast(str, event["event_id"]),
                reverse=True,
            )
            if snapshot_event_id is None:
                resolved_snapshot = (
                    cast(str, matching[0]["event_id"]) if matching else None
                )
                total = len(matching)
            else:
                resolved_snapshot = snapshot_event_id
                total = cast(int, snapshot_total)
            eligible = [
                event
                for event in matching
                if (
                    resolved_snapshot is not None
                    and cast(str, event["event_id"]) <= resolved_snapshot
                    and (
                        after_event_id is None
                        or cast(str, event["event_id"]) < after_event_id
                    )
                )
            ]
            return PrivacyEventPage(
                events=eligible[:limit],
                total_count=total,
                snapshot_event_id=resolved_snapshot,
                legacy_digest=resolved_legacy_digest,
                has_more=len(eligible) > limit,
            )


def _valid_event_id(value: object) -> bool:
    """Return whether a value is one canonical sortable event identity."""
    return isinstance(value, str) and _EVENT_ID_RE.fullmatch(value) is not None


def _ensure_log_dir(log_dir: Path) -> None:
    """Create missing log ancestors securely without changing existing owners."""
    missing_dirs = [path for path in (log_dir, *log_dir.parents) if not path.exists()]
    for created in reversed(missing_dirs):
        try:
            created.mkdir(mode=0o700)
            created.chmod(0o700)
        except FileExistsError:
            continue
        except OSError:
            continue


@contextmanager
def _privacy_log_lock(log_dir: Path) -> Generator[int]:
    """Serialize append, migration, and sequence state across processes."""
    _ensure_log_dir(log_dir)
    fd = os.open(log_dir / _LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield fd
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _new_event_id(lock_fd: int, log_dir: Path) -> str:
    """Return a persisted cross-process-monotonic opaque identity."""
    os.lseek(lock_fd, 0, os.SEEK_SET)
    raw = os.read(lock_fd, 32).decode(errors="ignore").strip()
    try:
        prior = int(raw, 16)
    except ValueError:
        prior = _max_event_id_prefix(log_dir)
    prefix = max(time.time_ns(), prior + 1)
    if prefix >= 1 << 64:
        raise OverflowError("privacy event identity sequence exhausted")
    encoded = f"{prefix:016x}".encode()
    os.lseek(lock_fd, 0, os.SEEK_SET)
    os.ftruncate(lock_fd, 0)
    os.write(lock_fd, encoded)
    os.fsync(lock_fd)
    return f"{prefix:016x}{secrets.token_hex(8)}"


def _max_event_id_prefix(log_dir: Path) -> int:
    """Return the greatest persisted identity prefix across existing logs."""
    maximum = 0
    for path in _privacy_log_files(newest_first=False, log_dir=log_dir):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            continue
        for line in lines:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = cast(dict[str, Any], value) if isinstance(value, dict) else None
            if event is None:
                continue
            event_id = event.get("event_id")
            prefix = (
                int(cast(str, event_id)[:16], 16)
                if _valid_event_id(event_id)
                else _legacy_event_prefix(event)
            )
            maximum = max(maximum, prefix)
    return maximum


def _privacy_events_with_ids(
    filters: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    """Return events with persisted or deterministic legacy identities."""
    oldest_first = list(reversed(list(_iter_privacy_events({}))))
    assigned: set[str] = set()
    occurrences: dict[str, int] = {}
    legacy_material: list[str] = []
    normalized: list[dict[str, Any]] = []
    for event in oldest_first:
        selected = dict(event)
        event_id = selected.get("event_id")
        if not _valid_event_id(event_id) or event_id in assigned:
            selected.pop("event_id", None)
            canonical = json.dumps(selected, sort_keys=True, separators=(",", ":"))
            occurrence = occurrences.get(canonical, 0)
            occurrences[canonical] = occurrence + 1
            legacy_material.append(f"{canonical}\0{occurrence}")
            salt = 0
            event_id = _legacy_event_id(selected, canonical, occurrence, salt)
            while event_id in assigned:
                salt += 1
                event_id = _legacy_event_id(selected, canonical, occurrence, salt)
            selected["event_id"] = event_id
        assigned.add(cast(str, event_id))
        if all(selected.get(key) == value for key, value in filters.items()):
            normalized.append(selected)
    digest = hashlib.sha256("\n".join(legacy_material).encode()).hexdigest()
    return normalized, digest


def _legacy_event_id(
    event: dict[str, Any],
    canonical: str,
    occurrence: int,
    salt: int,
) -> str:
    """Derive one stable sortable identity without mutating the legacy log."""
    digest = hashlib.blake2b(
        f"{canonical}\0{occurrence}\0{salt}".encode(),
        digest_size=8,
    ).hexdigest()
    return f"{_legacy_event_prefix(event):016x}{digest}"


def _legacy_event_prefix(event: dict[str, Any]) -> int:
    """Derive the 64-bit time prefix used by persisted event identities."""
    value = event.get("ts")
    if not isinstance(value, str):
        return 0
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        prefix = int(parsed.timestamp() * 1_000_000_000)
    except (OverflowError, ValueError):
        return 0
    return min(max(prefix, 0), (1 << 64) - 1)


def _privacy_log_files(
    *,
    newest_first: bool,
    log_dir: Path | None = None,
) -> list[Path]:
    """Return current and rotated logs in physical event-order groups."""
    log_dir = log_dir or _resolve_privacy_log_dir()
    if not log_dir.exists():
        return []
    current = log_dir / _LOG_FILE
    rotated = sorted(
        (
            path
            for path in log_dir.iterdir()
            if path != current
            and path.name.startswith(_ROTATED_PREFIX)
            and path.name.endswith(_ROTATED_SUFFIX)
        ),
        key=lambda path: path.name,
    )
    files = rotated + ([current] if current.exists() else [])
    return list(reversed(files)) if newest_first else files


def _iter_privacy_events(filters: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield matching privacy events newest-first across current and rotated logs."""
    for path in _privacy_log_files(newest_first=True):
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
            if not isinstance(event, dict):
                continue
            event_dict = cast(dict[str, Any], event)
            if all(event_dict.get(k) == v for k, v in filters.items()):
                yield event_dict
