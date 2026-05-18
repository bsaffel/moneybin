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
    """Rename ``path`` to ``privacy.log.<YYYY-MM-DD>.jsonl`` if its mtime is yesterday."""
    if not path.exists():
        return
    file_day = _file_date_utc(path)
    today = _today_utc()
    if file_day == today:
        return
    rotated = path.parent / f"{_ROTATED_PREFIX}{file_day}{_ROTATED_SUFFIX}"
    path.rename(rotated)


def write_privacy_event(event: dict[str, Any]) -> None:
    """Append a single event to ``privacy.log.jsonl`` (creating + rotating as needed).

    Fail-soft: file errors are logged at WARNING but never raised.
    """
    try:
        with _LOCK:
            log_dir = _resolve_privacy_log_dir()
            log_path = log_dir / _LOG_FILE
            _rotate_if_new_day(log_path)
            existed = log_path.exists()
            line = json.dumps(event, sort_keys=True, separators=(",", ":"))
            with log_path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
            if not existed:
                # First write — restrict to 0o600. Don't chmod on every
                # append (cheap but pointless; also some filesystems
                # don't support repeated chmods cleanly).
                log_path.chmod(0o600)
    except (OSError, PermissionError) as exc:
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
    # Move the current-day file to the front explicitly — its name sorts
    # AFTER "privacy.log.YYYY-MM-DD.jsonl" lexically.
    current = log_dir / _LOG_FILE
    if current in files:
        files = [current] + [f for f in files if f != current]

    out: list[dict[str, Any]] = []
    for path in files:
        for line in reversed(path.read_text(encoding="utf-8").splitlines()):
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
