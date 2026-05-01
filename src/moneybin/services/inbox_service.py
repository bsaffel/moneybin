"""Filesystem-state-as-API import inbox.

Wraps ImportService.import_file() with a watched-folder UX:
files dropped in <inbox_root>/<profile>/inbox/ are drained on demand,
moved to processed/YYYY-MM/ on success or failed/YYYY-MM/ + .error.yml
sidecar on failure. See docs/specs/smart-import-inbox.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from moneybin.config import MoneyBinSettings
from moneybin.database import Database

logger = logging.getLogger(__name__)

_DIR_MODE = 0o700


@dataclass
class InboxSyncResult:
    """Outcome of an inbox sync run, bucketed by per-file disposition."""

    processed: list[dict[str, object]] = field(default_factory=list)
    failed: list[dict[str, object]] = field(default_factory=list)
    skipped: list[dict[str, object]] = field(default_factory=list)
    ignored: list[dict[str, object]] = field(default_factory=list)


@dataclass
class InboxListResult:
    """Dry-run preview of what an inbox sync would touch."""

    would_process: list[dict[str, object]] = field(default_factory=list)
    ignored: list[dict[str, object]] = field(default_factory=list)


class InboxService:
    """Filesystem-state-as-API import inbox; see module docstring."""

    def __init__(self, db: Database, settings: MoneyBinSettings) -> None:
        """Bind the inbox to a database and settings (paths derive from profile)."""
        self._db = db
        self._settings = settings

    @property
    def root(self) -> Path:
        """Per-profile inbox root: <inbox_root>/<profile>/."""
        return self._settings.profile_inbox_dir

    @property
    def inbox_dir(self) -> Path:
        """Drop-zone where users place files awaiting import."""
        return self.root / "inbox"

    @property
    def processed_dir(self) -> Path:
        """Archive root for successfully imported files (organized by YYYY-MM)."""
        return self.root / "processed"

    @property
    def failed_dir(self) -> Path:
        """Quarantine root for files whose import raised (organized by YYYY-MM)."""
        return self.root / "failed"

    def ensure_layout(self) -> None:
        """Create <root>/{inbox,processed,failed}/ with 0700 perms (idempotent)."""
        for d in (self.root, self.inbox_dir, self.processed_dir, self.failed_dir):
            d.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
            # mkdir's mode is masked by umask on creation; chmod fixes existing dirs too.
            d.chmod(_DIR_MODE)
