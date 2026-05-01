"""Filesystem-state-as-API import inbox.

Wraps ImportService.import_file() with a watched-folder UX:
files dropped in <inbox_root>/<profile>/inbox/ are drained on demand,
moved to processed/YYYY-MM/ on success or failed/YYYY-MM/ + .error.yml
sidecar on failure. See docs/specs/smart-import-inbox.md.
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
import time
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import yaml

from moneybin.config import MoneyBinSettings
from moneybin.database import Database
from moneybin.metrics.registry import (
    INBOX_SYNC_DURATION_SECONDS,
    INBOX_SYNC_TOTAL,
)
from moneybin.services.import_service import ImportService

logger = logging.getLogger(__name__)

_DIR_MODE = 0o700

# Substring → (error_code, stage) for ValueError messages from ImportService.
_VALUE_ERROR_PATTERNS: tuple[tuple[str, str, str], ...] = (
    (
        "Single-account files require",
        "needs_account_name",
        "resolve_account",
    ),
    (
        "Could not reliably detect column mapping",
        "low_confidence_mapping",
        "map_columns",
    ),
    ("Unsupported file type", "unsupported_file_type", "detect_file_type"),
    ("No data rows found", "empty_file", "read_file"),
    ("Transform failed", "transform_error", "transform"),
)


class InboxBusyError(Exception):
    """Another sync is in progress for this profile."""


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

    @property
    def lock_path(self) -> Path:
        """Path to the per-profile lockfile."""
        return self.root / ".inbox.lock"

    @contextlib.contextmanager
    def acquire_lock(self) -> Generator[None, None, None]:
        """Hold an exclusive flock on .inbox.lock for the duration of the block."""
        self.ensure_layout()
        fh = open(self.lock_path, "a")  # noqa: SIM115  # contextmanager handles close
        try:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as e:
                fh.close()
                raise InboxBusyError(
                    "Another sync is in progress for this profile."
                ) from e
            try:
                yield
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()

    def ensure_layout(self) -> None:
        """Create <root>/{inbox,processed,failed}/ with 0700 perms (idempotent)."""
        for d in (self.root, self.inbox_dir, self.processed_dir, self.failed_dir):
            d.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
            # mkdir's mode is masked by umask on creation; chmod fixes existing dirs too.
            d.chmod(_DIR_MODE)

    def enumerate(self) -> InboxListResult:
        """Walk the inbox one level deep and classify each entry.

        Sync only acts on regular files in inbox/ root or in inbox/<one-subfolder>/.
        Hidden files, symlinks, and entries deeper than one level are ignored
        (returned with a reason) so the caller can show the user what was skipped.
        """
        self.ensure_layout()
        result = InboxListResult()
        for entry in sorted(self.inbox_dir.iterdir()):
            self._classify(entry, account_hint=None, result=result)
        return result

    def _classify(
        self,
        entry: Path,
        *,
        account_hint: str | None,
        result: InboxListResult,
    ) -> None:
        """Bucket a single entry into would_process or ignored based on type/depth."""
        rel = entry.relative_to(self.inbox_dir).as_posix()
        if entry.name.startswith("."):
            result.ignored.append({"path": rel, "reason": "hidden_file"})
            return
        if entry.is_symlink():
            result.ignored.append({"path": rel, "reason": "symlink"})
            return
        if entry.is_file():
            result.would_process.append({"filename": rel, "account_hint": account_hint})
            return
        if entry.is_dir():
            if account_hint is not None:
                # Already inside one subfolder; deeper levels are ignored.
                result.ignored.append({"path": rel, "reason": "nested_subfolder"})
                return
            for child in sorted(entry.iterdir()):
                self._classify(child, account_hint=entry.name, result=result)
            return
        result.ignored.append({"path": rel, "reason": "not_regular_file"})

    _OUTCOME_DIRS = ("processed", "failed")
    _STAGING_PREFIX = "staging-"

    def move_to_outcome(
        self,
        src: Path,
        *,
        outcome: str,
        year_month: str,
    ) -> Path:
        """Atomic two-step move: src → outcome/staging-name → outcome/YYYY-MM/name."""
        if outcome not in self._OUTCOME_DIRS:
            raise ValueError(f"Unknown outcome: {outcome}")
        outcome_root = self.root / outcome
        outcome_root.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
        outcome_root.chmod(_DIR_MODE)

        staging = outcome_root / f"{self._STAGING_PREFIX}{src.name}"
        staging = self._next_available_path(staging)
        src.rename(staging)

        dest_dir = outcome_root / year_month
        dest_dir.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
        dest_dir.chmod(_DIR_MODE)
        final = self._next_available_path(dest_dir / src.name)
        staging.rename(final)
        return final

    def recover_staging(self) -> list[Path]:
        """Move leftover staging-* files in outcome roots back to inbox/."""
        self.ensure_layout()
        recovered: list[Path] = []
        for outcome in self._OUTCOME_DIRS:
            outcome_root = self.root / outcome
            if not outcome_root.exists():
                continue
            for entry in outcome_root.iterdir():
                if not entry.is_file():
                    continue
                if not entry.name.startswith(self._STAGING_PREFIX):
                    continue
                original_name = entry.name[len(self._STAGING_PREFIX) :]
                dest = self._next_available_path(self.inbox_dir / original_name)
                entry.rename(dest)
                recovered.append(dest)
                logger.info(f"Recovered staging file → {dest.name}")
        return recovered

    @staticmethod
    def _next_available_path(candidate: Path) -> Path:
        """Append -1, -2, ... before the suffix until we find a free name."""
        if not candidate.exists():
            return candidate
        stem = candidate.stem
        suffix = candidate.suffix
        i = 1
        while True:
            attempt = candidate.with_name(f"{stem}-{i}{suffix}")
            if not attempt.exists():
                return attempt
            i += 1

    def sync(self, year_month: str | None = None) -> InboxSyncResult:
        """Drain the inbox: import each eligible file and move it."""
        ym = year_month or datetime.now(UTC).strftime("%Y-%m")
        try:
            with self.acquire_lock():
                self.recover_staging()
                listing = self.enumerate()
                result = InboxSyncResult(ignored=list(listing.ignored))
                t0 = time.monotonic()
                for item in listing.would_process:
                    self._sync_one(item, year_month=ym, result=result)
                INBOX_SYNC_DURATION_SECONDS.observe(time.monotonic() - t0)
                return result
        except InboxBusyError:
            INBOX_SYNC_TOTAL.labels(outcome="skipped").inc()
            return InboxSyncResult(skipped=[{"reason": "inbox_busy"}])

    def _sync_one(
        self,
        item: dict[str, object],
        *,
        year_month: str,
        result: InboxSyncResult,
    ) -> None:
        """Import one inbox item and move it to the processed/ bucket."""
        rel_filename = str(item["filename"])
        account_hint = item["account_hint"]
        src = self.inbox_dir / rel_filename
        importer = ImportService(self._db)
        try:
            import_result = importer.import_file(
                str(src),
                account_name=account_hint if isinstance(account_hint, str) else None,
            )
        except Exception as e:  # noqa: BLE001 — surfaced as structured failure entry
            self._handle_failure(src, rel_filename, e, year_month, result)
            return
        final = self.move_to_outcome(src, outcome="processed", year_month=year_month)
        result.processed.append({
            "filename": rel_filename,
            "moved_to": str(final.relative_to(self.root)),
            "transactions": import_result.transactions,
            "file_type": import_result.file_type,
        })
        INBOX_SYNC_TOTAL.labels(outcome="processed").inc()

    def _handle_failure(
        self,
        src: Path,
        rel_filename: str,
        error: Exception,
        year_month: str,
        result: InboxSyncResult,
    ) -> None:
        """Move failed file to failed/ and write YAML sidecar."""
        error_code, stage = self._classify_error(error)
        moved = self.move_to_outcome(src, outcome="failed", year_month=year_month)
        sidecar = self.write_error_sidecar(
            moved,
            error_code=error_code,
            stage=stage,
            message=str(error),
            suggestion=self._suggestion_for(error_code),
        )
        result.failed.append({
            "filename": rel_filename,
            "error_code": error_code,
            "stage": stage,
            "moved_to": str(moved.relative_to(self.root)),
            "sidecar": str(sidecar.relative_to(self.root)),
        })
        INBOX_SYNC_TOTAL.labels(outcome="failed").inc()
        logger.warning(f"Inbox import failed: {rel_filename} → {error_code}")

    @staticmethod
    def _classify_error(error: Exception) -> tuple[str, str]:
        """Map an exception to (error_code, stage)."""
        if isinstance(error, FileNotFoundError):
            return ("file_not_found", "open_file")
        if isinstance(error, ValueError):
            msg = str(error)
            for needle, code, stage in _VALUE_ERROR_PATTERNS:
                if needle in msg:
                    return (code, stage)
            return ("value_error", "import")
        return ("import_error", "import")

    @staticmethod
    def _suggestion_for(error_code: str) -> str | None:
        """User-facing hint for known error codes."""
        return {
            "needs_account_name": (
                "Move the file into inbox/<account-slug>/ "
                "(e.g., inbox/chase-checking/) and re-run sync."
            ),
            "low_confidence_mapping": (
                "Use 'moneybin import file <path> --override field=column' "
                "to map columns explicitly, then re-drop in inbox/."
            ),
            "unsupported_file_type": (
                "Convert to OFX/QFX, CSV, TSV, XLSX, Parquet, or PDF."
            ),
            "empty_file": "File contained no data rows; remove or replace.",
        }.get(error_code)

    @staticmethod
    def write_error_sidecar(
        moved_path: Path,
        *,
        error_code: str,
        stage: str,
        message: str,
        suggestion: str | None = None,
        extra: dict[str, object] | None = None,
    ) -> Path:
        """Write a <filename>.error.yml sidecar next to a failed file."""
        sidecar = moved_path.with_name(moved_path.name + ".error.yml")
        payload: dict[str, object] = {
            "error_code": error_code,
            "stage": stage,
            "message": message,
        }
        if suggestion is not None:
            payload["suggestion"] = suggestion
        if extra:
            payload.update(extra)
        sidecar.write_text(yaml.safe_dump(payload, sort_keys=False))
        return sidecar
