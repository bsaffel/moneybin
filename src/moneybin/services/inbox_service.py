"""Filesystem-state-as-API import inbox.

Wraps ImportService.import_file() with a watched-folder UX:
files dropped in <inbox_root>/<profile>/inbox/ are drained on demand,
moved to processed/YYYY-MM/ on success or failed/YYYY-MM/ + .error.yml
sidecar on failure. See docs/specs/smart-import-inbox.md.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import time
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import quote, unquote

import duckdb
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
_FILE_MODE = 0o600
_OUTCOME_DIRS: tuple[Literal["processed", "failed"], ...] = ("processed", "failed")
_STAGING_PREFIX = "staging-"
_ACCOUNT_SLUG_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
# Cap exception message length in sidecars so unfiltered exception text can't
# leak large amounts of PII (paths, parsed rows) past the log sanitizer.
_SIDECAR_MESSAGE_MAX = 200
# Defensive cap on filename collision suffixes (-1, -2, ...). Practical
# imports never reach this; the bound just makes the loop terminate.
_MAX_FILENAME_COLLISIONS = 9999

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
    """Outcome of an inbox sync run, bucketed by per-file disposition.

    Transform fields surface end-of-batch hook state so callers can render
    "rebuilt core tables" / "transform failed" in the same response that
    enumerates the per-file moves.
    """

    processed: list[dict[str, object]] = field(default_factory=list)
    failed: list[dict[str, object]] = field(default_factory=list)
    skipped: list[dict[str, object]] = field(default_factory=list)
    ignored: list[dict[str, object]] = field(default_factory=list)
    transforms_applied: bool = False
    transforms_duration_seconds: float | None = None
    transforms_error: str | None = None


@dataclass
class InboxListResult:
    """Dry-run preview of what an inbox sync would touch."""

    would_process: list[dict[str, object]] = field(default_factory=list)
    ignored: list[dict[str, object]] = field(default_factory=list)


class InboxService:
    """Filesystem-state-as-API import inbox; see module docstring."""

    def __init__(self, db: Database | None, settings: MoneyBinSettings) -> None:
        """Bind the inbox service to settings; db is required only for sync()."""
        self._db = db
        self._settings = settings

    @classmethod
    def for_active_profile(cls) -> InboxService:
        """Construct an InboxService against the active profile with a live DB."""
        from moneybin.config import get_settings
        from moneybin.database import get_database

        return cls(db=get_database(), settings=get_settings())

    @classmethod
    def for_active_profile_no_db(cls) -> InboxService:
        """Construct an InboxService for read-only filesystem operations.

        Skips opening the encrypted database so `enumerate()` and `path` work
        during onboarding/recovery flows when the DB is locked or its key is
        unavailable. Calling sync() on this instance raises.
        """
        from moneybin.config import get_settings

        return cls(db=None, settings=get_settings())

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
        """Archive root for successfully imported files."""
        return self.root / "processed"

    @property
    def failed_dir(self) -> Path:
        """Quarantine root for files whose import raised."""
        return self.root / "failed"

    @property
    def lock_path(self) -> Path:
        """Path to the per-profile lockfile."""
        return self.root / ".inbox.lock"

    @contextlib.contextmanager
    def acquire_lock(self) -> Generator[None, None, None]:
        """Hold an exclusive flock on .inbox.lock for the duration of the block."""
        # Lazy import: fcntl is Unix-only. Importing at module load would make
        # the entire CLI fail to start on Windows, since import_inbox is imported
        # during command dispatch. Deferring keeps non-inbox commands portable;
        # `inbox sync` itself remains Unix-only (see spec).
        import fcntl

        self.ensure_layout()
        # Open with explicit 0o600 so the lockfile is owner-only even if umask
        # would have produced 0644. Parent dir is 0700 already; this is
        # defense-in-depth per privacy-data-protection.md.
        fd = os.open(
            str(self.lock_path),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            _FILE_MODE,
        )
        fh = os.fdopen(fd, "a")  # noqa: SIM115  # contextmanager handles close
        try:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as e:
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
                result.ignored.append({"path": rel, "reason": "nested_subfolder"})
                return
            if not _ACCOUNT_SLUG_RE.match(entry.name):
                # Refuse to forward arbitrary folder names downstream as account hints.
                for child in sorted(entry.iterdir()):
                    child_rel = child.relative_to(self.inbox_dir).as_posix()
                    result.ignored.append({
                        "path": child_rel,
                        "reason": "invalid_account_slug",
                    })
                return
            for child in sorted(entry.iterdir()):
                self._classify(child, account_hint=entry.name, result=result)
            return
        result.ignored.append({"path": rel, "reason": "not_regular_file"})

    def move_to_outcome(
        self,
        src: Path,
        *,
        outcome: Literal["processed", "failed"],
        year_month: str,
    ) -> Path:
        """Atomic two-step move: src → outcome/staging-name → outcome/YYYY-MM/name."""
        if not _YEAR_MONTH_RE.fullmatch(year_month):
            raise ValueError(f"year_month must be YYYY-MM, got {year_month!r}")
        outcome_root = self.root / outcome
        # Encode subfolder context so crash recovery restores files under their
        # original <account-slug>/ subfolder instead of dropping to inbox root.
        encoded_name = self._encode_staging_name(src)
        staging = self._next_available_path(
            outcome_root / f"{_STAGING_PREFIX}{encoded_name}"
        )
        src.rename(staging)

        dest_dir = outcome_root / year_month
        dest_dir.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
        dest_dir.chmod(_DIR_MODE)  # mkdir's mode is masked by umask
        final = self._next_available_path(dest_dir / src.name)
        staging.rename(final)
        return final

    def _encode_staging_name(self, src: Path) -> str:
        """Reversibly encode src's inbox-relative path into a flat filename.

        Uses URL-encoding so any byte (including '/') round-trips exactly.
        Matters for filenames containing the encoded separator natively
        (e.g. `bank__may.csv`), which a non-bijective scheme would mangle
        on crash recovery.
        """
        try:
            rel = src.relative_to(self.inbox_dir).as_posix()
        except ValueError:
            rel = src.name
        return quote(rel, safe="")

    def recover_staging(self) -> list[Path]:
        """Move leftover staging-* files in outcome roots back to inbox/."""
        self.ensure_layout()
        recovered: list[Path] = []
        for outcome in _OUTCOME_DIRS:
            outcome_root = self.root / outcome
            if not outcome_root.exists():
                continue
            for entry in outcome_root.iterdir():
                if not entry.is_file():
                    continue
                if not entry.name.startswith(_STAGING_PREFIX):
                    continue
                encoded = entry.name[len(_STAGING_PREFIX) :]
                rel_path = unquote(encoded)
                dest_candidate = self.inbox_dir / rel_path
                # Defense-in-depth: even with reversible encoding, refuse any
                # decoded path that escapes inbox_dir (e.g., crafted "../../").
                inbox_root = self.inbox_dir.resolve()
                if not dest_candidate.resolve().is_relative_to(inbox_root):
                    logger.warning(
                        f"Skipping staging file with unsafe decoded path: "
                        f"{entry.name!r}"
                    )
                    continue
                dest_candidate.parent.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
                dest_candidate.parent.chmod(_DIR_MODE)  # mkdir's mode masked by umask
                dest = self._next_available_path(dest_candidate)
                entry.rename(dest)
                recovered.append(dest)
                logger.info(f"Recovered staging file → {dest.relative_to(self.root)}")
        return recovered

    @staticmethod
    def _next_available_path(candidate: Path) -> Path:
        """Append -1, -2, ... before the suffix until we find a free name."""
        if not candidate.exists():
            return candidate
        stem = candidate.stem
        suffix = candidate.suffix
        for i in range(1, _MAX_FILENAME_COLLISIONS + 1):
            attempt = candidate.with_name(f"{stem}-{i}{suffix}")
            if not attempt.exists():
                return attempt
        raise RuntimeError(
            f"Too many filename collisions for {candidate.name!r} "
            f"(>{_MAX_FILENAME_COLLISIONS})"
        )

    def sync(
        self,
        year_month: str | None = None,
        *,
        apply_transforms: bool = True,
    ) -> InboxSyncResult:
        """Drain the inbox: import each eligible file and move it.

        Per-file imports run with ``apply_transforms=False`` so SQLMesh runs
        once at end-of-batch instead of N times. When at least one file
        imported successfully, ``ImportService.apply_post_import_hooks()`` is
        invoked once and the timing/error fields land in the result.
        """
        if self._db is None:
            raise RuntimeError("InboxService.sync() requires a database connection")
        ym = year_month or datetime.now(UTC).strftime("%Y-%m")
        try:
            with self.acquire_lock():
                self.recover_staging()
                listing = self.enumerate()
                result = InboxSyncResult(ignored=list(listing.ignored))
                if listing.ignored:
                    INBOX_SYNC_TOTAL.labels(outcome="ignored").inc(len(listing.ignored))
                importer = ImportService(self._db)
                t0 = time.monotonic()
                for item in listing.would_process:
                    self._sync_one(
                        item, importer=importer, year_month=ym, result=result
                    )
                # Mirror ImportService.import_files: skip the SQLMesh apply
                # when nothing transformable landed. W-2 PDFs never populate
                # core.fct_transactions, so a pure-W-2 drain has nothing for
                # transforms to rebuild.
                any_transformable = any(
                    entry.get("file_type") in ("ofx", "tabular")
                    for entry in result.processed
                )
                if apply_transforms and any_transformable:
                    hook_result = importer.apply_post_import_hooks()
                    result.transforms_applied = hook_result.applied
                    result.transforms_duration_seconds = hook_result.duration_seconds
                    result.transforms_error = hook_result.error
                INBOX_SYNC_DURATION_SECONDS.observe(time.monotonic() - t0)
                return result
        except InboxBusyError:
            INBOX_SYNC_TOTAL.labels(outcome="skipped").inc()
            return InboxSyncResult(skipped=[{"reason": "inbox_busy"}])

    def _sync_one(
        self,
        item: dict[str, object],
        *,
        importer: ImportService,
        year_month: str,
        result: InboxSyncResult,
    ) -> None:
        """Import one inbox item and move it to the processed/ bucket.

        Transforms are deferred: passed ``apply_transforms=False`` here so the
        whole batch runs SQLMesh once at the end of :meth:`sync` instead of
        per-file.
        """
        rel_filename = str(item["filename"])
        account_hint = item["account_hint"]
        src = self.inbox_dir / rel_filename
        try:
            import_result = importer.import_file(
                str(src),
                apply_transforms=False,
                account_name=account_hint if isinstance(account_hint, str) else None,
            )
        except Exception as e:  # noqa: BLE001 — surfaced as structured failure entry
            self._handle_failure(src, rel_filename, e, year_month, result)
            return
        try:
            final = self.move_to_outcome(
                src, outcome="processed", year_month=year_month
            )
        except OSError as e:
            # File raced out from under us between import and move (external
            # mv, race with another tool). Record as failure so the rest of
            # the batch continues to drain.
            self._handle_failure(src, rel_filename, e, year_month, result)
            return
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
        error_class = type(error).__name__
        message = str(error)[:_SIDECAR_MESSAGE_MAX]
        suggestion = self._suggestion_for(error_code)
        log_line = f"Inbox import failed: {rel_filename} → {error_code} ({error_class})"

        def _base_entry() -> dict[str, object]:
            entry: dict[str, object] = {
                "filename": rel_filename,
                "error_code": error_code,
                "stage": stage,
                "error_class": error_class,
                "message": message,
            }
            if suggestion is not None:
                entry["suggestion"] = suggestion
            return entry

        if not src.exists():
            # File vanished between enumerate() and import — nothing to move.
            # Record the failure but continue draining remaining files.
            result.failed.append(_base_entry())
            INBOX_SYNC_TOTAL.labels(outcome="failed").inc()
            logger.warning(log_line)
            return
        try:
            moved = self.move_to_outcome(src, outcome="failed", year_month=year_month)
        except OSError as move_err:
            # Disk full, cross-device, permission, etc. Don't let a failed
            # move-to-failed/ abort the whole drain — record what we know and
            # continue to the next file.
            result.failed.append(_base_entry())
            INBOX_SYNC_TOTAL.labels(outcome="failed").inc()
            logger.warning(
                f"{log_line} (could not move to failed/: {move_err.__class__.__name__})"
            )
            return
        sidecar = self.write_error_sidecar(
            moved,
            error_code=error_code,
            stage=stage,
            message=message,
            suggestion=suggestion,
        )
        entry = _base_entry()
        entry["moved_to"] = str(moved.relative_to(self.root))
        entry["sidecar"] = str(sidecar.relative_to(self.root))
        result.failed.append(entry)
        INBOX_SYNC_TOTAL.labels(outcome="failed").inc()
        logger.warning(log_line)

    @staticmethod
    def _classify_error(error: Exception) -> tuple[str, str]:
        """Map an exception to (error_code, stage)."""
        if isinstance(error, FileNotFoundError):
            return ("file_not_found", "open_file")
        if isinstance(error, PermissionError):
            return ("permission_error", "open_file")
        if isinstance(error, ValueError):
            msg = str(error)
            for needle, code, stage in _VALUE_ERROR_PATTERNS:
                if needle in msg:
                    return (code, stage)
            return ("value_error", "import")
        # DuckDB binder/catalog errors signal a stale schema (e.g. table
        # missing a column the loader expects). Almost always means a
        # migration hasn't been applied to this DB; the suggestion below
        # tells the operator how to fix it.
        if isinstance(error, duckdb.BinderException | duckdb.CatalogException):
            return ("schema_mismatch", "load")
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
                "Use 'moneybin import files <path> --override field=column' "
                "to map columns explicitly, then re-drop in inbox/."
            ),
            "unsupported_file_type": (
                "Convert to OFX/QFX, CSV, TSV, XLSX, Parquet, or PDF."
            ),
            "empty_file": "File contained no data rows; remove or replace.",
            "permission_error": (
                "Check file ownership and permissions "
                "(e.g., chmod 644 or chown to your user)."
            ),
            "schema_mismatch": (
                "Database schema is out of date. Run "
                "'moneybin db migrate' to apply pending migrations, then "
                "re-run sync."
            ),
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
        sidecar.chmod(_FILE_MODE)
        return sidecar
