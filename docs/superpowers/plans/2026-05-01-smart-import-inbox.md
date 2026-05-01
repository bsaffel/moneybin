# Smart Import Inbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a watched-inbox UX (`~/Documents/MoneyBin/<profile>/{inbox,processed,failed}/`) so users can drop files in Finder and run `moneybin import inbox` (or call `import.inbox_sync` from MCP) to drain them — without exposing file contents to the LLM via chat-attachment uploads.

**Architecture:** A new `InboxService` orchestrates per-profile directory layout, per-profile lockfile, recovery of crashed staging files, and atomic moves between `inbox/` → `processed/YYYY-MM/` (or `failed/YYYY-MM/` with a YAML `.error.yml` sidecar). The service composes on top of the existing `ImportService.import_file()` — no new extractors, loaders, or DB tables. Two thin wrappers (a Typer sub-group and two FastMCP tools) expose the service over CLI and MCP.

**Tech Stack:** Python 3.12 stdlib only (`pathlib`, `os.rename`, `fcntl.flock`), `pyyaml` (already a dependency via SQLMesh), Typer, FastMCP, pytest.

**Spec:** [`docs/specs/smart-import-inbox.md`](../../specs/smart-import-inbox.md)

---

## File Structure

### Files to create

- `src/moneybin/services/inbox_service.py` — `InboxService` class. All inbox logic: directory resolution, locking, recovery pass, file movement, sync/list operations. Returns `InboxSyncResult` and `InboxListResult` dataclasses.
- `src/moneybin/cli/commands/import_inbox.py` — Typer sub-group with `inbox`, `inbox list`, `inbox path` commands, registered onto `import_cmd.app`.
- `src/moneybin/mcp/tools/import_inbox.py` — `import.inbox_sync` and `import.inbox_list` FastMCP tools.
- `tests/moneybin/test_services/test_inbox_service.py` — primary unit + integration coverage.
- `tests/moneybin/test_cli/test_cli_import_inbox.py` — CLI argument parsing and exit-code tests with the service mocked.
- `tests/moneybin/test_mcp/test_import_inbox_tools.py` — envelope shape, sensitivity tier, actions hints.
- `tests/e2e/test_e2e_inbox.py` — subprocess-style E2E: drop fixture file, run `moneybin import inbox`, assert it lands in `processed/`.

### Files to modify

- `src/moneybin/config.py` — add `ImportSettings` submodel with `inbox_root: Path`; add `import_: ImportSettings` field to `MoneyBinSettings` (the trailing underscore avoids shadowing the `import` keyword in attribute access). Add a `MoneyBinSettings.profile_inbox_dir` property that returns `inbox_root / profile` so a profile switch picks up the new path without a restart.
- `src/moneybin/cli/commands/import_cmd.py` — register the new sub-typer (`app.add_typer(inbox_app, name="inbox")`).
- `src/moneybin/mcp/server.py` — call `register_inbox_tools(mcp)` from `register_core_tools()`.
- `src/moneybin/metrics/registry.py` — add `INBOX_SYNC_TOTAL` Counter + `INBOX_SYNC_DURATION_SECONDS` Histogram.
- `tests/e2e/test_e2e_help.py` — add `import inbox`, `import inbox list`, `import inbox path` to `_HELP_COMMANDS`.
- `docs/specs/smart-import-inbox.md` — flip status `draft` → `in-progress` at start, `implemented` at end.
- `docs/specs/INDEX.md` — add an entry under Smart Import.
- `README.md` — add 📐 → ✅ in the roadmap and a paragraph under the import section.

---

## Task 1: Add `ImportSettings` and `profile_inbox_dir` to config

**Files:**
- Modify: `src/moneybin/config.py`
- Test: `tests/moneybin/test_config_inbox.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_config_inbox.py`:

```python
"""Tests for ImportSettings and the profile_inbox_dir derived property."""

from pathlib import Path

from moneybin.config import ImportSettings, MoneyBinSettings


class TestImportSettings:
    def test_default_inbox_root_is_documents_moneybin(self) -> None:
        settings = ImportSettings()
        assert settings.inbox_root == Path.home() / "Documents" / "MoneyBin"

    def test_inbox_root_overridable_via_init(self, tmp_path: Path) -> None:
        settings = ImportSettings(inbox_root=tmp_path / "custom")
        assert settings.inbox_root == tmp_path / "custom"


class TestProfileInboxDir:
    def test_derived_from_active_profile(self, tmp_path: Path) -> None:
        s = MoneyBinSettings(
            profile="alice",
            import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
        )
        assert s.profile_inbox_dir == tmp_path / "MoneyBin" / "alice"

    def test_switches_when_profile_changes(self, tmp_path: Path) -> None:
        # New settings instance for the other profile picks up the new path
        # without any restart of the inbox service.
        a = MoneyBinSettings(
            profile="alice",
            import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
        )
        b = MoneyBinSettings(
            profile="bob",
            import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
        )
        assert a.profile_inbox_dir != b.profile_inbox_dir
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_config_inbox.py -v`
Expected: FAIL — `ImportError: cannot import name 'ImportSettings' from 'moneybin.config'`.

- [ ] **Step 3: Implement `ImportSettings` and the derived property**

In `src/moneybin/config.py`, add the model near the other config submodels (e.g. just after `SyncConfig`):

```python
class ImportSettings(BaseModel):
    """File-import related settings (inbox layout)."""

    model_config = ConfigDict(frozen=True)

    inbox_root: Path = Field(
        default_factory=lambda: Path.home() / "Documents" / "MoneyBin",
        description=(
            "Parent directory for the user-facing import workspace. "
            "Per-profile subdirs (<inbox_root>/<profile>/{inbox,processed,failed}/) "
            "are created on first use. Defaults to ~/Documents/MoneyBin."
        ),
    )
```

In `MoneyBinSettings`, add the field and the derived property. The trailing underscore on `import_` avoids shadowing the keyword:

```python
import_: ImportSettings = Field(
    default_factory=ImportSettings,
    alias="import",
)


@property
def profile_inbox_dir(self) -> Path:
    """Active profile's inbox parent: <inbox_root>/<profile>/."""
    return self.import_.inbox_root / self.profile
```

`alias="import"` lets env vars use `MONEYBIN_IMPORT__INBOX_ROOT` without the trailing underscore. Add `populate_by_name=True` to the `model_config` if it isn't there already (BaseSettings has it on by default for aliased fields, but verify when running tests).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_config_inbox.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Run pyright on config.py**

Run: `uv run pyright src/moneybin/config.py tests/moneybin/test_config_inbox.py`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_config_inbox.py
git commit -m "Add ImportSettings with inbox_root and profile_inbox_dir derivation"
```

---

## Task 2: `InboxService` skeleton + directory bootstrap

**Files:**
- Create: `src/moneybin/services/inbox_service.py`
- Create: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_services/test_inbox_service.py`:

```python
"""Tests for InboxService."""

from __future__ import annotations

import stat
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import ImportSettings, MoneyBinSettings
from moneybin.database import Database
from moneybin.services.inbox_service import InboxService


def _make_settings(tmp_path: Path, profile: str = "test") -> MoneyBinSettings:
    return MoneyBinSettings(
        profile=profile,
        import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
    )


@pytest.fixture
def inbox_service(tmp_path: Path) -> InboxService:
    db = MagicMock(spec=Database)
    return InboxService(db=db, settings=_make_settings(tmp_path))


class TestDirectoryBootstrap:
    def test_first_call_creates_inbox_processed_failed(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        root = tmp_path / "MoneyBin" / "test"
        assert (root / "inbox").is_dir()
        assert (root / "processed").is_dir()
        assert (root / "failed").is_dir()

    def test_directories_have_0700_permissions(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        root = tmp_path / "MoneyBin" / "test"
        for sub in ("inbox", "processed", "failed"):
            mode = stat.S_IMODE((root / sub).stat().st_mode)
            assert mode == 0o700, f"{sub} mode is {oct(mode)}"

    def test_idempotent(self, tmp_path: Path, inbox_service: InboxService) -> None:
        inbox_service.ensure_layout()
        inbox_service.ensure_layout()  # must not raise
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the service skeleton**

Create `src/moneybin/services/inbox_service.py`:

```python
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
    processed: list[dict[str, object]] = field(default_factory=list)
    failed: list[dict[str, object]] = field(default_factory=list)
    skipped: list[dict[str, object]] = field(default_factory=list)
    ignored: list[dict[str, object]] = field(default_factory=list)


@dataclass
class InboxListResult:
    would_process: list[dict[str, object]] = field(default_factory=list)
    ignored: list[dict[str, object]] = field(default_factory=list)


class InboxService:
    def __init__(self, db: Database, settings: MoneyBinSettings) -> None:
        self._db = db
        self._settings = settings

    @property
    def root(self) -> Path:
        return self._settings.profile_inbox_dir

    @property
    def inbox_dir(self) -> Path:
        return self.root / "inbox"

    @property
    def processed_dir(self) -> Path:
        return self.root / "processed"

    @property
    def failed_dir(self) -> Path:
        return self.root / "failed"

    def ensure_layout(self) -> None:
        """Create <root>/{inbox,processed,failed}/ with 0700 perms (idempotent)."""
        for d in (self.root, self.inbox_dir, self.processed_dir, self.failed_dir):
            d.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
            # mkdir's mode is masked by umask on creation; chmod fixes existing dirs too.
            d.chmod(_DIR_MODE)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Add InboxService skeleton with 0700 directory bootstrap"
```

---

## Task 3: File enumeration with skip/ignore rules

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestEnumeration:
    def test_root_files_enumerated_with_no_account_hint(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        (inbox_service.inbox_dir / "statement.csv").write_text("a,b\n1,2\n")
        items = inbox_service.enumerate()
        assert len(items.would_process) == 1
        assert items.would_process[0]["filename"] == "statement.csv"
        assert items.would_process[0]["account_hint"] is None

    def test_subfolder_files_get_account_slug(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        sub = inbox_service.inbox_dir / "chase-checking"
        sub.mkdir()
        (sub / "march.csv").write_text("a,b\n1,2\n")
        items = inbox_service.enumerate()
        assert len(items.would_process) == 1
        assert items.would_process[0]["filename"] == "chase-checking/march.csv"
        assert items.would_process[0]["account_hint"] == "chase-checking"

    def test_hidden_files_ignored(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        (inbox_service.inbox_dir / ".DS_Store").write_text("")
        items = inbox_service.enumerate()
        assert items.would_process == []
        assert items.ignored == [{"path": ".DS_Store", "reason": "hidden_file"}]

    def test_symlinks_ignored(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        target = tmp_path / "outside.csv"
        target.write_text("a\n")
        (inbox_service.inbox_dir / "link.csv").symlink_to(target)
        items = inbox_service.enumerate()
        assert items.would_process == []
        assert items.ignored[0]["reason"] == "symlink"

    def test_nested_subfolders_ignored(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        nested = inbox_service.inbox_dir / "a" / "b"
        nested.mkdir(parents=True)
        (nested / "deep.csv").write_text("x\n")
        items = inbox_service.enumerate()
        assert items.would_process == []
        assert any(i["reason"] == "nested_subfolder" for i in items.ignored)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestEnumeration -v`
Expected: FAIL — `enumerate` not defined.

- [ ] **Step 3: Implement `enumerate`**

Append to `InboxService` in `src/moneybin/services/inbox_service.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (8 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Implement inbox enumeration with hidden/symlink/nested skip rules"
```

---

## Task 4: Atomic move + collision-suffix helpers

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestAtomicMove:
    def test_move_to_dated_subdir(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        src = inbox_service.inbox_dir / "a.csv"
        src.write_text("data\n")
        final = inbox_service.move_to_outcome(
            src, outcome="processed", year_month="2026-05"
        )
        assert final == inbox_service.processed_dir / "2026-05" / "a.csv"
        assert final.read_text() == "data\n"
        assert not src.exists()

    def test_collision_appends_numeric_suffix(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        # Pre-existing file in the destination dir
        dest = inbox_service.processed_dir / "2026-05"
        dest.mkdir(parents=True)
        (dest / "a.csv").write_text("old\n")

        src = inbox_service.inbox_dir / "a.csv"
        src.write_text("new\n")
        final = inbox_service.move_to_outcome(
            src, outcome="processed", year_month="2026-05"
        )
        assert final.name == "a-1.csv"
        assert final.read_text() == "new\n"

    def test_collision_handles_no_extension(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        dest = inbox_service.processed_dir / "2026-05"
        dest.mkdir(parents=True)
        (dest / "README").write_text("old\n")

        src = inbox_service.inbox_dir / "README"
        src.write_text("new\n")
        final = inbox_service.move_to_outcome(
            src, outcome="processed", year_month="2026-05"
        )
        assert final.name == "README-1"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestAtomicMove -v`
Expected: FAIL — `move_to_outcome` not defined.

- [ ] **Step 3: Implement move + collision suffix**

Append to `InboxService`:

```python
_OUTCOME_DIRS = ("processed", "failed")


def move_to_outcome(
    self,
    src: Path,
    *,
    outcome: str,
    year_month: str,
) -> Path:
    """Move ``src`` into the outcome's YYYY-MM bucket atomically.

    Uses os.rename via Path.rename — atomic on the same filesystem. If a
    file with the same name already exists in the destination, append
    ``-1``, ``-2``, ... before the extension to avoid clobbering.

    Returns the final destination path.
    """
    if outcome not in self._OUTCOME_DIRS:
        raise ValueError(f"Unknown outcome: {outcome}")
    dest_dir = self.root / outcome / year_month
    dest_dir.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
    dest_dir.chmod(_DIR_MODE)

    final = self._next_available_path(dest_dir / src.name)
    src.rename(final)
    return final


@staticmethod
def _next_available_path(candidate: Path) -> Path:
    """Append -1, -2, ... before the suffix until we find a free name."""
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix  # includes leading "." or empty string
    i = 1
    while True:
        attempt = candidate.with_name(f"{stem}-{i}{suffix}")
        if not attempt.exists():
            return attempt
        i += 1
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (11 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Add atomic move with numeric-suffix collision handling"
```

---

## Task 5: Per-profile lockfile

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestLock:
    def test_lock_acquired_and_released(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        with inbox_service.acquire_lock():
            pass  # entering and leaving without raising is the assertion
        # Re-entering after release should also succeed
        with inbox_service.acquire_lock():
            pass

    def test_concurrent_lock_raises_inbox_busy(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        from moneybin.services.inbox_service import InboxBusyError

        with inbox_service.acquire_lock():
            with pytest.raises(InboxBusyError):
                with inbox_service.acquire_lock():
                    pass

    def test_different_profiles_have_independent_locks(self, tmp_path: Path) -> None:
        db = MagicMock(spec=Database)
        a = InboxService(db=db, settings=_make_settings(tmp_path, profile="alice"))
        b = InboxService(db=db, settings=_make_settings(tmp_path, profile="bob"))
        with a.acquire_lock():
            with b.acquire_lock():
                pass  # both held, no contention
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestLock -v`
Expected: FAIL — `acquire_lock`/`InboxBusyError` not defined.

- [ ] **Step 3: Implement the lock**

Add at the top of `inbox_service.py`:

```python
import contextlib
import fcntl
from collections.abc import Iterator


class InboxBusyError(Exception):
    """Another sync is in progress for this profile."""
```

Add to `InboxService`:

```python
@property
def lock_path(self) -> Path:
    return self.root / ".inbox.lock"


@contextlib.contextmanager
def acquire_lock(self) -> Iterator[None]:
    """Hold an exclusive flock on .inbox.lock for the duration of the block.

    Per-profile lock (the path is under self.root, which is per-profile),
    so concurrent syncs of different profiles do not contend. fcntl.flock
    with LOCK_NB raises BlockingIOError when held; we surface that as
    InboxBusyError so callers can return a structured error rather than
    block.
    """
    self.ensure_layout()
    # Open in "a" so the file exists; do not truncate.
    fh = open(self.lock_path, "a")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            fh.close()
            raise InboxBusyError("Another sync is in progress for this profile.") from e
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    finally:
        fh.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (14 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Add per-profile fcntl lockfile with InboxBusyError on contention"
```

---

## Task 6: Error sidecar writer

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestErrorSidecar:
    def test_writes_yaml_alongside_failed_file(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        import yaml

        inbox_service.ensure_layout()
        failed_dir = inbox_service.failed_dir / "2026-05"
        failed_dir.mkdir(parents=True)
        moved = failed_dir / "unknown.csv"
        moved.write_text("col1\n1\n")

        sidecar = inbox_service.write_error_sidecar(
            moved,
            error_code="needs_account_name",
            stage="resolve_account",
            message="Single-account file requires an account hint",
            suggestion="Move into inbox/<account-slug>/ and re-run sync",
            extra={"available_accounts": ["chase-checking", "amex"]},
        )

        assert sidecar == failed_dir / "unknown.csv.error.yml"
        loaded = yaml.safe_load(sidecar.read_text())
        assert loaded["error_code"] == "needs_account_name"
        assert loaded["stage"] == "resolve_account"
        assert loaded["message"].startswith("Single-account")
        assert loaded["suggestion"].startswith("Move into")
        assert loaded["available_accounts"] == ["chase-checking", "amex"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestErrorSidecar -v`
Expected: FAIL — `write_error_sidecar` not defined.

- [ ] **Step 3: Implement the sidecar writer**

Add `import yaml` near the top of `inbox_service.py`, then add to `InboxService`:

```python
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
        """Write a <filename>.error.yml sidecar next to a failed file.

        Schema is intentionally flat so a future web UI can render it without
        a parser: error_code, stage, message, suggestion, plus any extra
        fields (e.g. available_accounts) the caller provides.
        """
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (15 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Add YAML error sidecar writer for failed inbox imports"
```

---

## Task 7: `sync()` happy path — drain inbox via ImportService

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestSyncHappyPath:
    def test_imports_root_file_and_moves_to_processed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        # Stub ImportService so we don't run the real pipeline.
        captured: list[dict[str, object]] = []

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                captured.append({"path": path, **kwargs})
                return ImportResult(
                    file_path=path, file_type="tabular", transactions=42
                )

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "statement.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")

        assert len(result.processed) == 1
        entry = result.processed[0]
        assert entry["filename"] == "statement.csv"
        assert entry["transactions"] == 42
        # File moved to processed/2026-05/
        assert not (svc.inbox_dir / "statement.csv").exists()
        assert (svc.processed_dir / "2026-05" / "statement.csv").exists()
        # ImportService received the still-in-inbox path, not the moved path
        assert captured[0]["path"].endswith("/inbox/statement.csv")

    def test_subfolder_passes_account_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        captured_kwargs: dict[str, object] = {}

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                captured_kwargs.update(kwargs)
                return ImportResult(file_path=path, file_type="tabular")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        sub = svc.inbox_dir / "chase-checking"
        sub.mkdir()
        (sub / "march.csv").write_text("a\n1\n")

        svc.sync(year_month="2026-05")

        assert captured_kwargs["account_name"] == "chase-checking"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestSyncHappyPath -v`
Expected: FAIL — `sync` not defined.

- [ ] **Step 3: Implement `sync` happy path**

Add to top of `inbox_service.py`:

```python
import time
from datetime import datetime, timezone

from moneybin.metrics.registry import (
    INBOX_SYNC_DURATION_SECONDS,
    INBOX_SYNC_TOTAL,
)
from moneybin.services.import_service import ImportService
```

(Note: `INBOX_SYNC_*` are added in Task 11 — for now the import will fail. Move this `import` line into Task 11 if you want the commits to stay green; or add the metrics first and reorder. Recommended: do Task 11 *before* this `import` line lands. The plan keeps them in this order for narrative reasons; if you prefer green commits, swap Tasks 7 and 11.)

Add to `InboxService`:

```python
def sync(self, year_month: str | None = None) -> InboxSyncResult:
    """Drain the inbox: import each eligible file and move it.

    Args:
        year_month: Override the YYYY-MM bucket (testing). Defaults to UTC now.
    """
    ym = year_month or datetime.now(timezone.utc).strftime("%Y-%m")
    with self.acquire_lock():
        listing = self.enumerate()
        result = InboxSyncResult(
            ignored=list(listing.ignored),
        )
        t0 = time.monotonic()
        for item in listing.would_process:
            self._sync_one(item, year_month=ym, result=result)
        INBOX_SYNC_DURATION_SECONDS.observe(time.monotonic() - t0)
        return result


def _sync_one(
    self,
    item: dict[str, object],
    *,
    year_month: str,
    result: InboxSyncResult,
) -> None:
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
```

`_handle_failure` is added in Task 8 — for now stub it:

```python
    def _handle_failure(
        self,
        src: Path,
        rel_filename: str,
        error: Exception,
        year_month: str,
        result: InboxSyncResult,
    ) -> None:
        # Filled in by Task 8.
        raise error
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (17 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Implement InboxService.sync() happy path with per-file import"
```

---

## Task 8: Failure handling — move to `failed/` and write sidecar

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestSyncFailure:
    def test_failed_import_lands_in_failed_with_sidecar(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import yaml

        from moneybin.services import inbox_service as mod

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise ValueError(
                    "Single-account files require --account-name or --account-id"
                )

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "unknown.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")

        assert len(result.failed) == 1
        entry = result.failed[0]
        assert entry["filename"] == "unknown.csv"
        assert entry["error_code"] == "needs_account_name"
        assert entry["sidecar"].endswith("unknown.csv.error.yml")

        # File and sidecar both exist
        moved = svc.failed_dir / "2026-05" / "unknown.csv"
        sidecar = moved.with_name("unknown.csv.error.yml")
        assert moved.exists()
        loaded = yaml.safe_load(sidecar.read_text())
        assert loaded["error_code"] == "needs_account_name"
        assert "stage" in loaded
        assert "message" in loaded

    def test_unknown_error_uses_generic_code(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise RuntimeError("disk full")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "x.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")
        assert result.failed[0]["error_code"] == "import_error"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestSyncFailure -v`
Expected: FAIL — current `_handle_failure` re-raises.

- [ ] **Step 3: Implement failure classification + handling**

Add module-level mapping near the dataclasses in `inbox_service.py`:

```python
# Substring → (error_code, stage) for ValueError messages from ImportService.
# Order matters: first match wins. Each entry documents which code path raises it.
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
```

Replace the stub `_handle_failure` in `InboxService`:

```python
def _handle_failure(
    self,
    src: Path,
    rel_filename: str,
    error: Exception,
    year_month: str,
    result: InboxSyncResult,
) -> None:
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
    return {
        "needs_account_name": (
            "Move the file into inbox/<account-slug>/ "
            "(e.g., inbox/chase-checking/) and re-run sync."
        ),
        "low_confidence_mapping": (
            "Use 'moneybin import file <path> --override field=column' "
            "to map columns explicitly, then re-drop in inbox/."
        ),
        "unsupported_file_type": "Convert to OFX/QFX, CSV, TSV, XLSX, Parquet, or PDF.",
        "empty_file": "File contained no data rows; remove or replace.",
    }.get(error_code)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (19 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Move failed inbox imports to failed/ with classified YAML sidecars"
```

---

## Task 9: Concurrency — `inbox_busy` returned, not raised, by `sync()`

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestSyncBusy:
    def test_concurrent_sync_returns_inbox_busy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                return ImportResult(file_path=path, file_type="tabular")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        outer = InboxService(db=db, settings=_make_settings(tmp_path))
        inner = InboxService(db=db, settings=_make_settings(tmp_path))
        outer.ensure_layout()

        with outer.acquire_lock():
            result = inner.sync(year_month="2026-05")
        # sync returns a result with skipped=[{reason: inbox_busy}], does not raise.
        assert result.processed == []
        assert result.failed == []
        assert result.skipped == [{"reason": "inbox_busy"}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestSyncBusy -v`
Expected: FAIL — `sync` currently raises `InboxBusyError`.

- [ ] **Step 3: Catch `InboxBusyError` inside `sync`**

Modify `InboxService.sync` in `inbox_service.py`:

```python
    def sync(self, year_month: str | None = None) -> InboxSyncResult:
        ym = year_month or datetime.now(timezone.utc).strftime("%Y-%m")
        try:
            with self.acquire_lock():
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (20 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Return inbox_busy in sync result instead of raising"
```

---

## Task 10: Crash recovery — staging-* → inbox at sync start

This task implements the staging-rename pattern from spec requirement #9 and the recovery pass.

**Files:**
- Modify: `src/moneybin/services/inbox_service.py`
- Modify: `tests/moneybin/test_services/test_inbox_service.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_inbox_service.py`:

```python
class TestRecovery:
    def test_staging_files_in_processed_revert_to_inbox(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        # Simulate a crash: a staging-* file lives in processed/ from a prior run.
        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        ghost = svc.processed_dir / "staging-statement.csv"
        ghost.write_text("partial\n")

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                return ImportResult(file_path=path, file_type="tabular")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        # First sync should restore the file before processing.
        result = svc.sync(year_month="2026-05")

        assert not ghost.exists(), "staging file should be moved out of processed/"
        # File ends up at processed/2026-05/statement.csv after re-import
        final = svc.processed_dir / "2026-05" / "statement.csv"
        assert final.exists()
        assert len(result.processed) == 1

    def test_staging_files_in_failed_also_recovered(self, tmp_path: Path) -> None:
        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        ghost = svc.failed_dir / "staging-x.csv"
        ghost.write_text("partial\n")

        svc.recover_staging()

        assert not ghost.exists()
        assert (svc.inbox_dir / "x.csv").exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py::TestRecovery -v`
Expected: FAIL — `recover_staging` not defined and `move_to_outcome` doesn't use staging yet.

- [ ] **Step 3: Make `move_to_outcome` use a staging step + add `recover_staging`**

Replace `move_to_outcome` in `inbox_service.py`:

```python
_STAGING_PREFIX = "staging-"


def move_to_outcome(
    self,
    src: Path,
    *,
    outcome: str,
    year_month: str,
) -> Path:
    """Atomic two-step move via a staging name in the outcome root.

    1. Rename src → <outcome>/staging-<name>  (atomic, same FS guaranteed
       because outcome dirs are siblings of inbox under self.root).
    2. Rename staging-<name> → <outcome>/<year_month>/<name>  (atomic).

    A crash between (1) and (2) leaves a discoverable staging-* file at
    the outcome root, which `recover_staging()` reverts on next sync.
    """
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
    """Move any leftover staging-* files in outcome roots back to inbox/.

    Run at the start of every sync. Returns the recovered destinations
    (in inbox/) so callers can log them if useful.
    """
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
```

Call `recover_staging()` at the top of `sync()`, before `enumerate()`:

```python
    def sync(self, year_month: str | None = None) -> InboxSyncResult:
        ym = year_month or datetime.now(timezone.utc).strftime("%Y-%m")
        try:
            with self.acquire_lock():
                self.recover_staging()
                listing = self.enumerate()
                ...
```

Note: existing `TestAtomicMove` tests will continue to pass — the final destination is unchanged, only the route (staging stop) is new.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (22 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/inbox_service.py tests/moneybin/test_services/test_inbox_service.py
git commit -m "Implement staging-rename + crash recovery for inbox moves"
```

---

## Task 11: Add inbox metrics to the registry

**Files:**
- Modify: `src/moneybin/metrics/registry.py`

This depends on the import in Task 7. If you encounter ImportError there, do this task first.

- [ ] **Step 1: Add metrics**

Append to the "Import pipeline" section in `src/moneybin/metrics/registry.py`:

```python
INBOX_SYNC_TOTAL = Counter(
    "moneybin_inbox_sync_total",
    "Inbox file outcomes per sync",
    ["outcome"],  # processed | failed | skipped | ignored
)

INBOX_SYNC_DURATION_SECONDS = Histogram(
    "moneybin_inbox_sync_duration_seconds",
    "Duration of one inbox drain (seconds)",
)
```

- [ ] **Step 2: Run service tests to confirm imports resolve**

Run: `uv run pytest tests/moneybin/test_services/test_inbox_service.py -v`
Expected: PASS (22 tests).

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/metrics/registry.py
git commit -m "Register inbox_sync_total and inbox_sync_duration_seconds metrics"
```

---

## Task 12: CLI sub-typer — `import inbox`, `inbox list`, `inbox path`

**Files:**
- Create: `src/moneybin/cli/commands/import_inbox.py`
- Modify: `src/moneybin/cli/commands/import_cmd.py`
- Create: `tests/moneybin/test_cli/test_cli_import_inbox.py`

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_cli/test_cli_import_inbox.py`:

```python
"""CLI tests for `moneybin import inbox` subcommands.

Business logic is exercised in tests/moneybin/test_services/test_inbox_service.py;
these tests verify argument parsing, exit codes, and output shape with
InboxService mocked at the import site.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.inbox_service import InboxListResult, InboxSyncResult


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


def test_inbox_drain_prints_summary(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = MagicMock()
    fake.sync.return_value = InboxSyncResult(
        processed=[{"filename": "chase-checking/march.csv", "transactions": 47}],
        failed=[],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.stderr
    assert "1 imported" in result.stdout
    assert "0 failed" in result.stdout


def test_inbox_drain_failure_exits_zero_but_warns(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = MagicMock()
    fake.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[
            {
                "filename": "x.csv",
                "error_code": "needs_account_name",
                "sidecar": "failed/2026-05/x.csv.error.yml",
            }
        ],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox"])

    # Failures are not a usage error and not a runtime error — they're a
    # state the user resolves by moving files. Exit 0 keeps shell scripts
    # composable; the stderr warning is the human channel.
    assert result.exit_code == 0
    assert "needs_account_name" in result.stdout + result.stderr
    assert "0 imported" in result.stdout
    assert "1 failed" in result.stdout


def test_inbox_drain_json_output(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = MagicMock()
    fake.sync.return_value = InboxSyncResult(
        processed=[{"filename": "a.csv", "transactions": 3}],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["processed"][0]["filename"] == "a.csv"


def test_inbox_list_prints_would_process(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = MagicMock()
    fake.enumerate.return_value = InboxListResult(
        would_process=[
            {"filename": "chase-checking/march.csv", "account_hint": "chase-checking"}
        ],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox", "list"])

    assert result.exit_code == 0
    assert "chase-checking/march.csv" in result.stdout


def test_inbox_path_prints_active_profile_root(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = MagicMock()
    fake.root = tmp_path / "MoneyBin" / "alice"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox", "path"])

    assert result.exit_code == 0
    assert str(fake.root) in result.stdout.strip()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_import_inbox.py -v`
Expected: FAIL — `inbox` subcommand not registered.

- [ ] **Step 3: Implement the CLI sub-typer**

Create `src/moneybin/cli/commands/import_inbox.py`:

```python
"""`moneybin import inbox` — drain, list, and locate the watched inbox.

Wraps moneybin.services.inbox_service.InboxService; argument parsing only.
See docs/specs/smart-import-inbox.md.
"""

from __future__ import annotations

import dataclasses
import json
import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.errors import DatabaseKeyError

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Drop files into the inbox and drain them into MoneyBin.",
    no_args_is_help=False,  # bare `inbox` runs the drain (the default action)
)


def _build_service() -> object:
    """Build an InboxService bound to the active profile.

    Indirected through this helper so CLI tests can monkeypatch it without
    constructing a real Database. Returned as ``object`` because the CLI
    only needs duck-typed access to ``.sync``, ``.enumerate``, and ``.root``.
    """
    from moneybin.config import get_settings
    from moneybin.database import get_database
    from moneybin.services.inbox_service import InboxService

    return InboxService(db=get_database(), settings=get_settings())


def _print_text(result: object) -> tuple[int, int]:
    """Render a text summary; return (processed_count, failed_count)."""
    processed = list(getattr(result, "processed", []))
    failed = list(getattr(result, "failed", []))
    skipped = list(getattr(result, "skipped", []))

    if skipped and any(s.get("reason") == "inbox_busy" for s in skipped):
        typer.echo("⚠️  Another sync is in progress; nothing done.", err=True)
        return (0, 0)

    for item in processed:
        typer.echo(
            f"✓ {item['filename']}  →  imported "
            f"({item.get('transactions', 0)} transactions)"
        )
    for item in failed:
        typer.echo(f"✗ {item['filename']}  →  failed ({item['error_code']})")
        if "sidecar" in item:
            typer.echo(f"   See {item['sidecar']}", err=True)

    typer.echo(f"Done: {len(processed)} imported, {len(failed)} failed.")
    return (len(processed), len(failed))


@app.callback(invoke_without_command=True)
def inbox_default(
    ctx: typer.Context,
    output: OutputFormat = output_option(),
    quiet: bool = quiet_option(),
) -> None:
    """Default action when invoked as `moneybin import inbox` (no subcommand)."""
    if ctx.invoked_subcommand is not None:
        return
    try:
        service = _build_service()
        result = service.sync()
    except DatabaseKeyError as e:
        typer.echo(f"❌ {e}. Run 'moneybin db unlock'.", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.json:
        typer.echo(json.dumps(dataclasses.asdict(result), default=str))
        return
    if quiet:
        return
    _print_text(result)


@app.command("list")
def inbox_list(
    output: OutputFormat = output_option(),
    quiet: bool = quiet_option(),
) -> None:
    """Show what a sync would do, without moving anything."""
    try:
        service = _build_service()
        result = service.enumerate()
    except DatabaseKeyError as e:
        typer.echo(f"❌ {e}. Run 'moneybin db unlock'.", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.json:
        typer.echo(json.dumps(dataclasses.asdict(result), default=str))
        return
    if quiet:
        return
    for item in result.would_process:
        hint = f"  [{item['account_hint']}]" if item.get("account_hint") else ""
        typer.echo(f"  {item['filename']}{hint}")
    if not result.would_process:
        typer.echo("(inbox empty)")


@app.command("path")
def inbox_path() -> None:
    """Print the active profile's inbox parent directory."""
    service = _build_service()
    typer.echo(str(service.root))
```

- [ ] **Step 4: Wire the sub-typer into the `import` group**

Edit `src/moneybin/cli/commands/import_cmd.py`. Near the top with the other `add_typer` calls:

```python
from moneybin.cli.commands import import_inbox

app.add_typer(import_inbox.app, name="inbox", help="Drain the watched import inbox")
```

(The `formats_app` registration is already present — add this on the next line.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_import_inbox.py -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/cli/commands/import_inbox.py src/moneybin/cli/commands/import_cmd.py tests/moneybin/test_cli/test_cli_import_inbox.py
git commit -m "Add `moneybin import inbox` CLI sub-typer (drain, list, path)"
```

---

## Task 13: MCP tools — `import.inbox_sync` and `import.inbox_list`

**Files:**
- Create: `src/moneybin/mcp/tools/import_inbox.py`
- Modify: `src/moneybin/mcp/server.py`
- Create: `tests/moneybin/test_mcp/test_import_inbox_tools.py`

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_mcp/test_import_inbox_tools.py`:

```python
"""Tests for import.inbox_sync / import.inbox_list MCP tools."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.mcp.tools.import_inbox import inbox_list as inbox_list_tool
from moneybin.mcp.tools.import_inbox import inbox_sync as inbox_sync_tool
from moneybin.services.inbox_service import InboxListResult, InboxSyncResult


@pytest.fixture
def patch_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    fake = MagicMock()
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr("moneybin.mcp.tools.import_inbox._build_service", lambda: fake)
    return fake


class TestInboxSyncTool:
    def test_returns_low_sensitivity_envelope(self, patch_service) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 3}],
        )
        envelope = inbox_sync_tool()
        assert envelope.summary["sensitivity"] == "low"
        assert envelope.data["processed"][0]["filename"] == "a.csv"

    def test_failure_includes_actions_hint(self, patch_service) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            failed=[{"filename": "x.csv", "error_code": "needs_account_name"}],
        )
        envelope = inbox_sync_tool()
        assert any("inbox/<account-slug>" in a for a in envelope.actions)

    def test_no_failure_no_resolution_hint(self, patch_service) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 1}],
        )
        envelope = inbox_sync_tool()
        assert not any("inbox/<account-slug>" in a for a in envelope.actions)


class TestInboxListTool:
    def test_returns_would_process_shape(self, patch_service) -> None:
        patch_service.enumerate.return_value = InboxListResult(
            would_process=[{"filename": "a.csv", "account_hint": None}],
        )
        envelope = inbox_list_tool()
        assert envelope.summary["sensitivity"] == "low"
        assert envelope.data["would_process"][0]["filename"] == "a.csv"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_import_inbox_tools.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the MCP tools**

Create `src/moneybin/mcp/tools/import_inbox.py`:

```python
"""Inbox MCP tools — drain and preview the watched import folder.

Tools:
    - import.inbox_sync — Drain the active profile's inbox (low sensitivity).
    - import.inbox_list — Preview the active profile's inbox (low sensitivity).

See docs/specs/smart-import-inbox.md for the filesystem contract.
"""

from __future__ import annotations

import dataclasses
import logging

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)


def _build_service() -> object:
    """Indirection point for tests to patch."""
    from moneybin.config import get_settings
    from moneybin.database import get_database
    from moneybin.services.inbox_service import InboxService

    return InboxService(db=get_database(), settings=get_settings())


@mcp_tool(sensitivity="low")
def inbox_sync() -> ResponseEnvelope:
    """Drain the active profile's import inbox.

    Imports each eligible file from
    ``<inbox_root>/<profile>/inbox/`` and moves it to
    ``processed/YYYY-MM/`` on success or ``failed/YYYY-MM/`` (plus a YAML
    error sidecar) on failure. Returns aggregate counts and per-file
    outcomes. File contents are never returned.
    """
    service = _build_service()
    result = dataclasses.asdict(service.sync())  # type: ignore[arg-type]

    actions: list[str] = ["Use transactions.search to view newly imported transactions"]
    if result["failed"]:
        actions.insert(
            0,
            "Move failed files into inbox/<account-slug>/ and re-run import.inbox_sync",
        )
    return build_envelope(
        data=result,
        sensitivity="low",
        actions=actions,
    )


@mcp_tool(sensitivity="low")
def inbox_list() -> ResponseEnvelope:
    """Preview the active profile's inbox without moving anything.

    Returns the same shape as ``import.inbox_sync`` but under
    ``would_process`` instead of ``processed``/``failed``. Useful when the
    user wants to see what's waiting before draining.
    """
    service = _build_service()
    result = dataclasses.asdict(service.enumerate())  # type: ignore[arg-type]
    return build_envelope(
        data=result,
        sensitivity="low",
        actions=[
            "Use import.inbox_sync to drain the inbox",
        ],
    )


def register_inbox_tools(mcp: FastMCP) -> None:
    register(
        mcp,
        inbox_sync,
        "import.inbox_sync",
        "Drain the active profile's import inbox; move successes to "
        "processed/ and failures to failed/ with structured error sidecars.",
    )
    register(
        mcp,
        inbox_list,
        "import.inbox_list",
        "Preview the active profile's import inbox without moving anything.",
    )
```

- [ ] **Step 4: Wire registration in the server**

Edit `src/moneybin/mcp/server.py`. In `register_core_tools`, after the existing `from moneybin.mcp.tools.import_tools import register_import_tools` line, add:

```python
    from moneybin.mcp.tools.import_inbox import register_inbox_tools
```

And in the same function body, after `register_import_tools(mcp)`:

```python
    register_inbox_tools(mcp)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_import_inbox_tools.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/mcp/tools/import_inbox.py src/moneybin/mcp/server.py tests/moneybin/test_mcp/test_import_inbox_tools.py
git commit -m "Add import.inbox_sync and import.inbox_list MCP tools"
```

---

## Task 14: E2E coverage — help text + workflow

**Files:**
- Modify: `tests/e2e/test_e2e_help.py`
- Create: `tests/e2e/test_e2e_inbox.py`

- [ ] **Step 1: Add help-coverage entries**

Open `tests/e2e/test_e2e_help.py` and find the `_HELP_COMMANDS` list. Add entries (matching the surrounding style):

```python
("import inbox",)
("import inbox list",)
("import inbox path",)
```

- [ ] **Step 2: Write the workflow E2E test**

Create `tests/e2e/test_e2e_inbox.py`:

```python
"""End-to-end: drop a CSV in the inbox, drain it, assert it lands in processed/."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.e2e
def test_inbox_workflow(tmp_path: Path, e2e_workflow_env) -> None:
    """User drops a single-account CSV into inbox/<slug>/ and runs sync.

    Asserts:
      - exit code 0
      - file moved to processed/YYYY-MM/
      - inbox empty afterwards
    """
    env = e2e_workflow_env  # provided by tests/e2e/conftest.py per testing.md
    inbox_root = Path(env["MONEYBIN_IMPORT__INBOX_ROOT"])
    profile_root = inbox_root / env["MONEYBIN_PROFILE"]
    sub = profile_root / "inbox" / "chase-checking"
    sub.mkdir(parents=True)

    # Use an existing fixture from the tabular E2E suite.
    fixture = (
        Path(__file__).parent.parent
        / "moneybin"
        / "test_services"
        / "fixtures"
        / "chase_checking_sample.csv"
    )
    if not fixture.exists():
        pytest.skip(f"fixture missing: {fixture}")
    shutil.copy(fixture, sub / "march.csv")

    proc = subprocess.run(
        ["uv", "run", "moneybin", "import", "inbox"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr

    # Inbox empty
    assert list((profile_root / "inbox").rglob("*.csv")) == []

    # Exactly one CSV under processed/
    processed_csvs = list((profile_root / "processed").rglob("*.csv"))
    assert len(processed_csvs) == 1
    assert processed_csvs[0].name == "march.csv"
```

If `e2e_workflow_env` doesn't already supply `MONEYBIN_IMPORT__INBOX_ROOT`, extend `make_workflow_env()` in `tests/e2e/conftest.py` to set it to `<tmp>/inbox-root`. Read that conftest before editing — keep the change additive.

If a Chase-style fixture isn't available at the path above, search `tests/` for existing single-account CSV fixtures (e.g., `grep -rln 'transaction_id' tests/.../fixtures`) and adjust the path. Skip-on-missing is already handled by the test.

- [ ] **Step 3: Run E2E tests**

Run: `uv run pytest tests/e2e/test_e2e_help.py tests/e2e/test_e2e_inbox.py -v -m e2e`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/test_e2e_help.py tests/e2e/test_e2e_inbox.py tests/e2e/conftest.py
git commit -m "Add E2E coverage for `import inbox` workflow and help"
```

---

## Task 15: Spec, INDEX, and README updates

**Files:**
- Modify: `docs/specs/smart-import-inbox.md`
- Modify: `docs/specs/INDEX.md`
- Modify: `README.md`

- [ ] **Step 1: Flip spec status to `implemented`**

Edit the Status section in `docs/specs/smart-import-inbox.md`:

```markdown
## Status

implemented
```

- [ ] **Step 2: Add INDEX entry**

In `docs/specs/INDEX.md`, find the Smart Import section and add an entry for `smart-import-inbox.md` (status: `implemented`). Mirror the formatting of neighboring rows exactly.

- [ ] **Step 3: README — flip 📐 → ✅ and add inbox blurb**

In `README.md`:
1. In the relevant roadmap table row (Smart Import / Import Inbox), replace `📐` with `✅`.
2. In the import section ("What Works Today"), add a short paragraph:

   > **Watched inbox.** Drop financial files into `~/Documents/MoneyBin/<profile>/inbox/` (or `inbox/<account-slug>/` for single-account files), then run `moneybin import inbox` or call `import.inbox_sync` from MCP. Successes move to `processed/YYYY-MM/`; failures move to `failed/YYYY-MM/` with a YAML sidecar describing the error and how to fix it.

- [ ] **Step 4: Commit**

```bash
git add docs/specs/smart-import-inbox.md docs/specs/INDEX.md README.md
git commit -m "Document smart-import-inbox as implemented"
```

---

## Task 16: Final pre-push pass

- [ ] **Step 1: Run the full pre-commit check**

Run: `make check test`
Expected: format/lint/pyright/tests all pass.

- [ ] **Step 2: Run `/simplify` per `.claude/rules/shipping.md`**

Invoke `/simplify` against the diff so it can flag copy-paste, redundant state, missing validations. Apply its fixes and re-run `make check test`.

- [ ] **Step 3: Push and open PR**

```bash
git push -u origin feat/import-ergonomics
gh pr create --title "Add smart-import-inbox: watched-folder import UX" --body "$(cat <<'EOF'
## Summary
- Add `~/Documents/MoneyBin/<profile>/{inbox,processed,failed}/` watched-folder import UX
- New `InboxService` orchestrates atomic moves, per-profile lockfile, crash recovery
- CLI: `moneybin import inbox`, `inbox list`, `inbox path`
- MCP: `import.inbox_sync`, `import.inbox_list` (both `low` sensitivity)

Implements [`docs/specs/smart-import-inbox.md`](docs/specs/smart-import-inbox.md).

## Test plan
- [ ] `make check test`
- [ ] `uv run pytest tests/e2e/test_e2e_inbox.py -m e2e -v`
- [ ] Manual: drop a CSV in inbox/, run `moneybin import inbox`, confirm processed/
- [ ] Manual: drop a single-account CSV at inbox root (no slug subfolder), confirm failed/ + sidecar

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| 1. Inbox layout + per-profile isolation | 1, 2 |
| 2. Auto-create dirs | 2 |
| 3. 0700 permissions | 2 |
| 4. Account-by-subfolder | 3, 7 |
| 5. Drain semantics + collision suffix | 4, 7, 8 |
| 6. Error sidecar contract | 6, 8 |
| 7. Idempotent re-runs | covered by enumerate (no-op on empty) + existing content-hash dedup; explicitly tested in Task 7's empty-inbox path is implicit — add a test if desired during review |
| 8. Concurrency / `inbox_busy` | 5, 9 |
| 9. Atomic-rename with staging recovery | 10 |
| 10. Skip rules (hidden, symlink, nested) | 3 |
| 11. CLI surface | 12 |
| 12. MCP surface | 13 |
| 13. No background processes | (out of scope; not implemented) |
| 14. No interactive resolution | (out of scope; not implemented) |
| 15. Response envelopes | 13 |
| 16. Logging hygiene | logger calls in 7, 8, 10 use filenames only |
| 17. Observability metrics | 11 |
| README + INDEX | 15 |

**Type consistency:** `InboxSyncResult` and `InboxListResult` field names are stable across Tasks 2–13. `_build_service` is the same indirection name in both CLI (Task 12) and MCP (Task 13) modules.

**Placeholder scan:** Each step contains either a complete code block or an exact command. The Task 7/Task 11 ordering note is documented inline so the engineer can swap if needed.

**Known soft spot:** the E2E test in Task 14 depends on a `e2e_workflow_env` fixture and a Chase-style fixture file. Both are conditional — the test skips if the fixture isn't present. The engineer should grep for an actual single-account CSV fixture during Task 14 step 2 and adjust the path before running, rather than guessing.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-01-smart-import-inbox.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
