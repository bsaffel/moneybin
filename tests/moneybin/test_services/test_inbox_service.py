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
    """Build an InboxService rooted under tmp_path with a mocked Database."""
    db = MagicMock(spec=Database)
    return InboxService(db=db, settings=_make_settings(tmp_path))


class TestDirectoryBootstrap:
    """ensure_layout creates the inbox/processed/failed tree with 0700 perms."""

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


class TestEnumeration:
    """enumerate() walks one level deep and classifies entries."""

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

    def test_invalid_account_slug_folder_ignored(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        """Folder names that fail the slug regex have their contents ignored."""
        inbox_service.ensure_layout()
        bad = inbox_service.inbox_dir / "weird name!"
        bad.mkdir()
        (bad / "march.csv").write_text("a,b\n1,2\n")
        items = inbox_service.enumerate()
        assert items.would_process == []
        assert any(i["reason"] == "invalid_account_slug" for i in items.ignored)


class TestAtomicMove:
    """move_to_outcome() moves files atomically with numeric-suffix collision handling."""

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

    def test_invalid_year_month_raises(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        """year_month must match YYYY-MM; path-traversal candidates rejected."""
        inbox_service.ensure_layout()
        src = inbox_service.inbox_dir / "a.csv"
        src.write_text("data\n")
        with pytest.raises(ValueError, match="year_month"):
            inbox_service.move_to_outcome(
                src, outcome="processed", year_month="2026-05/../sensitive"
            )


class TestLock:
    """Lockfile contention semantics."""

    def test_lock_acquired_and_released(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        with inbox_service.acquire_lock():
            pass
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
                pass


class TestErrorSidecar:
    """YAML error sidecar writer."""

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


class TestSyncHappyPath:
    """sync() happy path: import file, move to processed/."""

    def test_imports_root_file_and_moves_to_processed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult
        from moneybin.services.refresh import RefreshResult

        captured: list[dict[str, object]] = []

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                captured.append({"path": path, **kwargs})
                return ImportResult(
                    file_path=path, file_type="tabular", transactions=42
                )

        refresh_calls = 0

        def fake_refresh(db: object) -> RefreshResult:
            nonlocal refresh_calls
            refresh_calls += 1
            return RefreshResult(applied=True, duration_seconds=0.01)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.services.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "statement.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")

        assert len(result.processed) == 1
        entry = result.processed[0]
        assert entry["filename"] == "statement.csv"
        assert entry["transactions"] == 42
        assert not (svc.inbox_dir / "statement.csv").exists()
        assert (svc.processed_dir / "2026-05" / "statement.csv").exists()
        assert str(captured[0]["path"]).endswith("/inbox/statement.csv")
        # Per-file import must defer the refresh pipeline; sync runs it once at end.
        assert captured[0]["refresh"] is False
        assert refresh_calls == 1
        assert result.transforms_applied is True
        assert result.transforms_duration_seconds == 0.01

    def test_subfolder_passes_account_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult
        from moneybin.services.refresh import RefreshResult

        captured_kwargs: dict[str, object] = {}

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                captured_kwargs.update(kwargs)
                return ImportResult(file_path=path, file_type="tabular")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.services.refresh.refresh",
            lambda db: RefreshResult(applied=True, duration_seconds=0.0),
            raising=True,
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        sub = svc.inbox_dir / "chase-checking"
        sub.mkdir()
        (sub / "march.csv").write_text("a\n1\n")

        svc.sync(year_month="2026-05")

        assert captured_kwargs["account_name"] == "chase-checking"


class TestSyncRefreshOnce:
    """Regression: refresh runs exactly once per sync() call, not per file."""

    def test_multi_file_batch_runs_refresh_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two files should trigger exactly one refresh() call."""
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult
        from moneybin.services.refresh import RefreshResult

        refresh_call_count = 0
        per_file_kwargs: list[dict[str, object]] = []

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                per_file_kwargs.append(kwargs)
                return ImportResult(
                    file_path=path, file_type="tabular", transactions=10
                )

        def fake_refresh(db: object) -> RefreshResult:
            nonlocal refresh_call_count
            refresh_call_count += 1
            return RefreshResult(applied=True, duration_seconds=0.02)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.services.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "one.csv").write_text("a\n1\n")
        (svc.inbox_dir / "two.csv").write_text("a\n2\n")

        result = svc.sync(year_month="2026-05")

        # Two files processed, exactly one refresh call.
        assert len(result.processed) == 2
        assert refresh_call_count == 1
        # Each per-file import deferred the refresh.
        assert len(per_file_kwargs) == 2
        assert all(kw["refresh"] is False for kw in per_file_kwargs)
        # Refresh timing surfaces in the result.
        assert result.transforms_applied is True
        assert result.transforms_duration_seconds == 0.02

    def test_no_successes_skips_refresh(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """All-failure batch must NOT call refresh()."""
        from moneybin.services import inbox_service as mod
        from moneybin.services.refresh import RefreshResult

        refresh_calls = 0

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise RuntimeError("boom")

        def fake_refresh(db: object) -> RefreshResult:
            nonlocal refresh_calls
            refresh_calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.services.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "broken.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")

        assert refresh_calls == 0
        assert result.transforms_applied is False
        assert result.transforms_duration_seconds is None
        assert len(result.failed) == 1

    def test_refresh_false_skips_pipeline(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """sync(refresh=False) defers the refresh pipeline entirely."""
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult
        from moneybin.services.refresh import RefreshResult

        refresh_calls = 0

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                return ImportResult(file_path=path, file_type="tabular", transactions=3)

        def fake_refresh(db: object) -> RefreshResult:
            nonlocal refresh_calls
            refresh_calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.services.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "a.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05", refresh=False)

        assert refresh_calls == 0
        assert result.transforms_applied is False
        assert len(result.processed) == 1


class TestSyncFailure:
    """Failed imports get moved to failed/ with YAML sidecar."""

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
        assert str(entry["sidecar"]).endswith("unknown.csv.error.yml")

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

    def test_failed_entry_includes_message_and_class(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failed result entries surface error_class + message to MCP/CLI callers."""
        from moneybin.services import inbox_service as mod

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise RuntimeError("something specific went wrong")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "x.csv").write_text("a\n1\n")

        entry = svc.sync(year_month="2026-05").failed[0]
        assert entry["error_class"] == "RuntimeError"
        assert entry["message"] == "something specific went wrong"

    def test_duckdb_binder_error_classified_as_schema_mismatch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DuckDB binder errors map to schema_mismatch with a migrate suggestion.

        Covers the failure mode where a raw.* table is missing a column that
        the loader writes (e.g. import_id pre-V003) — the user-visible error
        should tell them to run db migrate, not surface as generic import_error.
        """
        import duckdb

        from moneybin.services import inbox_service as mod

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise duckdb.BinderException(
                    "Referenced update column import_id not found in table!"
                )

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "x.qfx").write_text("not really qfx\n")

        entry = svc.sync(year_month="2026-05").failed[0]
        assert entry["error_code"] == "schema_mismatch"
        assert entry["stage"] == "load"
        assert entry["error_class"] == "BinderException"
        assert "moneybin db migrate" in str(entry["suggestion"])

    def test_suggestion_preserved_when_source_vanishes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Vanished-source early-exit path still surfaces the suggestion.

        Regression: PR #93 originally hoisted error_class/message into the
        early-exit dicts but left suggestion in the success-only branch, so
        a schema_mismatch error during a flaky import would silently drop
        the "run moneybin db migrate" hint.
        """
        import duckdb

        from moneybin.services import inbox_service as mod

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                Path(path).unlink()
                raise duckdb.BinderException("import_id not found")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "x.qfx").write_text("not really qfx\n")

        entry = svc.sync(year_month="2026-05").failed[0]
        assert entry["error_code"] == "schema_mismatch"
        assert "moneybin db migrate" in str(entry["suggestion"])
        assert "sidecar" not in entry  # vanished — no sidecar written


class TestSyncBusy:
    """Concurrent sync returns inbox_busy in result instead of raising."""

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

        assert result.processed == []
        assert result.failed == []
        assert result.skipped == [{"reason": "inbox_busy"}]


class TestRecovery:
    """Crash-recovery: staging-* files in outcome roots revert to inbox/."""

    def test_staging_files_in_processed_revert_to_inbox(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult
        from moneybin.services.refresh import RefreshResult

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
        monkeypatch.setattr(
            "moneybin.services.refresh.refresh",
            lambda db: RefreshResult(applied=True, duration_seconds=0.0),
            raising=True,
        )

        result = svc.sync(year_month="2026-05")

        assert not ghost.exists()
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

    def test_staging_name_round_trip_preserves_subfolder(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        """A subfolder file moved + crashed mid-rename recovers to its subfolder."""
        inbox_service.ensure_layout()
        sub = inbox_service.inbox_dir / "chase-checking"
        sub.mkdir()
        src = sub / "march.csv"
        src.write_text("a,b\n1,2\n")

        # Simulate first-leg rename: src → outcome/staging-<url-encoded-rel-path>
        # `chase-checking/march.csv` URL-encodes to `chase-checking%2Fmarch.csv`.
        staging = inbox_service.processed_dir / "staging-chase-checking%2Fmarch.csv"
        src.rename(staging)
        assert staging.exists()
        assert not src.exists()

        # Crash before second-leg rename — recovery should restore subfolder layout.
        recovered = inbox_service.recover_staging()
        assert recovered == [inbox_service.inbox_dir / "chase-checking" / "march.csv"]
        assert (inbox_service.inbox_dir / "chase-checking" / "march.csv").exists()
        assert not staging.exists()


class TestRecoveryEncoding:
    """Staging-name encoding round-trips and rejects path traversal."""

    def test_double_underscore_filename_round_trips(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        """Filenames with `__` recover to original path (reversible encoding)."""
        inbox_service.ensure_layout()
        src = inbox_service.inbox_dir / "bank__may.csv"
        src.write_text("a,b\n1,2\n")
        # URL-encoded form preserves `__` as-is and ends with the original name.
        staging = inbox_service.processed_dir / "staging-bank__may.csv"
        src.rename(staging)

        recovered = inbox_service.recover_staging()

        assert recovered == [inbox_service.inbox_dir / "bank__may.csv"]
        assert (inbox_service.inbox_dir / "bank__may.csv").exists()
        # Critically, NOT decoded into `bank/may.csv`:
        assert not (inbox_service.inbox_dir / "bank" / "may.csv").exists()

    def test_path_traversal_in_staging_name_is_skipped(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        """Decoded paths that escape inbox_dir are skipped, not moved."""
        inbox_service.ensure_layout()
        # URL-encoded "../../evil.csv":
        evil = inbox_service.processed_dir / "staging-..%2F..%2Fevil.csv"
        evil.write_text("payload\n")

        recovered = inbox_service.recover_staging()

        assert recovered == []
        assert evil.exists()  # Skipped, not moved.
        # And nothing escaped to the parent of inbox_dir.
        assert not (inbox_service.inbox_dir.parent.parent / "evil.csv").exists()

    def test_fully_encoded_path_traversal_is_skipped(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        """Fully-encoded `%2E%2E%2F` traversal is also caught by the guard."""
        inbox_service.ensure_layout()
        # Every byte percent-encoded — decodes to "../../escaped":
        evil = inbox_service.processed_dir / "staging-%2E%2E%2F%2E%2E%2Fescaped"
        evil.write_text("payload\n")

        recovered = inbox_service.recover_staging()

        assert recovered == []
        assert evil.exists()
        assert not (inbox_service.inbox_dir.parent.parent / "escaped").exists()


class TestSyncMoveRace:
    """Successful import but file vanishes before move-to-processed."""

    def test_post_import_move_failure_routed_as_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If src vanishes after import_file() succeeds, batch continues."""
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult
        from moneybin.services.refresh import RefreshResult

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                # Import succeeds but external process removes file before move.
                Path(path).unlink()
                return ImportResult(file_path=path, file_type="tabular", transactions=5)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.services.refresh.refresh",
            lambda db: RefreshResult(applied=True, duration_seconds=0.0),
            raising=True,
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "raced.csv").write_text("a\n1\n")
        (svc.inbox_dir / "ok.csv").write_text("a\n1\n")

        # Force deterministic ordering: enumerate sorts entries.
        result = svc.sync(year_month="2026-05")

        # First file failed, but the batch still drained the second.
        assert len(result.processed) + len(result.failed) == 2
        # The vanished one is recorded as a failure with no sidecar.
        failed_filenames = [f["filename"] for f in result.failed]
        assert "raced.csv" in failed_filenames


class TestSidecarPIIBudget:
    """Exception message in sidecar is capped to limit PII surface area."""

    def test_long_error_message_truncated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import yaml

        from moneybin.services import inbox_service as mod

        long_msg = "X" * 5000

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise RuntimeError(long_msg)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "x.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")

        sidecar_rel = result.failed[0]["sidecar"]
        sidecar = svc.root / str(sidecar_rel)
        loaded = yaml.safe_load(sidecar.read_text())
        assert len(loaded["message"]) <= 200


class TestSyncVanishedSource:
    """sync() handles a file that disappears between enumeration and import."""

    def test_failure_with_vanished_source_does_not_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If src is gone before failure handling runs, record failure cleanly."""
        from moneybin.services import inbox_service as mod

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                # Delete the file then raise — simulates external mv during import.
                Path(path).unlink()
                raise RuntimeError("disk full")

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "ghost.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")

        assert len(result.failed) == 1
        entry = result.failed[0]
        assert entry["filename"] == "ghost.csv"
        assert entry["error_code"] == "import_error"
        assert "sidecar" not in entry  # no sidecar since file vanished
        assert not (svc.failed_dir / "2026-05" / "ghost.csv").exists()
