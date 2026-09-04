"""Tests for InboxService."""

from __future__ import annotations

import stat
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.config import ImportSettings, MoneyBinSettings
from moneybin.database import Database
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services.inbox_service import InboxService


def _make_settings(tmp_path: Path, profile: str = "test") -> MoneyBinSettings:
    return MoneyBinSettings(
        profile=profile,
        import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
    )


def _fake_refresh(_db: Database) -> RefreshResult:
    """Default no-op stand-in for moneybin.orchestration.refresh.refresh.

    Tests monkeypatch this in to keep refresh out of the inbox sync path
    when they only care about per-file move behavior.
    """
    return RefreshResult(applied=True, duration_seconds=0.0)


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
            error_code="schema_mismatch",
            stage="load",
            message="Database schema is out of date",
            suggestion="Run 'moneybin db migrate' and re-run sync",
            extra={"missing_column": "last_four"},
        )

        assert sidecar == failed_dir / "unknown.csv.error.yml"
        loaded = yaml.safe_load(sidecar.read_text())
        assert loaded["error_code"] == "schema_mismatch"
        assert loaded["stage"] == "load"
        assert loaded["message"].startswith("Database schema")
        assert loaded["suggestion"].startswith("Run")
        assert loaded["missing_column"] == "last_four"


class TestSyncHappyPath:
    """sync() happy path: import file, move to processed/."""

    def test_imports_root_file_and_moves_to_processed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

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
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
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

    def test_sync_reports_a_transfer_the_refresh_retired(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The drain runs the matcher unattended, so it can undo a user's decision.

        `sync` ends with one full refresh, whose match step reconciles and can
        reverse a standing transfer. This is the least supervised surface that
        reaches the reconciliation — nobody is watching a watched folder — so
        dropping the count here is the version of this bug that hides longest.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                return ImportResult(file_path=path, file_type="tabular", transactions=1)

        def fake_refresh(db: object) -> RefreshResult:
            return RefreshResult(
                applied=True, duration_seconds=0.01, transfers_retired=3
            )

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "statement.csv").write_text("a\n1\n")

        result = svc.sync(year_month="2026-05")

        assert result.transfers_retired == 3

    def test_processed_entry_carries_the_ratified_sign_replay_note(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The drain is the most unattended surface — a durable override is visible here.

        The watched-folder sync imports without a human present. A saved `--sign`
        override replaying onto a new statement bypasses the credit-card detector,
        so the per-file entry must say so or the decision acts invisibly exactly
        where nobody is watching.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **_kwargs: object) -> ImportResult:
                return ImportResult(
                    file_path=path,
                    file_type="pdf",
                    transactions=2,
                    sign_override_replayed=True,
                )

        def fake_refresh(db: object) -> RefreshResult:
            return RefreshResult(applied=True, duration_seconds=0.01)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "statement.pdf").write_bytes(b"%PDF-1.4 fake")

        result = svc.sync(year_month="2026-05")

        assert result.processed[0]["sign_override_replayed"] is True

    def test_processed_entry_names_the_account_the_drain_minted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Gating the merge but not the mint makes naming the mint the drain's job.

        A first-contact account is created without a confirm, so the per-file
        entry is the only place an unattended drain can say what it made. The
        CLI and MCP tests that reference this key mock ``InboxService.sync``
        wholesale and fabricate its return value, so ``_sync_one``'s own
        population logic only ever runs here.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import CreatedAccount, ImportResult

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **_kwargs: object) -> ImportResult:
                return ImportResult(
                    file_path=path,
                    file_type="ofx",
                    transactions=3,
                    accounts_created=(
                        CreatedAccount(
                            account_id="acct_new01", display_name="Chase Checking"
                        ),
                    ),
                )

        def fake_refresh(db: object) -> RefreshResult:
            return RefreshResult(applied=True, duration_seconds=0.01)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "statement.ofx").write_text("OFXHEADER:100")

        result = svc.sync(year_month="2026-05")

        # Projected to plain dicts, not CreatedAccount objects: the entry is
        # serialized straight into the CLI/MCP envelope.
        assert result.processed[0]["accounts_created"] == [
            {"account_id": "acct_new01", "display_name": "Chase Checking"}
        ]

    def test_processed_entry_omits_accounts_created_when_nothing_was_minted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The common row — a file joining an account already known — keeps its shape.

        The key is conditional, so absence needs proving too: an unconditional
        ``"accounts_created": []`` would read as "this drain created nothing"
        rather than "this drain had nothing to say", and would change the shape
        of every row that has ever drained.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **_kwargs: object) -> ImportResult:
                # Adopted an account that already existed: nothing minted.
                return ImportResult(file_path=path, file_type="ofx", transactions=3)

        def fake_refresh(db: object) -> RefreshResult:
            return RefreshResult(applied=True, duration_seconds=0.01)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "statement.ofx").write_text("OFXHEADER:100")

        result = svc.sync(year_month="2026-05")

        assert "accounts_created" not in result.processed[0]
        # Still a normal processed row, so the absence is not a failed import.
        assert result.processed[0]["transactions"] == 3

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
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh",
            _fake_refresh,
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

    def test_an_unreadable_subfolder_file_fails_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One unreadable file must not take the rest of the drain with it.

        Deciding whether to forward the folder hint means asking
        ``honors_account_name``, which routes through ``_detect_file_type`` —
        and its OFX sniff deliberately re-raises ``PermissionError`` rather than
        guessing from a suffix it could not verify. Asked *before* the per-file
        guard, that read error escapes ``_sync_one`` and aborts the whole drain,
        so a single locked file in one account folder strands every other file
        behind it. The batch is the thing the inbox exists to drain unattended.
        """
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **_kwargs: object) -> ImportResult:
                return ImportResult(file_path=path, file_type="ofx", transactions=1)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh", _fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        sub = svc.inbox_dir / "chase-checking"
        sub.mkdir()
        (sub / "a-unreadable.ofx").write_text("OFXHEADER:100")
        (sub / "b-fine.ofx").write_text("OFXHEADER:100")

        real_detect = mod.honors_account_name

        def _detect(path: Path) -> bool:
            if path.name == "a-unreadable.ofx":
                raise PermissionError(13, "Permission denied")
            return real_detect(path)

        monkeypatch.setattr(mod, "honors_account_name", _detect)

        result = svc.sync(year_month="2026-05")

        assert [f["filename"] for f in result.failed] == [
            "chase-checking/a-unreadable.ofx"
        ]
        assert [p["filename"] for p in result.processed] == [
            "chase-checking/b-fine.ofx"
        ]

    @pytest.mark.parametrize("channel_file", ["statement.ofx", "statement.pdf"])
    def test_subfolder_hint_does_not_fail_a_channel_that_cannot_use_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, channel_file: str
    ) -> None:
        """The hint is an inbox convention, not a caller signal, on every channel.

        ``_sync_one`` forwards the ``inbox/<account-slug>/`` folder name as
        ``account_name`` for whatever it finds there, but ``account_name`` is
        honored only by tabular — ``_HONORED_ACCOUNT_SIGNALS`` gives ``ofx`` an
        empty set and ``pdf`` only ``account_id``. So the refusal that exists to
        stop a caller from *believing they chose* an account fires on a folder
        name the user never passed as a signal, and the drain files a valid
        statement under ``failed/``.

        The sidecar makes it worse by recommending exactly this: "move the file
        into inbox/<account-slug>/ and re-run sync". That recovery has to work.

        Driven against the real refusal rather than a restated copy, so the two
        cannot drift: the fake importer calls the same function ``import_file``
        calls, in the same position — right after the file type is known.
        """
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import (
            ImportResult,
            reject_unhonored_account_signals,
        )

        class GuardedImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                file_type = "pdf" if path.endswith(".pdf") else "ofx"
                reject_unhonored_account_signals(
                    file_type,
                    account_id=kwargs.get("account_id"),  # type: ignore[arg-type]
                    account_name=kwargs.get("account_name"),  # type: ignore[arg-type]
                )
                return ImportResult(file_path=path, file_type=file_type)

        monkeypatch.setattr(mod, "ImportService", GuardedImportService)
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh", _fake_refresh, raising=True
        )

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        sub = svc.inbox_dir / "chase-checking"
        sub.mkdir()
        # Per-channel content, because _detect_file_type sniffs magic bytes
        # before trusting an ambiguous suffix: OFX text in a .pdf would be
        # routed to the ofx channel and the pdf case would never run.
        sub.joinpath(channel_file).write_text(
            "%PDF-1.4\n" if channel_file.endswith(".pdf") else "OFXHEADER:100"
        )

        result = svc.sync(year_month="2026-05")

        assert result.failed == [], result.failed
        assert len(result.processed) == 1


class TestSyncRefreshOnce:
    """Regression: refresh runs exactly once per sync() call, not per file."""

    def test_multi_file_batch_runs_refresh_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two files should trigger exactly one refresh() call."""
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

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
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
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
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod

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
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
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
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_service import ImportResult

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
            "moneybin.orchestration.refresh.refresh", fake_refresh, raising=True
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
    """Failed imports move to failed/; a bare no-name file routes to pending/."""

    def test_single_account_no_name_lands_in_pending(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import yaml

        from moneybin.extractors.confidence import Confidence
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_confirmation import (
            ConfirmationRequired,
            ImportConfirmationRequiredError,
            ProposedMapping,
        )

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise ImportConfirmationRequiredError(
                    ConfirmationRequired(
                        channel="tabular",
                        confidence=Confidence(
                            score=1.0, tier="high", flagged=(), missing_required=()
                        ),
                        proposed=ProposedMapping(
                            field_mapping={"Date": "transaction_date"},
                            sample_values={},
                            unmapped_columns=(),
                        ),
                        reason="account_confirmation",
                        account_proposals=[
                            {
                                "source_account_key": "unknown",
                                "proposal_ref": "@0",
                                "proposed_account_id": None,
                                "is_new": True,
                                "adopted_via": None,
                                "requires_confirm": True,
                                "candidates": [],
                            }
                        ],
                    )
                )

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "unknown.csv").write_text("Date\n2026-05-01\n")

        result = svc.sync(year_month="2026-05")

        assert len(result.failed) == 0
        assert len(result.pending) == 1
        entry = result.pending[0]
        assert entry["filename"] == "unknown.csv"
        assert entry["reason"] == "account_confirmation"

        moved = svc.pending_dir / "2026-05" / "unknown.csv"
        sidecar = moved.with_name("unknown.csv.pending.yml")
        assert moved.exists()
        loaded = yaml.safe_load(sidecar.read_text())
        assert loaded["reason"] == "account_confirmation"
        assert any("--account-binding" in a for a in loaded["actions"])

    def test_pending_entry_carries_account_proposals_in_envelope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """inbox_sync's pending entry carries account_proposals (with candidates).

        The candidate pick-list must ride in the response envelope, not only the
        on-disk .pending.yml sidecar: a REST/MCP client can't read the sidecar,
        and a CLI/JSON consumer shouldn't have to.
        """
        from typing import Any

        from moneybin.extractors.confidence import Confidence
        from moneybin.services import inbox_service as mod
        from moneybin.services.account_resolution_types import AccountProposalDict
        from moneybin.services.import_confirmation import (
            ConfirmationRequired,
            ImportConfirmationRequiredError,
            ProposedMapping,
        )

        proposal: AccountProposalDict = {
            "source_account_key": "unknown",
            "proposal_ref": "@0",
            "proposed_account_id": "prov123",
            "is_new": True,
            "adopted_via": None,
            "requires_confirm": True,
            "candidates": [
                {
                    "account_id": "acct_a",
                    "display_name": "WF CHECKING …1212",
                    "signal": "fallback",
                }
            ],
        }

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise ImportConfirmationRequiredError(
                    ConfirmationRequired(
                        channel="tabular",
                        confidence=Confidence(
                            score=1.0, tier="high", flagged=(), missing_required=()
                        ),
                        proposed=ProposedMapping(
                            field_mapping={"Date": "transaction_date"},
                            sample_values={},
                            unmapped_columns=(),
                        ),
                        reason="account_confirmation",
                        account_proposals=[proposal],
                    )
                )

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "unknown.csv").write_text("Date\n2026-05-01\n")

        result = svc.sync(year_month="2026-05")

        assert len(result.pending) == 1
        entry = result.pending[0]
        proposals: Any = entry["account_proposals"]
        assert len(proposals) == 1
        candidates: Any = proposals[0]["candidates"]
        assert [c["account_id"] for c in candidates] == ["acct_a"]
        assert candidates[0]["signal"] == "fallback"

    def test_card_statement_pending_sidecar_names_sign_recovery(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A first-contact card statement produces a sign-honest sidecar.

        "magic stays visible": a whole-ledger sign inversion the user can't see
        the evidence for must never be applied. The sidecar must name the
        --confirm / --sign recovery and carry the matched disclosures +
        printed-vs-recorded rows — NOT the tabular --accept / --mapping hint,
        which would silently invert every deposit (--accept) or dead-end loop on
        a PDF (--mapping).
        """
        import yaml

        from moneybin.extractors.confidence import Confidence
        from moneybin.services import inbox_service as mod
        from moneybin.services.import_confirmation import (
            ConfirmationRequired,
            ImportConfirmationRequiredError,
            SignConventionProposal,
        )

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> object:
                raise ImportConfirmationRequiredError(
                    ConfirmationRequired(
                        channel="pdf",
                        confidence=Confidence(
                            score=1.0, tier="high", flagged=(), missing_required=()
                        ),
                        proposed=SignConventionProposal(
                            sign_convention="negative_is_income",
                            evidence=("Minimum Payment Due", "New Balance"),
                            sample_rows=[
                                {
                                    "description": "COFFEE SHOP",
                                    "as_printed": "12.50",
                                    "as_recorded": "-12.50",
                                }
                            ],
                        ),
                        reason="sign_convention",
                        error_message=(
                            "This looks like a credit-card statement "
                            "(matched: Minimum Payment Due, New Balance)."
                        ),
                    )
                )

        monkeypatch.setattr(mod, "ImportService", FakeImportService)

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        (svc.inbox_dir / "statement.pdf").write_bytes(b"%PDF-1.4 fake\n")

        result = svc.sync(year_month="2026-05")

        assert len(result.failed) == 0
        assert len(result.pending) == 1
        assert result.pending[0]["reason"] == "sign_convention"

        moved = svc.pending_dir / "2026-05" / "statement.pdf"
        sidecar = moved.with_name("statement.pdf.pending.yml")
        assert moved.exists()
        loaded = yaml.safe_load(sidecar.read_text())

        assert loaded["reason"] == "sign_convention"
        actions = loaded["actions"]
        # The two honest recoveries — and nothing that mislabels the flip.
        assert any("--confirm" in a for a in actions)
        assert any("--sign negative_is_expense" in a for a in actions)
        assert not any("--accept" in a for a in actions)
        assert not any("--mapping" in a for a in actions)
        # Evidence + printed-vs-recorded rows must ride in the sidecar, not be
        # dropped (they are empty on a ProposedMapping-shaped payload).
        assert loaded["sign_evidence"] == ["Minimum Payment Due", "New Balance"]
        assert loaded["sign_sample_rows"][0]["as_printed"] == "12.50"
        assert loaded["sign_sample_rows"][0]["as_recorded"] == "-12.50"
        assert "credit-card statement" in loaded["error_message"]
        # None of the tabular mapping fields leak in as misleading empties.
        assert "proposed_mapping" not in loaded

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

    def test_permission_suggestion_differs_for_tcc_and_mode_denial(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Chmod advice is right for EACCES and useless for a macOS TCC denial.

        A macOS privacy (TCC) denial arrives as EPERM on a protected root, and
        no amount of chmod/chown clears it — so the sidecar must not hand both
        denials the same suggestion.

        Driven through ``sync()`` rather than by calling ``_suggestion_for``
        directly: that is how every sibling classification test here works, and
        it is the only shape that also proves ``_handle_failure`` forwards the
        exception. A direct call would still pass if the call site never did.
        """
        from moneybin.services import inbox_service as mod

        def _sync_with(error: PermissionError, profile: str) -> dict[str, object]:
            class FakeImportService:
                def __init__(self, db: object) -> None:
                    pass

                def import_file(self, path: str, **kwargs: object) -> object:
                    raise error

            monkeypatch.setattr(mod, "ImportService", FakeImportService)
            db = MagicMock(spec=Database)
            # Separate profiles so the two syncs get separate inbox trees.
            svc = InboxService(db=db, settings=_make_settings(tmp_path, profile))
            svc.ensure_layout()
            (svc.inbox_dir / "x.csv").write_text("a\n1\n")
            return svc.sync(year_month="2026-05").failed[0]

        mode_entry = _sync_with(
            PermissionError(13, "Permission denied", str(tmp_path / "a.csv")), "mode"
        )
        assert mode_entry["error_code"] == "permission_error"
        assert "chmod" in str(mode_entry["suggestion"])

        tcc_error = PermissionError(
            1, "Operation not permitted", str(Path.home() / "Documents" / "a.csv")
        )
        # Same patch target as the sibling tests in test_error_classification:
        # `moneybin.errors.platform` IS the stdlib module object, so setting
        # `system` on it applies wherever `platform.system` is resolved —
        # including InboxService. Pinning Darwin keeps the macOS branch
        # reachable on any CI host, since `permission_advice` takes the
        # platform from its caller rather than detecting it.
        with patch("moneybin.errors.platform.system", return_value="Darwin"):
            tcc_entry = _sync_with(tcc_error, "tcc")
        assert tcc_entry["error_code"] == "permission_error"
        assert "Full Disk Access" in str(tcc_entry["suggestion"])
        assert "chmod" not in str(tcc_entry["suggestion"])


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
            "moneybin.orchestration.refresh.refresh",
            _fake_refresh,
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

        class FakeImportService:
            def __init__(self, db: object) -> None:
                pass

            def import_file(self, path: str, **kwargs: object) -> ImportResult:
                # Import succeeds but external process removes file before move.
                Path(path).unlink()
                return ImportResult(file_path=path, file_type="tabular", transactions=5)

        monkeypatch.setattr(mod, "ImportService", FakeImportService)
        monkeypatch.setattr(
            "moneybin.orchestration.refresh.refresh",
            _fake_refresh,
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


class TestPendingSidecarAccountHint:
    """Pending sidecar actions thread the inbox subfolder account hint.

    When a file arrives via inbox/<account>/, the generated
    `moneybin import confirm` actions in the pending sidecar MUST include
    `--account-name <hint>`. Without the subfolder hint a bare single-account
    CSV elicits an ``account_confirmation``; the hint supplies the account
    identity directly so the suggested command resolves in one step.
    """

    def test_actions_include_account_name_when_subfolder_hint_present(
        self, tmp_path: Path
    ) -> None:
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / "statement.csv"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("Date,Amount\n2026-05-01,-10\n")

        sidecar = svc.write_pending_sidecar(
            _Path(moved),
            channel="tabular",
            tier="medium",
            score=0.72,
            reason="unknown_layout",
            proposed_mapping={"Date": "transaction_date", "Amount": "amount"},
            samples={},
            flagged=[],
            missing_required=[],
            unmapped_columns=[],
            account_hint="chase-checking",
        )

        import yaml

        payload = yaml.safe_load(sidecar.read_text())
        actions = payload["actions"]
        assert all("--account-name chase-checking" in a for a in actions), actions

    def test_every_sidecar_command_survives_a_path_with_spaces(
        self, tmp_path: Path
    ) -> None:
        """A pending file under "Bank Exports/" must still yield runnable commands.

        Quoting was fixed once in the shared recovery helper and then a new
        unquoted command was added beside it, so assert across *every* emitted
        `moneybin` command rather than the one line last reported.
        """
        import shlex
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / "jan stmt.csv"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("Date,Amount\n2026-05-01,-10\n")

        # header_row_consumed offers no command on purpose — nothing MoneyBin
        # runs un-consumes a header row — so it asserts the opposite.
        for reason, expects_command in (
            ("unknown_layout", True),
            ("unreadable_date", True),
            ("account_confirmation", True),
            ("header_row_consumed", False),
        ):
            sidecar = svc.write_pending_sidecar(
                _Path(moved),
                channel="tabular",
                tier="medium",
                score=0.75,
                reason=reason,
                proposed_mapping={"transaction_date": "Date", "amount": "Amount"},
                samples={},
                flagged=[],
                missing_required=[],
                unmapped_columns=[],
            )

            import yaml

            raw = str(moved)
            quoted = shlex.quote(raw)
            checked = 0
            for action in yaml.safe_load(sidecar.read_text())["actions"]:
                if "moneybin " not in action or raw not in action:
                    continue
                checked += 1
                assert quoted in action, f"{reason}: unquoted path in {action!r}"
            if expects_command:
                assert checked, f"{reason} emitted no command naming the file"
            else:
                assert not checked, f"{reason} must offer no command to run"

    def test_unreadable_date_sidecar_names_both_recoveries(
        self, tmp_path: Path
    ) -> None:
        """The sidecar is a fourth surface carrying the same recovery text.

        Its generic branch offers `--accept` (which re-hits the gate) and a
        bare `--mapping` with no hint that the date column is the one to
        correct. The unreadable-date branch must name the column correction
        *and* `--date-format`, since either can be the real fix depending on
        whether the right column was mapped.
        """
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / "compact.csv"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("Date,Amount\n20260501,-10\n")

        sidecar = svc.write_pending_sidecar(
            _Path(moved),
            channel="tabular",
            tier="medium",
            score=0.75,
            reason="unreadable_date",
            proposed_mapping={"transaction_date": "Date", "amount": "Amount"},
            samples={},
            flagged=[],
            missing_required=[],
            unmapped_columns=[],
        )

        import yaml

        actions = yaml.safe_load(sidecar.read_text())["actions"]
        recovery = [a for a in actions if "--date-format" in a]
        assert recovery, actions
        assert any("--mapping transaction_date=" in a for a in recovery), recovery
        # --accept is the one option that helps in neither case.
        assert not any("--accept" in a for a in actions), actions

    def test_actions_omit_account_name_when_no_hint(self, tmp_path: Path) -> None:
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / "statement.csv"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("Date,Amount\n2026-05-01,-10\n")

        sidecar = svc.write_pending_sidecar(
            _Path(moved),
            channel="tabular",
            tier="medium",
            score=0.72,
            reason="unknown_layout",
            proposed_mapping={"Date": "transaction_date", "Amount": "amount"},
            samples={},
            flagged=[],
            missing_required=[],
            unmapped_columns=[],
        )

        import yaml

        payload = yaml.safe_load(sidecar.read_text())
        actions = payload["actions"]
        assert all("--account-name" not in a for a in actions), actions

    def test_account_confirmation_sidecar_emits_binding_actions(
        self, tmp_path: Path
    ) -> None:
        """An account_confirmation pending sidecar emits account-binding hints.

        Offers --accept paired with --account-binding (the real source key) +
        the inbox/<account-slug>/ convention; no standalone --mapping override.
        """
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / "statement.csv"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("Date,Amount\n2026-05-01,-10\n")

        sidecar = svc.write_pending_sidecar(
            _Path(moved),
            channel="tabular",
            tier="high",
            score=1.0,
            reason="account_confirmation",
            proposed_mapping={"Date": "transaction_date", "Amount": "amount"},
            samples={},
            flagged=[],
            missing_required=[],
            unmapped_columns=[],
            account_proposals=[
                {
                    "source_account_key": "statement",
                    "proposal_ref": "@0",
                    "proposed_account_id": None,
                    "is_new": True,
                    "adopted_via": None,
                    "requires_confirm": True,
                    "candidates": [],
                }
            ],
        )

        import yaml

        payload = yaml.safe_load(sidecar.read_text())
        actions = payload["actions"]
        assert any("--account-binding @0=" in a for a in actions), actions
        assert any("inbox/<account-slug>" in a for a in actions), actions
        # --accept ratifies the settled mapping and pairs with the binding; no
        # standalone --mapping override for an account_confirmation.
        assert not any("--mapping" in a for a in actions), actions
        assert all("--accept" in a for a in actions if "--account-binding" in a), (
            actions
        )
        # Persisted masked, by the same declaration every other surface applies
        # — the field is ACCOUNT_IDENTIFIER whatever the channel, and this file
        # outlives the session. The ref above is what makes it answerable.
        assert payload["account_proposals"][0]["source_account_key"] == "****ment"

    @pytest.mark.parametrize("channel", ["ofx", "pdf"])
    def test_account_confirmation_sidecar_omits_subfolder_recovery_off_tabular(
        self, tmp_path: Path, channel: str
    ) -> None:
        """Only tabular can be answered by a folder name, so only it is offered one.

        The inbox forwards ``inbox/<account-slug>/`` as ``account_name``, which
        is tabular-only, so on OFX and PDF ``_sync_one`` drops the hint before
        importing. Advertising that move as a recovery for an account gate on
        those channels sends the file back to the identical gate — worse than
        offering nothing, because it reads as a fix and costs a full drain cycle
        to disprove.

        The binding recovery, which does work on every channel, stays.
        """
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / f"statement.{channel}"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("x\n")

        sidecar = svc.write_pending_sidecar(
            _Path(moved),
            channel=channel,
            tier="high",
            score=1.0,
            reason="account_confirmation",
            proposed_mapping={},
            samples={},
            flagged=[],
            missing_required=[],
            unmapped_columns=[],
            account_proposals=[
                {
                    "source_account_key": "000123456789",
                    "proposal_ref": "@0",
                    "proposed_account_id": None,
                    "is_new": True,
                    "adopted_via": None,
                    "requires_confirm": True,
                    "candidates": [],
                }
            ],
        )

        import yaml

        payload = yaml.safe_load(sidecar.read_text())
        actions = payload["actions"]
        assert all("<account-slug>" not in a for a in actions), actions
        # Same reason, same table: `account_name` is tabular-only, so neither
        # the folder move nor the flag that names an account directly can be
        # offered here. A PDF statement is single-account by construction, so
        # the `--account-name` suggestion would otherwise fire on essentially
        # every PDF account gate and fail with IMPORT_ACCOUNT_SIGNAL_UNSUPPORTED
        # when pasted.
        assert all("--account-name" not in a for a in actions), actions
        assert any("--account-binding" in a for a in actions), actions

    def test_account_confirmation_sidecar_never_writes_the_account_number(
        self, tmp_path: Path
    ) -> None:
        """The sidecar outlives the session, so it masks like every other surface.

        ``.claude/rules/security.md`` names two boundaries that matter, and the
        second is "any artifact that outlives the session" — a ``.pending.yml``
        on disk is exactly that. Every other surface carrying these proposals
        masks ``source_account_key`` (the ``<ACCTID>`` on OFX): the terminal via
        ``_echo_account_proposals``, the CLI/MCP envelopes via
        ``ImportInboxPendingEntry``. This file was the one that did not, and it
        is the one that persists.

        The generated ``--account-binding`` hint is the second half: keying it
        by the raw source key put the same number back through the other door,
        and every other surface now keys that command by ``proposal_ref``.
        """
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / "statement.ofx"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("x\n")
        acctid = "000123456789"

        sidecar = svc.write_pending_sidecar(
            _Path(moved),
            channel="ofx",
            tier="high",
            score=1.0,
            reason="account_confirmation",
            proposed_mapping={},
            samples={},
            flagged=[],
            missing_required=[],
            unmapped_columns=[],
            account_proposals=[
                {
                    "source_account_key": acctid,
                    "proposal_ref": "@0",
                    "proposed_account_id": "prov12345678",
                    "is_new": True,
                    "adopted_via": None,
                    "requires_confirm": True,
                    "candidates": [],
                }
            ],
        )

        import yaml

        raw = sidecar.read_text()
        assert acctid not in raw, raw
        payload = yaml.safe_load(raw)
        assert payload["account_proposals"][0]["source_account_key"] == "****6789"
        # The ref stays readable — it is what the printed command binds by, so
        # masking it would leave the file unanswerable.
        assert payload["account_proposals"][0]["proposal_ref"] == "@0"
        assert any("--account-binding @0=" in a for a in payload["actions"]), payload[
            "actions"
        ]

    def test_account_confirmation_multi_proposal_one_command_all_bindings(
        self, tmp_path: Path
    ) -> None:
        """Multiple proposals → ONE import-confirm command listing every binding.

        The account gate is all-or-nothing: supplying only some keys re-prompts
        and persists nothing, so per-key commands could never complete. The
        single-account --account-name shortcut is suppressed when >1 account.
        """
        from pathlib import Path as _Path

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        moved = svc.pending_dir / "2026-05" / "combined.csv"
        moved.parent.mkdir(parents=True, exist_ok=True)
        moved.write_text("Date,Amount,Account\n2026-05-01,-10,A\n")

        sidecar = svc.write_pending_sidecar(
            _Path(moved),
            channel="tabular",
            tier="high",
            score=1.0,
            reason="account_confirmation",
            proposed_mapping={"Date": "transaction_date", "Amount": "amount"},
            samples={},
            flagged=[],
            missing_required=[],
            unmapped_columns=[],
            account_proposals=[
                {
                    "source_account_key": "acct-a",
                    "proposal_ref": "@0",
                    "proposed_account_id": None,
                    "is_new": True,
                    "adopted_via": None,
                    "requires_confirm": True,
                    "candidates": [],
                },
                {
                    "source_account_key": "acct-b",
                    "proposal_ref": "@1",
                    "proposed_account_id": None,
                    "is_new": True,
                    "adopted_via": None,
                    "requires_confirm": True,
                    "candidates": [],
                },
            ],
        )

        import yaml

        actions = yaml.safe_load(sidecar.read_text())["actions"]
        confirm_cmds = [
            a for a in actions if "import confirm" in a and "--account-binding" in a
        ]
        assert len(confirm_cmds) == 1, actions  # exactly one command...
        assert "--account-binding @0=" in confirm_cmds[0]  # ...with both refs
        assert "--account-binding @1=" in confirm_cmds[0]
        # Keyed by ref, never by the source key: this command is persisted to a
        # sidecar, and on OFX that key is the institution's account number.
        assert "acct-a" not in confirm_cmds[0]
        assert "acct-b" not in confirm_cmds[0]
        assert "--accept" in confirm_cmds[0]
        # No single-account --account-name shortcut when there are >1 accounts.
        assert all("--account-name" not in a for a in actions), actions

    def test_account_confirmation_collision_rekeys_to_moved_path(
        self, tmp_path: Path
    ) -> None:
        """A collision-moved bare pending file is keyed to the moved path.

        `import confirm <moved>` then matches the binding. _handle_pending moves
        statement.csv -> statement-1.csv (collision suffix) after the proposal
        was built from the original name; the bare content key must be repointed
        or the generated --account-binding command fails.
        """
        from moneybin.extractors.confidence import Confidence
        from moneybin.services.account_resolution_types import AccountProposal
        from moneybin.services.import_confirmation import (
            ConfirmationRequired,
            ImportConfirmationRequiredError,
            ProposedMapping,
        )
        from moneybin.services.import_service import (
            _bare_account_key,  # pyright: ignore[reportPrivateUsage]  # tested directly
        )
        from moneybin.services.inbox_service import InboxSyncResult

        db = MagicMock(spec=Database)
        svc = InboxService(db=db, settings=_make_settings(tmp_path))
        svc.ensure_layout()
        src = svc.inbox_dir / "statement.csv"
        src.write_text("Date,Amount\n2026-05-01,-10\n")
        # Pre-existing pending file with the same name forces a -1 suffix.
        collision_dir = svc.pending_dir / "2026-05"
        collision_dir.mkdir(parents=True, exist_ok=True)
        (collision_dir / "statement.csv").write_text("earlier\n")

        orig_key = _bare_account_key(src)
        error = ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="tabular",
                confidence=Confidence(
                    score=1.0, tier="high", flagged=(), missing_required=()
                ),
                proposed=ProposedMapping(
                    field_mapping={"Date": "transaction_date", "Amount": "amount"},
                    sample_values={},
                    unmapped_columns=(),
                ),
                reason="account_confirmation",
                account_proposals=[
                    AccountProposal(
                        source_account_key=orig_key,
                        proposed_account_id=None,
                        is_new=True,
                        candidates=(),
                        adopted_via=None,
                    ).to_dict(proposal_ref="@0")
                ],
            )
        )
        result = InboxSyncResult()
        svc._handle_pending(  # pyright: ignore[reportPrivateUsage]  # exercising the move+rekey path
            src, "statement.csv", error, "2026-05", result
        )

        moved = collision_dir / "statement-1.csv"
        assert moved.exists()  # collision suffix applied
        moved_key = _bare_account_key(moved)
        assert moved_key != orig_key  # stem changed → key changed

        import yaml

        sidecar = moved.with_name(moved.name + ".pending.yml")
        payload = yaml.safe_load(sidecar.read_text())
        # Asserted on the service result, not the sidecar: the sidecar now
        # persists this key masked (it outlives the session), so it can no
        # longer witness *which* key was stored. The in-process entry still
        # carries the raw value, which is where the rekey is observable — and
        # the rekey is what this test is about. The original-name key would not
        # match a re-import of the moved path.
        stored = result.pending[0]["account_proposals"]
        assert isinstance(stored, list)
        assert stored[0]["source_account_key"] == moved_key
        assert stored[0]["source_account_key"] != orig_key
        # The command binds by ref, so a moved path cannot desynchronize it.
        assert any("--account-binding @0=" in a for a in payload["actions"]), payload[
            "actions"
        ]
        assert orig_key not in yaml.safe_dump(payload)


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


class TestArchiveConfirmedFile:
    """archive_confirmed_file() archives a confirmed pending file to processed/."""

    def test_moves_pending_file_to_processed_and_removes_sidecar(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        pending_month = inbox_service.pending_dir / "2026-06"
        pending_month.mkdir(parents=True)
        src = pending_month / "BankExport-Checking.csv"
        src.write_text("a,b\n1,2\n")
        sidecar = pending_month / "BankExport-Checking.csv.pending.yml"
        sidecar.write_text("reason: account_confirmation\n")

        final = inbox_service.archive_confirmed_file(src)

        assert final is not None
        assert (
            final == inbox_service.processed_dir / "2026-06" / "BankExport-Checking.csv"
        )
        assert final.exists()
        assert not src.exists()
        assert not sidecar.exists()

    def test_noops_for_path_outside_pending_bucket(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        # A file passed to import_files directly never entered the inbox buckets;
        # confirming it must not move or delete anything.
        outside = tmp_path / "elsewhere" / "foo.csv"
        outside.parent.mkdir(parents=True)
        outside.write_text("a,b\n1,2\n")

        result = inbox_service.archive_confirmed_file(outside)

        assert result is None
        assert outside.exists()

    def test_noops_when_file_already_gone(
        self, tmp_path: Path, inbox_service: InboxService
    ) -> None:
        inbox_service.ensure_layout()
        pending_month = inbox_service.pending_dir / "2026-06"
        pending_month.mkdir(parents=True)
        missing = pending_month / "vanished.csv"

        result = inbox_service.archive_confirmed_file(missing)

        assert result is None

    def test_sidecar_cleanup_failure_does_not_fail_archive(
        self,
        tmp_path: Path,
        inbox_service: InboxService,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A locked/transient sidecar must not turn a committed import to failure.

        By the time ``archive_confirmed_file`` runs, the import has committed and
        the file has moved to ``processed/``. A failure deleting the
        ``.pending.yml`` sidecar is best-effort cleanup — it must be logged, not
        raised, or the caller reports ``import confirm`` as failed with no data
        file left at the pending path to retry.
        """
        inbox_service.ensure_layout()
        pending_month = inbox_service.pending_dir / "2026-06"
        pending_month.mkdir(parents=True)
        src = pending_month / "stmt.csv"
        src.write_text("a,b\n1,2\n")
        sidecar = pending_month / "stmt.csv.pending.yml"
        sidecar.write_text("reason: account_confirmation\n")

        real_unlink = Path.unlink

        def _unlink(self_path: Path, missing_ok: bool = False) -> None:
            if self_path.name.endswith(".pending.yml"):
                raise OSError("sidecar locked")
            real_unlink(self_path, missing_ok=missing_ok)

        monkeypatch.setattr(Path, "unlink", _unlink)
        final = inbox_service.archive_confirmed_file(src)

        assert final is not None
        assert final.exists()  # file archived despite the sidecar failure
        assert not src.exists()

    def test_crash_mid_archive_does_not_resurrect_file_into_inbox(
        self,
        tmp_path: Path,
        inbox_service: InboxService,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A crash before final placement must not push the file into inbox/.

        ``archive_confirmed_file`` moves a file under ``pending/`` — not under
        ``inbox/`` — so it must not use the inbox-relative staging format that
        ``sync`` uses for crash recovery. If it did, a crash between the
        staging rename and the final rename would leave a
        ``processed/staging-<name>`` that ``recover_staging()`` decodes as an
        inbox file and moves into ``inbox/``, re-importing an already-committed
        file. A direct move leaves a failed archive in ``pending/`` and
        produces no inbox-decodable staging artifact.
        """
        inbox_service.ensure_layout()
        pending_month = inbox_service.pending_dir / "2026-06"
        pending_month.mkdir(parents=True)
        src = pending_month / "stmt.csv"
        src.write_text("a,b\n1,2\n")

        # Crash just before the final placement under processed/YYYY-MM/, after
        # any intermediate rename. Old staging-based code leaves a recoverable
        # staging-* file; the direct move leaves the file in pending/.
        real_rename = Path.rename

        def _crash_on_final(self_path: Path, target: Path) -> Path:
            if Path(target).parent.name == "2026-06":
                raise OSError("crash before final placement")
            return real_rename(self_path, target)

        monkeypatch.setattr(Path, "rename", _crash_on_final)
        result = inbox_service.archive_confirmed_file(src)
        monkeypatch.undo()

        assert result is None
        assert src.exists()  # stayed in pending/, not lost
        recovered = inbox_service.recover_staging()
        assert recovered == []
        assert not list(inbox_service.inbox_dir.rglob("*"))
