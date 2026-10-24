"""Tests for the db migrate CLI commands."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.migrate import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.migrations import Migration

runner = CliRunner()


def _pager_policy(*, no_pager: bool = False) -> TerminalPolicy:
    """Make a synthetic terminal small enough that complete reads page.

    Height 1 alone already forces pagination (`emit_human_result` pages on
    height, never width), so width is a realistic terminal size (100, the
    width `cli-output-coherence.md`'s wrapping requirements are measured
    against) rather than an artificially narrow one: at 20 columns, a label
    as long as `Applied migrations:` alone consumes the whole line and
    `build_summary`'s label/value grid wraps both columns hard enough that
    these tests' `_compact()` substring checks can no longer reconstruct the
    original text, which was never what this fixture's narrowness was for.
    """
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=not no_pager,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=100,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )


def _compact(text: str) -> str:
    """Join renderer wrapping so assertions check content rather than width."""
    return text.replace("\n", "")


@pytest.fixture(autouse=True)
def sqlmesh_state() -> object:
    """Default every CLI migrate test to 'no SQLMesh drift, no migration needed'.

    Both helpers read a real database; against these tests' mock ``db`` they
    would otherwise misfire. Tests exercising the drift/repair path override
    ``.drift`` (status/dry-run message) and/or ``.assessment`` (the apply gate,
    a ``(drift_message, needs_migration)`` tuple).
    """
    from types import SimpleNamespace

    with (
        patch(
            "moneybin.cli.commands.migrate.sqlmesh_state_drift", return_value=None
        ) as drift,
        patch(
            "moneybin.cli.commands.migrate.sqlmesh_state_assessment",
            return_value=(None, False),
        ) as assessment,
    ):
        yield SimpleNamespace(drift=drift, assessment=assessment)


def _migration(
    version: int = 1,
    name: str = "test",
    filename: str = "V001__test.sql",
    checksum: str = "abc123",
) -> Migration:
    """Build a Migration with sensible defaults for CLI tests."""
    return Migration(
        version=version,
        name=name,
        filename=filename,
        checksum=checksum,
        content=b"SELECT 1;",
        path=Path(f"/tmp/{filename}"),  # noqa: S108  # temp path in test only
        file_type="sql",
    )


class TestMigrateApply:
    """moneybin db migrate apply command."""

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_runs_pending(
        self, mock_runner_cls: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Apply command runs pending migrations and exits 0."""
        from moneybin.migrations import MigrationResult

        mock_runner = mock_runner_cls.return_value
        mock_runner.apply_all.return_value = MigrationResult(applied_count=2)
        mock_runner.check_drift.return_value = []

        result = runner.invoke(app, ["apply"])
        assert result.exit_code == 0
        mock_runner.apply_all.assert_called_once()

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_no_pending_exits_0(
        self, mock_runner_cls: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Apply with no pending migrations exits 0."""
        from moneybin.migrations import MigrationResult

        mock_runner = mock_runner_cls.return_value
        mock_runner.apply_all.return_value = MigrationResult(applied_count=0)
        mock_runner.check_drift.return_value = []

        result = runner.invoke(app, ["apply"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_dry_run(
        self, mock_runner_cls: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Dry run lists pending migrations without executing."""
        mock_runner = mock_runner_cls.return_value
        mock_runner.pending.return_value = [_migration()]

        result = runner.invoke(app, ["apply", "--dry-run"])
        assert result.exit_code == 0
        mock_runner.apply_all.assert_not_called()

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_dry_run_no_pending(
        self, mock_runner_cls: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Dry run with no pending migrations exits 0."""
        mock_runner = mock_runner_cls.return_value
        mock_runner.pending.return_value = []

        result = runner.invoke(app, ["apply", "--dry-run"])
        assert result.exit_code == 0
        mock_runner.apply_all.assert_not_called()

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_failure_exits_1(
        self, mock_runner_cls: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Failed migration exits with code 1."""
        from moneybin.migrations import MigrationResult

        mock_runner = mock_runner_cls.return_value
        mock_runner.apply_all.return_value = MigrationResult(
            failed_migration="V002__bad.sql",
            error_message="Migration V002__bad.sql failed",
        )
        mock_runner.check_drift.return_value = []

        result = runner.invoke(app, ["apply"])
        assert result.exit_code == 1

    @patch("moneybin.cli.commands.migrate.get_database")
    def test_apply_database_key_error_exits_1(self, mock_get_db: MagicMock) -> None:
        """DatabaseKeyError causes exit 1."""
        from moneybin.database import DatabaseKeyError

        mock_get_db.side_effect = DatabaseKeyError("key not found")

        result = runner.invoke(app, ["apply"])
        assert result.exit_code == 1

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_drift_warnings_shown(
        self,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Drift warnings are emitted after apply."""
        import logging

        from moneybin.migrations import DriftWarning, MigrationResult

        mock_runner = mock_runner_cls.return_value
        mock_runner.apply_all.return_value = MigrationResult(applied_count=1)
        mock_runner.check_drift.return_value = [
            DriftWarning(
                version=1, filename="V001__init.sql", reason="Checksum mismatch"
            )
        ]

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.commands.migrate"):
            result = runner.invoke(app, ["apply"])

        assert result.exit_code == 0
        assert "Checksum mismatch" in result.stdout

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_repairs_sqlmesh_state_when_behind(
        self,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        sqlmesh_state: object,
    ) -> None:
        """When SQLMesh state is behind, apply calls repair and exits 0."""
        from moneybin.migrations import MigrationResult

        mock_runner = mock_runner_cls.return_value
        mock_runner.apply_all.return_value = MigrationResult(applied_count=0)
        mock_runner.check_drift.return_value = []
        sqlmesh_state.assessment.return_value = (  # type: ignore[attr-defined]
            "SQLMesh state (v100) behind — run migrate.",
            True,
        )
        mock_db = mock_get_db.return_value.__enter__.return_value
        mock_db.repair_sqlmesh_state.return_value = True

        result = runner.invoke(app, ["apply"])

        assert result.exit_code == 0
        mock_db.repair_sqlmesh_state.assert_called_once_with()

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_exits_1_when_sqlmesh_repair_fails(
        self,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        sqlmesh_state: object,
    ) -> None:
        """A repair that doesn't advance the durable state surfaces as exit 1."""
        from moneybin.migrations import MigrationResult

        mock_runner = mock_runner_cls.return_value
        mock_runner.apply_all.return_value = MigrationResult(applied_count=0)
        mock_runner.check_drift.return_value = []
        sqlmesh_state.assessment.return_value = (  # type: ignore[attr-defined]
            "SQLMesh state (v100) behind — run migrate.",
            True,
        )
        mock_db = mock_get_db.return_value.__enter__.return_value
        mock_db.repair_sqlmesh_state.return_value = False

        result = runner.invoke(app, ["apply"])

        assert result.exit_code == 1

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_warns_but_succeeds_when_state_ahead(
        self,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        sqlmesh_state: object,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """State AHEAD of the package can't be migrated — warn, don't repair/fail."""
        import logging

        from moneybin.migrations import MigrationResult

        mock_runner = mock_runner_cls.return_value
        mock_runner.apply_all.return_value = MigrationResult(applied_count=0)
        mock_runner.check_drift.return_value = []
        sqlmesh_state.assessment.return_value = (  # type: ignore[attr-defined]
            "Transform state schema (v105) is ahead of the schema "
            "this install supports (v101). Upgrade MoneyBin to match.",
            False,
        )
        mock_db = mock_get_db.return_value.__enter__.return_value

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.commands.migrate"):
            result = runner.invoke(app, ["apply"])

        assert result.exit_code == 0
        mock_db.repair_sqlmesh_state.assert_not_called()
        assert "ahead" in result.stdout

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    def test_apply_dry_run_reports_sqlmesh_drift(
        self,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        sqlmesh_state: object,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Dry run reports SQLMesh drift without executing any migration."""
        import logging

        mock_runner = mock_runner_cls.return_value
        mock_runner.pending.return_value = []
        sqlmesh_state.drift.return_value = (  # type: ignore[attr-defined]
            "SQLMesh state (v100) behind — run migrate."
        )
        mock_db = mock_get_db.return_value.__enter__.return_value

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.commands.migrate"):
            result = runner.invoke(app, ["apply", "--dry-run"])

        assert result.exit_code == 0
        mock_runner.apply_all.assert_not_called()
        mock_db.migrate_sqlmesh_state.assert_not_called()
        assert "behind" in result.stdout


class TestMigrateStatus:
    """moneybin db migrate status command."""

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    @patch("moneybin.cli.commands.migrate.get_current_versions")
    def test_status_shows_applied_and_pending(
        self,
        mock_get_versions: MagicMock,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Status command exits 0 and logs applied and pending migrations."""
        import logging
        from datetime import datetime

        from moneybin.migrations import AppliedMigration

        mock_runner = mock_runner_cls.return_value
        mock_runner.pending.return_value = [
            _migration(
                version=2, name="new", filename="V002__new.sql", checksum="def456"
            )
        ]
        mock_runner.applied_details.return_value = [
            AppliedMigration(
                version=1,
                filename="V001__init.sql",
                success=True,
                execution_ms=42,
                applied_at=datetime(2026, 1, 1),
            )
        ]
        mock_runner.check_drift.return_value = []
        mock_get_versions.return_value = {"moneybin": "0.2.0"}

        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.migrate"):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        messages = _compact(result.stdout)
        assert "V001__init.sql" in messages
        assert "V002__new.sql" in messages

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    @patch("moneybin.cli.commands.migrate.get_current_versions")
    def test_status_no_applied(
        self,
        mock_get_versions: MagicMock,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Status with no applied migrations logs 'No applied migrations'."""
        import logging

        mock_runner = mock_runner_cls.return_value
        mock_runner.pending.return_value = []
        mock_runner.applied_details.return_value = []
        mock_runner.check_drift.return_value = []
        mock_get_versions.return_value = {}

        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.migrate"):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "Applied migrations: 0" in result.stdout

    @patch("moneybin.cli.commands.migrate.get_database")
    def test_status_database_key_error_exits_1(self, mock_get_db: MagicMock) -> None:
        """DatabaseKeyError on status causes exit 1."""
        from moneybin.database import DatabaseKeyError

        mock_get_db.side_effect = DatabaseKeyError("key not found")

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 1

    @patch("moneybin.cli.commands.migrate.get_database")
    @patch("moneybin.cli.commands.migrate.MigrationRunner")
    @patch("moneybin.cli.commands.migrate.get_current_versions")
    def test_status_surfaces_sqlmesh_state_drift(
        self,
        mock_get_versions: MagicMock,
        mock_runner_cls: MagicMock,
        mock_get_db: MagicMock,
        sqlmesh_state: object,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Status warns when SQLMesh internal state has drifted behind the package."""
        import logging

        mock_runner = mock_runner_cls.return_value
        mock_runner.pending.return_value = []
        mock_runner.applied_details.return_value = []
        mock_runner.check_drift.return_value = []
        mock_get_versions.return_value = {}
        sqlmesh_state.drift.return_value = (  # type: ignore[attr-defined]
            "Transform state schema (v100) is behind the schema "
            "this install supports (v101). Run `moneybin db migrate apply` to migrate the state."
        )

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.commands.migrate"):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "migrate apply" in result.stdout


def test_status_pages_complete_migration_state_and_no_pager_prints_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status is a finite read; quiet retains the requested state and drift."""
    from moneybin.migrations import AppliedMigration, DriftWarning

    mock_db = MagicMock()
    mock_runner = MagicMock()
    mock_runner.applied_details.return_value = [
        AppliedMigration(
            version=1,
            filename="V001__initialize_schema.sql",
            success=True,
            execution_ms=42,
            applied_at=datetime(2026, 1, 1),
        )
    ]
    mock_runner.pending.return_value = [_migration(2, filename="V002__next.sql")]
    mock_runner.check_drift.return_value = [
        DriftWarning(1, "V001__initialize_schema.sql", "Checksum mismatch")
    ]
    pages: list[str] = []

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    def fake_versions(_db: object) -> dict[str, str]:
        return {"moneybin": "0.2.0"}

    def capture_page(text: str, **_kwargs: object) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", MagicMock())
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_current_versions",
        fake_versions,
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )
    monkeypatch.setattr(
        "moneybin.cli.pager.page_text",
        capture_page,
    )

    paged = runner.invoke(app, ["status"])
    direct = runner.invoke(app, ["status", "--quiet", "--no-pager"])

    assert paged.exit_code == direct.exit_code == 0
    assert len(pages) == 1
    assert "Migration status" in pages[0]
    assert "V001__initialize_schema.sql" in _compact(pages[0])
    assert "Checksum mismatch" in pages[0]
    assert "Migration status" in direct.stdout
    assert "V001__initialize_schema.sql" in _compact(direct.stdout)
    assert "Checksum mismatch" in direct.stdout
    assert mock_db is not None


@pytest.mark.parametrize(
    ("pending", "expected"),
    [([_migration()], "Planned migrations"), ([], "No pending migrations")],
)
def test_apply_dry_run_is_an_unpaged_preview(
    monkeypatch: pytest.MonkeyPatch,
    pending: list[Migration],
    expected: str,
) -> None:
    """A migration preview never opens a pager, including its no-change result."""
    mock_runner = MagicMock()
    mock_runner.pending.return_value = pending
    pages: list[str] = []

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    def capture_page(text: str, **_kwargs: object) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", MagicMock())
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )
    monkeypatch.setattr(
        "moneybin.cli.pager.page_text",
        capture_page,
    )

    result = runner.invoke(app, ["apply", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert expected in _compact(result.stdout)
    assert pages == []
    mock_runner.apply_all.assert_not_called()


def test_interrupted_dry_run_does_not_claim_unknown_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A preview can be cancelled before planning without any applied migration."""
    mock_runner = MagicMock()
    mock_runner.pending.side_effect = KeyboardInterrupt

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", MagicMock())
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )

    result = runner.invoke(app, ["apply", "--dry-run"])

    assert result.exit_code == 130
    assert "Migration preview cancelled" in _compact(result.stdout)
    assert "No migrations were applied" in _compact(result.stdout)
    assert "Saved scope is unknown" not in _compact(result.stdout)


def test_apply_repair_failure_keeps_known_applied_migrations_in_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later transform-state failure must not erase known saved migrations."""
    from moneybin.migrations import MigrationResult

    mock_runner = MagicMock()
    mock_runner.apply_all.return_value = MigrationResult(applied_count=2)
    mock_runner.check_drift.return_value = []
    mock_db = MagicMock()
    mock_db.repair_sqlmesh_state.return_value = False
    get_db = MagicMock()
    get_db.return_value.__enter__.return_value = mock_db
    pages: list[str] = []

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    def fake_assessment(_db: object) -> tuple[str, bool]:
        return "Transform state is behind", True

    def capture_page(text: str, **_kwargs: object) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", get_db)
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.sqlmesh_state_assessment",
        fake_assessment,
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )
    monkeypatch.setattr("moneybin.cli.pager.page_text", capture_page)

    result = runner.invoke(app, ["apply"])

    assert result.exit_code == 1
    assert "2 migration(s) applied" in _compact(result.stdout)
    assert "Transform state repair" in _compact(result.stdout)
    assert "Failed" in result.stdout
    assert "Remaining state" in result.stdout
    assert pages == []


def test_apply_failure_retains_known_applied_migration_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct migration failure retains its known applied count."""
    from moneybin.migrations import MigrationResult

    mock_runner = MagicMock()
    mock_runner.apply_all.return_value = MigrationResult(
        applied_count=1,
        failed_migration="V002__bad.sql",
        error_message="Migration V002__bad.sql failed",
    )
    mock_runner.check_drift.return_value = []

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", MagicMock())
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )

    failed = runner.invoke(app, ["apply"])

    assert failed.exit_code == 1
    assert "1 migration(s) applied" in _compact(failed.stdout)
    assert "V002__bad.sql" in failed.stdout


@pytest.mark.parametrize("stage", ["drift", "assessment", "repair"])
def test_post_apply_classified_error_preserves_known_saved_scope(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    """Each post-result stage reports saved migrations and classified failure."""
    from moneybin.errors import UserError
    from moneybin.migrations import MigrationResult

    mock_runner = MagicMock()
    mock_runner.apply_all.return_value = MigrationResult(applied_count=2)
    mock_runner.check_drift.return_value = []
    mock_db = MagicMock()
    get_db = MagicMock()
    get_db.return_value.__enter__.return_value = mock_db

    error = UserError(
        "Transform state could not be inspected",
        code="infra_io_error",
        hint="Run moneybin db migrate status",
    )

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    if stage == "drift":
        mock_runner.check_drift.side_effect = error
    elif stage == "assessment":
        monkeypatch.setattr(
            "moneybin.cli.commands.migrate.sqlmesh_state_assessment",
            MagicMock(side_effect=error),
        )
    else:
        monkeypatch.setattr(
            "moneybin.cli.commands.migrate.sqlmesh_state_assessment",
            MagicMock(return_value=("Transform state is behind", True)),
        )
        mock_db.repair_sqlmesh_state.side_effect = error

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", get_db)
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )

    result = runner.invoke(app, ["apply"])

    assert result.exit_code == 1
    assert "2 migration(s) applied" in _compact(result.stdout)
    assert "Transform state could not be inspected" in _compact(result.stdout)
    assert "freshness is unknown" in _compact(result.stdout)


@pytest.mark.parametrize("applied_count", [0, 1])
def test_post_failure_diagnostic_keeps_direct_migration_failure(
    monkeypatch: pytest.MonkeyPatch,
    applied_count: int,
) -> None:
    """A drift diagnostic cannot replace the already-known migration failure."""
    from moneybin.errors import UserError
    from moneybin.migrations import MigrationResult

    mock_runner = MagicMock()
    mock_runner.apply_all.return_value = MigrationResult(
        applied_count=applied_count,
        failed_migration="V002__bad.sql",
        error_message="Migration V002__bad.sql failed",
    )
    mock_runner.check_drift.side_effect = UserError(
        "Drift inspection failed", code="infra_io_error"
    )
    pages: list[str] = []

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    def capture_page(text: str, **_kwargs: object) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", MagicMock())
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )
    monkeypatch.setattr("moneybin.cli.pager.page_text", capture_page)

    result = runner.invoke(app, ["apply"])

    assert result.exit_code == 1
    assert "Migration apply failed" in _compact(result.stdout)
    assert f"{applied_count} migration(s) applied" in _compact(result.stdout)
    assert "V002__bad.sql" in _compact(result.stdout)
    assert "Migration V002__bad.sql failed" in _compact(result.stdout)
    assert "Drift inspection failed" in _compact(result.stdout)
    assert pages == []


def test_post_apply_interrupt_preserves_known_count_and_unknown_remainder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation after apply distinguishes known migrations from unknown remainder."""
    from moneybin.migrations import MigrationResult

    mock_runner = MagicMock()
    mock_runner.apply_all.return_value = MigrationResult(applied_count=2)
    mock_runner.check_drift.side_effect = KeyboardInterrupt

    def fake_runner(_db: object) -> MagicMock:
        return mock_runner

    monkeypatch.setattr("moneybin.cli.commands.migrate.get_database", MagicMock())
    monkeypatch.setattr("moneybin.cli.commands.migrate.MigrationRunner", fake_runner)
    monkeypatch.setattr(
        "moneybin.cli.commands.migrate.get_terminal_policy", _pager_policy
    )

    result = runner.invoke(app, ["apply"])

    assert result.exit_code == 130
    assert "2 migration(s) applied" in _compact(result.stdout)
    assert "Remaining state" in result.stdout
    assert "freshness is unknown" in _compact(result.stdout)


def test_apply_preserves_text_only_success_surface() -> None:
    """Apply keeps its established text-only interface; status owns JSON."""
    result = runner.invoke(app, ["apply", "--output", "json"])

    assert result.exit_code == 2
