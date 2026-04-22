"""Tests for the db migrate CLI commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.migrate import app
from moneybin.migrations import Migration

runner = CliRunner()


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
        assert any("Checksum mismatch" in r.message for r in caplog.records)


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
                applied_at="2026-01-01 00:00:00",
            )
        ]
        mock_runner.check_drift.return_value = []
        mock_get_versions.return_value = {"moneybin": "0.2.0"}

        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.migrate"):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        messages = " ".join(r.message for r in caplog.records)
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
        assert any("No applied migrations" in r.message for r in caplog.records)

    @patch("moneybin.cli.commands.migrate.get_database")
    def test_status_database_key_error_exits_1(self, mock_get_db: MagicMock) -> None:
        """DatabaseKeyError on status causes exit 1."""
        from moneybin.database import DatabaseKeyError

        mock_get_db.side_effect = DatabaseKeyError("key not found")

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 1
