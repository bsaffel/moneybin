# ruff: noqa: S101,S106,S108
"""Tests for synthetic data CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.synthetic import app


class TestGenerateCommand:
    """Test the 'synthetic generate' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_get_database(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.database.get_database",
            return_value=MagicMock(),
        )

    @pytest.fixture
    def mock_engine(self, mocker: Any) -> MagicMock:
        mock_result = MagicMock()
        mock_result.persona = "basic"
        mock_result.seed = 42
        mock_result.accounts = [MagicMock()]
        mock_result.transactions = [MagicMock()] * 100
        mock_result.start_date = MagicMock(__str__=lambda s: "2024-01-01")  # type: ignore[reportUnknownLambdaType]  # MagicMock dunder override
        mock_result.end_date = MagicMock(__str__=lambda s: "2024-12-31")  # type: ignore[reportUnknownLambdaType]  # MagicMock dunder override
        mock_cls = mocker.patch(
            "moneybin.testing.synthetic.engine.GeneratorEngine",
        )
        mock_cls.return_value.generate.return_value = mock_result
        return mock_cls

    @pytest.fixture
    def mock_writer(self, mocker: Any) -> MagicMock:
        mock_cls = mocker.patch(
            "moneybin.testing.synthetic.writer.SyntheticWriter",
        )
        mock_cls.return_value.write.return_value = {
            "ofx_accounts": 1,
            "ofx_transactions": 80,
            "csv_transactions": 20,
            "ground_truth": 100,
        }
        return mock_cls

    @pytest.fixture
    def mock_run_transforms(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.services.import_service.run_transforms",
            return_value=True,
        )

    def test_generate_requires_persona(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["generate"])
        assert result.exit_code != 0

    def test_generate_success(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
        mock_engine: MagicMock,
        mock_writer: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        result = runner.invoke(app, ["generate", "--persona", "basic", "--seed", "42"])
        assert result.exit_code == 0
        mock_engine.assert_called_once()

    def test_generate_unknown_persona(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
    ) -> None:
        with patch(
            "moneybin.testing.synthetic.engine.GeneratorEngine",
            side_effect=FileNotFoundError("Unknown persona: 'bad'"),
        ):
            result = runner.invoke(app, ["generate", "--persona", "bad"])
            assert result.exit_code == 1


class TestResetCommand:
    """Test the 'synthetic reset' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_reset_requires_persona(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reset"])
        assert result.exit_code != 0

    def test_reset_requires_yes_or_prompt(self, runner: CliRunner) -> None:
        """Without --yes, reset should prompt for confirmation."""
        # CliRunner sends EOF on stdin by default, so prompt is declined
        with patch("moneybin.database.get_database") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1,)
            mock_db.return_value.conn = mock_conn
            mock_db.return_value.path = Path("/tmp/test.duckdb")
            result = runner.invoke(app, ["reset", "--persona", "basic"])
            # Should either prompt and abort, or succeed with --yes
            assert result.exit_code != 0 or "Aborted" in (result.output or "")
