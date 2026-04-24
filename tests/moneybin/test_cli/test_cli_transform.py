"""Tests for transform CLI commands."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transform import app

runner = CliRunner()


def _mock_sqlmesh_context() -> tuple[Any, MagicMock]:
    """Create a mock sqlmesh_context that yields a MagicMock context."""
    mock_ctx = MagicMock()
    mock_ctx.state_reader.get_environment.return_value = None

    @contextmanager
    def _ctx(**kwargs: Any) -> Generator[MagicMock, None, None]:  # noqa: ARG001 — absorb sqlmesh_root kwarg
        yield mock_ctx

    return _ctx, mock_ctx


class TestTransformStatus:
    """Test transform status command."""

    @patch("moneybin.cli.commands.transform.get_database")
    @patch("moneybin.cli.commands.transform.sqlmesh_context")
    def test_status_succeeds(
        self, mock_ctx_factory: MagicMock, _mock_get_db: MagicMock
    ) -> None:
        """Transform status calls SQLMesh info."""
        ctx_fn, _mock_ctx = _mock_sqlmesh_context()
        mock_ctx_factory.side_effect = ctx_fn
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0


class TestTransformValidate:
    """Test transform validate command."""

    @patch("moneybin.cli.commands.transform.get_database")
    @patch("moneybin.cli.commands.transform.sqlmesh_context")
    def test_validate_succeeds(
        self, mock_ctx_factory: MagicMock, _mock_get_db: MagicMock
    ) -> None:
        """Transform validate runs plan in dry-run mode."""
        ctx_fn, mock_ctx = _mock_sqlmesh_context()
        mock_ctx_factory.side_effect = ctx_fn
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
        mock_ctx.plan.assert_called_once()


class TestTransformAudit:
    """Test transform audit command."""

    @patch("moneybin.cli.commands.transform.get_database")
    @patch("moneybin.cli.commands.transform.sqlmesh_context")
    def test_audit_succeeds(
        self, mock_ctx_factory: MagicMock, _mock_get_db: MagicMock
    ) -> None:
        """Transform audit runs SQLMesh audit."""
        ctx_fn, mock_ctx = _mock_sqlmesh_context()
        mock_ctx_factory.side_effect = ctx_fn
        result = runner.invoke(
            app, ["audit", "--start", "2026-01-01", "--end", "2026-01-31"]
        )
        assert result.exit_code == 0
        mock_ctx.audit.assert_called_once()


class TestTransformRestate:
    """Test transform restate command."""

    @patch("moneybin.cli.commands.transform.get_database")
    @patch("moneybin.cli.commands.transform.sqlmesh_context")
    def test_restate_requires_confirmation(
        self, mock_ctx_factory: MagicMock, _mock_get_db: MagicMock
    ) -> None:
        """Transform restate prompts for confirmation."""
        ctx_fn, mock_ctx = _mock_sqlmesh_context()
        mock_ctx_factory.side_effect = ctx_fn
        result = runner.invoke(
            app,
            ["restate", "--model", "core.fct_transactions", "--start", "2026-01-01"],
            input="n\n",
        )
        assert result.exit_code == 0
        mock_ctx.plan.assert_not_called()

    @patch("moneybin.cli.commands.transform.get_database")
    @patch("moneybin.cli.commands.transform.sqlmesh_context")
    def test_restate_with_yes(
        self, mock_ctx_factory: MagicMock, _mock_get_db: MagicMock
    ) -> None:
        """Transform restate --yes skips confirmation."""
        ctx_fn, _mock_ctx = _mock_sqlmesh_context()
        mock_ctx_factory.side_effect = ctx_fn
        result = runner.invoke(
            app,
            [
                "restate",
                "--model",
                "core.fct_transactions",
                "--start",
                "2026-01-01",
                "--yes",
            ],
        )
        assert result.exit_code == 0
