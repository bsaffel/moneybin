"""CLI tests for moneybin accounts balance subcommands."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.balance_service import BalanceService


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer/Click CliRunner with split streams."""
    return CliRunner()


class TestAccountsBalanceHelp:
    """Tests for `accounts balance --help` surface."""

    @pytest.mark.unit
    def test_balance_help_lists_subcommands(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["accounts", "balance", "--help"])
        assert result.exit_code == 0
        for cmd in ["show", "history", "assert", "list", "delete", "reconcile"]:
            assert cmd in result.stdout


class TestAccountsBalanceShow:
    """Tests for `accounts balance show`."""

    @pytest.mark.unit
    def test_show_lists_current_balances(self, runner: CliRunner) -> None:
        mock_obs = MagicMock(
            account_id="acct_a",
            balance_date=date(2026, 1, 31),
            balance=Decimal("1234.56"),
            is_observed=True,
            observation_source="ofx",
            reconciliation_delta=None,
        )
        mock_obs.to_dict.return_value = {
            "account_id": "acct_a",
            "balance_date": "2026-01-31",
            "balance": Decimal("1234.56"),
            "is_observed": True,
            "observation_source": "ofx",
            "reconciliation_delta": None,
        }
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.current_balances.return_value = [mock_obs]
            result = runner.invoke(
                app, ["accounts", "balance", "show", "--output", "json"]
            )
        assert result.exit_code == 0, result.stderr
        payload = json.loads(result.stdout)
        assert "balances" in payload
        assert len(payload["balances"]) == 1


class TestAccountsBalanceHistory:
    """Tests for `accounts balance history`."""

    @pytest.mark.unit
    def test_history_requires_account(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["accounts", "balance", "history"])
        assert result.exit_code == 2

    @pytest.mark.unit
    def test_history_returns_series(self, runner: CliRunner) -> None:
        mock_obs = MagicMock()
        mock_obs.to_dict.return_value = {
            "account_id": "acct_a",
            "balance_date": "2026-01-01",
            "balance": Decimal("100.00"),
            "is_observed": True,
            "observation_source": "ofx",
            "reconciliation_delta": None,
        }
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.history.return_value = [mock_obs]
            result = runner.invoke(
                app,
                [
                    "accounts",
                    "balance",
                    "history",
                    "--account",
                    "acct_a",
                    "--output",
                    "json",
                ],
            )
        assert result.exit_code == 0, result.stderr
        mock_service_class.return_value.history.assert_called_once()


class TestAccountsBalanceAssert:
    """Tests for `accounts balance assert`."""

    @pytest.mark.unit
    def test_assert_writes(self, runner: CliRunner) -> None:
        # spec=BalanceService allows "assert_balance" — MagicMock rejects attribute
        # names starting with "assert" unless the mock is spec'd to a real class.
        mock_service = MagicMock(spec=BalanceService)
        mock_service.assert_balance.return_value = MagicMock(
            account_id="acct_a",
            assertion_date=date(2026, 1, 31),
            balance=Decimal("1234.56"),
        )
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService",
                return_value=mock_service,
            ),
        ):
            result = runner.invoke(
                app,
                [
                    "accounts",
                    "balance",
                    "assert",
                    "acct_a",
                    "2026-01-31",
                    "1234.56",
                    "--yes",
                ],
            )
        assert result.exit_code == 0, result.stderr
        mock_service.assert_balance.assert_called_once()
        call_args = mock_service.assert_balance.call_args
        assert (
            call_args.kwargs.get("account_id") == "acct_a"
            or call_args.args[0] == "acct_a"
        )

    @pytest.mark.unit
    def test_assert_with_notes(self, runner: CliRunner) -> None:
        mock_service = MagicMock(spec=BalanceService)
        mock_service.assert_balance.return_value = MagicMock()
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService",
                return_value=mock_service,
            ),
        ):
            result = runner.invoke(
                app,
                [
                    "accounts",
                    "balance",
                    "assert",
                    "acct_a",
                    "2026-01-31",
                    "1234.56",
                    "--notes",
                    "from paper statement",
                    "--yes",
                ],
            )
        assert result.exit_code == 0, result.stderr


class TestAccountsBalanceList:
    """Tests for `accounts balance list`."""

    @pytest.mark.unit
    def test_list_returns_assertions(self, runner: CliRunner) -> None:
        mock_assertion = MagicMock()
        mock_assertion.to_dict.return_value = {
            "account_id": "acct_a",
            "assertion_date": "2026-01-31",
            "balance": Decimal("1234.56"),
            "notes": None,
            "created_at": "2026-01-31 12:00:00",
        }
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.list_assertions.return_value = [
                mock_assertion
            ]
            result = runner.invoke(
                app, ["accounts", "balance", "list", "--output", "json"]
            )
        assert result.exit_code == 0, result.stderr
        payload = json.loads(result.stdout)
        assert "assertions" in payload


class TestAccountsBalanceDelete:
    """Tests for `accounts balance delete`."""

    @pytest.mark.unit
    def test_delete_calls_service(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService"
            ) as mock_service_class,
        ):
            result = runner.invoke(
                app,
                ["accounts", "balance", "delete", "acct_a", "2026-01-31", "--yes"],
            )
        assert result.exit_code == 0, result.stderr
        mock_service_class.return_value.delete_assertion.assert_called_once()


class TestAccountsBalanceReconcile:
    """Tests for `accounts balance reconcile`."""

    @pytest.mark.unit
    def test_reconcile_returns_deltas(self, runner: CliRunner) -> None:
        mock_obs = MagicMock()
        mock_obs.to_dict.return_value = {
            "account_id": "acct_a",
            "balance_date": "2026-01-31",
            "balance": Decimal("1234.56"),
            "is_observed": True,
            "observation_source": "ofx",
            "reconciliation_delta": Decimal("5.00"),
        }
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.reconcile.return_value = [mock_obs]
            result = runner.invoke(
                app, ["accounts", "balance", "reconcile", "--output", "json"]
            )
        assert result.exit_code == 0, result.stderr
        mock_service_class.return_value.reconcile.assert_called_once()

    @pytest.mark.unit
    def test_reconcile_threshold_passed_through(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.accounts.BalanceService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.reconcile.return_value = []
            result = runner.invoke(
                app,
                ["accounts", "balance", "reconcile", "--threshold", "5.00"],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service_class.return_value.reconcile.call_args.kwargs
        assert call_kwargs.get("threshold") == Decimal("5.00")
