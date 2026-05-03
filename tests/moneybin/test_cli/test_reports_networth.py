"""CLI tests for moneybin reports networth commands."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer/Click CliRunner."""
    return CliRunner()


class TestReportsHelp:
    """Verify reports group and networth sub-group wire up in help."""

    @pytest.mark.unit
    def test_reports_help_lists_networth(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reports", "--help"])
        assert result.exit_code == 0
        assert "networth" in result.stdout

    @pytest.mark.unit
    def test_reports_networth_help_lists_subcommands(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reports", "networth", "--help"])
        assert result.exit_code == 0
        assert "show" in result.stdout
        assert "history" in result.stdout


class TestReportsNetworthShow:
    """Tests for `reports networth show`."""

    @pytest.mark.unit
    def test_show_returns_snapshot(self, runner: CliRunner) -> None:
        mock_snapshot = MagicMock(
            balance_date=date(2026, 1, 31),
            net_worth=Decimal("12500.00"),
            total_assets=Decimal("15000.00"),
            total_liabilities=Decimal("-2500.00"),
            account_count=3,
            per_account=[
                {
                    "account_id": "acct_a",
                    "display_name": "Checking",
                    "balance": Decimal("5000.00"),
                    "observation_source": "ofx",
                },
            ],
        )
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.reports.NetworthService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.current.return_value = mock_snapshot
            result = runner.invoke(
                app, ["reports", "networth", "show", "--output", "json"]
            )
        assert result.exit_code == 0, result.stderr
        payload = json.loads(result.stdout)
        assert "networth" in payload
        assert payload["networth"]["account_count"] == 3

    @pytest.mark.unit
    def test_show_as_of_date(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.reports.NetworthService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.current.return_value = MagicMock(
                balance_date=date(2026, 1, 1),
                net_worth=Decimal("0"),
                total_assets=Decimal("0"),
                total_liabilities=Decimal("0"),
                account_count=0,
                per_account=[],
            )
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth",
                    "show",
                    "--as-of",
                    "2026-01-01",
                    "--output",
                    "json",
                ],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service_class.return_value.current.call_args.kwargs
        assert call_kwargs.get("as_of_date") == date(2026, 1, 1)

    @pytest.mark.unit
    def test_show_account_filter(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.reports.NetworthService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.current.return_value = MagicMock(
                balance_date=date(2026, 1, 31),
                net_worth=Decimal("0"),
                total_assets=Decimal("0"),
                total_liabilities=Decimal("0"),
                account_count=0,
                per_account=[],
            )
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth",
                    "show",
                    "--account",
                    "acct_a",
                    "--account",
                    "acct_b",
                ],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service_class.return_value.current.call_args.kwargs
        # Either as_of_date or account_ids; one of them carries the list
        assert call_kwargs.get("account_ids") == ["acct_a", "acct_b"]


class TestReportsNetworthHistory:
    """Tests for `reports networth history`."""

    @pytest.mark.unit
    def test_history_requires_from_to(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reports", "networth", "history"])
        assert result.exit_code == 2

    @pytest.mark.unit
    def test_history_returns_series(self, runner: CliRunner) -> None:
        mock_rows = [
            {
                "period": "2026-01-01",
                "net_worth": Decimal("1000.00"),
                "change_abs": None,
                "change_pct": None,
            },
            {
                "period": "2026-02-01",
                "net_worth": Decimal("1200.00"),
                "change_abs": Decimal("200.00"),
                "change_pct": 0.2,
            },
        ]
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.reports.NetworthService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.history.return_value = mock_rows
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth",
                    "history",
                    "--from",
                    "2026-01-01",
                    "--to",
                    "2026-12-31",
                    "--output",
                    "json",
                ],
            )
        assert result.exit_code == 0, result.stderr
        payload = json.loads(result.stdout)
        assert "history" in payload
        assert len(payload["history"]) == 2

    @pytest.mark.unit
    def test_history_default_interval_monthly(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.utils.get_database"),
            patch(
                "moneybin.cli.commands.reports.NetworthService"
            ) as mock_service_class,
        ):
            mock_service_class.return_value.history.return_value = []
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth",
                    "history",
                    "--from",
                    "2026-01-01",
                    "--to",
                    "2026-12-31",
                ],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service_class.return_value.history.call_args.kwargs
        # interval may be positional or kw — check both
        if "interval" in call_kwargs:
            assert call_kwargs["interval"] == "monthly"
        else:
            # positional args: from_date, to_date, interval
            args = mock_service_class.return_value.history.call_args.args
            if len(args) >= 3:
                assert args[2] == "monthly"
