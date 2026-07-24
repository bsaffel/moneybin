"""CLI tests for moneybin reports networth commands."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.execute import ReportResult


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer/Click CliRunner."""
    return CliRunner()


def _result(records: list[dict[str, object]]) -> ReportResult:
    columns = list(records[0]) if records else []
    return ReportResult(
        records=records,
        columns=columns,
        output_classes=dict.fromkeys(columns, DataClass.AGGREGATE),
        tier=Tier.LOW,
        total_count=len(records),
        truncated=False,
    )


def _snapshot_result(
    *,
    balance_date: date | None = date(2026, 1, 31),
    net_worth: Decimal | None = Decimal("12500.00"),
    total_assets: Decimal | None = Decimal("15000.00"),
    total_liabilities: Decimal | None = Decimal("-2500.00"),
    account_count: int = 3,
    account_id: str | None = "****acct_a",
    account_name: str | None = "Checking",
    account_balance: Decimal | None = Decimal("5000.00"),
    observation_source: str | None = "ofx",
) -> ReportResult:
    return _result([
        {
            "balance_date": balance_date,
            "net_worth": net_worth,
            "total_assets": total_assets,
            "total_liabilities": total_liabilities,
            "account_count": account_count,
            "account_id": account_id,
            "account_name": account_name,
            "account_balance": account_balance,
            "observation_source": observation_source,
        }
    ])


class TestReportsHelp:
    """Verify reports group lists the networth leaf commands."""

    @pytest.mark.unit
    def test_reports_help_lists_networth(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reports", "--help"])
        assert result.exit_code == 0
        assert "networth" in result.stdout
        assert "networth-history" in result.stdout


class TestReportsNetworth:
    """Tests for `reports networth`."""

    @pytest.mark.unit
    def test_returns_snapshot(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.commands.reports.networth.get_database"),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = _snapshot_result()
            result = runner.invoke(app, ["reports", "networth", "--output", "json"])
        assert result.exit_code == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["status"] == "ok"
        assert payload["data"][0]["account_count"] == 3

    @pytest.mark.unit
    def test_as_of_date(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.commands.reports.networth.get_database"),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = _snapshot_result(
                balance_date=date(2026, 1, 1)
            )
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth",
                    "--as-of",
                    "2026-01-01",
                    "--output",
                    "json",
                ],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_catalog.return_value.execute.call_args.kwargs
        assert call_kwargs["report_id"] == "core:networth"
        assert call_kwargs["parameters"]["as_of"] == "2026-01-01"

    @pytest.mark.unit
    def test_no_data_renders_null_snapshot_coherently(self, runner: CliRunner) -> None:
        snapshot = _snapshot_result(
            balance_date=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=0,
            account_id=None,
            account_name=None,
            account_balance=None,
            observation_source=None,
        )
        with (
            patch("moneybin.cli.commands.reports.networth.get_database"),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = snapshot
            text_result = runner.invoke(app, ["reports", "networth"])
            json_result = runner.invoke(
                app,
                ["reports", "networth", "--output", "json"],
            )

        assert text_result.exit_code == 0, text_result.stderr
        assert text_result.stdout.strip() == "No net worth data available."
        assert json_result.exit_code == 0, json_result.stderr
        payload = json.loads(json_result.stdout)
        assert payload["data"] == snapshot.records

    @pytest.mark.unit
    def test_account_filter(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.commands.reports.networth.get_database"),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = _snapshot_result()
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth",
                    "--account",
                    "acct_a",
                    "--account",
                    "acct_b",
                ],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_catalog.return_value.execute.call_args.kwargs
        assert call_kwargs["parameters"]["account_ids"] == ["acct_a", "acct_b"]


class TestReportsNetworthHistory:
    """Tests for `reports networth-history`."""

    @pytest.mark.unit
    def test_requires_from_to(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reports", "networth-history"])
        assert result.exit_code == 2

    @pytest.mark.unit
    def test_returns_series(self, runner: CliRunner) -> None:
        mock_rows: list[dict[str, object]] = [
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
            patch("moneybin.cli.commands.reports.networth.get_database"),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = _result(mock_rows)
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth-history",
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
        assert payload["status"] == "ok"
        assert len(payload["data"]) == 2

    @pytest.mark.unit
    def test_default_interval_monthly(self, runner: CliRunner) -> None:
        with (
            patch("moneybin.cli.commands.reports.networth.get_database"),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = _result([])
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth-history",
                    "--from",
                    "2026-01-01",
                    "--to",
                    "2026-12-31",
                ],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_catalog.return_value.execute.call_args.kwargs
        assert call_kwargs["report_id"] == "core:networth_history"
        assert call_kwargs["parameters"]["interval"] == "monthly"
