"""CLI tests for moneybin reports networth commands."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.output import UNKNOWN_CURRENCY
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.execute import ReportResult
from tests.database_mocks import no_profile_database


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


def _totals_row(
    *,
    balance_date: date | None = date(2026, 1, 31),
    currency_code: str | None = "USD",
    net_worth: Decimal | None = Decimal("12500.00"),
    total_assets: Decimal | None = Decimal("15000.00"),
    total_liabilities: Decimal | None = Decimal("-2500.00"),
    account_count: int | None = 3,
) -> dict[str, object]:
    """One currency's position, carrying no account of its own."""
    return {
        "balance_date": balance_date,
        "currency_code": currency_code,
        "net_worth": net_worth,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
        "account_count": account_count,
        "account_id": None,
        "account_name": None,
        "account_balance": None,
        "observation_source": None,
    }


def _account_row(
    *,
    balance_date: date | None = date(2026, 1, 31),
    currency_code: str | None = "USD",
    account_id: str | None = "****acct_a",
    account_name: str | None = "Checking",
    account_balance: Decimal | None = Decimal("5000.00"),
    observation_source: str | None = "ofx",
) -> dict[str, object]:
    """One account's balance, carrying none of its currency's totals."""
    return {
        "balance_date": balance_date,
        "currency_code": currency_code,
        "net_worth": None,
        "total_assets": None,
        "total_liabilities": None,
        "account_count": None,
        "account_id": account_id,
        "account_name": account_name,
        "account_balance": account_balance,
        "observation_source": observation_source,
    }


def _snapshot_result(
    *,
    balance_date: date | None = date(2026, 1, 31),
    net_worth: Decimal | None = Decimal("12500.00"),
    total_assets: Decimal | None = Decimal("15000.00"),
    total_liabilities: Decimal | None = Decimal("-2500.00"),
    account_count: int | None = 3,
    currency_code: str | None = "USD",
    account_id: str | None = "****acct_a",
    account_name: str | None = "Checking",
    account_balance: Decimal | None = Decimal("5000.00"),
    observation_source: str | None = "ofx",
) -> ReportResult:
    """One currency's totals row, plus its account row when there is one.

    The report emits the two kinds as separate rows — a currency's position
    never rides an account's row — so a fused fixture would exercise a shape
    the executor cannot produce.
    """
    records = [
        _totals_row(
            balance_date=balance_date,
            currency_code=currency_code,
            net_worth=net_worth,
            total_assets=total_assets,
            total_liabilities=total_liabilities,
            account_count=account_count,
        )
    ]
    if account_id is not None:
        records.append(
            _account_row(
                balance_date=balance_date,
                currency_code=currency_code,
                account_id=account_id,
                account_name=account_name,
                account_balance=account_balance,
                observation_source=observation_source,
            )
        )
    return _result(records)


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
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
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
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
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
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
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
    def test_text_render_shows_one_headline_per_currency(
        self, runner: CliRunner
    ) -> None:
        """Two currencies print two positions and no combined total.

        This is the user-facing half of Requirement 7, and text is the only
        surface where it can go wrong quietly: JSON carries per_currency
        whatever the renderer does, while the text path previously printed the
        first row's subtotal under a single "Net worth" heading — a number
        that looked like the whole position and was one currency's share of it.
        """
        snapshot = _result([
            _totals_row(
                currency_code="USD",
                net_worth=Decimal("12500.00"),
                total_assets=Decimal("15000.00"),
                total_liabilities=Decimal("-2500.00"),
                account_count=2,
            ),
            _totals_row(
                currency_code="EUR",
                net_worth=Decimal("800.00"),
                total_assets=Decimal("800.00"),
                total_liabilities=Decimal("0.00"),
                account_count=1,
            ),
            _account_row(currency_code="USD"),
            _account_row(
                currency_code="EUR",
                account_id="****acct_b",
                account_name="Euro Savings",
                account_balance=Decimal("800.00"),
            ),
        ])
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = snapshot
            result = runner.invoke(app, ["reports", "networth"])

        assert result.exit_code == 0, result.stderr
        out = result.stdout + result.stderr
        # One `render_summary` block per currency, each headed by its own code.
        assert "USD as of" in out
        assert "12,500.00" in out
        assert "EUR as of" in out
        assert "800.00" in out
        # 13300.00 is the blend; its absence is the assertion that matters.
        # Both spellings, because `format_money` now separates thousands and a
        # check for only the bare digits would stop catching the blend.
        assert "13300" not in out
        assert "13,300" not in out

    @pytest.mark.unit
    def test_text_render_adds_up_segments_priced_in_one_currency(
        self, runner: CliRunner
    ) -> None:
        """Converted segments are one position again (Requirement 16).

        Conversion prices each currency's totals row into the display currency
        and relabels it, so the result holds several rows sharing one label —
        one per currency the profile actually holds. Printing the first of them
        as "the" position is the same defect segmentation was introduced to
        prevent, one step later: it reports the dollar share of a dollar-and-
        euro position as the whole of it.
        """
        snapshot = _result([
            _totals_row(
                currency_code="USD",
                net_worth=Decimal("12500.00"),
                total_assets=Decimal("15000.00"),
                total_liabilities=Decimal("-2500.00"),
                account_count=2,
            ),
            # The euro segment, priced into USD and relabelled by conversion.
            _totals_row(
                currency_code="USD",
                net_worth=Decimal("880.00"),
                total_assets=Decimal("880.00"),
                total_liabilities=Decimal("0.00"),
                account_count=1,
            ),
            _account_row(currency_code="USD"),
            _account_row(
                currency_code="USD",
                account_id="****acct_b",
                account_name="Euro Savings",
                account_balance=Decimal("880.00"),
            ),
        ])
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = snapshot
            result = runner.invoke(app, ["reports", "networth"])

        assert result.exit_code == 0, result.stderr
        out = result.stdout + result.stderr
        assert "USD as of" in out
        assert "Net worth:   13,380.00" in out
        assert "Assets:      15,880.00" in out
        # U+2212, not a hyphen: liabilities are stored negative and
        # `format_money` renders the minus the design system specifies.
        assert "Liabilities: −2,500.00" in out
        assert "Accounts:    3" in out
        # The dollar share, printed alone, is what this test exists to catch.
        # Both spellings — `format_money` separates thousands, so the bare-digit
        # form alone would stop catching a regression.
        assert "12500.00" not in out
        assert "12,500.00" not in out

    @pytest.mark.unit
    def test_text_render_says_why_a_conversion_fell_back(
        self, runner: CliRunner
    ) -> None:
        """Segmented positions without a reason are the silent-masking defect.

        A caller who asked for one currency and got several is owed the reason;
        the JSON and MCP paths read it off `summary.degraded_reason`, and the
        text path renders no envelope at all, so it has to echo it.
        """
        snapshot = replace(
            _result([
                _totals_row(currency_code="USD", net_worth=Decimal("12500.00")),
                _totals_row(currency_code="EUR", net_worth=Decimal("800.00")),
            ]),
            degraded=True,
            degraded_reason="no stored rate covers EUR→JPY on 2026-01-31",
            actions=["Run `moneybin refresh` to gather the missing rate"],
        )
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = snapshot
            result = runner.invoke(
                app, ["reports", "networth", "--display-currency", "JPY"]
            )

        assert result.exit_code == 0, result.stderr
        out = result.stdout + result.stderr
        assert "no stored rate covers EUR→JPY on 2026-01-31" in out
        assert "Run `moneybin refresh` to gather the missing rate" in out

    @pytest.mark.unit
    def test_breakdown_renders_unknown_currency_without_the_word_none(
        self, runner: CliRunner
    ) -> None:
        """A null currency must not reach the user as the string "None".

        The headline block guards this already, so the assertion is scoped to
        the per-account line: a whole-output check would pass on the headline's
        label alone and prove nothing about the breakdown.
        """
        snapshot = _snapshot_result(currency_code=None, account_name="Checking")
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = snapshot
            result = runner.invoke(app, ["reports", "networth"])

        assert result.exit_code == 0, result.stderr
        out = result.stdout + result.stderr
        breakdown = next(line for line in out.splitlines() if "Checking" in line)
        assert "None" not in breakdown
        assert UNKNOWN_CURRENCY in breakdown

    @pytest.mark.unit
    def test_account_filter(self, runner: CliRunner) -> None:
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
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
    def test_text_render_says_why_a_conversion_fell_back(
        self, runner: CliRunner
    ) -> None:
        """The series renderer owes the same reason the snapshot does.

        `core:networth_history` aggregates per currency, so a display currency
        never prices it — which makes this the surface most likely to be read
        as "the conversion worked and the answer is two series".
        """
        series = replace(
            _result([
                {
                    "period": "2026-01-01",
                    "currency_code": "USD",
                    "net_worth": Decimal("1000.00"),
                    "change_abs": None,
                    "change_pct": None,
                }
            ]),
            degraded=True,
            degraded_reason=(
                "each period's net worth is aggregated per currency_code, so rows "
                "stay segmented per currency_code, never blended"
            ),
        )
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
            patch(
                "moneybin.reports._framework.catalog.get_report_catalog"
            ) as mock_catalog,
        ):
            mock_catalog.return_value.execute.return_value = series
            result = runner.invoke(
                app,
                [
                    "reports",
                    "networth-history",
                    "--from",
                    "2026-01-01",
                    "--to",
                    "2026-02-01",
                    "--display-currency",
                    "EUR",
                ],
            )

        assert result.exit_code == 0, result.stderr
        out = result.stdout + result.stderr
        assert "aggregated per currency_code" in out

    @pytest.mark.unit
    def test_returns_series(self, runner: CliRunner) -> None:
        mock_rows: list[dict[str, object]] = [
            {
                "period": "2026-01-01",
                "currency_code": "USD",
                "net_worth": Decimal("1000.00"),
                "change_abs": None,
                "change_pct": None,
            },
            {
                "period": "2026-02-01",
                "currency_code": "USD",
                "net_worth": Decimal("1200.00"),
                "change_abs": Decimal("200.00"),
                "change_pct": 0.2,
            },
        ]
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
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
    def test_series_labels_unknown_currency_like_the_snapshot(
        self, runner: CliRunner
    ) -> None:
        """History and snapshot must name an unknown currency the same way.

        Two spellings for one state is what let the breakdown's raw ``!s`` sit
        beside two correct guards without looking wrong.
        """
        mock_rows: list[dict[str, object]] = [
            {
                "period": "2026-01-01",
                "currency_code": None,
                "net_worth": Decimal("1000.00"),
                "change_abs": None,
                "change_pct": None,
            }
        ]
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
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
                    "2026-02-01",
                ],
            )

        assert result.exit_code == 0, result.stderr
        out = result.stdout + result.stderr
        series = next(line for line in out.splitlines() if "1,000.00" in line)
        assert "None" not in series
        assert UNKNOWN_CURRENCY in series

    @pytest.mark.unit
    def test_default_interval_monthly(self, runner: CliRunner) -> None:
        with (
            patch(
                "moneybin.cli.commands.reports.networth.get_database",
                return_value=no_profile_database(),
            ),
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
