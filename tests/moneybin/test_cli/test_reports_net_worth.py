"""CLI tests for the three generated `reports net-worth*` commands.

`core:net_worth`, `core:net_worth_currencies`, and `core:net_worth_accounts`
are ordinary `@report` runners generated onto the CLI by
`register_reports_cli` — there is no hand-written command left
(reports-net-worth-sql-surface.md §Files to Delete). The generic
generated-command mechanism (text/json/wide/quiet/drift/truncation/errors) is
already covered report-agnostically in `test_cli_register.py` against a
synthetic spec; this file instead drives the three real, registered
net-worth commands by name — text rendering, `--output json`, and
`--display-currency` — proving the runners are actually wired to the CLI.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.execute import ReportResult
from tests.database_mocks import no_profile_database

runner = CliRunner()


def _result(
    records: list[dict[str, object]],
    columns: list[str],
    classes: dict[str, DataClass],
) -> ReportResult:
    return ReportResult(
        records=records,
        columns=columns,
        output_classes=classes,
        tier=Tier.HIGH,
        total_count=len(records),
        truncated=False,
    )


def test_net_worth_text_renders_the_day_grain_headline() -> None:
    result = _result(
        [
            {
                "home_currency_code": "USD",
                "balance_date": date(2026, 1, 31),
                "account_count": 3,
                "carried_forward_count": 0,
                "currency_count": 1,
                "unpriced_currency_count": 0,
                "total_assets": Decimal("15000.00"),
                "total_liabilities": Decimal("-2500.00"),
                "net_worth": Decimal("12500.00"),
            }
        ],
        [
            "home_currency_code",
            "balance_date",
            "account_count",
            "carried_forward_count",
            "currency_count",
            "unpriced_currency_count",
            "total_assets",
            "total_liabilities",
            "net_worth",
        ],
        {
            "home_currency_code": DataClass.CURRENCY,
            "balance_date": DataClass.TXN_DATE,
            "account_count": DataClass.AGGREGATE,
            "carried_forward_count": DataClass.AGGREGATE,
            "currency_count": DataClass.AGGREGATE,
            "unpriced_currency_count": DataClass.AGGREGATE,
            "total_assets": DataClass.BALANCE,
            "total_liabilities": DataClass.BALANCE,
            "net_worth": DataClass.BALANCE,
        },
    )
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = result
        invocation = runner.invoke(app, ["reports", "net-worth"])

    assert invocation.exit_code == 0, invocation.output
    out = invocation.stdout
    # The default text projection: home_currency_code, balance_date,
    # unpriced_currency_count, net_worth — total_assets/total_liabilities stay
    # --wide-only. Requirement 9: net_worth is a home-currency total with no
    # other column naming its denomination, so the currency must be visible.
    assert "USD" in out
    assert "12,500.00" in out
    assert "15,000.00" not in out
    call = mock_catalog.return_value.execute.call_args
    assert call.kwargs["report_id"] == "core:net_worth"


def test_net_worth_currencies_json_carries_every_projected_column() -> None:
    result = _result(
        [
            {
                "currency_code": "USD",
                "home_currency_code": "USD",
                "rate_source": "identity",
                "balance_date": date(2026, 1, 31),
                "rate_published_date": date(2026, 1, 31),
                "account_count": 2,
                "carried_forward_count": 0,
                "total_assets": Decimal("15000.00"),
                "total_liabilities": Decimal("-2500.00"),
                "net_worth": Decimal("12500.00"),
                "total_assets_home": Decimal("15000.00"),
                "total_liabilities_home": Decimal("-2500.00"),
                "net_worth_home": Decimal("12500.00"),
            }
        ],
        [
            "currency_code",
            "home_currency_code",
            "rate_source",
            "balance_date",
            "rate_published_date",
            "account_count",
            "carried_forward_count",
            "total_assets",
            "total_liabilities",
            "net_worth",
            "total_assets_home",
            "total_liabilities_home",
            "net_worth_home",
        ],
        {
            "currency_code": DataClass.CURRENCY,
            "home_currency_code": DataClass.CURRENCY,
            "rate_source": DataClass.TXN_TYPE,
            "balance_date": DataClass.TXN_DATE,
            "rate_published_date": DataClass.TXN_DATE,
            "account_count": DataClass.AGGREGATE,
            "carried_forward_count": DataClass.AGGREGATE,
            "total_assets": DataClass.BALANCE,
            "total_liabilities": DataClass.BALANCE,
            "net_worth": DataClass.BALANCE,
            "total_assets_home": DataClass.BALANCE,
            "total_liabilities_home": DataClass.BALANCE,
            "net_worth_home": DataClass.BALANCE,
        },
    )
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = result
        invocation = runner.invoke(
            app, ["reports", "net-worth-currencies", "--output", "json"]
        )

    assert invocation.exit_code == 0, invocation.output
    payload = json.loads(invocation.stdout)
    assert payload["status"] == "ok"
    row = payload["data"][0]
    # JSON always carries the whole projection, unlike the narrowed text table.
    assert row["currency_code"] == "USD"
    assert row["account_count"] == 2
    assert "total_assets" in row
    assert "net_worth_home" in row
    call = mock_catalog.return_value.execute.call_args
    assert call.kwargs["report_id"] == "core:net_worth_currencies"


def test_net_worth_accounts_forwards_display_currency_to_the_catalog() -> None:
    """``--display-currency`` reaches the catalog call, not the runner's params.

    It is a framework option every generated command shares
    (`test_cli_command_forwards_display_currency_to_the_catalog` proves the
    mechanism generically); this pins it for the real, registered
    `core:net_worth_accounts` command specifically.
    """
    result = _result(
        [
            {
                "account_id": "acct_11112222",
                "account_name": "Checking",
                "currency_code": "USD",
                "home_currency_code": "USD",
                "account_type": "depository",
                "is_observed": True,
                "observation_source": "ofx",
                "rate_source": "identity",
                "balance_date": date(2026, 1, 31),
                "rate_published_date": date(2026, 1, 31),
                "days_since_observed": 0,
                "reconciliation_delta": None,
                "account_balance": Decimal("5000.00"),
                "account_balance_home": Decimal("5000.00"),
            }
        ],
        [
            "account_id",
            "account_name",
            "currency_code",
            "home_currency_code",
            "account_type",
            "is_observed",
            "observation_source",
            "rate_source",
            "balance_date",
            "rate_published_date",
            "days_since_observed",
            "reconciliation_delta",
            "account_balance",
            "account_balance_home",
        ],
        {
            "account_id": DataClass.RECORD_ID,
            "account_name": DataClass.USER_NOTE,
            "currency_code": DataClass.CURRENCY,
            "home_currency_code": DataClass.CURRENCY,
            "account_type": DataClass.TXN_TYPE,
            "is_observed": DataClass.TXN_TYPE,
            "observation_source": DataClass.TXN_TYPE,
            "rate_source": DataClass.TXN_TYPE,
            "balance_date": DataClass.TXN_DATE,
            "rate_published_date": DataClass.TXN_DATE,
            "days_since_observed": DataClass.AGGREGATE,
            "reconciliation_delta": DataClass.BALANCE,
            "account_balance": DataClass.BALANCE,
            "account_balance_home": DataClass.BALANCE,
        },
    )
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = result
        invocation = runner.invoke(
            app,
            [
                "reports",
                "net-worth-accounts",
                "--display-currency",
                "EUR",
                "--output",
                "json",
            ],
        )

    assert invocation.exit_code == 0, invocation.output
    call = mock_catalog.return_value.execute.call_args
    assert call.kwargs["report_id"] == "core:net_worth_accounts"
    assert call.kwargs["display_currency"] == "EUR"
    assert "display_currency" not in call.kwargs["parameters"]
