"""core:net_worth_currencies / core:net_worth_accounts runners.

Reuses the account-grain model fixtures from ``test_net_worth_models.py``
(``model_db``, ``_install_net_worth_sources``, ``_install_report``, and the
``_account`` / ``_balance`` / ``_rate`` / ``_home`` builders) rather than
redefining a second copy of the same ``core.*`` stub schema.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.contract import ReportQuery, bound_value
from moneybin.reports.definitions._shared import resolve_date_range
from moneybin.tables import REPORTS_NET_WORTH_ACCOUNTS

# These sibling test modules deliberately name their fixture builders private
# (leading underscore) as an internal-to-the-file convention, not to fence
# off reuse: test_net_worth_models.py's own docstring says its helpers "are
# reused by later tasks in the net-worth SQL rungs plan ... so keep them
# general." Reusing them here (rather than a second copy of the same core.*
# stub schema) is exactly that reuse.
from tests.moneybin.test_reports.test_definitions import (
    _install_cash_flow_view,  # pyright: ignore[reportPrivateUsage]
)
from tests.moneybin.test_reports.test_net_worth_models import (
    _account,  # pyright: ignore[reportPrivateUsage]
    _balance,  # pyright: ignore[reportPrivateUsage]
    _home,  # pyright: ignore[reportPrivateUsage]
    _install_net_worth_sources,  # pyright: ignore[reportPrivateUsage]
    _install_report,  # pyright: ignore[reportPrivateUsage]
    _rate,  # pyright: ignore[reportPrivateUsage]
)
from tests.moneybin.test_reports.test_net_worth_models import model_db as _model_db

# Re-exported under its original fixture name so pytest resolves it for the
# test functions below by parameter name, without a second copy of its body.
model_db = _model_db

pytestmark = pytest.mark.unit


def _run(db: Database, rq: ReportQuery) -> list[dict[str, object]]:
    """Execute a runner's query and return rows as name-keyed dicts."""
    params = rq.params
    bindings = (
        [bound_value(v) for v in params.values()]
        if isinstance(params, dict)
        else [bound_value(v) for v in params]
    )
    cursor = db.execute(rq.sql, bindings)
    columns = [d[0] for d in cursor.description] if cursor.description else []
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _seed_cached_rate(
    db: Database, base: str, quote: str, on: date, rate: Decimal
) -> None:
    """Put one provider rate in CurrencyService's cache, as a refresh would."""
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type, loaded_at)
        VALUES (?, ?, ?, ?, 'frankfurter', ?)
        """,
        [base, quote, on, rate, datetime(2026, 1, 5, 12, 0, 0)],
    )


# ---------------------------------------------------------------------------
# Step 1/2: resolve_date_range
# ---------------------------------------------------------------------------


def test_resolve_date_range_rejects_an_inverted_range() -> None:
    with pytest.raises(UserError) as excinfo:
        resolve_date_range(
            "2026-03-01",
            "2026-01-01",
            report_id="core:net_worth_accounts",
            view=REPORTS_NET_WORTH_ACCOUNTS,
        )
    assert excinfo.value.code == "report_parameter_invalid_range"


@pytest.mark.parametrize("bad", ["2026-02-30", "2026/01/01"])
def test_resolve_date_range_rejects_a_malformed_bound(bad: str) -> None:
    with pytest.raises(UserError) as excinfo:
        resolve_date_range(
            bad,
            None,
            report_id="core:net_worth_accounts",
            view=REPORTS_NET_WORTH_ACCOUNTS,
        )
    assert excinfo.value.code == "report_parameter_invalid_value"


def test_resolve_date_range_to_date_alone_stays_open_below() -> None:
    rng = resolve_date_range(
        None,
        "2026-03-31",
        report_id="core:net_worth_accounts",
        view=REPORTS_NET_WORTH_ACCOUNTS,
    )
    assert rng.where_sql == " AND balance_date <= ?"
    assert [b.value for b in rng.params] == ["2026-03-31"]


def test_resolve_date_range_neither_bound_defaults_to_the_latest_day() -> None:
    rng = resolve_date_range(
        None, None, report_id="core:net_worth_accounts", view=REPORTS_NET_WORTH_ACCOUNTS
    )
    assert rng.where_sql == (
        " AND balance_date = (SELECT MAX(balance_date) FROM reports.net_worth_accounts)"
    )
    assert rng.params == []


# ---------------------------------------------------------------------------
# Step 6: runner tests against a real model database
# ---------------------------------------------------------------------------


def test_currencies_runner_defaults_to_the_latest_day(model_db: Database) -> None:
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    for day in ("2026-01-05", "2026-01-06", "2026-01-07"):
        _balance(model_db, "acct-a", day, "100.00", "USD")
    _install_report(model_db, "net_worth_currencies")

    from moneybin.reports.definitions.net_worth_currencies import net_worth_currencies

    rows = _run(model_db, net_worth_currencies(model_db))
    assert [row["balance_date"] for row in rows] == [date(2026, 1, 7)]


def test_currencies_runner_to_date_alone_stays_open_below(model_db: Database) -> None:
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    for day in ("2026-01-05", "2026-01-06", "2026-01-07"):
        _balance(model_db, "acct-a", day, "100.00", "USD")
    _install_report(model_db, "net_worth_currencies")

    from moneybin.reports.definitions.net_worth_currencies import net_worth_currencies

    rows = _run(model_db, net_worth_currencies(model_db, to_date="2026-01-06"))
    assert [row["balance_date"] for row in rows] == [
        date(2026, 1, 5),
        date(2026, 1, 6),
    ]


def test_accounts_runner_rejects_an_inverted_range_before_querying(
    model_db: Database,
) -> None:
    from moneybin.reports.definitions.net_worth_accounts import net_worth_accounts

    with pytest.raises(UserError) as excinfo:
        net_worth_accounts(model_db, from_date="2026-03-01", to_date="2026-01-01")
    assert excinfo.value.code == "report_parameter_invalid_range"


def test_net_worth_runners_mirror_declared_column_order(model_db: Database) -> None:
    """Requirement: execution-order guard (column-ordering.md -> Enforcement)."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _install_report(model_db, "net_worth_currencies")
    _install_report(model_db, "net_worth_accounts")

    catalog = get_report_catalog()
    for report_id in ("core:net_worth_currencies", "core:net_worth_accounts"):
        spec = catalog.resolve(report_id)
        result = catalog.execute(
            model_db, report_id=report_id, parameters={}, limit=100
        )
        assert result.columns == [c.name for c in spec.columns]


def test_accounts_runner_display_conversion_does_not_double_price(
    model_db: Database,
) -> None:
    """Converting into the home currency leaves account_balance_home unchanged."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "250.00", "USD")
    _home(model_db, "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _install_report(model_db, "net_worth_accounts")

    catalog = get_report_catalog()
    result = catalog.execute(
        model_db,
        report_id="core:net_worth_accounts",
        parameters={},
        limit=100,
        display_currency="USD",
        home_currency="USD",
    )
    row = result.records[0]
    assert row["account_balance"] == Decimal("250.00")
    assert row["account_balance_home"] == Decimal("250.00")


def test_accounts_runner_display_conversion_prices_home_basis_from_home(
    model_db: Database,
) -> None:
    """A third-currency read prices account_balance_home FROM home, not currency_code.

    A GBP account's own balance converts at the GBP->EUR rate; its
    account_balance_home (already in USD, the home currency) must convert at
    the USD->EUR rate instead. Using the row's own GBP->EUR rate for both
    would produce a different (wrong) number, so the two rates are chosen to
    disagree.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "UK Checking", "GBP")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "GBP")
    _home(model_db, "USD")
    # Priced by the view itself: account_balance_home = 100.00 * 1.25 = 125.00 USD.
    _rate(model_db, "GBP", "USD", "2026-01-05", "1.25")
    _install_report(model_db, "net_worth_accounts")
    _seed_cached_rate(model_db, "GBP", "EUR", date(2026, 1, 5), Decimal("1.15"))
    _seed_cached_rate(model_db, "USD", "EUR", date(2026, 1, 5), Decimal("0.90"))

    catalog = get_report_catalog()
    result = catalog.execute(
        model_db,
        report_id="core:net_worth_accounts",
        parameters={},
        limit=100,
        display_currency="EUR",
        home_currency="USD",
    )
    row = result.records[0]
    assert row["account_balance"] == Decimal("115.00")  # 100.00 GBP * 1.15
    assert row["account_balance_home"] == Decimal("112.50")  # 125.00 USD * 0.90

    envelope = result.to_envelope()
    assert envelope.summary.home_currency == "USD"


def test_cash_flow_envelope_omits_home_currency(model_db: Database) -> None:
    """A report with no home-basis column never carries summary.home_currency."""
    _install_cash_flow_view(model_db)

    catalog = get_report_catalog()
    result = catalog.execute(
        model_db,
        report_id="core:cash_flow",
        parameters={"by": "account-and-category"},
        limit=100,
        display_currency="EUR",
    )
    envelope = result.to_envelope()
    assert envelope.summary.home_currency is None
    assert "home_currency" not in envelope.to_dict()["summary"]
