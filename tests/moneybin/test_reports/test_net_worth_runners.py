"""core:net_worth_currencies / core:net_worth_accounts runners.

Reuses the account-grain model fixtures from ``test_net_worth_models.py``
(``model_db``, ``_install_net_worth_sources``, ``_install_report``, and the
``_account`` / ``_balance`` / ``_rate`` / ``_home`` builders) rather than
redefining a second copy of the same ``core.*`` stub schema.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
import typer
from typer.testing import CliRunner

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.contract import ReportQuery, ReportSpec, bound_value
from moneybin.reports.definitions._shared import (
    resolve_date_range,
    unanchored_candidates_ctes,
)
from moneybin.reports.definitions.net_worth import net_worth
from moneybin.tables import REPORTS_NET_WORTH, REPORTS_NET_WORTH_ACCOUNTS

# These sibling test modules deliberately name their fixture builders private
# (leading underscore) as an internal-to-the-file convention, not to fence
# off reuse: test_net_worth_models.py's own docstring says its helpers "are
# reused by later tasks in the net-worth SQL rungs plan ... so keep them
# general." Reusing them here (rather than a second copy of the same core.*
# stub schema) is exactly that reuse.
from tests.moneybin.test_reports.test_definitions import (
    _install_balance_drift,  # pyright: ignore[reportPrivateUsage]
)
from tests.moneybin.test_reports.test_net_worth_models import (
    _account,  # pyright: ignore[reportPrivateUsage]
    _balance,  # pyright: ignore[reportPrivateUsage]
    _home,  # pyright: ignore[reportPrivateUsage]
    _install_net_worth_sources,  # pyright: ignore[reportPrivateUsage]
    _install_report,  # pyright: ignore[reportPrivateUsage]
    _rate,  # pyright: ignore[reportPrivateUsage]
    _unanchored,  # pyright: ignore[reportPrivateUsage]
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


def test_resolve_date_range_default_latest_false_leaves_the_whole_history_open() -> (
    None
):
    """core:net_worth passes this when interval is given (see its docstring)."""
    rng = resolve_date_range(
        None,
        None,
        report_id="core:net_worth",
        view=REPORTS_NET_WORTH,
        default_latest=False,
    )
    assert rng.where_sql == ""
    assert rng.params == []
    assert rng.period is None


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


def test_balance_drift_envelope_omits_home_currency_despite_a_real_conversion(
    model_db: Database,
) -> None:
    """A converted report declaring no home-basis column never carries home_currency.

    Isolates ``_priced_home_currency``'s ``if not home_basis: return None``
    branch specifically: unlike a cash_flow read (which never converts at
    all, since it declares no ``fx_date``), this read genuinely converts —
    ``core:balance_drift`` declares ``fx_date="assertion_date"`` — so
    ``execution.applied_rates`` is non-empty. The only thing that can still
    withhold ``home_currency`` is the absence of any ``currency_basis="home"``
    column on this report.
    """
    _install_balance_drift(model_db)
    _seed_cached_rate(model_db, "USD", "EUR", date(2026, 4, 1), Decimal("0.90"))

    catalog = get_report_catalog()
    result = catalog.execute(
        model_db,
        report_id="core:balance_drift",
        parameters={},
        limit=100,
        display_currency="EUR",
        # A caller-supplied home_currency unrelated to this report's own
        # basis — present so `execution.home_currency` is non-None, which
        # isolates the `home_basis` check from the "no home_currency at all"
        # short-circuit ahead of it.
        home_currency="USD",
    )
    assert result.applied_rates  # a real, non-identity conversion happened
    envelope = result.to_envelope()
    assert envelope.summary.home_currency is None


def test_accounts_runner_envelope_omits_home_currency_when_every_home_basis_value_is_null(
    model_db: Database,
) -> None:
    """A converted net_worth_accounts read with account_balance_home all-NULL never carries summary.home_currency.

    Isolates ``_priced_home_currency``'s "no surviving row holds a value"
    check specifically: this report *does* declare a ``currency_basis="home"``
    column (unlike the balance_drift case above), and the row-basis column
    genuinely converts, but no profile home currency was ever set, so the
    view itself never priced ``account_balance_home`` on any row.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "UK Checking", "GBP")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "GBP")
    # Deliberately no _home(...) call: account_balance_home stays NULL for
    # every row (test_accounts_rung_null_home_currency_prices_nothing).
    _install_report(model_db, "net_worth_accounts")
    _seed_cached_rate(model_db, "GBP", "EUR", date(2026, 1, 5), Decimal("1.15"))

    catalog = get_report_catalog()
    result = catalog.execute(
        model_db,
        report_id="core:net_worth_accounts",
        parameters={},
        limit=100,
        display_currency="EUR",
        home_currency="USD",
    )
    assert result.applied_rates  # the row-basis column really converted
    assert all(row["account_balance_home"] is None for row in result.records)
    envelope = result.to_envelope()
    assert envelope.summary.home_currency is None


# ---------------------------------------------------------------------------
# core:net_worth — the day-grain rung
# ---------------------------------------------------------------------------


def test_net_worth_runner_latest_day_by_default(model_db: Database) -> None:
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _home(model_db, "USD")
    for day in ("2026-01-05", "2026-01-06", "2026-01-07"):
        _balance(model_db, "acct-a", day, "100.00", "USD")
        _rate(model_db, "USD", "USD", day, "1.0", source="identity")
    _install_report(model_db, "net_worth")

    from moneybin.reports.definitions.net_worth import net_worth

    rows = _run(model_db, net_worth(model_db))
    assert [row["balance_date"] for row in rows] == [date(2026, 1, 7)]


def test_net_worth_runner_monthly_takes_each_months_last_day_and_computes_change(
    model_db: Database,
) -> None:
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _home(model_db, "USD")
    balances = {
        "2026-01-05": "100.00",
        "2026-01-06": "110.00",
        "2026-01-07": "120.00",
        "2026-02-03": "200.00",
        "2026-02-04": "220.00",
        "2026-03-01": "300.00",
        "2026-03-02": "310.00",
        "2026-03-03": "330.00",
    }
    for day, balance in balances.items():
        _balance(model_db, "acct-a", day, balance, "USD")
        _rate(model_db, "USD", "USD", day, "1.0", source="identity")
    _install_report(model_db, "net_worth")

    from moneybin.reports.definitions.net_worth import net_worth

    rows = _run(model_db, net_worth(model_db, interval="monthly"))

    assert [row["balance_date"] for row in rows] == [
        date(2026, 1, 7),
        date(2026, 2, 4),
        date(2026, 3, 3),
    ]
    assert rows[0]["change_abs"] is None
    assert rows[0]["change_pct"] is None
    net_worth_0, net_worth_1, net_worth_2 = (
        rows[0]["net_worth"],
        rows[1]["net_worth"],
        rows[2]["net_worth"],
    )
    change_abs_1, change_abs_2 = rows[1]["change_abs"], rows[2]["change_abs"]
    change_pct_1 = rows[1]["change_pct"]
    assert isinstance(net_worth_0, Decimal)
    assert isinstance(net_worth_1, Decimal)
    assert isinstance(net_worth_2, Decimal)
    assert isinstance(change_abs_1, Decimal)
    assert isinstance(change_abs_2, Decimal)
    assert isinstance(change_pct_1, float)
    assert change_abs_1 == net_worth_1 - net_worth_0
    assert change_pct_1 == pytest.approx(  # pyright: ignore[reportUnknownMemberType]  # pytest.approx stub incomplete
        float(change_abs_1) / float(net_worth_0)
    )
    assert change_abs_2 == net_worth_2 - net_worth_1


def test_net_worth_runner_change_pct_is_null_when_prior_is_zero(
    model_db: Database,
) -> None:
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _account(model_db, "acct-b", "Credit", "USD")
    _home(model_db, "USD")
    # Month 1: an equal asset and liability net to exactly zero.
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _balance(model_db, "acct-b", "2026-01-05", "-100.00", "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    # Month 2: the liability clears; net worth becomes 100.
    _balance(model_db, "acct-a", "2026-02-05", "100.00", "USD")
    _balance(model_db, "acct-b", "2026-02-05", "0.00", "USD")
    _rate(model_db, "USD", "USD", "2026-02-05", "1.0", source="identity")
    _install_report(model_db, "net_worth")

    from moneybin.reports.definitions.net_worth import net_worth

    rows = _run(model_db, net_worth(model_db, interval="monthly"))

    assert rows[0]["net_worth"] == Decimal("0.00")
    assert rows[1]["net_worth"] == Decimal("100.00")
    assert rows[1]["change_abs"] == Decimal("100.00")
    assert rows[1]["change_pct"] is None


def test_net_worth_runner_weekly_buckets_start_on_monday(model_db: Database) -> None:
    """DuckDB's date_trunc('week', ...) is ISO-week Monday-start, not Sunday-start.

    A Sunday and the following Monday fall in different buckets under
    Monday-start (each is its own week's only day, so each survives as its
    own row); under Sunday-start they would share a bucket and only the
    later date would survive.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _home(model_db, "USD")
    for day in ("2026-01-04", "2026-01-05"):  # a Sunday, then the next Monday
        _balance(model_db, "acct-a", day, "100.00", "USD")
        _rate(model_db, "USD", "USD", day, "1.0", source="identity")
    _install_report(model_db, "net_worth")

    from moneybin.reports.definitions.net_worth import net_worth

    rows = _run(model_db, net_worth(model_db, interval="weekly"))

    assert [row["balance_date"] for row in rows] == [
        date(2026, 1, 4),
        date(2026, 1, 5),
    ]


def test_net_worth_default_columns_pin_the_fail_closed_guard_both_ways() -> None:
    """`unpriced_currency_count` survives in both the plain and bucketed sets.

    A null `change_abs`/`change_pct` reads the same whether the date itself
    is unpriced or it is merely the first returned bucket — only
    `unpriced_currency_count` tells the two apart, so it is never dropped.
    """
    from moneybin.reports.definitions.net_worth import (
        _default_columns,  # pyright: ignore[reportPrivateUsage]
    )

    assert _default_columns({}) == (
        "balance_date",
        "unpriced_currency_count",
        "net_worth",
    )
    assert _default_columns({"interval": "monthly"}) == (
        "balance_date",
        "unpriced_currency_count",
        "net_worth",
        "change_abs",
    )


def test_net_worth_runner_rejects_an_unknown_interval() -> None:
    """A bad `interval` is a `UserError`.

    Matching every other report parameter validation failure — not a bare
    `ValueError`.
    """
    from moneybin.reports.definitions.net_worth import net_worth

    with pytest.raises(UserError) as excinfo:
        net_worth(None, interval="yearly")  # type: ignore[arg-type,call-overload]  # validation precedes db use
    assert excinfo.value.code == "report_parameter_invalid_value"
    assert excinfo.value.details is not None
    assert excinfo.value.details["parameter"] == "interval"


def test_net_worth_runner_rejects_an_inverted_range(model_db: Database) -> None:
    """On a profile that has data.

    The raise precedes any query, so it never produces a row either way.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _home(model_db, "USD")

    from moneybin.reports.definitions.net_worth import net_worth

    with pytest.raises(UserError) as excinfo:
        net_worth(model_db, from_date="2026-03-01", to_date="2026-01-01")
    assert excinfo.value.code == "report_parameter_invalid_range"


def test_net_worth_runner_interval_is_a_cli_choice() -> None:
    # Local import: `cli_register` pulls in `moneybin.cli.output`, which
    # (via `moneybin.cli.__init__`) circularly re-imports `cli_register`
    # itself when nothing has primed the package first — a module-level
    # import here would break collection of this file in isolation.
    from moneybin.reports._framework.cli_register import register_report_cli

    spec = get_report_catalog().resolve("core:net_worth")
    assert isinstance(spec, ReportSpec)  # core:net_worth is @report-backed
    app = typer.Typer()
    register_report_cli(spec, app)
    app.command("noop")(lambda: None)

    result = CliRunner().invoke(app, ["net-worth", "--interval", "yearly"])

    assert result.exit_code == 2, result.output


def test_net_worth_runner_converted_read_keeps_its_identity(
    model_db: Database,
) -> None:
    """Converting into a third currency keeps net_worth == assets + liabilities.

    Every money column here is home-basis and converts independently, so
    without `on_converted` restating `net_worth`
    (`_recompute_net_worth_and_change`) the two sides would drift by a
    rounding cent.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-usd", "Checking", "USD")
    _account(model_db, "acct-eur", "Euro Checking", "EUR")
    _home(model_db, "USD")
    for day, usd_balance, eur_balance in (
        ("2026-01-05", "100.00", "50.00"),
        ("2026-01-06", "120.00", "-20.00"),
    ):
        _balance(model_db, "acct-usd", day, usd_balance, "USD")
        _balance(model_db, "acct-eur", day, eur_balance, "EUR")
        _rate(model_db, "USD", "USD", day, "1.0", source="identity")
        _rate(model_db, "EUR", "USD", day, "1.10")
    _install_report(model_db, "net_worth")
    _seed_cached_rate(model_db, "USD", "GBP", date(2026, 1, 5), Decimal("0.80"))
    _seed_cached_rate(model_db, "USD", "GBP", date(2026, 1, 6), Decimal("0.82"))

    catalog = get_report_catalog()
    result = catalog.execute(
        model_db,
        report_id="core:net_worth",
        parameters={"from_date": "2026-01-05", "to_date": "2026-01-06"},
        limit=100,
        display_currency="GBP",
        home_currency="USD",
    )

    assert result.applied_rates
    assert len(result.records) == 2
    for row in result.records:
        assert row["total_assets"] is not None  # every day priced
        assert row["net_worth"] == row["total_assets"] + row["total_liabilities"]
    envelope = result.to_envelope()
    assert envelope.summary.home_currency == "USD"


def test_net_worth_currencies_converted_read_keeps_both_identities(
    model_db: Database,
) -> None:
    """Each converted net worth still equals its converted components.

    At 0.5, 1.00 and -0.99 round half-up to 0.50 and -0.50, while their net
    of 0.01 rounds to 0.01 — so without `on_converted`
    (`_recompute_segment_totals`) net_worth reads 0.01 against components
    summing to 0.00, on both the native and the home-basis side.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _account(model_db, "acct-l", "Euro Card", "EUR")
    _home(model_db, "USD")
    _balance(model_db, "acct-a", "2026-01-05", "1.00", "EUR")
    _balance(model_db, "acct-l", "2026-01-05", "-0.99", "EUR")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.0")
    _install_report(model_db, "net_worth_currencies")
    _seed_cached_rate(model_db, "EUR", "GBP", date(2026, 1, 5), Decimal("0.5"))
    _seed_cached_rate(model_db, "USD", "GBP", date(2026, 1, 5), Decimal("0.5"))

    result = get_report_catalog().execute(
        model_db,
        report_id="core:net_worth_currencies",
        parameters={"from_date": "2026-01-05", "to_date": "2026-01-05"},
        limit=100,
        display_currency="GBP",
        home_currency="USD",
    )

    assert result.applied_rates
    (row,) = result.records
    assert row["total_assets"] == Decimal("0.50")
    assert row["total_liabilities"] == Decimal("-0.50")
    assert row["net_worth"] == Decimal("0.00")
    assert row["net_worth_home"] == (
        row["total_assets_home"] + row["total_liabilities_home"]
    )


def test_net_worth_currencies_repair_leaves_an_unpriced_segment_null() -> None:
    """An unpriced currency's home side stays NULL rather than becoming zero."""
    from moneybin.reports.definitions.net_worth_currencies import (
        _recompute_segment_totals,  # pyright: ignore[reportPrivateUsage]
    )

    row: dict[str, Any] = {
        "total_assets": Decimal("0.50"),
        "total_liabilities": Decimal("-0.50"),
        "net_worth": Decimal("0.01"),
        "total_assets_home": None,
        "total_liabilities_home": None,
        "net_worth_home": None,
    }

    _recompute_segment_totals([row], "GBP")

    assert row["net_worth"] == Decimal("0.00")
    assert row["net_worth_home"] is None


def test_net_worth_runner_mirrors_declared_column_order(model_db: Database) -> None:
    """Requirement: execution-order guard (column-ordering.md -> Enforcement).

    `change_abs`/`change_pct` are absent from the unbucketed projection
    (legal — the cash_flow precedent), so only the bucketed read is checked
    against the full declared tuple.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _home(model_db, "USD")
    _install_report(model_db, "net_worth")

    catalog = get_report_catalog()
    spec = catalog.resolve("core:net_worth")
    declared = [column.name for column in spec.columns]

    plain = catalog.execute(
        model_db, report_id="core:net_worth", parameters={}, limit=100
    )
    assert plain.columns == declared[:10]

    bucketed = catalog.execute(
        model_db,
        report_id="core:net_worth",
        parameters={"interval": "daily"},
        limit=100,
    )
    assert bucketed.columns == declared


# ---------------------------------------------------------------------------
# core:net_worth's ranged fallback for an unanchored candidate (M2B.3)
# ---------------------------------------------------------------------------


def _today(db: Database) -> date:
    row = db.execute("SELECT CURRENT_DATE").fetchone()
    assert row is not None
    return row[0]


def _nw_rows(db: Database, **params: str) -> list[dict[str, object]]:
    return _run(db, net_worth(db, **params))  # type: ignore[arg-type]


def _wholly_unanchored(db: Database, *, archived_at: str | None = None) -> None:
    _install_net_worth_sources(db)
    _home(db, "USD")
    _account(
        db,
        "brk",
        "Brokerage",
        "USD",
        archived=archived_at is not None,
        archived_at=archived_at,
    )
    _unanchored(db, "brk")
    _install_report(db, "net_worth")


def test_resolve_date_range_exposes_its_bounds() -> None:
    rng = resolve_date_range(
        "2026-01-01", None, report_id="core:net_worth", view=REPORTS_NET_WORTH
    )
    assert (rng.from_bound, rng.to_bound, rng.is_ranged) == ("2026-01-01", None, True)
    unranged = resolve_date_range(
        None, None, report_id="core:net_worth", view=REPORTS_NET_WORTH
    )
    assert unranged.is_ranged is False


def test_unanchored_candidates_ctes_refuses_an_unranged_read() -> None:
    unranged = resolve_date_range(
        None, None, report_id="core:net_worth", view=REPORTS_NET_WORTH
    )
    with pytest.raises(ValueError, match="explicit range"):
        unanchored_candidates_ctes(unranged)


def test_net_worth_runner_historical_range_synthesizes_at_to_date(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db)
    (row,) = _nw_rows(model_db, from_date="2025-01-01", to_date="2025-03-31")
    assert str(row["balance_date"]) == "2025-03-31"
    assert row["unanchored_account_count"] == 1
    assert row["account_count"] == 0
    assert row["net_worth"] is None
    assert row["home_currency_code"] == "USD"


def test_net_worth_runner_to_date_alone_synthesizes_at_to_date(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db)
    (row,) = _nw_rows(model_db, to_date="2025-03-31")
    assert str(row["balance_date"]) == "2025-03-31"


def test_net_worth_runner_future_only_lower_bound_synthesizes_nothing(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db)
    future = (_today(model_db) + timedelta(days=30)).isoformat()
    assert _nw_rows(model_db, from_date=future) == []


def test_net_worth_runner_future_single_day_synthesizes_nothing(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db)
    future = (_today(model_db) + timedelta(days=30)).isoformat()
    assert _nw_rows(model_db, from_date=future, to_date=future) == []


def test_net_worth_runner_dates_at_the_archive_floor(model_db: Database) -> None:
    """A range straddling archived_at dates the row there, never at to_date."""
    _wholly_unanchored(model_db, archived_at="2025-02-10")
    (row,) = _nw_rows(model_db, from_date="2025-01-01", to_date="2025-03-31")
    assert str(row["balance_date"]) == "2025-02-10"
    assert row["unanchored_account_count"] == 1


def test_net_worth_runner_range_after_archival_synthesizes_nothing(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db, archived_at="2025-02-10")
    assert _nw_rows(model_db, from_date="2025-03-01", to_date="2025-03-31") == []


def test_net_worth_runner_unranged_archived_candidate_is_empty(
    model_db: Database,
) -> None:
    """Unranged never reaches the fallback: the view's arm answers, correctly empty."""
    _wholly_unanchored(model_db, archived_at="2025-02-10")
    assert _nw_rows(model_db) == []


def test_net_worth_runner_unranged_wholly_unanchored_is_the_views_row(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db)
    (row,) = _nw_rows(model_db)
    assert row["balance_date"] == _today(model_db)
    assert row["unanchored_account_count"] == 1


def test_net_worth_runner_inverted_range_with_a_candidate_raises(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db)
    with pytest.raises(UserError) as excinfo:
        net_worth(model_db, from_date="2025-03-31", to_date="2025-01-01")
    assert excinfo.value.code == "report_parameter_invalid_range"


def test_net_worth_runner_range_with_rows_never_synthesizes(
    model_db: Database,
) -> None:
    """A non-empty filtered result is the answer; the fallback adds nothing."""
    _install_net_worth_sources(model_db)
    _home(model_db, "USD")
    _account(model_db, "chk", "Checking", "USD")
    _account(model_db, "brk", "Brokerage", "USD")
    _balance(model_db, "chk", "2025-02-01", "100.00", "USD")
    _unanchored(model_db, "brk")
    _install_report(model_db, "net_worth")
    rows = _nw_rows(model_db, from_date="2025-01-01", to_date="2025-03-31")
    assert [str(r["balance_date"]) for r in rows] == ["2025-02-01"]
    assert rows[0]["unanchored_account_count"] == 1


def test_net_worth_runner_range_before_every_balance_still_synthesizes(
    model_db: Database,
) -> None:
    """The trigger is the RANGE's emptiness, not the profile's."""
    _install_net_worth_sources(model_db)
    _home(model_db, "USD")
    _account(model_db, "chk", "Checking", "USD")
    _account(model_db, "brk", "Brokerage", "USD")
    _balance(model_db, "chk", "2025-06-01", "100.00", "USD")
    _unanchored(model_db, "brk")
    _install_report(model_db, "net_worth")
    (row,) = _nw_rows(model_db, from_date="2025-01-01", to_date="2025-03-31")
    assert str(row["balance_date"]) == "2025-03-31"
    assert row["unanchored_account_count"] == 1


def test_net_worth_runner_bucketed_range_with_no_spine_rows_synthesizes_one_bucket(
    model_db: Database,
) -> None:
    _wholly_unanchored(model_db)
    (row,) = _nw_rows(
        model_db, from_date="2025-01-01", to_date="2025-03-31", interval="monthly"
    )
    assert str(row["balance_date"]) == "2025-03-31"
    assert row["change_abs"] is None
    assert row["unanchored_account_count"] == 1


def test_net_worth_runner_range_without_candidates_is_empty(
    model_db: Database,
) -> None:
    _install_net_worth_sources(model_db)
    _install_report(model_db, "net_worth")
    assert _nw_rows(model_db, from_date="2025-01-01", to_date="2025-03-31") == []
