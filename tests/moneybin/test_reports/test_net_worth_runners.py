"""core:net_worth_currencies / core:net_worth_accounts runners.

Reuses the account-grain model fixtures from ``test_net_worth_models.py``
(``model_db``, ``_install_net_worth_sources``, ``_install_report``, and the
``_account`` / ``_balance`` / ``_rate`` / ``_home`` builders) rather than
redefining a second copy of the same ``core.*`` stub schema.
"""

from __future__ import annotations

from datetime import date, datetime
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
)
from moneybin.reports.definitions.net_worth import net_worth
from moneybin.reports.definitions.net_worth_accounts import net_worth_accounts
from moneybin.tables import REPORTS_NET_WORTH, REPORTS_NET_WORTH_ACCOUNTS
from tests.moneybin.db_helpers import CORE_FCT_INVESTMENT_TRANSACTIONS_DDL

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
)
from tests.moneybin.test_reports.test_net_worth_models import model_db as _model_db

# Re-exported under its original fixture name so pytest resolves it for the
# test functions below by parameter name, without a second copy of its body.
model_db = _model_db

pytestmark = pytest.mark.unit


def test_unprojected_account_makes_total_incomplete(
    model_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.connectors.sync_client import SyncClient
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    def forbidden_broker(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("A report must not construct a broker client")

    monkeypatch.setattr(SyncClient, "__init__", forbidden_broker)
    _install_net_worth_sources(model_db)
    _account(model_db, "visible", "Synthetic checking", "USD")
    _balance(model_db, "visible", "2026-09-20", "100.00", "USD")
    _home(model_db, "USD")
    _rate(model_db, "USD", "USD", "2026-09-20", "1.00")
    _install_report(model_db, "net_worth")
    load(model_db, source())

    result = get_report_catalog().execute(
        model_db, report_id="core:net_worth", parameters={}, limit=10
    )

    assert result.records[0]["unpriced_currency_count"] == 0
    assert result.records[0]["net_worth"] == Decimal("100.00")
    summary = result.to_envelope().to_dict()["summary"]
    coverage = summary.get("sync_completeness")
    assert coverage is not None, (
        "A fully priced visible subtotal must disclose the missing source account"
    )
    assert coverage["complete"] is False
    assert any(r["kind"] == "unresolved_identity" for r in coverage["reasons"])
    assert result.degraded is True
    assert "native-connection" not in result.to_envelope().to_json()


def test_profile_without_sync_evidence_has_no_source_completeness_claim(
    model_db: Database,
) -> None:
    _install_net_worth_sources(model_db)
    _account(model_db, "visible", "Synthetic checking", "USD")
    _balance(model_db, "visible", "2026-09-20", "100.00", "USD")
    _home(model_db, "USD")
    _rate(model_db, "USD", "USD", "2026-09-20", "1.00")
    _install_report(model_db, "net_worth")

    result = get_report_catalog().execute(
        model_db, report_id="core:net_worth", parameters={}, limit=10
    )

    assert "sync_completeness" not in result.to_envelope().to_dict()["summary"]
    assert result.degraded is False


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
        "home_currency_code",
        "balance_date",
        "unpriced_currency_count",
        "net_worth",
    )
    assert _default_columns({"interval": "monthly"}) == (
        "home_currency_code",
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


def test_balance_drift_without_sync_evidence_preserves_existing_contract(
    model_db: Database,
) -> None:
    _install_balance_drift(model_db)

    result = get_report_catalog().execute(
        model_db,
        report_id="core:balance_drift",
        parameters={"account": "Beta"},
        limit=10,
    )

    assert len(result.records) == 1
    assert result.records[0]["account_id"] == "A2"
    assert "sync_completeness" not in result.to_envelope().to_dict()["summary"]


@pytest.mark.parametrize("name", ["net_worth_accounts", "net_worth_currencies"])
def test_net_worth_variants_disclose_unprojected_scope(
    model_db: Database, name: str
) -> None:
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    _install_net_worth_sources(model_db)
    _account(model_db, "visible", "Synthetic checking", "USD")
    _balance(model_db, "visible", "2026-09-20", "100.00", "USD")
    _install_report(model_db, name)
    load(model_db, source())

    result = get_report_catalog().execute(
        model_db, report_id=f"core:{name}", parameters={}, limit=10
    )

    assert result.records
    coverage = result.to_envelope().to_dict()["summary"]["sync_completeness"]
    assert coverage["complete"] is False
    assert "unresolved_identity" in {reason["kind"] for reason in coverage["reasons"]}


def test_source_metadata_survives_fx_fallback_drift_and_truncation(
    model_db: Database,
) -> None:
    from moneybin.reports._framework.catalog import ReportCatalog, ReportStatus
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    _install_net_worth_sources(model_db)
    _account(model_db, "visible-usd", "Synthetic USD", "USD")
    _account(model_db, "visible-eur", "Synthetic EUR", "EUR")
    _balance(model_db, "visible-usd", "2026-09-20", "100.00", "USD")
    _balance(model_db, "visible-eur", "2026-09-20", "200.00", "EUR")
    _install_report(model_db, "net_worth_accounts")
    load(model_db, source())
    spec = get_report_catalog().resolve("core:net_worth_accounts")
    catalog = ReportCatalog(
        [spec],
        status={
            spec.report_id: ReportStatus(
                degraded=True, degraded_reason="Synthetic saved definition drift"
            )
        },
    )

    result = catalog.execute(
        model_db,
        report_id=spec.report_id,
        parameters={},
        display_currency="GBP",
        limit=1,
    )

    assert len(result.records) == 1
    assert result.truncated is True
    assert result.degraded is True
    assert result.degraded_reason is not None
    assert "Synthetic saved definition drift" in result.degraded_reason
    assert "unresolved_identity" in result.degraded_reason
    assert "EUR->GBP is unfetched" in result.degraded_reason
    assert "moneybin profile set display_currency_targets GBP" in result.degraded_reason
    assert "'moneybin refresh'" in result.degraded_reason
    assert result.records[0]["currency_code"] == "EUR"
    assert result.records[0]["account_balance"] == Decimal("200.00")
    assert (
        result.to_envelope().to_dict()["summary"]["sync_completeness"]["complete"]
        is False
    )


def test_balance_drift_resolved_filter_excludes_unrelated_held_scope(
    model_db: Database,
) -> None:
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source
    from tests.moneybin.test_services.test_sync_completeness import (
        _bind,  # pyright: ignore[reportPrivateUsage]
        _history,  # pyright: ignore[reportPrivateUsage]
    )

    data = source()
    data.batches.extend(
        source(batch=2, account_changes={"id": "held", "balance": "100.001"}).batches
    )
    data.history = _history(2)
    data.history.accounts.append(
        data.history.accounts[0].model_copy(update={"account_id": "held"})
    )
    load(model_db, data)
    _bind(model_db, canonical_id="selected")
    _bind(model_db, "held", "unrelated")
    model_db.execute("""UPDATE core.dim_accounts SET display_name = 'Selected account'
        WHERE account_id = 'selected'""")
    model_db.execute("""INSERT INTO app.balance_assertions
        (account_id, assertion_date, balance)
        VALUES ('selected', DATE '2026-09-20', 100.25)""")
    model_db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _install_report(model_db, "balance_drift")

    result = get_report_catalog().execute(
        model_db,
        report_id="core:balance_drift",
        parameters={"account": "Selected account"},
        limit=10,
    )

    assert len(result.records) == 1
    assert result.records[0]["account_id"] == "selected"
    coverage = result.to_envelope().to_dict()["summary"]["sync_completeness"]
    assert coverage["complete"] is True
    assert coverage["reasons"] == []


def _nw_rows(db: Database, **params: str) -> list[dict[str, object]]:
    return _run(db, net_worth(db, **params))  # type: ignore[arg-type]


def _acct_rows(db: Database, **params: str) -> list[dict[str, object]]:
    return _run(db, net_worth_accounts(db, **params))  # type: ignore[arg-type]


def _mixed(db: Database) -> None:
    """Chk anchored 2025-01-01..2025-01-31; brk unanchored from 2025-01-20."""
    _install_net_worth_sources(db)
    _home(db, "USD")
    _account(db, "chk", "Checking", "USD")
    _account(db, "brk", "Brokerage", "USD", account_type="investment")
    for day in range(1, 32):
        _rate(db, "USD", "USD", f"2025-01-{day:02d}", "1", source="identity")
    _balance(db, "chk", "2025-01-01", "100.00", "USD")
    for day in range(2, 32):
        _balance(db, "chk", f"2025-01-{day:02d}", "100.00", "USD", observed=False)
    for day in range(20, 32):
        _balance(db, "brk", f"2025-01-{day:02d}", None, "USD")
    _install_report(db, "net_worth")
    _install_report(db, "net_worth_accounts")


def test_net_worth_ranged_and_unranged_reads_agree_on_the_latest_date(
    model_db: Database,
) -> None:
    _mixed(model_db)
    (unranged,) = _nw_rows(model_db)
    ranged = _nw_rows(model_db, from_date="2025-01-25", to_date="2025-01-31")
    assert ranged[-1] == unranged
    assert unranged["unanchored_account_count"] == 1
    assert unranged["net_worth"] is None


def test_net_worth_range_before_first_evidence_has_no_unanchored_row(
    model_db: Database,
) -> None:
    """Brk had no evidence before 2025-01-20, so the range is complete without it."""
    _mixed(model_db)
    rows = _nw_rows(model_db, from_date="2025-01-01", to_date="2025-01-10")
    assert len(rows) == 10
    assert all(r["unanchored_account_count"] == 0 for r in rows)
    assert all(r["net_worth"] == Decimal("100.00") for r in rows)


def test_net_worth_range_after_the_spine_end_is_empty(model_db: Database) -> None:
    _mixed(model_db)
    assert _nw_rows(model_db, from_date="2025-02-01", to_date="2025-02-28") == []


def test_net_worth_bucketed_read_withholds_the_bucket_with_a_null_balance(
    model_db: Database,
) -> None:
    _mixed(model_db)
    (row,) = _nw_rows(
        model_db, from_date="2025-01-01", to_date="2025-01-31", interval="monthly"
    )
    assert str(row["balance_date"]) == "2025-01-31"
    assert row["unanchored_account_count"] == 1
    assert row["net_worth"] is None
    assert row["change_abs"] is None


def _null_balance_spine(db: Database, *, currency: str | None = "USD") -> None:
    _install_net_worth_sources(db)
    _home(db, "USD")
    _account(db, "brk", "Brokerage", currency, account_type="investment")
    _balance(db, "brk", "2025-03-31", None, currency)


@pytest.mark.parametrize("interval", [None, "daily", "monthly"])
def test_unanchored_null_total_retains_incomplete_sync_metadata(
    model_db: Database, interval: str | None
) -> None:
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    _null_balance_spine(model_db)
    _install_report(model_db, "net_worth")
    load(model_db, source())
    parameters = {"from_date": "2025-03-31", "to_date": "2025-03-31"}
    if interval is not None:
        parameters["interval"] = interval

    result = get_report_catalog().execute(
        model_db, report_id="core:net_worth", parameters=parameters, limit=10
    )

    (row,) = result.records
    assert str(row["balance_date"]) == "2025-03-31"
    assert row["unanchored_account_count"] == 1
    assert row["net_worth"] is None
    assert row["total_assets"] is None
    assert row["total_liabilities"] is None
    coverage = result.to_envelope().to_dict()["summary"]["sync_completeness"]
    assert coverage["complete"] is False
    assert "unresolved_identity" in {reason["kind"] for reason in coverage["reasons"]}
    assert result.degraded is True


def test_ranged_unanchored_account_retains_incomplete_sync_metadata(
    model_db: Database,
) -> None:
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    _null_balance_spine(model_db)
    _install_report(model_db, "net_worth_accounts")
    load(model_db, source())

    result = get_report_catalog().execute(
        model_db,
        report_id="core:net_worth_accounts",
        parameters={"from_date": "2025-03-31", "to_date": "2025-03-31"},
        limit=10,
    )

    (row,) = result.records
    assert row["account_id"] == "brk"
    assert str(row["balance_date"]) == "2025-03-31"
    assert row["account_balance"] is None
    assert row["account_balance_home"] is None
    coverage = result.to_envelope().to_dict()["summary"]["sync_completeness"]
    assert coverage["complete"] is False
    assert "unresolved_identity" in {reason["kind"] for reason in coverage["reasons"]}
    assert result.degraded is True


def test_accounts_ranged_and_unranged_reads_agree_on_the_latest_date(
    model_db: Database,
) -> None:
    _mixed(model_db)
    unranged = _acct_rows(model_db)
    ranged = _acct_rows(model_db, from_date="2025-01-31", to_date="2025-01-31")
    assert ranged == unranged
    assert {r["account_id"] for r in unranged} == {"chk", "brk"}


def test_accounts_range_before_first_evidence_omits_the_unanchored_account(
    model_db: Database,
) -> None:
    _mixed(model_db)
    rows = _acct_rows(model_db, from_date="2025-01-01", to_date="2025-01-10")
    assert {r["account_id"] for r in rows} == {"chk"}


def test_runner_sql_reads_no_candidate_relation() -> None:
    """Both runners filter the view by date and nothing else (Requirement 6)."""
    for rq in (
        net_worth(None, from_date="2025-01-01", to_date="2025-01-31"),  # type: ignore[arg-type]
        net_worth_accounts(None, from_date="2025-01-01", to_date="2025-01-31"),  # type: ignore[arg-type]
    ):
        assert "unanchored_candidates" not in rq.sql
        assert "dim_unanchored_accounts" not in rq.sql
        assert "UNION ALL" not in rq.sql


def test_accounts_range_after_the_spine_end_is_empty(model_db: Database) -> None:
    _mixed(model_db)
    assert _acct_rows(model_db, from_date="2025-02-01") == []


def test_unknown_currency_null_segment_retains_incomplete_sync_metadata(
    model_db: Database,
) -> None:
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    _null_balance_spine(model_db, currency=None)
    _account(model_db, "chk", "Checking", "USD")
    _balance(model_db, "chk", "2025-03-31", "100.00", "USD")
    _rate(model_db, "USD", "USD", "2025-03-31", "1", source="identity")
    _install_report(model_db, "net_worth_currencies")
    load(model_db, source())

    result = get_report_catalog().execute(
        model_db,
        report_id="core:net_worth_currencies",
        parameters={"from_date": "2025-03-31", "to_date": "2025-03-31"},
        limit=10,
    )

    assert len(result.records) == 2
    segments = {row["currency_code"]: row for row in result.records}
    assert segments["USD"]["net_worth"] == Decimal("100.00")
    assert segments["USD"]["unanchored_account_count"] == 0
    unknown = segments[None]
    assert unknown["unanchored_account_count"] == 1
    for measure in (
        "total_assets",
        "total_liabilities",
        "net_worth",
        "total_assets_home",
        "total_liabilities_home",
        "net_worth_home",
    ):
        assert unknown[measure] is None
    coverage = result.to_envelope().to_dict()["summary"]["sync_completeness"]
    assert coverage["complete"] is False
    assert "unresolved_identity" in {reason["kind"] for reason in coverage["reasons"]}
    assert result.degraded is True


@pytest.mark.parametrize(
    "name", ["net_worth", "net_worth_accounts", "net_worth_currencies"]
)
def test_range_before_balance_evidence_is_empty_with_incomplete_sync_metadata(
    model_db: Database, name: str
) -> None:
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    _null_balance_spine(model_db)
    _install_report(model_db, name)
    load(model_db, source())

    result = get_report_catalog().execute(
        model_db,
        report_id=f"core:{name}",
        parameters={"from_date": "2025-03-01", "to_date": "2025-03-30"},
        limit=10,
    )

    assert result.records == []
    coverage = result.to_envelope().to_dict()["summary"]["sync_completeness"]
    assert coverage["complete"] is False
    assert "unresolved_identity" in {reason["kind"] for reason in coverage["reasons"]}
    assert result.degraded is True
