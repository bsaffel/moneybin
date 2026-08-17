"""Display conversion of report rows — multi-currency.md Requirements 9 and 15.

The conversion primitive is exercised against a real ``CurrencyService`` over a
real cache rather than a stub: the invariant under test is that a report read
resolves from stored rates and never reaches a provider, and a stub adapter
would satisfy the assertions while proving nothing about that.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.catalog import ReportCatalog
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    ReportSpec,
)
from moneybin.reports._framework.convert import convert_records
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    convert_execution,
)
from moneybin.reports.definitions.balance_drift import (
    _rebucket_status,  # pyright: ignore[reportPrivateUsage]  # the repair under test
)
from moneybin.reports.service_reports import (
    _restate_networth_total,  # pyright: ignore[reportPrivateUsage]  # ditto
)
from moneybin.services.currency_service import CurrencyService
from moneybin.tables import TableRef

_CLASSES: Mapping[str, DataClass] = {
    "txn_date": DataClass.TXN_DATE,
    "currency_code": DataClass.CURRENCY,
    "amount": DataClass.TXN_AMOUNT,
    "txn_count": DataClass.AGGREGATE,
}


def _semantics(
    *,
    fx_date: str | None = "txn_date",
    fx_basis: str | None = (
        "converted per row at its own date when a display currency is given"
    ),
) -> ReportSemantics:
    return ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="negative expense; positive income",
        kind="flow",
        valuation_basis="transaction amount",
        fx_basis=fx_basis,
        time_basis="transaction date",
        denominator=None,
        comparison_window=None,
        exclusions=(),
        provenance=("reports.test_summary",),
        fx_date=fx_date,
    )


def _seed_rate(db: Database, base: str, quote: str, on: date, rate: Decimal) -> None:
    """Put one provider rate in the cache, as a refresh backfill would."""
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type, loaded_at)
        VALUES (?, ?, ?, ?, 'frankfurter', ?)
        """,
        [base, quote, on, rate, datetime(2026, 3, 6, 12, 0, 0)],
    )


def _row(**overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "txn_date": date(2026, 3, 5),
        "currency_code": "EUR",
        "amount": Decimal("100.00"),
        "txn_count": 3,
    }
    record.update(overrides)
    return record


def test_converts_a_money_column_at_the_rows_own_date(saved_db: Database) -> None:
    # 100.00 EUR at 1.09 USD/EUR = 109.00 USD, derived before running anything.
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is None
    assert outcome.display_currency == "USD"
    assert outcome.records[0]["amount"] == Decimal("109.00")


def test_converted_rows_report_the_display_currency_not_the_original(
    saved_db: Database,
) -> None:
    """A converted row whose ``currency_code`` still said EUR would re-segment.

    Every consumer that groups by ``currency_code`` — the net-worth combined
    total, the envelope's own resolver — would read a USD amount as EUR.
    """
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.records[0]["currency_code"] == "USD"


def test_a_converted_row_carries_the_rate_that_priced_it(saved_db: Database) -> None:
    """Requirement 10: the rate behind a converted figure stays reachable.

    ``convert_records`` is the last place that still knows it — the rate is
    folded into the amount and ``currency_code`` is relabelled to the target, so
    a caller handed only the rows cannot work backwards to what priced them.
    """
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert len(outcome.applied_rates) == 1
    applied = outcome.applied_rates[0]
    assert applied.from_currency == "EUR"
    assert applied.to_currency == "USD"
    assert applied.rate == Decimal("1.09")
    assert applied.rate_date == date(2026, 3, 5)


def test_one_applied_rate_per_currency_and_date_however_many_rows(
    saved_db: Database,
) -> None:
    """Provenance names the distinct rates, not one entry per row.

    A thousand rows on one date were priced by one rate, and repeating it a
    thousand times would bury the fact that only one was ever consulted.
    """
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(), _row(amount=Decimal("50.00")), _row(amount=Decimal("25.00"))],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert len(outcome.records) == 3
    assert len(outcome.applied_rates) == 1


def test_a_segmented_report_names_no_applied_rate(saved_db: Database) -> None:
    """Nothing was priced, so nothing may claim to have priced it.

    The empty tuple is the whole signal: a caller reading provenance must not
    have to cross-check ``display_currency`` to learn whether the rates it found
    describe the rows it is holding.
    """
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.display_currency is None
    assert outcome.degraded_reason is not None
    assert outcome.applied_rates == ()


def test_non_money_columns_are_left_alone(saved_db: Database) -> None:
    """Only declared money classes convert; a count is not an amount."""
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.records[0]["txn_count"] == 3
    assert outcome.records[0]["txn_date"] == date(2026, 3, 5)


def test_a_row_already_in_the_target_currency_needs_no_stored_rate(
    saved_db: Database,
) -> None:
    """Requirement 7: a single-currency profile must not depend on the rate layer.

    Nothing is seeded here on purpose. If an identity pair consulted the cache,
    a USD-only user would need exchange rates to read their own USD reports.
    """
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(currency_code="USD")],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is None
    assert outcome.display_currency == "USD"
    assert outcome.records[0]["amount"] == Decimal("100.00")


# --- Requirement 15: segmentation is the fallback, never an error -------------


def test_an_uncovered_pair_segments_instead_of_raising(saved_db: Database) -> None:
    """The whole point of Requirement 15.

    ``resolve_rate`` raises for an uncovered pair. A report that let that
    propagate would turn one exotic holding into a broken read.
    """
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.display_currency is None
    assert outcome.degraded_reason is not None
    # Segmented rows are the originals, in their own currency, untouched.
    assert outcome.records[0]["amount"] == Decimal("100.00")
    assert outcome.records[0]["currency_code"] == "EUR"


def test_one_unconvertible_row_segments_the_whole_result(saved_db: Database) -> None:
    """No partial conversion: a mixed result under one label is a blended number."""
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(), _row(currency_code="ZWL")],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.display_currency is None
    assert [record["amount"] for record in outcome.records] == [
        Decimal("100.00"),
        Decimal("100.00"),
    ]
    assert [record["currency_code"] for record in outcome.records] == ["EUR", "ZWL"]


def test_a_never_priced_pair_and_a_date_gap_give_different_remedies(
    saved_db: Database,
) -> None:
    """The two absences need different next actions, so they read differently.

    A pair with rates on other dates needs more dates gathered; a pair with none
    at all may not be published, which no amount of refreshing will fix.
    """
    service = CurrencyService(saved_db)
    never_priced = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    _seed_rate(saved_db, "EUR", "USD", date(2026, 2, 2), Decimal("1.08"))
    date_gap = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert never_priced.degraded_reason is not None
    assert date_gap.degraded_reason is not None
    assert never_priced.degraded_reason != date_gap.degraded_reason
    assert "fx set" in never_priced.degraded_reason
    assert "fx set" not in date_gap.degraded_reason


def test_the_reason_names_no_date(saved_db: Database) -> None:
    """A date someone asked about is a date money moved on.

    ``degraded_reason`` rides the response envelope and the CLI's durable log,
    which is why ``CurrencyService`` keeps dates out of its own messages too.
    """
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is not None
    assert "2026" not in outcome.degraded_reason


def test_a_row_with_no_currency_is_never_guessed(saved_db: Database) -> None:
    """M1K.1's rule: unknown currency segments, it does not default."""
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(currency_code=None)],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.display_currency is None
    assert outcome.records[0]["amount"] == Decimal("100.00")


def test_a_malformed_currency_code_segments_rather_than_raising(
    saved_db: Database,
) -> None:
    """``currency_code`` is whatever the source file put in it.

    ``require_currency`` refuses a code that would match nothing, and it raises
    a plain ``UserError`` — not ``RateUnavailableError``. Letting that escape
    would break a whole net-worth read over one mis-mapped cell, which is the
    failure Requirement 15 exists to prevent.
    """
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(), _row(currency_code="Dollars")],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.display_currency is None
    assert outcome.degraded_reason is not None
    assert [record["amount"] for record in outcome.records] == [
        Decimal("100.00"),
        Decimal("100.00"),
    ]


def test_a_malformed_currency_code_stays_out_of_the_reason(
    saved_db: Database,
) -> None:
    """A mis-mapped cell holds arbitrary source text, so it is never echoed.

    ``require_currency`` keeps a rejected code out of its own ``message`` for
    this reason; the reports layer must not reintroduce it one level up.
    """
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(currency_code="4111111111111111")],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is not None
    assert "4111111111111111" not in outcome.degraded_reason


def test_a_report_that_declares_no_fx_date_segments(saved_db: Database) -> None:
    """Fail closed: an undeclared date column must not fall back to a guess."""
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(fx_date=None),
        to_currency="USD",
        service=service,
    )

    assert outcome.display_currency is None
    assert outcome.degraded_reason is not None
    assert outcome.records[0]["amount"] == Decimal("100.00")


def test_a_report_that_cannot_be_priced_gives_its_own_declared_reason(
    saved_db: Database,
) -> None:
    """The refusal quotes the report's `fx_basis` rather than inferring one.

    Why a report cannot be priced differs per report. `core:cashflow` has a
    single date at its grain and still cannot convert, because `currency_code`
    sits in its GROUP BY — pricing each row into one display currency would
    return several rows sharing a grain key. `core:merchants` additionally
    spans a range of dates rather than one. Deriving a reason from which
    declaration happens to be missing would state one report's obstacle for
    another's, so the author writes it once and it surfaces both here and
    through `reports explain`.
    """
    basis = (
        "amounts are aggregated per currency_code, so pricing a row into one "
        "display currency would leave several rows sharing a grain key"
    )

    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(fx_date=None, fx_basis=basis),
        to_currency="USD",
        service=CurrencyService(saved_db),
    )

    assert outcome.degraded_reason == basis


def test_a_report_declaring_no_basis_at_all_says_that_much(
    saved_db: Database,
) -> None:
    """Every user-created report lands here — `dynamic.py` declares no semantics.

    Nobody wrote an `fx_basis` for a report the user authored, so there is no
    stated reason to quote and the generic reading is the honest one.
    """
    outcome = convert_records(
        [_row()],
        classes=_CLASSES,
        semantics=_semantics(fx_date=None, fx_basis=None),
        to_currency="USD",
        service=CurrencyService(saved_db),
    )

    assert outcome.degraded_reason is not None
    assert "does not declare" in outcome.degraded_reason


def test_a_report_with_no_money_columns_is_not_degraded(saved_db: Database) -> None:
    """Having no amounts is not a failed conversion — it is nothing to convert."""
    service = CurrencyService(saved_db)
    classes = {"txn_date": DataClass.TXN_DATE, "txn_count": DataClass.AGGREGATE}

    outcome = convert_records(
        [{"txn_date": date(2026, 3, 5), "txn_count": 3}],
        classes=classes,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is None
    assert outcome.display_currency is None
    assert outcome.records[0]["txn_count"] == 3


def test_a_placeholder_row_holding_no_amount_is_not_degraded(
    saved_db: Database,
) -> None:
    """Declaring a money column is not the same as holding money in it.

    ``core:networth`` on an empty profile returns one placeholder row whose
    amounts and ``currency_code`` are all NULL. Reading that as "some rows do
    not record which currency they hold" reports a conversion that failed, when
    in fact there was nothing to price — the same state as a report with no
    money columns at all, which is already not degraded.
    """
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(amount=None, currency_code=None)],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is None
    assert outcome.display_currency is None
    # The row survives untouched — nothing to convert is not nothing to return.
    assert outcome.records[0]["txn_count"] == 3


def test_a_report_with_no_rows_names_no_currency(saved_db: Database) -> None:
    """Zero rows cannot be denominated in the currency that was asked for.

    The row loop is the only thing that resolves a rate, so an empty result
    would otherwise label itself converted without a single rate having been
    read. It is the same nothing-to-convert state as the placeholder above and
    answers identically, rather than the two disagreeing on one case.
    """
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is None
    assert outcome.display_currency is None
    assert outcome.records == []


def test_a_monthly_grain_prices_at_the_month_close(saved_db: Database) -> None:
    """A ``YYYY-MM`` row has no single day, so it prices at the month's close.

    February 2026 closed on the 28th. Seeding only the 28th proves the rule is
    the month end rather than the first of the month or the date it was run.
    """
    _seed_rate(saved_db, "EUR", "USD", date(2026, 2, 28), Decimal("1.08"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(txn_date="2026-02")],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is None
    assert outcome.records[0]["amount"] == Decimal("108.00")


def test_a_month_that_is_not_a_month_segments(saved_db: Database) -> None:
    """``YYYY-MM`` is matched by shape, so month 13 reaches the date builder.

    The row is well-formed to the regex and impossible to the calendar. It has
    to segment like any other unpriceable row: raising out of ``_rate_date``
    would fail the whole report over one bad cell.
    """
    _seed_rate(saved_db, "EUR", "USD", date(2026, 2, 28), Decimal("1.08"))
    service = CurrencyService(saved_db)

    outcome = convert_records(
        [_row(txn_date="2026-13")],
        classes=_CLASSES,
        semantics=_semantics(),
        to_currency="USD",
        service=service,
    )

    assert outcome.degraded_reason is not None
    assert outcome.records[0]["amount"] == Decimal("100.00")


# --- convert_execution: what the envelope reports after a fallback ------------


def _execution(**overrides: Any) -> CatalogReportExecution:
    fields: dict[str, Any] = {
        "report_id": "test:summary",
        "parameters": {},
        "sql": None,
        "records": [_row()],
        "columns": list(_CLASSES),
        "column_types": ["DATE", "VARCHAR", "DECIMAL(18,2)", "BIGINT"],
        "output_classes": dict(_CLASSES),
        "tier": Tier.HIGH,
        "total_count": 1,
        "truncated": False,
        "actions": [],
        "period": None,
        "semantics": _semantics(),
        "provenance": ("reports.test_summary",),
        "display_currency": "EUR",
    }
    fields.update(overrides)
    return CatalogReportExecution(**fields)


def test_a_failed_conversion_keeps_the_currency_the_rows_are_actually_in(
    saved_db: Database,
) -> None:
    """Segmenting must not cost the caller the answer it already had.

    ``build_catalog_execution`` derives ``display_currency`` from the rows
    themselves — one agreed code, else null. When conversion falls back those
    rows are unchanged, so the derived answer is still true. Replacing it with
    the failed conversion's ``None`` reports a mixed-currency result where the
    rows are in fact all EUR — a worse answer than the caller would have got
    without asking.
    """
    service = CurrencyService(saved_db)

    converted = convert_execution(_execution(), to_currency="USD", service=service)

    assert converted.degraded_reason is not None
    assert converted.display_currency == "EUR"


def test_a_successful_conversion_reports_the_currency_it_converted_into(
    saved_db: Database,
) -> None:
    """The other half: a real conversion must replace the derived value."""
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    converted = convert_execution(_execution(), to_currency="USD", service=service)

    assert converted.degraded_reason is None
    assert converted.display_currency == "USD"


# --- Requirement 9: the default display currency is the profile's home --------


def _money_runner(db: Database) -> ReportQuery:  # noqa: ARG001  # contract handle
    """One EUR amount on a fixed date.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery(
        "SELECT DATE '2026-03-05' AS txn_date, 'EUR' AS currency_code, "
        "CAST(100.00 AS DECIMAL(18,2)) AS amount",
        [],
    )


def _money_report() -> ReportSpec:
    return ReportSpec(
        report_id="core:money",
        name="money",
        description="Test money report.",
        view=TableRef("reports", "test_summary"),
        runner=_money_runner,
        classes={
            "txn_date": DataClass.TXN_DATE,
            "currency_code": DataClass.CURRENCY,
            "amount": DataClass.TXN_AMOUNT,
        },
        columns=(
            OutputColumn("txn_date", "Transaction date.", DataClass.TXN_DATE),
            OutputColumn("currency_code", "Currency.", DataClass.CURRENCY),
            OutputColumn("amount", "Amount.", DataClass.TXN_AMOUNT),
        ),
        semantics=_semantics(),
        params=(),
    )


def _run(saved_db: Database, **currency: str | None) -> Any:
    return ReportCatalog((_money_report(),)).execute(
        saved_db,
        report_id="core:money",
        parameters={},
        limit=100,
        **currency,
    )


def test_the_home_currency_prices_a_report_nobody_gave_a_currency(
    saved_db: Database,
) -> None:
    """Requirement 9's default. Without it a two-currency user retypes the flag."""
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))

    result = _run(saved_db, home_currency="USD")

    assert result.display_currency == "USD"
    assert result.degraded is False


def test_an_explicit_currency_outranks_the_home_currency(saved_db: Database) -> None:
    _seed_rate(saved_db, "EUR", "GBP", date(2026, 3, 5), Decimal("0.85"))

    result = _run(saved_db, display_currency="GBP", home_currency="USD")

    assert result.display_currency == "GBP"
    assert result.degraded is False


def test_a_defaulted_currency_that_cannot_resolve_says_nothing(
    saved_db: Database,
) -> None:
    """Silence here is the whole reason the default is safe to turn on.

    Nobody asked for a currency, so nobody is owed an explanation for not
    getting one — and the rows are still correct, in their own currency. A
    warning on every read of every money-bearing report is how a reader learns
    to skip the one that matters.
    """
    result = _run(saved_db, home_currency="USD")

    assert result.degraded is False
    assert result.degraded_reason is None
    # Not converted: the rows are still the EUR they always were.
    assert result.display_currency == "EUR"


def test_a_requested_currency_that_cannot_resolve_says_why(
    saved_db: Database,
) -> None:
    """The other half. Asking and silently not getting it is the worse failure."""
    result = _run(saved_db, display_currency="USD", home_currency="USD")

    assert result.degraded is True
    assert result.degraded_reason is not None


# --- A target that names no currency is refused, not segmented ----------------


def _empty_runner(db: Database) -> ReportQuery:  # noqa: ARG001  # contract handle
    """The same columns as ``_money_runner``, and no rows.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery(
        "SELECT DATE '2026-03-05' AS txn_date, 'EUR' AS currency_code, "
        "CAST(100.00 AS DECIMAL(18,2)) AS amount WHERE false",
        [],
    )


def test_a_requested_currency_that_names_nothing_is_refused(
    saved_db: Database,
) -> None:
    """A code matching no currency is the caller's mistake, not a rate gap.

    Segmenting would be the wrong mercy: it returns the rows in their own
    currency and blames a missing rate, sending the caller to
    ``moneybin refresh`` to gather a pair that cannot exist.
    """
    with pytest.raises(UserError) as caught:
        _run(saved_db, display_currency="NOTACURRENCY")

    assert caught.value.code == error_codes.FX_CURRENCY_INVALID


def test_an_empty_report_refuses_a_currency_that_names_nothing_too(
    saved_db: Database,
) -> None:
    """The row loop is not a validator — with no rows it never runs.

    ``convert_records`` reaches ``resolve_rate`` only when there is a row to
    price, so a report returning nothing would otherwise report itself
    denominated in whatever string arrived.
    """
    empty = replace(_money_report(), report_id="core:empty", runner=_empty_runner)

    with pytest.raises(UserError) as caught:
        ReportCatalog((empty,)).execute(
            saved_db,
            report_id="core:empty",
            parameters={},
            limit=100,
            display_currency="NOTACURRENCY",
        )

    assert caught.value.code == error_codes.FX_CURRENCY_INVALID


def test_a_malformed_home_currency_degrades_rather_than_failing_every_report(
    saved_db: Database,
) -> None:
    """A standing profile setting must not break reads nobody parameterized.

    Requirement 9 makes the home currency the default target, so raising on one
    would fail every money-bearing report the profile owns until it is fixed —
    including the reads that would show the user what is wrong. It is not this
    call's request, so it degrades on the same rule every other silent default
    fallback follows.
    """
    result = _run(saved_db, home_currency="NOTACURRENCY")

    assert result.degraded is False
    assert result.display_currency == "EUR"


# --- derived columns: what conversion leaves stale unless a report restates it -


def test_a_converted_execution_carries_its_rates_onward(saved_db: Database) -> None:
    """Provenance has to survive the step that builds the envelope's input.

    ``convert_records`` retaining the rate is worthless if ``convert_execution``
    drops it on the way to ``CatalogReportResult`` — the seam Requirement 10
    actually has to cross.
    """
    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    service = CurrencyService(saved_db)

    converted = convert_execution(_execution(), to_currency="USD", service=service)

    assert converted.display_currency == "USD"
    assert [rate.from_currency for rate in converted.applied_rates] == ["EUR"]


def test_derived_columns_are_restated_only_when_a_conversion_happened(
    saved_db: Database,
) -> None:
    """A segmented result still holds original amounts, so nothing is stale.

    Running the repair anyway would restate a value against a currency the rows
    were never priced into — the one case where doing nothing is correct, and
    the easiest to get backwards.
    """
    seen: list[str] = []

    def _restate(rows: list[dict[str, Any]], currency: str) -> None:
        seen.append(currency)

    convert_execution(
        _execution(on_converted=_restate),
        to_currency="USD",
        service=CurrencyService(saved_db),
    )
    assert seen == []

    _seed_rate(saved_db, "EUR", "USD", date(2026, 3, 5), Decimal("1.09"))
    convert_execution(
        _execution(on_converted=_restate),
        to_currency="USD",
        service=CurrencyService(saved_db),
    )
    assert seen == ["USD"]


def test_drift_status_is_rebucketed_against_the_converted_amount() -> None:
    """A verdict bucketed pre-conversion contradicts the figure beside it.

    500 JPY is ``drift`` at thresholds calibrated in whole currency units; the
    same position shown as 3.40 USD is a ``warning``. Printing 3.40 beside
    ``drift`` is the self-contradiction this repair exists to remove.
    """
    rows = [{"drift": Decimal("3.40"), "drift_abs": Decimal("500"), "status": "drift"}]

    _rebucket_status(rows, "USD")

    assert rows[0]["status"] == "warning"
    # Derived from the same column, so it is restated here rather than left to a
    # second independent rounding.
    assert rows[0]["drift_abs"] == Decimal("3.40")


def test_rebucketing_leaves_a_status_that_names_no_magnitude() -> None:
    """``no-data`` and ``currency-mismatch`` report that no drift was computable.

    Re-bucketing either from an amount that does not exist would invent a
    reconciliation verdict — concretely, reading a null drift as ``clean``.
    """
    rows: list[dict[str, Any]] = [
        {"drift": None, "drift_abs": None, "status": "no-data"},
        {"drift": Decimal("0.00"), "drift_abs": Decimal("0.00"), "status": "no-data"},
        {"drift": Decimal("0.00"), "status": "currency-mismatch"},
    ]

    _rebucket_status(rows, "USD")

    assert [row["status"] for row in rows] == [
        "no-data",
        "no-data",
        "currency-mismatch",
    ]


def test_net_worth_is_restated_from_its_own_converted_parts() -> None:
    """Rounding each column alone can leave a report's own identity false.

    assets 1.00 and liabilities -0.01 at rate 0.5 round to 0.50 and -0.01, while
    net worth 0.99 rounds to 0.50 — so the parts sum to 0.49 while the total
    says 0.50. Each number is individually correct, which is what makes the
    disagreement hard to spot.
    """
    rows: list[dict[str, Any]] = [
        {
            "total_assets": Decimal("0.50"),
            "total_liabilities": Decimal("-0.01"),
            "net_worth": Decimal("0.50"),
        }
    ]

    _restate_networth_total(rows, "USD")

    assert rows[0]["net_worth"] == Decimal("0.49")


def test_restating_the_total_skips_an_account_breakdown_row() -> None:
    """Only a totals row states the identity, so only it can break it.

    An account row carries nulls in all three; computing over it would turn a
    breakdown line into a second, wrong headline.
    """
    rows: list[dict[str, Any]] = [
        {
            "total_assets": None,
            "total_liabilities": None,
            "net_worth": None,
            "account_balance": Decimal("12.00"),
        }
    ]

    _restate_networth_total(rows, "USD")

    assert rows[0]["net_worth"] is None
