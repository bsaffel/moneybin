"""Display conversion of report rows — multi-currency.md Requirements 9 and 15.

The conversion primitive is exercised against a real ``CurrencyService`` over a
real cache rather than a stub: the invariant under test is that a report read
resolves from stored rates and never reaches a provider, and a stub adapter
would satisfy the assertions while proving nothing about that.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportSemantics
from moneybin.reports._framework.convert import convert_records
from moneybin.services.currency_service import CurrencyService

_CLASSES: Mapping[str, DataClass] = {
    "txn_date": DataClass.TXN_DATE,
    "currency_code": DataClass.CURRENCY,
    "amount": DataClass.TXN_AMOUNT,
    "txn_count": DataClass.AGGREGATE,
}


def _semantics(*, fx_date: str | None = "txn_date") -> ReportSemantics:
    return ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="negative expense; positive income",
        kind="flow",
        valuation_basis="transaction amount",
        fx_basis="converted per row at its own date when a display currency is given",
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

    ``CurrencyService._require_currency`` refuses a code that would match
    nothing, and it raises a plain ``UserError`` — not ``RateUnavailableError``.
    Letting that escape would break a whole net-worth read over one mis-mapped
    cell, which is the failure Requirement 15 exists to prevent.
    ``rate_backfill`` guards the same case with ``_usable_currency``.
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

    ``_require_currency`` keeps a rejected code out of its own ``message`` for
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
