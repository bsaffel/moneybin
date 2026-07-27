"""``build_envelope`` reads the currency off the payload instead of guessing.

Nine of eleven money-bearing MCP tools never passed ``display_currency``, so
they inherited a hardcoded ``"USD"``. Patching each call site is what rounds 10
and 11 tried; the class reopened both times because the next money tool still
inherits the default. Deriving from the payload's own ``currency_code`` closes
it for every current and future caller: a payload that states its currency can
no longer be labelled with a different one.

Derivation is not inference — ``resolve_display_currency`` has exactly one
correct answer for a given set of rows. An explicit argument still wins, so a
caller that resolved the currency over a wider set than the returned page (the
reports framework) keeps its answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import build_envelope


@dataclass(frozen=True, slots=True)
class _Row:
    """A money row that states its own currency."""

    amount: Annotated[Decimal, DataClass.BALANCE]
    currency_code: Annotated[str | None, DataClass.CURRENCY]


@dataclass(frozen=True, slots=True)
class _RowsPayload:
    """The shape every list-returning money tool uses."""

    rows: list[_Row]


@dataclass(frozen=True, slots=True)
class _RecordPayload:
    """The shape the single-record money tools use."""

    amount: Annotated[Decimal, DataClass.BALANCE]
    currency_code: Annotated[str | None, DataClass.CURRENCY]


def test_derives_the_one_currency_the_rows_agree_on() -> None:
    payload = _RowsPayload(
        rows=[
            _Row(Decimal("1.00"), "EUR"),
            _Row(Decimal("2.00"), "EUR"),
        ]
    )

    assert build_envelope(data=payload).summary.display_currency == "EUR"


def test_declines_when_rows_disagree() -> None:
    """Two currencies in one response name neither — never the first one."""
    payload = _RowsPayload(
        rows=[
            _Row(Decimal("1.00"), "EUR"),
            _Row(Decimal("2.00"), "JPY"),
        ]
    )

    assert build_envelope(data=payload).summary.display_currency is None


def test_declines_when_a_row_has_no_currency() -> None:
    """Unknown is its own segment; it never borrows the other rows' code."""
    payload = _RowsPayload(
        rows=[
            _Row(Decimal("1.00"), "EUR"),
            _Row(Decimal("2.00"), None),
        ]
    )

    assert build_envelope(data=payload).summary.display_currency is None


def test_derives_from_a_single_record_payload() -> None:
    """accounts_get / accounts_set shape: currency on the payload itself."""
    payload = _RecordPayload(Decimal("500.00"), "GBP")

    assert build_envelope(data=payload).summary.display_currency == "GBP"


def test_explicit_argument_wins_over_derivation() -> None:
    """A caller that resolved over a wider set than this page keeps its answer."""
    payload = _RowsPayload(rows=[_Row(Decimal("1.00"), "EUR")])

    envelope = build_envelope(data=payload, display_currency=None)

    assert envelope.summary.display_currency is None


def test_derives_through_a_pydantic_view_model() -> None:
    """The coarse read tools return Pydantic views, not dataclasses.

    ``accounts``, ``accounts_balances`` and ``investments`` all return
    ``BaseModel`` view wrappers around dataclass rows, so derivation that only
    understood dataclasses would silently skip exactly the tools this fixes.
    """
    from pydantic import BaseModel

    class _View(BaseModel):
        rows: list[_Row]

        model_config = {"arbitrary_types_allowed": True}

    view = _View(rows=[_Row(Decimal("3.00"), "CHF")])

    assert build_envelope(data=view).summary.display_currency == "CHF"


def test_ignores_auxiliary_lists_when_finding_the_rows() -> None:
    """A payload's warnings list must not be mistaken for its row collection."""

    @dataclass(frozen=True, slots=True)
    class _WithWarnings:
        rows: list[_Row]
        warnings: list[str]

    payload = _WithWarnings(rows=[_Row(Decimal("1.00"), "SEK")], warnings=["careful"])

    assert build_envelope(data=payload).summary.display_currency == "SEK"


def test_payload_with_no_currency_field_stays_unknown() -> None:
    """Money with no currency anywhere cannot be labelled — not even USD."""

    @dataclass(frozen=True, slots=True)
    class _NoCurrency:
        amount: Annotated[Decimal, DataClass.BALANCE]

    assert (
        build_envelope(data=_NoCurrency(Decimal("1.00"))).summary.display_currency
        is None
    )


def test_derives_currency_from_mapping_rows() -> None:
    """`sql_query`'s rows are plain dicts and must derive like typed rows.

    `execute_sql_query` returns `list[dict[str, Any]]` (`SqlQueryResult.records`),
    so a dataclass/`BaseModel`-only check left every ad-hoc query reporting
    `display_currency: null` — including `SELECT amount, currency_code ...`
    where every row agrees. Null is honest rather than wrong, so this never
    mislabelled a result, but it contradicted the tool's own docstring on the
    surface agents reach for most.
    """
    envelope = build_envelope(
        data=[
            {"amount": Decimal("-12.00"), "currency_code": "EUR"},
            {"amount": Decimal("-8.00"), "currency_code": "EUR"},
        ]
    )

    assert envelope.summary.display_currency == "EUR"


def test_mapping_rows_that_disagree_stay_unknown() -> None:
    """A mixed-currency ad-hoc query must not be labelled with one of them."""
    envelope = build_envelope(
        data=[
            {"amount": Decimal("-12.00"), "currency_code": "EUR"},
            {"amount": Decimal("-8.00"), "currency_code": "USD"},
        ]
    )

    assert envelope.summary.display_currency is None


def test_mapping_rows_without_a_currency_column_stay_unknown() -> None:
    """A projection that omits currency_code says nothing about denomination."""
    envelope = build_envelope(
        data=[{"amount": Decimal("-12.00")}, {"amount": Decimal("-8.00")}]
    )

    assert envelope.summary.display_currency is None
