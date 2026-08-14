"""Properties of applying an exchange rate to an amount.

These quantify over the whole amount x rate space, which is why they are
property-based: `multi-currency.md`'s testing strategy states the conversion
guarantee for all amounts and rates, and no finite set of examples establishes
that. The example tests in `tests/moneybin/test_services/test_currency_service.py`
cover resolution, precedence, and failure; these cover the arithmetic.

They test `apply_rate` rather than `CurrencyService.convert` for two reasons.
Hypothesis reuses one function-scoped fixture across every example, so a
database-backed rate seeded per example would hit `on_conflict="ignore"` and
leave the *first* example's rate answering all the rest — the property would
pass while testing one rate. And `apply_rate` is the whole of the arithmetic;
`convert` adds only resolution, which the example tests already pin.
"""

from __future__ import annotations

from decimal import Decimal

from hypothesis import assume, given
from hypothesis import strategies as st

from moneybin.services.currency_service import MONEY_QUANTUM, RATE_QUANTUM, apply_rate

#: Half a cent — the most a single half-up quantization can move a value.
_HALF_CENT = MONEY_QUANTUM / 2

#: The relative error of a rate stored at DECIMAL(18,8), rounded up from the
#: true 0.5e-8 half-ulp to leave the bound legible.
_RATE_EPSILON = Decimal("0.0000001")

#: Ledger amounts, both signs: the sign convention is negative = expense, and
#: half-up rounds away from zero, so a positives-only strategy would never
#: exercise the branch that carries an expense.
amounts = st.decimals(
    min_value=Decimal("-1000000.00"),
    max_value=Decimal("1000000.00"),
    places=2,
    allow_nan=False,
    allow_infinity=False,
)

#: Published reference rates span roughly 0.00006 (IDR) to 155 (JPY); the
#: strategy covers four orders of magnitude either side of that.
rates = st.decimals(
    min_value=Decimal("0.00010000"),
    max_value=Decimal("10000.00000000"),
    places=8,
    allow_nan=False,
    allow_infinity=False,
)


@given(amount=amounts, rate=rates)
def test_a_conversion_never_departs_from_the_exact_product_by_half_a_cent(
    amount: Decimal, rate: Decimal
) -> None:
    """The only difference from exact arithmetic is the final rounding."""
    assert abs(apply_rate(amount, rate) - amount * rate) <= _HALF_CENT


@given(amount=amounts, rate=rates)
def test_a_converted_amount_lands_on_a_whole_cent(
    amount: Decimal, rate: Decimal
) -> None:
    """Money is DECIMAL(18,2); a sub-cent result would not survive storage."""
    converted = apply_rate(amount, rate)
    assert converted == converted.quantize(MONEY_QUANTUM)


@given(amount=amounts, rate=rates)
def test_a_conversion_preserves_sign(amount: Decimal, rate: Decimal) -> None:
    """An expense stays an expense.

    Sign is the whole accounting convention, so a conversion that flipped it
    would turn spending into income. Rounding may take a small amount to zero,
    which is a magnitude loss rather than a sign change, so zero is allowed on
    either side.
    """
    converted = apply_rate(amount, rate)
    if converted != 0:
        assert (converted > 0) == (amount > 0)


@given(amount=amounts, rate=rates)
def test_converting_zero_yields_zero(amount: Decimal, rate: Decimal) -> None:
    """No rate invents money from nothing."""
    assert apply_rate(Decimal("0.00"), rate) == 0


@given(smaller=amounts, larger=amounts, rate=rates)
def test_a_conversion_preserves_order(
    smaller: Decimal, larger: Decimal, rate: Decimal
) -> None:
    """A bigger amount never converts to a smaller one.

    Ordering is what makes a converted column sortable and a converted total
    comparable; a non-monotone rounding would reorder rows against the originals.
    """
    assume(smaller <= larger)
    assert apply_rate(smaller, rate) <= apply_rate(larger, rate)


@given(
    rows=st.lists(amounts, min_size=1, max_size=50),
    rate=rates,
)
def test_converting_rows_then_summing_matches_converting_the_total(
    rows: list[Decimal], rate: Decimal
) -> None:
    """Penny conservation across a split, mirroring the cost-basis engine's.

    This is the defect display conversion actually risks: a report that
    converts each row and a total that converts the sum disagree, so the column
    does not add up to the figure printed under it. Each of the n rows and the
    total round once each, so the gap cannot exceed (n + 1) half-cents.

    The bound follows from the half-cent property above rather than being
    independent of it, so this does not catch a defect that one would miss.
    It earns its place by stating the number PR 2 has to design against, and
    by being where the contract changes if `apply_rate` ever grows a running
    residual the way the cost-basis engine's proration did.
    """
    per_row = sum((apply_rate(row, rate) for row in rows), Decimal("0.00"))
    on_total = apply_rate(sum(rows, Decimal("0.00")), rate)

    tolerance = (len(rows) + 1) * _HALF_CENT
    assert abs(per_row - on_total) <= tolerance, (
        f"{len(rows)} rows at {rate}: per-row {per_row} vs total {on_total}"
    )


@given(amount=amounts, rate=rates)
def test_a_round_trip_costs_at_most_one_cent_in_each_currency(
    amount: Decimal, rate: Decimal
) -> None:
    """Convert out and back, against the reciprocal a provider would publish.

    The tolerance has three named terms and no slack beyond them: one cent of
    the destination currency (`0.01 / rate`, the intermediate's rounding
    expressed in source units), one cent of the source currency (the final
    rounding), and the destination magnitude times the relative error of a rate
    stored at eight places.

    A flat one-cent tolerance is arithmetically false and was the plan's
    original claim. Two rates stored independently at eight places are not
    inverses: at rate 9524, one million dollars round-trips to 1000020.00, off
    by twenty dollars. Widening the flat tolerance until that passes would hide
    the real behaviour; naming the three terms explains it. Nothing here is a
    defect in `apply_rate` — it is what finite precision costs, which is why
    the original amount stays canonical and conversion stays presentational.
    """
    reciprocal = (Decimal(1) / rate).quantize(RATE_QUANTUM)
    assume(reciprocal > 0)

    out = apply_rate(amount, rate)
    back = apply_rate(out, reciprocal)

    tolerance = MONEY_QUANTUM + MONEY_QUANTUM / rate + _RATE_EPSILON * abs(out)
    assert abs(back - amount) <= tolerance, (
        f"{amount} -> {out} -> {back} at rate {rate} (1/rate stored as {reciprocal})"
    )


def test_converting_into_a_coarser_currency_loses_the_amount_below_its_cent() -> None:
    """The round-trip limit, pinned as a fact rather than left as a surprise.

    At a rate this small a cent of the destination currency is worth ten
    thousand source units, so the source amount below that granularity cannot
    survive the trip. PR 2 displays converted figures beside originals for
    exactly this reason: the original is the auditable number.
    """
    rate = Decimal("0.00010000")
    out = apply_rate(Decimal("999999.99"), rate)
    assert out == Decimal("100.00")

    back = apply_rate(out, (Decimal(1) / rate).quantize(RATE_QUANTUM))
    assert back == Decimal("1000000.00")
