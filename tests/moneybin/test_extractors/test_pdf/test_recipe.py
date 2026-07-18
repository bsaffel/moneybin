"""Tests for Recipe schema + bounded executor (Req 9b)."""

from __future__ import annotations

from datetime import date

import pytest

from moneybin.extractors.pdf.recipe import (
    ExtractedRows,
    Recipe,
    execute_recipe,
)

# ---------------------------------------------------------------------------
# Test fixture factory
# ---------------------------------------------------------------------------


def _make_field(
    name: str = "amount",
    pattern: str = r"\d+\.\d{2}",
    cast: str = "decimal",
    date_format: str | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {"name": name, "pattern": pattern, "cast": cast}
    if date_format is not None:
        result["date_format"] = date_format
    return result


def _make_recipe(**overrides: object) -> dict[str, object]:
    """Return a minimal valid recipe dict; callers override only what they care about."""
    base: dict[str, object] = {
        "metadata_anchors": [],
        "row_region": {"start_anchor": "TRANSACTIONS", "end_anchor": "TOTAL"},
        "row_split": r"\s{2,}",
        "fields": [
            _make_field("date", r"\d{2}/\d{2}/\d{4}", "date", "%m/%d/%Y"),
            _make_field("amount", r"-?\d+\.\d{2}", "decimal"),
        ],
        "sign_convention": "negative_is_expense",
        "routing": "transactions",
    }
    base.update(overrides)
    return base


def _make_recipe_with_pattern(pattern: str) -> dict[str, object]:
    """Minimal recipe with one field whose pattern is under test."""
    return _make_recipe(
        fields=[_make_field("amount", pattern, "decimal")],
    )


# ---------------------------------------------------------------------------
# Validation: static bounds
# ---------------------------------------------------------------------------


def test_recipe_save_rejects_overlong_pattern() -> None:
    recipe = _make_recipe_with_pattern("a" * 300)
    with pytest.raises(ValueError, match="max_pattern_len"):
        Recipe.model_validate(recipe)


def test_recipe_save_rejects_nested_unbounded_quantifier() -> None:
    # Catastrophic-backtracking shape: (a+)+
    recipe = _make_recipe_with_pattern(r"(a+)+")
    with pytest.raises(ValueError, match="nested.*quantifier"):
        Recipe.model_validate(recipe)


def test_recipe_valid_pattern_accepts() -> None:
    # Positive path: well-formed patterns should validate without error.
    recipe = _make_recipe_with_pattern(r"-?\d{1,10}\.\d{2}")
    result = Recipe.model_validate(recipe)
    assert result.fields[0].pattern == r"-?\d{1,10}\.\d{2}"


def test_recipe_rejects_nested_star_quantifier() -> None:
    # (X*)+ is equally catastrophic as (X+)+
    recipe = _make_recipe_with_pattern(r"(a*)+")
    with pytest.raises(ValueError, match="nested.*quantifier"):
        Recipe.model_validate(recipe)


def test_recipe_save_bounds_apply_to_row_split() -> None:
    # row_split is also executed against document text — same bounds must apply.
    recipe = _make_recipe(row_split=r"(a+)+")
    with pytest.raises(ValueError, match="row_split"):
        Recipe.model_validate(recipe)


def test_recipe_rejects_uncompilable_field_pattern() -> None:
    # Passes the length + nested-quantifier bounds but isn't a compilable regex
    # — must be rejected at validation, not left to raise deep in execute_recipe.
    recipe = _make_recipe_with_pattern("(unterminated")
    with pytest.raises(ValueError, match="invalid regex"):
        Recipe.model_validate(recipe)


def test_recipe_rejects_uncompilable_row_split() -> None:
    recipe = _make_recipe(row_split="[")  # unterminated character class
    with pytest.raises(ValueError, match="invalid regex"):
        Recipe.model_validate(recipe)


def test_recipe_accepts_anchor_with_regex_metacharacters() -> None:
    # Anchors are matched literally (str.find), NOT compiled as regexes, so a
    # special-char anchor like "Balance ($)" must validate fine.
    recipe = _make_recipe(
        row_region={"start_anchor": "Balance ($)", "end_anchor": "Total ["}
    )
    result = Recipe.model_validate(recipe)
    assert result.row_region.start_anchor == "Balance ($)"


def test_recipe_model_dump_round_trips() -> None:
    raw = _make_recipe()
    r1 = Recipe.model_validate(raw)
    dumped = r1.model_dump()
    r2 = Recipe.model_validate(dumped)
    assert r1 == r2


def test_recipe_saved_before_sign_ratified_defaults_to_unratified() -> None:
    """Recipes already in app.pdf_formats predate the field — they must still load.

    ``extraction_recipe`` is a JSON blob, so the field is additive with a default
    and needs no migration. This pins that: a recipe dict with no ``sign_ratified``
    key validates, and defaults to False — the safe side, where the replay guard
    still applies. A future ``extra="forbid"`` or a non-defaulted field would break
    every saved format on upgrade, and this is what would catch it.
    """
    legacy = _make_recipe()
    assert "sign_ratified" not in legacy

    recipe = Recipe.model_validate(legacy)

    assert recipe.sign_ratified is False


# ---------------------------------------------------------------------------
# Executor: timeout (dynamic bound)
# ---------------------------------------------------------------------------


def test_executor_drops_rows_when_pattern_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The TimeoutError-catch branch drops the offending row and returns cleanly.

    The `regex` package raises TimeoutError when wall-clock timeout is exceeded.
    We can't reliably reproduce a real ReDoS in-process (the regex engine
    optimizes most known shapes), so we patch fullmatch to raise TimeoutError
    directly — proving the executor handles the bound enforcement path.
    """
    recipe = Recipe.model_validate(
        _make_recipe(
            row_region={"start_anchor": "TRANSACTIONS", "end_anchor": "TOTAL"},
            row_split=r"\s{2,}",
            fields=[
                _make_field("date", r"\d{2}/\d{2}/\d{4}", "date", "%m/%d/%Y"),
                _make_field("amount", r"-?\d+\.\d{2}", "decimal"),
            ],
        )
    )
    text = "TRANSACTIONS\n01/15/2024  -4.50\nTOTAL"

    def _raise_timeout(*_args: object, **_kwargs: object) -> None:
        raise TimeoutError("simulated pattern timeout")

    monkeypatch.setattr("moneybin.extractors.pdf.recipe._re.fullmatch", _raise_timeout)
    result = execute_recipe(recipe, text)

    # Row dropped (fullmatch raised) but executor returned cleanly — no hang,
    # no uncaught exception. This is the Phase 2b safety contract.
    assert isinstance(result, ExtractedRows)
    assert result.rows == []


def test_executor_rejects_unsupported_number_format() -> None:
    # Only "us" is honoured by _cast today; fail loud rather than silently mis-parse.
    recipe = Recipe.model_validate(_make_recipe(number_format="european"))
    with pytest.raises(NotImplementedError, match="european"):
        execute_recipe(recipe, "")


# ---------------------------------------------------------------------------
# Executor: extraction correctness
# ---------------------------------------------------------------------------


_STATEMENT_TEXT = """\
Account Summary
TRANSACTIONS
01/15/2024  Coffee Shop       -4.50
01/16/2024  Paycheck        1500.00
TOTAL
"""


def test_executor_extracts_valid_rows() -> None:
    """Executor parses a simple two-column statement region into structured rows."""
    recipe = Recipe.model_validate(
        _make_recipe(
            row_region={"start_anchor": "TRANSACTIONS", "end_anchor": "TOTAL"},
            row_split=r"\s{2,}",
            fields=[
                _make_field("date", r"\d{2}/\d{2}/\d{4}", "date", "%m/%d/%Y"),
                _make_field("description", r".+", "str"),
                _make_field("amount", r"-?\d+\.\d{2}", "decimal"),
            ],
        )
    )
    result = execute_recipe(recipe, _STATEMENT_TEXT)
    assert len(result.rows) == 2
    amounts = [str(r["amount"]) for r in result.rows]
    assert "-4.50" in amounts
    assert "1500.00" in amounts


def test_executor_drops_rows_with_wrong_field_count() -> None:
    """Lines whose cell count != field count are silently dropped."""
    text = "TRANSACTIONS\nonly_one_cell\n01/15/2024  valid      -4.50\nTOTAL"
    recipe = Recipe.model_validate(
        _make_recipe(
            row_region={"start_anchor": "TRANSACTIONS", "end_anchor": "TOTAL"},
            row_split=r"\s{2,}",
            fields=[
                _make_field("date", r"\d{2}/\d{2}/\d{4}", "date", "%m/%d/%Y"),
                _make_field("desc", r".+", "str"),
                _make_field("amount", r"-?\d+\.\d{2}", "decimal"),
            ],
        )
    )
    result = execute_recipe(recipe, text)
    assert len(result.rows) == 1


def test_executor_decimal_cast_strips_dollar_sign() -> None:
    """Amount pattern emits $1,500.00 → _cast must strip both , and $."""
    text = "TRANSACTIONS\n01/15/2024  $1,500.00\nTOTAL"
    recipe = Recipe.model_validate(
        _make_recipe(
            row_region={"start_anchor": "TRANSACTIONS", "end_anchor": "TOTAL"},
            row_split=r"\s{2,}",
            fields=[
                _make_field("date", r"\d{2}/\d{2}/\d{4}", "date", "%m/%d/%Y"),
                _make_field("amount", r"-?\$?[\d,]+\.\d{2}", "decimal"),
            ],
        )
    )
    result = execute_recipe(recipe, text)
    assert len(result.rows) == 1
    assert str(result.rows[0]["amount"]) == "1500.00"


def test_cast_decimal_handles_empty_string() -> None:
    """Direct unit test on _cast — empty string returns Decimal(0)."""
    from decimal import Decimal

    from moneybin.extractors.pdf.recipe import (  # type: ignore[attr-defined]
        FieldExtraction,
        _cast,  # pyright: ignore[reportPrivateUsage] -- intentional cast contract probe
    )

    fld = FieldExtraction(name="amount", pattern=r".*", cast="decimal")
    assert _cast(fld, "") == Decimal("0")


# ---------------------------------------------------------------------------
# Year-less date resolution (_resolve_yearless_date)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "period", "expected"),
    [
        # Cycle crosses year-end: the two period years both resolve the row.
        ("12/26", (date(2024, 12, 23), date(2025, 1, 22)), date(2024, 12, 26)),
        ("01/15", (date(2024, 12, 23), date(2025, 1, 22)), date(2025, 1, 15)),
        # Cycle within ONE calendar year, row just BEFORE it (posted-date drift
        # across the year boundary): must resolve to the PRIOR year, not this one.
        # The period offers only 2025 as a period year, so without an adjacent-year
        # candidate this silently stored 2025-12-31 — ~a year late.
        ("12/31", (date(2025, 1, 1), date(2025, 1, 31)), date(2024, 12, 31)),
        # Cycle within one year, row just AFTER it → the following year.
        ("01/02", (date(2024, 12, 1), date(2024, 12, 31)), date(2025, 1, 2)),
    ],
)
def test_resolve_yearless_date_brackets_year_including_adjacent(
    raw: str,
    period: tuple[date, date],
    expected: date,
) -> None:
    """A year-less MM/DD row resolves via the billing period, adjacent years included.

    The within-window year wins when the date lands inside the cycle; a date that
    drifts just past a year boundary falls back to the closest year — which may be
    the period's adjacent year, not one of its two endpoints.
    """
    from moneybin.extractors.pdf.recipe import (
        _resolve_yearless_date,  # pyright: ignore[reportPrivateUsage] -- year-bracket probe
    )

    assert _resolve_yearless_date(raw, "%m/%d", period) == expected


def test_resolve_yearless_date_rejects_far_out_of_period_date() -> None:
    """A MM/DD far outside the billing cycle is an anomaly — reject, don't guess.

    The out-of-window fallback tolerates a few days of posting drift, but a date
    months from the cycle (an OCR garble or a misparsed line) has no correct year.
    Reconciliation can't catch a wrong year (it sums amounts, never validates
    dates), so the resolver refuses rather than silently assign an arbitrary one —
    the row then fails to cast and the statement routes to seed.
    """
    from moneybin.extractors.pdf.recipe import (
        _resolve_yearless_date,  # pyright: ignore[reportPrivateUsage] -- drift-bound probe
    )

    period = (date(2025, 1, 1), date(2025, 1, 31))
    with pytest.raises(ValueError, match="outside the billing period"):
        _resolve_yearless_date("06/15", "%m/%d", period)  # ~5 months out


def test_resolve_yearless_date_resolves_leap_day() -> None:
    """A 02/29 transaction resolves in a leap year, not strptime's non-leap 1900.

    Parsing "02/29" against "%m/%d" alone defaults to year 1900 (not a leap year)
    and raises before any candidate year is tried; the leap-safe parse extracts
    month/day first, then brackets the real (leap) year from the period.
    """
    from moneybin.extractors.pdf.recipe import (
        _resolve_yearless_date,  # pyright: ignore[reportPrivateUsage] -- leap-day probe
    )

    period = (date(2024, 2, 1), date(2024, 2, 29))  # 2024 is a leap year
    assert _resolve_yearless_date("02/29", "%m/%d", period) == date(2024, 2, 29)


def test_execute_recipe_raises_on_unresolvable_yearless_row() -> None:
    """A year-less row with no capturable period fails the whole extraction.

    Rather than silently skip the row (risking a net-zero silent loss), the
    executor raises YearlessDateError so the caller can route to seed/bridge
    instead of importing a partial ledger.
    """
    from moneybin.extractors.pdf.recipe import YearlessDateError

    recipe = Recipe.model_validate(
        _make_recipe(
            metadata_anchors=[],  # decline metadata capture → no period available
            fields=[
                _make_field("date", r"\d{2}/\d{2}", "date", "%m/%d"),
                _make_field("amount", r"-?\d+\.\d{2}", "decimal"),
            ],
        )
    )
    text = "\n".join(["TRANSACTIONS", "12/26   -100.00", "TOTAL"])

    with pytest.raises(YearlessDateError):
        execute_recipe(recipe, text)


def test_execute_recipe_resolves_yearless_year_from_declared_period_anchors() -> None:
    """A recipe's own period anchors let execute_recipe resolve year-less rows.

    This is the bridge's durable capability: an agent that declares
    period_start/period_end anchors for a non-default cycle label ("Cycle …" here,
    which DEFAULT_ANCHORS don't recognise) gets its MM/DD rows bracketed to full
    dates, so a year-less statement imports instead of dead-ending in a seed.
    """
    from decimal import Decimal

    recipe = Recipe.model_validate(
        _make_recipe(
            metadata_anchors=[
                _make_field("period_start", r"Cycle\s+(\d{2}/\d{2}/\d{2})", "date"),
                _make_field(
                    "period_end",
                    r"Cycle\s+\d{2}/\d{2}/\d{2}\s*-\s*(\d{2}/\d{2}/\d{2})",
                    "date",
                ),
            ],
            fields=[
                _make_field("date", r"\d{2}/\d{2}", "date", "%m/%d"),
                _make_field("description", r".+", "str"),
                _make_field("amount", r"-?\d+\.\d{2}", "decimal"),
            ],
        )
    )
    text = "\n".join([
        "Cycle 12/23/24 - 01/22/25",  # a non-default period label the agent read
        "TRANSACTIONS",
        "12/26   PAYMENT THANK YOU   -100.00",
        "01/15   BOOKSTORE   40.00",
        "TOTAL",
    ])

    result = execute_recipe(recipe, text)

    # The cycle crosses year-end, so 12/xx → opening year, 01/xx → closing year.
    assert [(r["date"], r["amount"]) for r in result.rows] == [
        (date(2024, 12, 26), Decimal("-100.00")),
        (date(2025, 1, 15), Decimal("40.00")),
    ]
