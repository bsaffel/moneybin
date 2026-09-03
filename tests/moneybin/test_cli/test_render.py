"""The CLI's three text renderers and the money vocabulary they read.

Covers `docs/specs/cli-output-coherence.md` requirements 1-5 (the renderers and
their streams), 11-15 (money formatting, alignment, colour), 35 (no renderer
deduplicates), and 36 (the palette is declared once).

The money tests carry most of the weight. A sign is the only encoding that
survives a pipe, so every kind states what it does with a negative value, and
`balance` states it twice — a dropped minus turns a −50,000.00 net worth into a
+50,000.00 one, which is the worst misread this surface can ship.
"""

from __future__ import annotations

import ast
import io
import re
import sys
import time
from collections.abc import Iterator, Sequence
from decimal import Decimal
from itertools import product
from pathlib import Path
from typing import Any

import pytest

import moneybin.cli
from moneybin.cli.render import (
    ELISION,
    MINUS,
    Money,
    Placeholder,
    Style,
    _fit_columns,  # pyright: ignore[reportPrivateUsage]  # the fit is a property, not a rendering
    _table_width,  # pyright: ignore[reportPrivateUsage]  # so the check agrees with it on "fits"
    color_enabled,
    format_money,
    render_note,
    render_rows,
    render_summary,
)

CLI_ROOT = Path(moneybin.cli.__file__).parent
RENDERER = CLI_ROOT / "render.py"


class _Terminal(io.StringIO):
    """A stream that claims to be a terminal.

    Not a mock of the code under test: `color_enabled` takes a stream and reads
    `isatty()` off it, so this is an input value, the same as the `StringIO`
    used for the piped case.
    """

    def isatty(self) -> bool:
        return True


# --- format_money: separators and precision (requirement 11) ---


def test_format_money_always_separates_thousands() -> None:
    """Requirement 11: thousands separators always."""
    assert format_money(Decimal("1234567.89"), "balance") == "1,234,567.89"


def test_format_money_always_shows_two_decimal_places() -> None:
    """Requirement 11: two decimal places always, even on a whole amount."""
    assert format_money(Decimal("42"), "balance") == "42.00"


def test_a_boolean_in_a_money_column_renders_absent_not_as_one() -> None:
    """A true/false in a money column is a declaration bug — do not price it.

    `bool` is an `int`, so an unguarded coercion prints `1.00` and the wrong
    declaration reads as a real amount. Absent is the honest rendering, and it
    is what the sibling helper in `reports/_framework/convert.py` already does.
    """
    assert format_money(True, "flow") == "-"
    assert format_money(False, "balance") == "-"


def test_an_unparseable_money_cell_prints_itself_rather_than_raising() -> None:
    """The render layer is the wrong place to raise InvalidOperation.

    The traceback would name neither the column nor the report that declared
    it, and it would take down a whole table over one cell. It prints the text
    it was given rather than a dash, because a dash is this CLI's spelling of
    *absent* and text in the cell means something was there.
    """
    assert format_money("n/a", "flow") == "n/a"


def test_a_masked_money_cell_prints_its_mask_rather_than_a_dash() -> None:
    """Withheld and absent are different facts, so they cannot share a glyph.

    `redact_records` runs before rendering, so a money column carrying a
    whole-masking class (`ROUTING_NUMBER`, `COMPOSITE_IDENTIFIER`,
    `UNRESOLVED`) reaches this function already replaced by its sentinel.
    Formatting it as `-` would make the text CLI contradict both its own
    masking and the JSON/MCP result for the same query, and would leave a
    reader unable to tell a withheld amount from a SQL NULL.

    The rule is *text is not an amount*, not a list of the sentinels in use
    today: matching `"*****"` by value would miss the partial mask below and
    would pin a privacy constant into the render layer, where it would drift
    from `privacy/redaction.py`.
    """
    assert format_money("*****", "flow") == "*****"
    assert format_money("****1098", "balance") == "****1098"


def test_a_non_finite_money_cell_renders_absent_rather_than_crashing() -> None:
    """A NaN never reaches the reader — the comparison that would print it raises.

    `format_money` branches on `amount < 0`, and ordering a `Decimal("NaN")`
    raises `InvalidOperation` rather than returning False. That is not a
    `UserError`, so it leaves `handle_cli_errors` as a raw traceback and takes
    the whole command down over one cell. An infinity is quieter and worse: it
    orders fine and formats under `,.2f` as the word `Infinity`, so the money
    column prints `+Infinity` in income green and calls it an amount.

    Neither is one, so both take the route the unparseable cases already take.
    The check belongs in `_as_decimal` rather than in `format_money`, because
    `style_for` orders the same value and would raise identically.
    """
    for value in (float("nan"), Decimal("NaN"), float("inf"), Decimal("-Infinity")):
        assert format_money(value, "flow") == "-"
        assert Money("flow").style_for(value) is Style.NEUTRAL


def test_a_non_finite_money_cell_that_arrived_as_text_still_prints_itself() -> None:
    """Where the two rules meet, *text is not an amount* is the outer one.

    A column holding the string `"nan"` was handed text rather than a number
    that went non-finite in transit, so it prints what it was given for the
    same reason `"n/a"` and a `****1098` mask do.
    """
    assert format_money("nan", "flow") == "nan"


def test_format_money_renders_a_missing_amount_as_a_dash() -> None:
    """A NULL money cell is absent, not zero — rendering 0.00 would invent data.

    A dash rather than a blank so one table spells absence one way: the first
    period of a ``networth history`` series has no prior to difference against,
    and its `change_abs` sits beside a `change_pct` that has always printed
    ``-``.
    """
    assert format_money(None, "balance") == "-"
    assert format_money(None, "delta") == "-"


# --- format_money: the four money kinds (requirement 12) ---


def test_a_positive_flow_carries_an_explicit_plus() -> None:
    """Requirement 12: a flow renders its sign, so income reads as income."""
    assert format_money(Decimal("6240"), "flow") == "+6,240.00"


def test_a_negative_flow_carries_a_unicode_minus() -> None:
    """Requirement 12: U+2212, matching the design system's Amount component."""
    assert format_money(Decimal("-84.27"), "flow") == "−84.27"


def test_a_zero_flow_carries_no_sign() -> None:
    """Zero is not movement in either direction, so it claims neither.

    `+0.00` would read as income and `−0.00` as expense; the sign glyph means
    something only when there is a direction to name.
    """
    assert format_money(Decimal("0"), "flow") == "0.00"


def test_a_magnitude_renders_unsigned() -> None:
    """Requirement 12: `total_spend` is SUM(ABS(amount)) — a positive outflow."""
    assert format_money(Decimal("5506"), "magnitude") == "5,506.00"


def test_a_negative_magnitude_keeps_its_minus() -> None:
    """A negative magnitude means the declaration is wrong; do not hide it.

    Rendering it unsigned would print 50.00 for −50.00 — a renderer inventing a
    number. The declaration is the thing at fault, and it is only findable if
    the value stays legible.
    """
    assert format_money(Decimal("-50"), "magnitude") == "−50.00"


def test_a_delta_renders_signed() -> None:
    """Requirement 12: the direction is the column's whole purpose.

    Polarity decides the colour, never the glyph — a rise is `+` whether the
    thing rising is income or spending.
    """
    assert format_money(Decimal("312.5"), "delta") == "+312.50"


def test_a_non_negative_balance_renders_unsigned() -> None:
    """Requirement 12: the design system's "balances unsigned" rule."""
    assert format_money(Decimal("12480.22"), "balance") == "12,480.22"


def test_a_negative_balance_keeps_its_minus() -> None:
    """Requirement 12's load-bearing case: a negative net worth is reachable.

    "Balances unsigned" exists so a checking balance carries no decorative `+`.
    It never licensed dropping a `−`, which would render −50,000.00 and
    +50,000.00 identically.
    """
    assert format_money(Decimal("-50000"), "balance") == "−50,000.00"


# --- Colour is driven by kind plus value, never value alone (requirement 14) ---


def test_a_positive_flow_is_income_coloured() -> None:
    """Requirement 14: a flow reads its sign under the accounting convention."""
    assert Money("flow").style_for(Decimal("6240")) is Style.POSITIVE


def test_a_negative_flow_is_expense_coloured() -> None:
    assert Money("flow").style_for(Decimal("-84.27")) is Style.NEGATIVE


def test_a_magnitude_is_never_income_coloured() -> None:
    """Requirement 14: `total_spend` is positive by construction, not income.

    Colouring on the raw sign would paint every spending figure green.
    """
    assert Money("magnitude").style_for(Decimal("5506")) is not Style.POSITIVE


def test_a_rising_expense_delta_reads_as_expense() -> None:
    """Requirement 14: a delta colours against its declared polarity.

    `mom_delta` is `total_spend - prev_month_spend`, so a positive value means
    spending rose. Colouring on the raw sign would call that income.
    """
    assert Money("delta", polarity="expense").style_for(Decimal("312.5")) is (
        Style.NEGATIVE
    )


def test_a_falling_expense_delta_reads_as_income() -> None:
    """The other half: spending fell, which is the good direction."""
    assert Money("delta", polarity="expense").style_for(Decimal("-312.5")) is (
        Style.POSITIVE
    )


def test_a_rising_income_delta_reads_as_income() -> None:
    """Polarity inverts the reading — a rise in income is the good direction."""
    assert Money("delta", polarity="income").style_for(Decimal("312.5")) is (
        Style.POSITIVE
    )


def test_a_balance_is_never_coloured() -> None:
    """Requirement 14: a position is not a movement, so nothing is signalled.

    The negative case is why this matters: it carries `−` with no colour, so
    the glyph is the only channel and must never be the one dropped.
    """
    assert Money("balance").style_for(Decimal("-50000")) is Style.NEUTRAL


def test_a_delta_declaration_requires_a_polarity() -> None:
    """A delta without polarity cannot be coloured, so it must not be built.

    Defaulting the polarity would silently pick a direction, and the wrong
    default paints a spending rise green.
    """
    with pytest.raises(ValueError, match="polarity"):
        Money("delta")


# --- Every money column declares a kind (requirement 12) ---


def test_every_money_column_in_the_catalog_declares_a_money_kind() -> None:
    """Requirement 12: *every* money column, not just the ones a renderer hits.

    Which columns are money is not a judgement call here — the privacy taxonomy
    already answers it, so the guard derives its subject from `DataClass`
    rather than from a hand-kept list that would drift as reports are added. A
    new report with an undeclared amount fails here rather than rendering a
    spend figure as green income in front of a user.
    """
    from moneybin.privacy.taxonomy import DataClass
    from moneybin.reports._framework.registry import spec_of
    from moneybin.reports.definitions import ALL_REPORTS
    from moneybin.reports.service_reports import SERVICE_REPORTS

    money_classes = {DataClass.TXN_AMOUNT, DataClass.BALANCE}
    specs = [spec_of(runner) for runner in ALL_REPORTS] + list(SERVICE_REPORTS)
    assert specs, "the catalog came back empty, so this guard checked nothing"
    undeclared = [
        f"{spec.report_id}.{column.name}"
        for spec in specs
        for column in spec.columns
        if column.data_class in money_classes and column.money_kind is None
    ]
    assert undeclared == [], (
        "these money columns render without a declared kind, so the renderer "
        f"would have to guess what their sign means: {undeclared}"
    )


def test_every_declared_delta_column_names_its_polarity() -> None:
    """The half a missing declaration fails silently rather than loudly.

    `Money` refuses a delta with no polarity, but that refusal only fires when
    something builds one. A report declaring `delta` and no polarity would sail
    through registration and raise at render time, in front of the user.
    """
    from moneybin.reports._framework.registry import spec_of
    from moneybin.reports.definitions import ALL_REPORTS
    from moneybin.reports.service_reports import SERVICE_REPORTS

    specs = [spec_of(runner) for runner in ALL_REPORTS] + list(SERVICE_REPORTS)
    declared_deltas = [
        column
        for spec in specs
        for column in spec.columns
        if column.money_kind == "delta"
    ]
    assert declared_deltas, "no report declares a delta, so this guard is inert"
    unpolarized = [column.name for column in declared_deltas if column.polarity is None]
    assert unpolarized == [], (
        f"these delta columns do not say which direction is favourable: {unpolarized}"
    )


# --- Colour gating (requirement 15) ---


def test_colour_is_enabled_on_a_bare_terminal() -> None:
    assert color_enabled(_Terminal(), {}) is True


def test_colour_is_disabled_when_the_stream_is_not_a_terminal() -> None:
    """Requirement 15: a redirect or a pipe gets no escape codes."""
    assert color_enabled(io.StringIO(), {}) is False


def test_colour_is_disabled_when_no_color_is_set() -> None:
    """Requirement 15: https://no-color.org — presence is what counts."""
    assert color_enabled(_Terminal(), {"NO_COLOR": ""}) is False


def test_rendering_to_a_pipe_emits_no_escape_codes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The end-to-end half of requirement 15: capsys is not a terminal."""
    render_rows(["amount"], [(Decimal("-84.27"),)], money={"amount": Money("flow")})

    assert "\x1b[" not in capsys.readouterr().out


# --- render_rows (requirements 2, 13, 35) ---


def test_render_rows_writes_the_table_to_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 2: rows are the result, so they go to stdout."""
    render_rows(["account"], [("Checking",)])

    captured = capsys.readouterr()
    assert "Checking" in captured.out
    assert "Checking" not in captured.err


def test_render_rows_formats_a_declared_money_column(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 11: the renderer is the only place amounts are stringified."""
    render_rows(["amount"], [(Decimal("-1234.5"),)], money={"amount": Money("flow")})

    assert "−1,234.50" in capsys.readouterr().out


def test_render_rows_shows_a_masked_amount_as_withheld_not_absent(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The table must not contradict the masking the framework already applied.

    A declared money column redacted to its sentinel reaches `render_rows`
    as a string. Printing `-` there would tell the reader the report returned
    nothing, while `--output json` for the same query shows the mask.
    """
    render_rows(["amount"], [("*****",)], money={"amount": Money("flow")})

    out = capsys.readouterr().out
    assert "*****" in out
    assert "-" not in out.replace("─", "")


def test_render_rows_right_aligns_amounts(capsys: pytest.CaptureFixture[str]) -> None:
    """Requirement 13: unequal-width amounts line up on their last digit."""
    render_rows(
        ["amount"],
        [(Decimal("5"),), (Decimal("1234567.89"),)],
        money={"amount": Money("balance")},
    )

    out = capsys.readouterr().out
    short = next(line for line in out.splitlines() if "5.00" in line)
    long = next(line for line in out.splitlines() if "1,234,567.89" in line)
    assert short.rindex("5.00") + len("5.00") == long.rindex("89") + len("89")


def test_render_rows_never_collapses_identical_rows(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 35: two rows in, two rows out.

    `reports networth` sums an account once per balance source, so a duplicated
    account shows as repeated rows. Collapsing them here would make the output
    look right while the total stayed wrong — removing the symptom that finds
    the defect.
    """
    render_rows(
        ["account", "balance"],
        [("Checking", Decimal("100")), ("Checking", Decimal("100"))],
        money={"balance": Money("balance")},
    )

    assert capsys.readouterr().out.count("Checking") == 2


@pytest.mark.parametrize(
    "cell",
    [
        "spend [excluding rent]",
        "user [archived]",
        "[bold red]not a style[/bold red]",
    ],
    ids=["user-bracket", "status-marker", "style-tag"],
)
def test_a_bracketed_cell_reaches_the_table_intact(
    cell: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every bracketed form survives: a user's prose, our marker, a style tag.

    Cells are data, much of it user-authored — a report description, a merchant
    name. Rich reads ``[...]`` as a style tag, so a default console drops
    "spend [excluding rent]" to "spend " and lets stored text steer the
    terminal. The style-tag case is the one that proves markup is off rather
    than merely tolerated: a console with markup enabled renders it as styling
    and emits none of its literal characters.
    """
    render_rows(["description"], [(cell,)])

    printed = capsys.readouterr().out
    # Rich wraps a cell too wide for the terminal, so compare on the characters
    # rather than the run: what must not happen is a bracket disappearing.
    assert "[" in printed and "]" in printed
    for token in cell.replace("[", " ").replace("]", " ").split():
        assert token in printed


def test_a_cell_wider_than_the_terminal_keeps_every_character(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No character is ever dropped to make a row fit.

    The account-link review queue is the case that decides this: a resolved
    display name ends in the masked last four, so elision would remove exactly
    the digits that tell two candidates at one institution apart. Wrapping a
    long value makes a row ragged; eliding it makes the answer wrong.

    The name is deliberately wider than the 80 columns Rich assumes for a
    non-terminal stream, so the row cannot fit and the renderer has to choose
    between wrapping and eliding. A name that fits proves nothing.
    """
    name = (
        "Chase Sapphire Preferred Joint Household Reserve Signature "
        "Rewards Account ****1098"
    )
    assert len(name) > 80, "the fixture must not fit, or the choice never arises"
    render_rows(["candidate", "signal"], [(name, "last-four match")])

    printed = capsys.readouterr().out
    assert "…" not in printed
    # Word by word rather than as one run: a wrapped row splits the name across
    # lines, with the second column's text between the halves.
    for word in name.split():
        assert word in printed, f"{word!r} was dropped from the rendered row"


def test_an_unbreakable_cell_wider_than_the_terminal_survives_too(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The harder half: a value with no space to wrap on.

    Wrapping saves a long *phrase*; it cannot save a single long token, which
    is what the values most likely to overflow actually are — an account id, a
    checksum, an export path. Those are also the values whose whole purpose is
    to be compared character by character, so eliding one is worse than
    eliding prose.
    """
    token = "a" * 120
    render_rows(["checksum"], [(token,)])

    printed = "".join(capsys.readouterr().out.split())
    assert "…" not in printed
    assert printed.count("a") == 120


def test_a_money_cell_never_folds_while_a_text_column_could_shrink_instead(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Folding is right for an identifier and wrong for an amount.

    The two tests above establish that a long identifier wraps rather than
    elides, because its characters are what tell two rows apart. An amount
    inverts that: `1,200.00` folded after the decimal point renders `1,200.`
    above `00`, and the first line reads as a complete, plausible, much smaller
    number. `cli.md` calls a folded amount a correctness bug for exactly that
    reason.

    Nine ordinary columns at 80 is where it bites — the widths here are the
    real `investments holdings --wide` projection with unremarkable values, not
    a contrived overflow. Money columns are unwrappable, so Rich spends the
    squeeze on the text columns first and every amount survives whole.
    """
    monkeypatch.setenv("COLUMNS", "80")
    render_rows(
        [
            "security",
            "quantity",
            "cost basis",
            "avg cost",
            "market value",
            "unrealized",
            "currency",
            "status",
            "as of",
        ],
        [
            (
                "VTSAX",
                "120.5",
                Decimal("1000.00"),
                "8.2987654321",
                Decimal("1200.00"),
                Decimal("200.00"),
                "USD",
                "priced",
                "2026-08-29 (3d)",
            )
        ],
        money={
            "cost basis": Money("balance"),
            "market value": Money("balance"),
            "unrealized": Money("delta", polarity="income"),
        },
    )

    lines = capsys.readouterr().out.splitlines()
    for amount in ("1,000.00", "1,200.00", "+200.00"):
        assert any(amount in line for line in lines), (
            f"{amount!r} was split across lines; a folded amount reads as a smaller one"
        )


def test_a_number_that_is_not_an_amount_never_folds_either(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guarantee above belongs to numbers, not to the `money` declaration.

    Same projection and same values as the test above, which asserts only that
    the three *amounts* survive. `quantity` and `avg cost` are numbers too, and
    they are absent from `money=` for a reason that has nothing to do with
    folding: `format_money` rounds to two places, which renders a
    `DECIMAL(28,10)` per-unit price as `0.00`. That exclusion used to take the
    no-fold guarantee with it, so `8.2987654321` folded to `8.29` above `8765`
    — a complete, plausible, three-orders-of-magnitude-wrong price, which is
    the same defect `cli.md` calls a correctness bug for amounts.

    `numeric=` carries atomicity without formatting, which is why both survive
    here. Drop it from the call and this test fails on `avg cost` first.
    """
    monkeypatch.setenv("COLUMNS", "80")
    render_rows(
        [
            "security",
            "quantity",
            "cost basis",
            "avg cost",
            "market value",
            "unrealized",
            "currency",
            "status",
            "as of",
        ],
        [
            (
                "VTSAX",
                "120.5",
                Decimal("1000.00"),
                "8.2987654321",
                Decimal("1200.00"),
                Decimal("200.00"),
                "USD",
                "priced",
                "2026-08-29 (3d)",
            )
        ],
        money={
            "cost basis": Money("balance"),
            "market value": Money("balance"),
            "unrealized": Money("delta", polarity="income"),
        },
        numeric=("quantity", "avg cost"),
    )

    lines = capsys.readouterr().out.splitlines()
    for number in ("120.5", "8.2987654321"):
        assert any(number in line for line in lines), (
            f"{number!r} was split across lines; a folded per-unit price or share "
            "count reads as a different, plausible number exactly as an amount does"
        )


def test_an_unfittable_money_cell_is_marked_truncated_rather_than_shortened(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """When even the squeeze is not enough, say so in the cell.

    Two money columns and nothing else to shrink is the case no layout can
    satisfy. Cropping silently would print `1,234,56`, which is a complete and
    entirely believable number that is off by a factor of a hundred. The
    ellipsis costs one character and makes the cell unmistakably partial, so a
    reader can tell "wider terminal needed" from "this is what you own".
    """
    monkeypatch.setenv("COLUMNS", "24")
    render_rows(
        ["market value", "unrealized"],
        [(Decimal("1234567.89"), Decimal("9876543.21"))],
        money={
            "market value": Money("balance"),
            "unrealized": Money("delta", polarity="income"),
        },
    )

    printed = capsys.readouterr().out
    assert "1,234,5…" in printed
    # The bare prefix must not appear unmarked: that is the silent-crop failure.
    assert "1,234,56 " not in printed


def test_render_rows_does_not_auto_highlight_a_bare_number(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Rich colours numbers on sight, which would decide colour from the value.

    Requirement 14 puts that decision in the money kind. An undeclared column
    is not money at all, so its digits must reach the terminal unstyled — the
    highlighter would otherwise paint a row count or an account id.
    """
    render_rows(["txn_count"], [(42,)])

    assert "\x1b[" not in capsys.readouterr().out


# --- Result framing (requirement 10) ---


def test_render_rows_names_the_columns_it_omitted(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 10: silent truncation is prohibited.

    The line states the count and the flag that recovers the rest, so a reader
    who wants the omitted columns learns both that there are some and how to
    see them without consulting `--help`.
    """
    render_rows(["year_month"], [("2026-08",)], total_columns=9)

    assert "1 of 9 columns shown — --wide for all" in capsys.readouterr().out


def test_the_framing_line_shares_the_stream_carrying_the_table(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 10: the framing rides stdout, with the result it describes.

    `moneybin reports spending > report.txt` must capture the disclosure along
    with the table. Routing it to stderr would let the redirected file record a
    truncated result with nothing in it saying so, which is the silent
    truncation this requirement forbids arriving by another route.
    """
    render_rows(["year_month"], [("2026-08",)], total_columns=9)

    captured = capsys.readouterr()
    assert "columns shown" in captured.out
    assert "columns shown" not in captured.err


def test_no_framing_line_when_every_column_is_shown(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A complete result frames nothing — there is no omission to disclose."""
    render_rows(["year_month", "net"], [("2026-08", 1)], total_columns=2)

    assert "columns shown" not in capsys.readouterr().out


def test_render_rows_frames_the_rows_it_did_not_show(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 34: a partial page discloses that it is one.

    The count `transactions list` already has is the whole disclosure. It
    replaces a `Next page: --cursor <base64>` line that spent a row of screen
    on an opaque token the reader can neither read nor act on.
    """
    render_rows(["date"], [("2026-08-01",)], total_rows=50)

    assert "1 of 50 shown" in capsys.readouterr().out


def test_the_row_framing_names_a_continuation_the_reader_can_type(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 34: the continuation is `--limit`, and never `--cursor`.

    `--cursor` takes a base64 token the text branch deliberately no longer
    supplies, so naming it would send the reader into a usage error. `--limit`
    is a number they can raise unaided. The total is grouped, because the count
    this replaces exists to answer "how much is there?" and `2046` answers it
    less well than `2,046` at a glance.
    """
    render_rows(["date"], [("2026-08-01",)], total_rows=2046, has_more=True)

    out = capsys.readouterr().out
    assert "1 of 2,046 shown · raise --limit for more" in out
    assert "--cursor" not in out


def test_no_row_framing_when_the_page_is_the_whole_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A complete result frames nothing — there is no remainder to disclose."""
    render_rows(["date"], [("2026-08-01",)], total_rows=1)

    assert "shown" not in capsys.readouterr().out


def test_render_rows_counts_the_unmapped_placeholder(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 30: a taxonomy gap is disclosed, not silently absorbed.

    Collapsing every unmapped value to one placeholder is what keeps a single
    column from mixing two vocabularies, but it also makes the gap invisible —
    the reader cannot tell a genuinely uncategorized row from one whose
    provider code MoneyBin has no mapping for. The count is the disclosure.
    """
    render_rows(
        ["date", "category"],
        [("2026-08-01", "Food & Drink"), ("2026-08-02", None)],
        placeholder=Placeholder("category", "Uncategorized"),
    )

    assert "1 uncategorized" in capsys.readouterr().out


def test_the_placeholder_count_fires_with_every_column_shown(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 30 widens requirement 10's trigger, and this is why.

    Framing that fired only on an omitted column would vanish in exactly the
    cases the disclosure matters — under `--wide`, and for any result whose
    full projection already fits 80 columns. Neither omits a column.
    """
    render_rows(
        ["category"],
        [(None,)],
        total_columns=1,
        placeholder=Placeholder("category", "Uncategorized"),
    )

    captured = capsys.readouterr().out
    assert "columns shown" not in captured
    assert "1 uncategorized" in captured


def test_no_placeholder_framing_when_every_value_mapped(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A fully-mapped column has no gap, so it says nothing about one."""
    render_rows(
        ["category"],
        [("Food & Drink",)],
        placeholder=Placeholder("category", "Uncategorized"),
    )

    assert "uncategorized" not in capsys.readouterr().out


def test_the_framing_clauses_share_one_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 30: the two framing clauses may share one line.

    Three separate lines beneath a two-row table would cost more screen than
    the result, which is the opposite of what curation is for.
    """
    render_rows(
        ["category"],
        [(None,)],
        total_columns=4,
        total_rows=9,
        has_more=True,
        placeholder=Placeholder("category", "Uncategorized"),
    )

    framing = [
        line
        for line in capsys.readouterr().out.splitlines()
        if "shown" in line or "uncategorized" in line
    ]
    assert len(framing) == 1, framing
    assert "1 of 9 shown" in framing[0]
    assert "1 of 4 columns shown" in framing[0]
    assert "1 uncategorized" in framing[0]


def test_no_continuation_offered_when_no_next_page_exists(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The remedy is gated on a next page, not on the remainder.

    ``total_rows`` is every row matching the filters and holds steady across a
    cursor walk, so on the last page of one it still exceeds the page length.
    Offering `--limit` there names a remedy that fetches nothing: the reader
    has already walked past those rows, and raising the limit on this call
    returns the same tail. The count still prints — the table really is a
    slice of 2,046 — but the sentence that promises more does not.
    """
    render_rows(["date"], [("2026-08-01",)], total_rows=2046, has_more=False)

    out = capsys.readouterr().out
    assert "1 of 2,046 shown" in out
    assert "--limit" not in out


def test_the_placeholder_count_reads_only_its_own_column(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 30's count means a taxonomy gap, so it counts one column.

    Every cell here is user-authored text, and a bank description reading
    `Uncategorized` is a value, not a gap. Scanning the whole row for the
    string would count that description as a missing category and inflate the
    one number whose only value is being exact.
    """
    render_rows(
        ["description", "category"],
        [("Uncategorized", "Food & Drink"), ("Coffee", None)],
        placeholder=Placeholder("category", "Uncategorized"),
    )

    assert "1 uncategorized" in capsys.readouterr().out


def test_a_category_authored_as_the_placeholder_word_is_not_a_gap(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Absence is read off the stored value, never off the rendered string.

    The `tabular` and `manual` sources keep whatever category text a person
    wrote, and plenty of tools export the literal word `Uncategorized` as their
    own placeholder — importing one puts that string in the column as a real,
    authored value. Substituting first and matching the string back would make
    it indistinguishable from a NULL, and `--output json` promises the caller
    those two are distinguishable. Only the NULL is a gap; the authored word
    renders as itself and is not counted.
    """
    render_rows(
        ["category"],
        [("Uncategorized",), (None,)],
        placeholder=Placeholder("category", "Uncategorized"),
    )

    out = capsys.readouterr().out
    assert "1 uncategorized" in out
    assert "2 uncategorized" not in out


def test_a_blank_category_is_not_counted_as_absent(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Only NULL is absent, and the restraint is deliberate.

    A whitespace-only category is reachable — Polars reads an empty CSV cell as
    NULL, but a cell holding spaces keeps them — and it is tempting to treat it
    as a gap. `core.uncategorized_queue` selects `WHERE category IS NULL`, so
    such a row is not in the queue: counting it would advertise a gap
    `transactions categorize run` cannot act on, which is the same class of lie
    as the provider code this milestone removed from the column. Making the two
    agree is a staging change — `NULLIF(TRIM(category), '')` — not a rendering
    one, so this renderer follows the queue rather than getting ahead of it.
    """
    render_rows(
        ["category"],
        [(None,), ("   ",), ("Food & Drink",)],
        placeholder=Placeholder("category", "Uncategorized"),
    )

    out = capsys.readouterr().out
    assert "1 uncategorized" in out
    assert "2 uncategorized" not in out


def test_a_placeholder_naming_no_column_of_this_table_is_refused() -> None:
    """A typo in the declaration fails loudly, as an undeclared column does.

    Counting nothing renders exactly like a table with no gaps, so a silent
    skip would leave the mistake in place for as long as the column stayed
    misspelled — and the disclosure it was meant to make missing that whole
    time.
    """
    with pytest.raises(ValueError, match="undeclared placeholder column"):
        render_rows(
            ["date", "category"],
            [("2026-08-01", None)],
            placeholder=Placeholder("categroy", "Uncategorized"),
        )


def test_a_placeholder_on_a_money_column_is_refused() -> None:
    """The same silent-zero failure as an undeclared column, one column over.

    `format_money` already spells a missing amount `-` and owns that cell, so a
    placeholder declared there would never substitute and the count would read
    zero forever — a disclosure that cannot fire is worse than none, because it
    renders as a table with no gaps.
    """
    with pytest.raises(ValueError, match="placeholder on money column"):
        render_rows(
            ["amount"],
            [(None,)],
            money={"amount": Money("flow")},
            placeholder=Placeholder("amount", "Unknown"),
        )


def test_the_placeholder_count_ignores_a_column_the_fit_dropped(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A gap in a column that is not on screen is not a gap the reader can see.

    The count describes this table. When the width fit drops the declared
    column, the disclosure it carried goes with it rather than describing
    cells nobody can read.
    """
    before = [f"leading_column_{i}" for i in range(1, 7)]
    after = [f"trailing_column_{i}" for i in range(1, 7)]
    render_rows(
        # Mid-projection, because the fit keeps a prefix and a suffix: a
        # declared column at either end survives and proves nothing.
        [*before, "category", *after],
        [
            (
                *(f"value{i}" for i in before),
                None,
                *(f"value{i}" for i in after),
            )
        ],
        placeholder=Placeholder("category", "Uncategorized"),
        fit=True,
    )

    out = capsys.readouterr().out
    assert "columns shown" in out
    assert "uncategorized" not in out


_FIT_COLUMNS = [f"column_number_{i}" for i in range(1, 15)]
_FIT_ROW = tuple(f"value{i}" for i in range(1, 15))


def test_a_fitted_table_keeps_the_ends_and_marks_the_gap(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Requirement 6, for a report that declared no columns of its own.

    The ends are what identify a row and carry its answer, so a squeeze drops
    the middle — the behaviour DuckDB's box renderer and pandas both have. The
    gap is marked rather than spliced shut, because adjacent columns that were
    never adjacent read as the whole projection.
    """
    monkeypatch.setenv("COLUMNS", "80")

    render_rows(_FIT_COLUMNS, [_FIT_ROW], fit=True)

    out = capsys.readouterr().out
    assert "column_number_1 " in out
    assert "column_number_14" in out
    assert "column_number_7" not in out
    assert "…" in out


def test_a_fitted_table_discloses_what_it_dropped(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Requirement 10 reaches a narrowing the caller never asked for.

    The renderer decided this one from the terminal width, so the caller passed
    no `total_columns` and could not have framed it. Counting from what was
    printed is what keeps the disclosure attached to the decision.
    """
    monkeypatch.setenv("COLUMNS", "80")

    render_rows(_FIT_COLUMNS, [_FIT_ROW], fit=True)

    out = capsys.readouterr().out
    # Counted off the rendered headers, not by substring: `column_number_1`
    # occurs inside `column_number_14`, so a naive count agrees with a wrong
    # framing line as readily as a right one.
    shown = len(set(re.findall(r"column_number_\d+", out)))
    assert f"{shown} of 14 columns shown — --wide for all" in out
    assert shown < len(_FIT_COLUMNS)


def test_a_wide_terminal_fits_more_of_the_same_result(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The point of fitting rather than capping: the window decides."""

    def _shown(out: str) -> int:
        found = re.search(r"(\d+) of 14 columns shown", out)
        assert found is not None, out
        return int(found.group(1))

    monkeypatch.setenv("COLUMNS", "200")
    render_rows(_FIT_COLUMNS, [_FIT_ROW], fit=True)
    wide = _shown(capsys.readouterr().out)

    monkeypatch.setenv("COLUMNS", "80")
    render_rows(_FIT_COLUMNS, [_FIT_ROW], fit=True)
    narrow = _shown(capsys.readouterr().out)

    assert wide > narrow


def test_a_result_that_already_fits_is_left_whole(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fitting is not a cap: nothing is dropped or disclosed when it all fits."""
    monkeypatch.setenv("COLUMNS", "80")

    render_rows(["year_month", "net"], [("2026-08", 1)], fit=True)

    out = capsys.readouterr().out
    assert "…" not in out
    assert "columns shown" not in out


def test_the_fit_measures_values_not_just_headers(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A column is as wide as its widest cell, which the headers do not say.

    Deciding from header lengths alone is how a table of short names holding
    long values overflows anyway — the failure the fixed count had.
    """
    # Headers alone measure 17 characters and would fit; the values make the
    # real table 56, so only a value-aware fit narrows here.
    monkeypatch.setenv("COLUMNS", "30")
    columns = ["a", "b", "c", "d"]

    render_rows(columns, [("x" * 40, "y", "z", "w")], fit=True)

    assert "columns shown" in capsys.readouterr().out


def test_one_unkeepable_column_does_not_end_the_fit(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A column too wide to keep costs only itself, not the ones behind it.

    The walk alternates between the two ends, so abandoning it at the first
    candidate that does not fit discards everything still unvisited on the
    other side. Here `sprawling` can never be kept at any realistic terminal
    size, while `third` and `fourth` are a handful of characters each and there
    is room for both — stopping at `sprawling` would render a two-column table
    in an eighty-column window.
    """
    monkeypatch.setenv("COLUMNS", "80")
    columns = ["first", "sprawling", "third", "fourth", "fifth"]

    render_rows(columns, [("a", "w" * 100, "c", "d", "e")], fit=True)

    out = capsys.readouterr().out
    assert "4 of 5 columns shown — --wide for all" in out
    assert "third" in out
    assert "fourth" in out
    # Not "wide": the framing line's own `--wide for all` contains it.
    assert "sprawling" not in out


def _widest_one_gap_fit(widths: Sequence[int], available: int) -> tuple[int, ...]:
    """The most columns any single-gap projection can show in ``available``.

    Derived from the invariant `_fit_columns` documents — everything it drops
    is one contiguous run, so a projection is exactly a head length and a tail
    length — and not from the walk that function performs. It measures each
    candidate with the renderer's own width function so the two agree on what
    "fits" and disagree on nothing else.
    """
    total = len(widths)
    best: tuple[int, ...] = ()
    for head in range(1, total):
        for tail in range(1, total - head + 1):
            if head + tail == total:
                continue  # the whole projection, which has no gap to mark
            kept = (*range(head), *range(total - tail, total))
            shown = [*(widths[i] for i in kept), len(ELISION)]
            if _table_width(shown) <= available and len(kept) > len(best):
                best = kept
    return best


def test_the_fit_keeps_every_column_the_width_allows() -> None:
    """Requirement 8: the fit buys the most columns, not the nearest ones.

    Walking outside-in and closing a side at its first oversized column pays
    for that column and drops every narrow one behind it, so an eighty-column
    window renders four columns where six would fit. The candidates are few —
    a head length and a tail length, at most n²/2 of them for a column count —
    so this checks the whole space rather than sampling it.
    """
    for total in (4, 5, 6):
        for widths in product((1, 5, 13, 27), repeat=total):
            for available in (40, 55, 70, 85):
                kept = _fit_columns(widths, available)
                best = _widest_one_gap_fit(widths, available)
                assert len(kept) >= len(best), (
                    f"widths={list(widths)} available={available} keeps "
                    f"{len(kept)} columns {kept}, but {best} keeps "
                    f"{len(best)} and fits the same width"
                )


def test_the_fit_stays_cheap_on_a_projection_with_thousands_of_columns() -> None:
    """The fit runs before Rich prints anything, so its cost is felt as a hang.

    Nothing caps a report's output columns: a saved or extension report is
    whatever `SELECT` its author wrote, and the fit path exists precisely for
    the reports nobody declared columns for. Scoring each candidate by summing
    its own head-and-tail list is cubic, which is 5 seconds at 800 columns and
    unbounded past that — the CLI appears to hang on a legitimate query.

    The budget is loose on purpose. A fit whose cost tracks the column count
    finishes both of these in single-digit milliseconds, so a second leaves
    room for a loaded CI worker while still failing anything super-linear.
    """
    for total in (800, 6400):
        widths = [1] * total
        start = time.perf_counter()
        kept = _fit_columns(widths, 80)
        elapsed = time.perf_counter() - start

        assert elapsed < 1.0, (
            f"fitting {total} columns took {elapsed:.2f}s, so the cost grows "
            "faster than the projection does"
        )
        # Asserted alongside the timing so a fit that got fast by giving up on
        # the columns cannot pass this.
        assert len(kept) > 2


def test_an_unfitted_table_streams_its_rows(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the fitted path may buffer, because only it has to measure.

    `reports run` defaults to `CLI_MAX_ROWS` (1,000,000) rows, and Rich's
    `Table` already retains every cell it is given. Holding a second copy of
    the whole result alongside it is what turns a large-but-renderable report
    into an out-of-memory failure, so a table that is not being fitted hands
    each record straight to Rich and keeps none of its own.
    """
    from rich.table import Table

    produced: list[int] = []
    seen_when_added: list[int] = []
    add_row = Table.add_row

    def _spy(self: Table, *cells: Any, **kwargs: Any) -> None:
        seen_when_added.append(len(produced))
        add_row(self, *cells, **kwargs)

    monkeypatch.setattr(Table, "add_row", _spy)

    def _rows() -> Iterator[tuple[str]]:
        for i in range(4):
            produced.append(i)
            yield (f"row{i}",)

    render_rows(["value"], _rows())

    # Streaming: each record reaches Rich before the next one is produced.
    # Buffering first would make every entry 4.
    assert seen_when_added == [1, 2, 3, 4]
    assert "row3" in capsys.readouterr().out


def test_an_undeclared_total_frames_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The commands with no column policy print no framing line.

    `accounts list` and its siblings render every column they build, so there
    is nothing for them to declare and nothing to disclose.
    """
    render_rows(["account"], [("Checking",)])

    assert "columns shown" not in capsys.readouterr().out


# --- render_summary (requirement 3) ---


def test_render_summary_writes_label_and_value_to_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 3: a labelled scalar block is a result, so it is stdout."""
    render_summary([("Net worth", "12,480.22")])

    captured = capsys.readouterr()
    assert "Net worth" in captured.out
    assert "12,480.22" in captured.out
    assert captured.err == ""


def test_render_summary_prints_its_title_above_the_block(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A scalar block often answers a question the heading asks.

    `reports networth` prints one block per currency the profile holds; without
    a heading the second block's figures read as a continuation of the first.
    """
    render_summary([("Net worth", "12,480.22")], title="USD")

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "USD"
    assert "Net worth" in lines[1]


def test_render_summary_aligns_values_under_each_other(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 3: aligned pairs — labels of unequal length still line up."""
    render_summary([("Assets", "1.00"), ("Liabilities", "2.00")])

    lines = capsys.readouterr().out.splitlines()
    assert lines[0].index("1.00") == lines[1].index("2.00")


# --- render_note (requirements 4, 5) ---


def test_render_note_writes_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    """Requirement 4: a note is chatter about the answer, not the answer."""
    render_note("Converted from EUR at 1.08")

    captured = capsys.readouterr()
    assert "Converted from EUR" in captured.err
    assert captured.out == ""


def test_render_note_is_suppressed_by_quiet(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 4: -q silences notes."""
    render_note("Converted from EUR at 1.08", quiet=True)

    assert capsys.readouterr().err == ""


def test_quiet_does_not_suppress_result_rows(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Requirement 5: -q never reaches the data.

    The renderers are the enforcement point, so `render_rows` takes no quiet
    parameter at all — there is no way for a caller to route rows through it
    and have them silenced.
    """
    import inspect

    assert "quiet" not in inspect.signature(render_rows).parameters
    assert "quiet" not in inspect.signature(render_summary).parameters


# --- The renderers are the only renderers (requirements 1, 36) ---
#
# Three structural guards, not one string scan. Requirement 1 forbids a command
# building a `rich.Table` or hand-formatting an aligned list; requirement 36
# forbids a colour literal outside the palette. Both reduce to the same two
# capabilities — Rich, and Typer's own styling — so confining those is what
# makes the requirements enforceable by reading `render.py` rather than
# auditing every call site. The colour guards are deliberately preventive: the
# CLI has zero colour markup today, which is exactly the moment to fence the
# pattern off, before it is scattered across every command as the surface grows.


def _cli_modules() -> list[Path]:
    """Every CLI module the guards apply to — the renderer itself is exempt."""
    return sorted(p for p in CLI_ROOT.rglob("*.py") if p != RENDERER)


def _imported_roots(module: Path) -> set[str]:
    """Return the top-level package name of every import in ``module``."""
    roots: set[str] = set()
    for node in ast.walk(ast.parse(module.read_text())):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def _alignment_specs(module: Path) -> list[str]:
    """Return the aligned f-string fields this module echoes at the user.

    A ``:<8`` / ``:>18`` format spec inside a ``typer.echo`` is the signature of
    a hand-built column: nothing else needs a field padded to a fixed width.

    The scan reads the call rather than the whole file, so a padded width
    elsewhere — a computed value, a log line — does not register. That is a
    real gap for `logger.info` and a deliberate one: the logger writes to
    stderr, so a table built there is already the wrong stream for result data
    and fails `test_error_routing.py` on that count instead. What this guard
    owns is the stream results actually travel on.
    """
    found: list[str] = []
    for node in ast.walk(ast.parse(module.read_text())):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if not (
            isinstance(target, ast.Attribute)
            and getattr(target.value, "id", "") == "typer"
            and target.attr in {"echo", "secho"}
        ):
            continue
        for part in ast.walk(node):
            if not isinstance(part, ast.FormattedValue) or part.format_spec is None:
                continue
            spec = "".join(
                piece.value
                for piece in ast.walk(part.format_spec)
                if isinstance(piece, ast.Constant) and isinstance(piece.value, str)
            )
            if spec and spec[0] in "<>^":
                found.append(spec)
    return found


def _styling_calls(module: Path) -> list[str]:
    """Return this module's calls to Typer's own colour helpers."""
    return [
        node.func.attr
        for node in ast.walk(ast.parse(module.read_text()))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and getattr(node.func.value, "id", "") == "typer"
        and node.func.attr in {"secho", "style"}
    ]


def test_only_the_render_module_imports_rich() -> None:
    """Requirements 1 and 36: Rich is the render layer's private dependency.

    A command that cannot import Rich cannot build a second table idiom beside
    `render_rows`, and cannot write a style literal beside the palette. One
    guard covers both requirements because both failures need the same import.
    """
    offenders = [
        str(module.relative_to(CLI_ROOT))
        for module in _cli_modules()
        if "rich" in _imported_roots(module)
    ]
    assert offenders == [], (
        "these modules import Rich directly instead of calling "
        f"moneybin.cli.render: {offenders}"
    )


def test_no_command_hand_formats_an_aligned_column() -> None:
    """Requirement 1: a padded f-string column is a table by another name.

    This guard carried an exemption set (`_AWAITING_RENDER_ROWS`) for the
    eight modules the audit's own file list did not name, plus a second guard
    asserting set equality so the list could only shrink. The set is now
    empty, so both are gone: every CLI module is held to the rule
    unconditionally, which is what the list existed to converge on.
    """
    offenders = sorted(
        str(module.relative_to(CLI_ROOT))
        for module in _cli_modules()
        if _alignment_specs(module)
    )
    assert offenders == [], (
        "these commands hand-format aligned columns instead of calling "
        f"render_rows: {offenders}"
    )


def test_no_command_colours_output_through_typer() -> None:
    """Requirement 36: the palette is the only source of colour.

    `typer.secho` and `typer.style` take a colour name as an argument, so they
    are the one way to paint a line without importing Rich.
    """
    offenders = {
        str(module.relative_to(CLI_ROOT)): calls
        for module in _cli_modules()
        if (calls := _styling_calls(module))
    }
    assert offenders == {}, (
        "these commands colour output directly instead of going through "
        f"moneybin.cli.render.Style: {offenders}"
    )


def test_the_palette_names_a_meaning_for_every_colour() -> None:
    """Requirement 36: the palette is a fixed vocabulary, not a growing bag.

    Set equality rather than a count, so both a silent addition and a silent
    removal surface. Each member is a *meaning* a renderer asks for; a new one
    means a new semantic, which is a decision, not an implementation detail.
    """
    assert {member.name for member in Style} == {
        "POSITIVE",
        "NEGATIVE",
        "WARNING",
        "NEUTRAL",
    }


def _render_to_terminal(
    monkeypatch: pytest.MonkeyPatch,
    columns: list[str],
    rows: list[tuple[object, ...]],
    money: dict[str, Money],
) -> str:
    """Render through a stream that claims to be a terminal, and return it.

    The behavioural partner to the source scans: those prove no call site
    writes a colour literal, and this proves the palette's declared values are
    the ones that reach a terminal. Rich resolves `sys.stdout` at write time
    and consults `isatty()`, so substituting the stream is enough — no console
    internals are patched.
    """
    stream = _Terminal()
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("COLUMNS", "200")
    render_rows(columns, rows, money=money)
    return stream.getvalue()


def test_a_negative_flow_reaches_the_terminal_in_the_palettes_red(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 36: the palette's declared value is the one emitted."""
    out = _render_to_terminal(
        monkeypatch, ["amount"], [(Decimal("-84.27"),)], {"amount": Money("flow")}
    )

    assert "\x1b[31m" in out, f"expected {Style.NEGATIVE} for a negative flow: {out!r}"


def test_a_positive_flow_reaches_the_terminal_in_the_palettes_green(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half: swapping POSITIVE and NEGATIVE must fail something."""
    out = _render_to_terminal(
        monkeypatch, ["amount"], [(Decimal("84.27"),)], {"amount": Money("flow")}
    )

    assert "\x1b[32m" in out, f"expected {Style.POSITIVE} for a positive flow: {out!r}"


def test_a_magnitude_reaches_the_terminal_uncoloured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 14's whole point, asserted where the codes are emitted.

    `spending_trend.total_spend` is `SUM(ABS(amount))`. Colouring it by sign
    would paint every spend row green, which is the defect the money kinds
    exist to prevent — and a `style_for` assertion alone would not catch a
    renderer that coloured it anyway.
    """
    out = _render_to_terminal(
        monkeypatch, ["spend"], [(Decimal("84.27"),)], {"spend": Money("magnitude")}
    )

    assert "\x1b[32m" not in out, f"a magnitude must never be green: {out!r}"
    assert "\x1b[31m" not in out, f"a magnitude must never be red: {out!r}"


def test_a_negative_balance_reaches_the_terminal_uncoloured_but_signed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A balance is a position: it keeps its `\u2212` and earns no colour."""
    out = _render_to_terminal(
        monkeypatch, ["net"], [(Decimal("-50000"),)], {"net": Money("balance")}
    )

    assert f"{MINUS}50,000.00" in out
    assert "\x1b[31m" not in out, f"a balance carries no direction to colour: {out!r}"


# ---------------------------------------------------------------------------
# Curated default column sets (requirement 9's bar, applied to list commands)
# ---------------------------------------------------------------------------


def _declared_tables() -> list[tuple[str, Sequence[Any], Sequence[str]]]:
    """Every command table that curates a default view, with its declaration.

    Imported inside the function so this module keeps its cold-start hygiene:
    the command modules pull in service code that `render.py` deliberately does
    not.
    """
    from moneybin.cli.commands.import_cmd import (  # noqa: PLC0415, PLC2701
        _HISTORY_COLUMNS,  # pyright: ignore[reportPrivateUsage]
        _HISTORY_DEFAULT,  # pyright: ignore[reportPrivateUsage]
        _PDF_FORMAT_COLUMNS,  # pyright: ignore[reportPrivateUsage]
        _PDF_FORMAT_DEFAULT,  # pyright: ignore[reportPrivateUsage]
    )
    from moneybin.cli.commands.investments import (  # noqa: PLC0415, PLC2701
        _EVENTS_COLUMNS,  # pyright: ignore[reportPrivateUsage]
        _EVENTS_DEFAULT,  # pyright: ignore[reportPrivateUsage]
        _GAINS_COLUMNS,  # pyright: ignore[reportPrivateUsage]
        _GAINS_DEFAULT,  # pyright: ignore[reportPrivateUsage]
        _HOLDINGS_COLUMNS,  # pyright: ignore[reportPrivateUsage]
        _HOLDINGS_DEFAULT,  # pyright: ignore[reportPrivateUsage]
    )
    from moneybin.cli.commands.investments.lots import (  # noqa: PLC0415, PLC2701
        _LOTS_ALL_DEFAULT,  # pyright: ignore[reportPrivateUsage]
        _LOTS_COLUMNS,  # pyright: ignore[reportPrivateUsage]
        _LOTS_DEFAULT,  # pyright: ignore[reportPrivateUsage]
    )

    # `investments lots list` declares two default sets, and both are shipped
    # views: `--all` returns closed lots too, so it pays for a `state` column
    # the `--open` view would render as a constant. A second set that skipped
    # this list would skip both guards below with it.
    return [
        ("investments holdings", _HOLDINGS_COLUMNS, _HOLDINGS_DEFAULT),
        ("investments gains", _GAINS_COLUMNS, _GAINS_DEFAULT),
        ("investments list", _EVENTS_COLUMNS, _EVENTS_DEFAULT),
        ("investments lots list", _LOTS_COLUMNS, _LOTS_DEFAULT),
        ("investments lots list --all", _LOTS_COLUMNS, _LOTS_ALL_DEFAULT),
        ("import history", _HISTORY_COLUMNS, _HISTORY_DEFAULT),
        ("import formats list --type=pdf", _PDF_FORMAT_COLUMNS, _PDF_FORMAT_DEFAULT),
    ]


def test_every_curated_default_names_a_column_that_exists() -> None:
    """A typo in a default set must fail here, not render a view nobody declared.

    `column_view` refuses an undeclared name at runtime, but a command whose
    default set is only exercised by a test that passes `--wide` would never
    reach that refusal.
    """
    tables = _declared_tables()
    # Both checks in this section are over a comprehension, so an empty list
    # would pass them vacuously. Pin the population.
    assert len(tables) == 7
    undeclared = {
        command: sorted(set(default) - {name for name, _ in columns})
        for command, columns, default in tables
        if set(default) - {name for name, _ in columns}
    }
    assert undeclared == {}


def test_every_curated_default_fits_eighty_columns_on_headers_alone() -> None:
    """Requirement 9's bar: the curated view is chosen to survive 80 columns.

    Headers are the floor, not the whole story — a long value still widens a
    column — but a default set whose headers alone overflow cannot fit any
    data, and that is worth catching at declaration time.
    """
    tables = _declared_tables()
    assert len(tables) == 7
    too_wide = {
        command: _table_width([len(name) for name in default])
        for command, _columns, default in tables
        if _table_width([len(name) for name in default]) > 80
    }
    assert too_wide == {}
