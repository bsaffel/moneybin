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
import sys
from decimal import Decimal
from pathlib import Path

import pytest

import moneybin.cli
from moneybin.cli.render import (
    MINUS,
    Money,
    Style,
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


def test_an_unparseable_money_cell_renders_absent_rather_than_raising() -> None:
    """The render layer is the wrong place to raise InvalidOperation.

    The traceback would name neither the column nor the report that declared
    it, and it would take down a whole table over one cell.
    """
    assert format_money("n/a", "flow") == "-"


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


# DEPRECATED: hand-formatted-column — modules still building their own aligned
# columns instead of calling `render_rows`. Requirement 1 forbids the pattern
# outright; these are the sites the requirement's own file list did not name,
# found by this guard rather than by the audit, and they migrate in M3K.3's
# third pull request. The list only shrinks: it is asserted by set equality
# below, so a module that migrates must be removed from it and a module that
# acquires the pattern fails.
#
# Residual, stated rather than hidden: an already-listed module could grow a
# *second* hand-formatted table without this guard noticing, because the pin is
# per module rather than per call site. Pinning the format specs themselves
# would catch that and would also churn on every unrelated width change; since
# the whole list is scheduled to reach empty inside this milestone, the coarser
# pin is the proportionate one. What it does catch is the regression that
# matters — the pattern spreading to a module that had shed it.
_AWAITING_RENDER_ROWS = frozenset({
    "commands/db.py",
    "commands/demo.py",
    "commands/fx.py",
    "commands/import_cmd.py",
    "commands/investments/__init__.py",
    "commands/investments/lots.py",
    "commands/investments/prices.py",
    "commands/investments/securities.py",
})


def test_no_command_hand_formats_an_aligned_column() -> None:
    """Requirement 1: a padded f-string column is a table by another name."""
    offenders = {
        name
        for module in _cli_modules()
        if _alignment_specs(module)
        and (name := str(module.relative_to(CLI_ROOT))) not in _AWAITING_RENDER_ROWS
    }
    assert offenders == set(), (
        "these commands hand-format aligned columns instead of calling "
        f"render_rows: {sorted(offenders)}"
    )


def test_every_deferred_module_still_has_the_pattern_it_is_excused_for() -> None:
    """The exemption list is set equality, so it cannot rot in either direction.

    A stale entry is the failure mode that matters: a module that has since
    been migrated would keep a standing excuse, and the next hand-formatted
    table added to it would pass. Deriving the live set from the same scan the
    guard above uses means the two can never disagree.
    """
    live = {
        str(module.relative_to(CLI_ROOT))
        for module in _cli_modules()
        if _alignment_specs(module)
    }
    assert live & _AWAITING_RENDER_ROWS == _AWAITING_RENDER_ROWS, (
        "these modules are excused from requirement 1 but no longer need to be; "
        f"remove them from _AWAITING_RENDER_ROWS: "
        f"{sorted(_AWAITING_RENDER_ROWS - live)}"
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
