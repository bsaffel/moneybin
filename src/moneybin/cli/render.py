"""The CLI's text renderers — one way to print each shape of result.

Three renderers cover every text branch in the CLI, per
`docs/specs/cli-output-coherence.md` requirements 1-5:

- :func:`render_rows` — a collection of records, to **stdout**.
- :func:`render_summary` — a labelled scalar block, to **stdout**.
- :func:`render_note` — an informational status line, to **stderr**, silenced
  by ``-q``.

Rows and summaries are the answer the user asked for, so neither renderer
accepts a ``quiet`` parameter at all: there is no way to route a result through
this module and have it suppressed (requirement 5).

This is deliberately not a pluggable formatter registry. The abstraction exists
to remove ambiguity about which shape to use, not to add flexibility.

Rich is imported inside the functions that need it, per `cli.md` cold-start
hygiene — importing this module must stay cheap enough for `--help`.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from itertools import accumulate
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from rich.console import RenderableType

    from moneybin.reports._framework.contract import MoneyKind, Polarity

__all__ = [
    "UNCATEGORIZED_LABEL",
    "ColumnView",
    "Money",
    "Placeholder",
    "Style",
    "color_enabled",
    "column_view",
    "format_money",
    "render_note",
    "render_rows",
    "render_summary",
]

MINUS = "\N{MINUS SIGN}"
"""U+2212, not the hyphen-minus.

The design system's ``Amount`` component signs money with this glyph; matching
it keeps a CLI amount and a web amount the same string.
"""


class Style(StrEnum):
    """The CLI's semantic colour palette — declared once (requirement 36).

    Members name a *meaning*; values are the Rich style that carries it. No
    command or renderer branch writes a colour literal inline, so verifying
    requirement 14 ("a magnitude is never green") means reading this table
    rather than auditing every call site, and a colourblind-safe variant is a
    one-file change.

    The values are terminal palette names rather than the design system's
    ``--pos-income`` / ``--neg-expense`` hexes on purpose. Those tokens come in
    a dark pair and a light pair, and a terminal does not tell us which
    background it is painting on: #45B27B clears the contrast floor on the dark
    theme's ground and falls under it on paper, and #157A52 does the reverse.
    A palette name resolves to whatever the user configured, which is legible
    against their own background by construction. Colour is redundant with the
    sign glyph either way (requirement 14), so fidelity is the cheaper thing to
    trade.
    """

    POSITIVE = "green"
    NEGATIVE = "red"
    WARNING = "yellow"
    NEUTRAL = ""


@dataclass(frozen=True, slots=True)
class Money:
    """How one column's amounts are rendered and coloured.

    ``kind`` is declared per column by whoever knows what the number means; the
    renderer never infers it from the value (requirement 12). ``polarity`` says
    which direction is the good one for a ``delta``, and is meaningless for the
    other three kinds.
    """

    kind: MoneyKind
    polarity: Polarity | None = None

    def __post_init__(self) -> None:
        """Reject a delta with no polarity rather than pick a direction for it.

        A delta's whole purpose is the direction it reports, and which
        direction is *good* is not derivable from the number: +312.50 of
        `mom_delta` is spending rising, while +312.50 of an income delta is
        earnings rising. Defaulting would paint one of those the wrong colour
        silently, so the declaration is required where it is load-bearing.
        """
        if self.kind == "delta" and self.polarity is None:
            raise ValueError("a delta column must declare its polarity")

    def style_for(self, value: object) -> Style:
        """Return the colour this value earns under this column's declaration.

        Requirement 14: colour reads the kind plus the value, never the value
        alone. Colouring on the raw sign would render `SUM(ABS(amount))`
        spending as income and invert the meaning of a rising delta.
        """
        amount = _as_decimal(value)
        if amount is None or not amount:
            return Style.NEUTRAL
        rising = amount > 0
        if self.kind == "flow":
            return Style.POSITIVE if rising else Style.NEGATIVE
        if self.kind == "delta":
            good = rising if self.polarity == "income" else not rising
            return Style.POSITIVE if good else Style.NEGATIVE
        # `magnitude` is positive by construction — its polarity is carried by
        # the column, not the value — and `balance` is a position rather than a
        # movement. Neither has a direction to signal, and colouring a
        # magnitude on its sign is exactly the green-spending bug.
        return Style.NEUTRAL


def format_money(value: object, kind: MoneyKind) -> str:
    """Stringify one amount — the only place text output does so (req 11).

    Thousands separators always, two decimal places always. A missing amount
    renders ``-``: a NULL cell is absent, and printing ``0.00`` would invent a
    number nobody stored. It is a dash rather than a blank because that is what
    absence is already spelled as across this CLI — ``confidence_cell``, the
    review queues, ``accounts list`` — and because a blank money cell beside a
    dashed one reads as two different facts. The first period of a
    ``networth history`` series has no prior to difference against, so this is
    the common case, not an edge.

    A ``−`` is never dropped, whatever the kind. "Balances unsigned" exists so
    a checking balance carries no decorative ``+``; read as licence to drop the
    minus it would render a −50,000.00 net worth as +50,000.00.
    """
    amount = _as_decimal(value)
    if amount is None:
        # Text in a money cell is a value, not a missing one. `redact_records`
        # masks a whole-masked money column to a sentinel before the renderer
        # sees it, and spelling that withheld amount `-` would both contradict
        # the JSON/MCP result for the same query and read as a SQL NULL.
        # Matched by shape rather than against the sentinels in use, so a new
        # mask in `privacy/redaction.py` cannot quietly start reading as absent.
        if isinstance(value, str) and value:
            return value
        return "-"
    digits = f"{abs(amount):,.2f}"
    if amount < 0:
        return f"{MINUS}{digits}"
    # `flow` and `delta` state their direction; a zero has none to state, so it
    # goes unsigned rather than claiming income with a `+`.
    if kind in ("flow", "delta") and amount > 0:
        return f"+{digits}"
    return digits


def color_enabled(stream: object, env: Mapping[str, str]) -> bool:
    """Return whether ``stream`` should carry colour (requirement 15).

    Takes the stream and the environment rather than reading globals so the
    gate is testable with real values instead of a patched module.
    """
    if "NO_COLOR" in env:
        return False
    isatty = getattr(stream, "isatty", None)
    return bool(isatty and isatty())


#: Stands in for the columns a width fit dropped, in the position it dropped
#: them. DuckDB's box renderer and pandas both mark the gap this way rather
#: than silently splicing the ends together, which would read as a projection
#: that never had a middle.
ELISION = "…"

# The one word text output uses for a category it cannot name (requirement 30).
# It lives beside the renderer that lowercases it for the framing clause rather
# than in a domain module, because nothing below the CLI has an opinion about
# it: `core.fct_transactions.category` is NULL for these rows and the JSON
# branch passes that NULL through untouched. Text is the only surface that owes
# the reader a word here, and one word is the point — a column carrying both
# `Uncategorized` and a raw provider code is the defect, not a disclosure of it.
UNCATEGORIZED_LABEL = "Uncategorized"


@dataclass(frozen=True, slots=True)
class Placeholder:
    """What an absent value renders as in one declared column.

    The caller passes the stored value through — ``None`` and all — and the
    renderer substitutes. Declaring it rather than substituting at the call
    site is what makes the count beneath the table exact: absence is read off
    the stored value, so neither a description nor a category a person
    *authored* as ``Uncategorized`` can be mistaken for a missing one. That is
    the same distinction ``--output json`` keeps by carrying the NULL through.

    The column is the other half of the declaration. A taxonomy gap lives in
    one column, and every other cell in the row is data.
    """

    column: str
    value: str


def _cell_width(cell: RenderableType) -> int:
    from rich.cells import cell_len  # noqa: PLC0415 — defer heavy import
    from rich.text import Text  # noqa: PLC0415 — defer heavy import

    return cell_len(cell.plain if isinstance(cell, Text) else str(cell))


def _table_width(widths: Sequence[int]) -> int:
    """Rendered width of a default-box table whose columns hold ``widths``.

    Measured against Rich rather than derived from its source: one padding
    column either side of each cell plus one border between and around them
    comes to ``3n + 1``, exact for every column count and content width tried.
    """
    return sum(widths) + 3 * len(widths) + 1


def _fit_columns(widths: Sequence[int], available: int) -> tuple[int, ...]:
    """Indices of the columns that fit ``available``, keeping first and last.

    The ends are what identify a row and carry the answer — a spending table
    reads as `month … total`, and dropping either end for two middle dimensions
    loses the question and keeps the qualifiers. The rest of the budget buys
    the most columns it can, split as evenly as it can between the two ends, so
    what survives a hard squeeze is the widest possible frame rather than a
    prefix. This is DuckDB's and pandas' behaviour, measured.

    Returns every index when the whole projection fits, so a caller can tell an
    elided table from a complete one by length alone.
    """
    total = len(widths)
    if total == 0 or _table_width(widths) <= available:
        return tuple(range(total))
    if total == 1:
        return (0,)
    # Everything dropped is one contiguous run, so a candidate projection is
    # exactly a head length and a tail length. Running sums price one in
    # constant time, and the widest affordable tail only shrinks as the head
    # grows — so one pass over the heads reaches every candidate that could
    # win. Rebuilding and re-summing each pair instead is cubic, which is five
    # seconds at 800 columns, and nothing caps a report's output width.
    prefix = list(accumulate(widths, initial=0))
    suffix = list(accumulate(reversed(widths), initial=0))

    def fits(head: int, tail: int) -> bool:
        # One column of ELISION stands between the two halves, so its width is
        # reserved before anything competes for the remainder.
        cells = prefix[head] + suffix[tail] + len(ELISION)
        return cells + 3 * (head + tail + 1) + 1 <= available

    # The ends are kept whether or not they fit: a table squeezed past its own
    # frame still has to render something, and they are what identify the row.
    best = (1, 1)
    tail = total - 2
    for head in range(1, total - 1):
        tail = min(tail, total - head - 1)  # a dropped run needs somewhere to be
        while tail >= 1 and not fits(head, tail):
            tail -= 1
        if tail < 1:
            # A longer head costs strictly more, so no later head fits either.
            break
        # Most columns wins. Between two that show the same number, the more
        # balanced split, then the head — so a squeeze keeps the widest frame
        # rather than a prefix, and an odd count leans left.
        if (head + tail, -abs(head - tail), head) > (
            best[0] + best[1],
            -abs(best[0] - best[1]),
            best[0],
        ):
            best = (head, tail)
    head, tail = best
    return (*range(head), *range(total - tail, total))


@dataclass(frozen=True)
class ColumnView:
    """What a command renders now, and how many columns it could have."""

    names: list[str]
    rows: Iterable[tuple[object, ...]]
    total: int


def column_view[T](
    columns: Sequence[tuple[str, Callable[[T], object]]],
    records: Iterable[T],
    *,
    default: Sequence[str],
    wide: bool,
) -> ColumnView:
    """Project ``records`` onto the columns a narrow terminal should show.

    The header list and every row come from one declaration, so they cannot
    drift apart — a column moved in ``columns`` moves in both. Naming the
    columns beside a separately-built positional row tuple is the same list
    written twice, and the copy that goes wrong is the one no reader checks.

    ``default`` is the curated answer to "what does this command report": the
    columns that survive an 80-column terminal, chosen by the author rather
    than by measuring widths, exactly as a report's ``default_columns`` is.
    Width-based fitting is the fallback for a caller that has no such answer;
    it cannot know that ``market value`` matters more than ``avg cost``.

    Rows stay lazy so the non-fitting render path keeps streaming.
    """
    extract = dict(columns)
    chosen = [name for name, _ in columns] if wide else list(default)
    unknown = [name for name in chosen if name not in extract]
    if unknown:
        # Refused rather than skipped: a silently dropped column renders a view
        # nobody declared and leaves the typo in place indefinitely.
        raise ValueError(f"undeclared column(s) {unknown} for this table")
    return ColumnView(
        names=chosen,
        rows=(tuple(extract[name](record) for name in chosen) for record in records),
        total=len(columns),
    )


def render_rows(
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
    *,
    money: Mapping[str, Money] | None = None,
    numeric: Sequence[str] | None = None,
    total_columns: int | None = None,
    total_rows: int | None = None,
    has_more: bool = False,
    placeholder: Placeholder | None = None,
    fit: bool = False,
) -> None:
    """Render ``rows`` as a table to stdout (requirement 2).

    ``money`` declares the columns holding amounts, keyed by header name.
    Declared columns are formatted by :func:`format_money`, right-aligned
    (requirement 13), and coloured from their kind (requirement 14). Every
    other column prints its value as-is.

    ``numeric`` names the columns that hold a bare number that is not an
    amount — a share count, a per-unit price, an FX rate, a match score. They
    are printed as stored and stay left-aligned, because requirement 13 covers
    amounts only. What they take from ``money`` is the one guarantee that has
    nothing to do with being an amount: they do not fold.

    Measured, not assumed. While a table fits its terminal nothing is squeezed
    and this declaration changes no output at all. It decides only what gives
    way when a table does not fit — and a curated default reaches that point
    too, since a header-width contract bounds the header and a
    ``DECIMAL(28,10)`` quantity or a ``carried_forward`` status is wider than
    its name. There the declaration moves the fold off the number and onto the
    text beside it, which is the trade this module already states as "nothing
    lies": a folded id is ugly and a folded ``8.2987654321`` is a different,
    plausible price. Past that, on a projection far too wide — nine columns at
    80 — Rich pays for every unwrappable column out of the wrappable ones and
    can leave a text column at zero width. An empty column announces itself
    and a reader widens the terminal; a wrong number does not. Curation, not
    this flag, is the answer for a table that does not fit.

    ``total_columns`` is the width of the full projection when ``columns`` is a
    narrowed view of it, and produces the result-framing line beneath the table
    (requirement 10). A caller with no column policy leaves it ``None`` and
    frames nothing.

    ``total_rows`` is the size of the whole result when ``rows`` is one page of
    it, and frames the remainder (requirement 34). ``has_more`` says whether a
    further page exists, and gates the continuation the frame offers: the
    count and the remedy answer different questions. A filtered total holds
    steady across a cursor walk, so on the last page of one it still exceeds
    the page length — the table is honestly a slice of 2,046, and `--limit`
    would still fetch nothing. It defaults to ``False`` so a caller that has
    not thought about paging discloses the slice without promising more.

    ``placeholder`` declares what an absent value renders as, and in which
    column. The caller passes the stored value through; this renderer
    substitutes and counts the substitutions on the same line (requirement
    30). Both widen requirement 10's trigger: framing is not only about
    omitted columns, so a complete projection under ``--wide`` still discloses
    a taxonomy gap.

    Every clause shares one line. Four lines beneath a two-row table would cost
    more screen than the result they describe.

    **One line per record, always** (requirement 35). This renderer never
    deduplicates, merges, or suppresses a row. `reports networth` currently
    sums an account once per balance source, so a doubled account shows as
    repeated rows; collapsing them here would make the output look right while
    the total stayed wrong, removing the symptom that finds the defect.
    """
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    declared = money or {}
    # Formatting and atomicity are separate declarations. A per-unit price is
    # deliberately absent from `money` so `format_money` cannot round it to
    # `0.00`, and that exclusion silently took the no-fold guarantee with it.
    unwrappable = set(declared) | set(numeric or ())
    # markup=False because every cell is data, much of it user-authored — a
    # merchant name, a report description. Rich reads `[...]` as a style tag, so
    # a default console drops "spend [excluding rent]" to "spend " and lets
    # stored text steer the terminal. highlight=False for the same reason one
    # level up: Rich's auto-highlighter colours bare numbers on sight, which
    # would decide a money column's colour from its value and defeat req 14.
    console = Console(
        markup=False,
        highlight=False,
        no_color=not color_enabled(sys.stdout, os.environ),
    )
    absent_at: int | None = None
    absent_as = ""
    if placeholder is not None:
        if placeholder.column not in columns:
            # Refused rather than skipped, for the reason `column_view` gives:
            # a disclosure that silently counts nothing reads exactly like a
            # table with no gaps, and leaves the typo in place indefinitely.
            raise ValueError(f"undeclared placeholder column {placeholder.column!r}")
        if placeholder.column in declared:
            # Same silent-zero failure, one column over: `_cells` formats a
            # money cell through `format_money`, which spells a missing amount
            # `-` itself, so a placeholder here would never substitute and the
            # count would sit at zero forever.
            raise ValueError(f"placeholder on money column {placeholder.column!r}")
        absent_at = columns.index(placeholder.column)
        absent_as = placeholder.value
    cells_source: Iterable[tuple[list[RenderableType], bool]] = (
        _cells(columns, row, declared, absent_at, absent_as) for row in rows
    )
    kept = tuple(range(len(columns)))
    if fit and columns:
        # The one path that buffers the whole result, and it has no choice: a
        # column is as wide as its widest value, so every record has to be
        # rendered before the first column can be sized. Every other path
        # streams. `reports run` defaults to `CLI_MAX_ROWS` (1,000,000) records
        # and Rich already keeps its own copy of each cell, so a second copy of
        # the result is what turns a large-but-renderable report into an
        # out-of-memory failure.
        cells_source = list(cells_source)
        widths = [
            max(
                _cell_width(name),
                max((_cell_width(cells[i]) for cells, _ in cells_source), default=0),
            )
            for i, name in enumerate(columns)
        ]
        kept = _fit_columns(widths, console.width)
    # The single gap in a prefix-plus-suffix selection, or None when nothing
    # was dropped.
    gap = next(
        (at for at, i in enumerate(kept[:-1]) if kept[at + 1] != i + 1),
        None,
    )

    table = Table()
    for at, i in enumerate(kept):
        name = columns[i]
        is_money = name in declared
        is_number = name in unwrappable
        table.add_column(
            name,
            justify="right" if is_money else "left",
            # Text folds; a number does not. Folding only saves a value that
            # has a space to break on, and the text values most likely to
            # overflow here have none — an account id, a checksum, a display
            # name ending in a masked last four. Rich's default elides those,
            # dropping exactly the characters that tell two rows apart, so a
            # ragged row is the cheaper failure than a wrong one.
            #
            # A number inverts that trade. Folded after the decimal point,
            # `1,200.00` renders `1,200.` above `00`, and the first line is a
            # complete, plausible number two orders of magnitude off; `cli.md`
            # calls that a correctness bug rather than a cosmetic one. Marking
            # numbers unwrappable spends the squeeze on the text columns first,
            # which is what makes nine ordinary columns fit 80 at all. When
            # even that is not enough the ellipsis leaves the cell visibly
            # partial, because a silently cropped `1,234,56` is the failure
            # this whole branch exists to prevent.
            #
            # The test is `unwrappable`, not `declared`: the misread does not
            # need an amount. `avg cost` is DECIMAL(28,10) and is kept out of
            # `money` precisely so it is not rounded to two places, and folding
            # `8.2987654321` into `8.298` above `7654321` is the same wrong
            # number by the same mechanism. A share count folds identically.
            overflow="ellipsis" if is_number else "fold",
            no_wrap=is_number,
        )
        if at == gap:
            table.add_column(ELISION, justify="center", overflow="fold")
    # Counted off the kept columns, not the caller's whole projection: the
    # disclosure is about what this table shows, and a placeholder in a column
    # the fit dropped is not on screen to be misread.
    count_absent = absent_at is not None and absent_at in kept
    rendered = 0
    flagged = 0
    for cells, absent in cells_source:
        row_cells = [cells[i] for i in kept]
        if gap is not None:
            row_cells.insert(gap + 1, ELISION)
        table.add_row(*row_cells)
        rendered += 1
        if absent and count_absent:
            flagged += 1
    # Rich holds every cell now, so let a buffered measurement copy go before
    # rendering allocates its own.
    del cells_source
    console.print(table)
    # Counted from what was actually printed, so a caller's declared narrowing
    # and the renderer's own width fit are disclosed by one line rather than
    # two — and a fit the caller never asked about still cannot happen silently.
    whole = total_columns if total_columns is not None else len(columns)
    clauses: list[str] = []
    if total_rows is not None and total_rows > rendered:
        # Grouped, because the count exists to answer "how much is there?" and
        # 2046 answers it less well than 2,046 at a glance.
        clauses.append(f"{rendered:,} of {total_rows:,} shown")
    if has_more:
        # `--limit`, never `--cursor`: the cursor takes an opaque token text
        # output does not supply, so naming it sends the reader into a usage
        # error. Offered only against a page that exists, because a remedy
        # that fetches nothing is worse than no remedy at all.
        clauses.append("raise --limit for more")
    if whole > len(kept):
        clauses.append(f"{len(kept)} of {whole} columns shown — --wide for all")
    if placeholder is not None and flagged:
        clauses.append(f"{flagged} {placeholder.value.lower()}")
    if clauses:
        # stdout, and reachable under `-q` (this renderer takes no such
        # parameter): both are load-bearing. `moneybin reports spending >
        # report.txt` has to capture the disclosure with the table it describes,
        # or the file records a truncated result that reads as a whole one.
        typer.echo(" · ".join(clauses))


def render_summary(
    pairs: Sequence[tuple[str, str]], *, title: str | None = None
) -> None:
    """Render labelled scalars to stdout as aligned pairs (requirement 3).

    Values are pre-rendered strings — amounts arrive already through
    :func:`format_money`, so this renderer never stringifies one itself.

    ``title`` heads the block when a command prints more than one of them and
    the reader needs to know which is which; `reports networth` emits one per
    currency the profile holds.
    """
    if not pairs:
        return
    if title is not None:
        typer.echo(title)
    width = max(len(label) for label, _ in pairs) + 1
    for label, value in pairs:
        typer.echo(f"{label + ':':<{width}} {value}")


def render_note(message: str, *, quiet: bool = False, warn: bool = False) -> None:
    """Render one informational line to stderr (requirement 4).

    stderr because a note is a diagnostic *about* the answer, not the answer:
    redirecting a command to a file must capture the result without prose mixed
    into it. Silenced by ``-q``, which never reaches a row or a summary.
    """
    if quiet:
        return
    if warn and color_enabled(sys.stderr, os.environ):
        from rich.console import Console  # noqa: PLC0415 — defer heavy import

        Console(stderr=True, markup=False, highlight=False).print(
            message, style=Style.WARNING
        )
        return
    typer.echo(message, err=True)


def _cells(
    columns: Sequence[str],
    row: Sequence[object],
    declared: Mapping[str, Money],
    absent_at: int | None = None,
    absent_as: str = "",
) -> tuple[list[RenderableType], bool]:
    """Render one record's cells, and report whether the declared value was absent.

    A money cell becomes a ``Text`` carrying its own style, which is how a
    per-value colour survives ``markup=False`` — the alternative is embedding
    style tags in the string, which is the markup this renderer disables.

    ``absent_at`` names the column a caller declared a placeholder for. A
    ``None`` there renders as ``absent_as`` and is reported in the second
    element. The substitution and the report come from the same test, on the
    stored value, so the count cannot be confused by a *stored* string that
    happens to equal the placeholder — which is the distinction ``--output
    json`` preserves by carrying the NULL through untouched.
    """
    from rich.text import Text  # noqa: PLC0415 — defer heavy import

    cells: list[RenderableType] = []
    # strict: a row and its header are built together at every call site, so a
    # length mismatch is a programming error. Zipping leniently would drop the
    # trailing cell silently, which is the failure requirement 35 exists to
    # prevent — a wrong table that looks right.
    absent = False
    for at, (name, value) in enumerate(zip(columns, row, strict=True)):
        column_money = declared.get(name)
        if column_money is None:
            if value is None and at == absent_at:
                # NULL only, deliberately. A whitespace-only category is
                # reachable and is *not* treated as absent here, because
                # `core.uncategorized_queue` selects `WHERE category IS NULL`
                # and would not surface it: labelling it would advertise a row
                # `transactions categorize run` cannot act on. Making the two
                # agree means normalizing blanks in staging, which is a change
                # to what the queue contains rather than to how it renders.
                absent = True
                cells.append(absent_as)
            else:
                cells.append("" if value is None else str(value))
            continue
        cells.append(
            Text(
                format_money(value, column_money.kind),
                style=str(column_money.style_for(value)),
            )
        )
    return cells, absent


def _as_decimal(value: object) -> Decimal | None:
    """Coerce a money cell to Decimal, or None when there is no amount.

    Goes through ``str`` for a float so a value that arrived as one is read at
    the precision it displays rather than its binary expansion.

    Matches the hardening of the same-named helper in
    ``reports/_framework/convert.py`` rather than reinventing a weaker one:
    ``bool`` is excluded even though it is an ``int`` — a true/false in a money
    column is a declaration bug, and formatting it as ``1.00`` would hide that
    — and an unparseable string renders as absent rather than raising
    ``InvalidOperation`` out of the render layer, where the traceback would
    name neither the column nor the report that declared it.

    A non-finite value is rejected for that same reason, one step earlier than
    it would otherwise surface. ``Decimal("NaN")`` parses without complaint and
    only raises when something *orders* it, which both callers do —
    ``format_money`` at ``amount < 0`` and ``style_for`` at ``amount > 0`` — so
    catching it at construction covers both rather than each separately. An
    infinity does not raise at all; it formats under ``,.2f`` as the word
    ``Infinity``, which a money column would print signed and coloured as
    though it were an amount.
    """
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, int | float | str):
        try:
            parsed = Decimal(str(value))
        except InvalidOperation:
            return None
        return parsed if parsed.is_finite() else None
    return None
