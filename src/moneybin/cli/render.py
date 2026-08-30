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
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from rich.console import RenderableType

    from moneybin.reports._framework.contract import MoneyKind, Polarity

__all__ = [
    "Money",
    "Style",
    "color_enabled",
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


def render_rows(
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
    *,
    money: Mapping[str, Money] | None = None,
    total_columns: int | None = None,
) -> None:
    """Render ``rows`` as a table to stdout (requirement 2).

    ``money`` declares the columns holding amounts, keyed by header name.
    Declared columns are formatted by :func:`format_money`, right-aligned
    (requirement 13), and coloured from their kind (requirement 14). Every
    other column prints its value as-is.

    ``total_columns`` is the width of the full projection when ``columns`` is a
    narrowed view of it, and produces the result-framing line beneath the table
    (requirement 10). A caller with no column policy leaves it ``None`` and
    frames nothing.

    **One line per record, always** (requirement 35). This renderer never
    deduplicates, merges, or suppresses a row. `reports networth` currently
    sums an account once per balance source, so a doubled account shows as
    repeated rows; collapsing them here would make the output look right while
    the total stayed wrong, removing the symptom that finds the defect.
    """
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    declared = money or {}
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
    table = Table()
    for name in columns:
        table.add_column(
            name,
            justify="right" if name in declared else "left",
            # Fold rather than inherit Rich's default: wrapping only saves a
            # value that has a space to break on, and the values most likely
            # to overflow here have none — an account id, a checksum, a
            # display name ending in a masked last four. The default elides
            # those, which drops exactly the characters that tell two rows
            # apart. A ragged row is the cheaper failure than a wrong one.
            overflow="fold",
        )
    for row in rows:
        table.add_row(*_cells(columns, row, declared))
    console.print(table)
    if total_columns is not None and total_columns > len(columns):
        # stdout, and reachable under `-q` (this renderer takes no such
        # parameter): both are load-bearing. `moneybin reports spending >
        # report.txt` has to capture the disclosure with the table it describes,
        # or the file records a truncated result that reads as a whole one.
        typer.echo(f"{len(columns)} of {total_columns} columns shown — --wide for all")


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
) -> list[RenderableType]:
    """Render one record's cells, formatting the declared money columns.

    A money cell becomes a ``Text`` carrying its own style, which is how a
    per-value colour survives ``markup=False`` — the alternative is embedding
    style tags in the string, which is the markup this renderer disables.
    """
    from rich.text import Text  # noqa: PLC0415 — defer heavy import

    cells: list[RenderableType] = []
    # strict: a row and its header are built together at every call site, so a
    # length mismatch is a programming error. Zipping leniently would drop the
    # trailing cell silently, which is the failure requirement 35 exists to
    # prevent — a wrong table that looks right.
    for name, value in zip(columns, row, strict=True):
        column_money = declared.get(name)
        if column_money is None:
            cells.append("" if value is None else str(value))
            continue
        cells.append(
            Text(
                format_money(value, column_money.kind),
                style=str(column_money.style_for(value)),
            )
        )
    return cells


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
