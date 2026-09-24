"""Generate and register a Typer command from a report spec.

Builds a command whose ``__signature__`` carries the report's params (each as a
``typer.Option``, flag auto-derived from the name) plus the shared
``--output`` / ``--quiet`` / ``--display-currency`` options, then runs the stable
report ID through the shared catalog and renders text or a JSON envelope via
``render_or_json``.

Every option this module injects must also be listed in ``introspect``'s
``_RESERVED_CLI_PARAMS``, or a report whose runner happens to use the same
parameter name takes down the whole reports command group at import.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple

import typer

from moneybin.cli.output import (
    CLI_MAX_ROWS,
    OutputFormat,
    applied_rates_note,
    display_currency_option,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
    wide_option,
)
from moneybin.cli.render import Money, build_rows, count_wide_request, render_note
from moneybin.cli.utils import (
    generated_cli_command,
    get_terminal_policy,
    handle_cli_errors,
)
from moneybin.database import get_database
from moneybin.errors import NextStep
from moneybin.reports._framework.contract import (
    ORIGINAL_CURRENCY_COLUMN,
    ReportSpec,
    reject_repeated_default_columns,
)

if TYPE_CHECKING:
    from moneybin.cli.terminal import TerminalSymbols

    # Type-only: importing `execute` here would pull sql_lineage → sqlglot into
    # the CLI cold-start path, which this module exists to keep clear. `catalog`
    # is deferred for the same reason — it reaches `execute`.
    from moneybin.reports._framework.execute import CatalogReportResult


def _cli_signature(spec: ReportSpec) -> inspect.Signature:
    params = [
        inspect.Parameter(
            p.name,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=typer.Option(... if p.required else p.default, help=p.help or None),
            annotation=p.annotation if p.annotation is not None else str,
        )
        for p in spec.params
    ]
    params.append(
        inspect.Parameter(
            "display_currency",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=display_currency_option,
            annotation=str | None,
        )
    )
    params.append(
        inspect.Parameter(
            "no_pager",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=no_pager_option,
            annotation=bool,
        )
    )
    params.append(
        inspect.Parameter(
            "output",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=output_option,
            annotation=OutputFormat,
        )
    )
    params.append(
        inspect.Parameter(
            "quiet",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=quiet_option,
            annotation=bool,
        )
    )
    # Injected on every report, including one whose default set is its whole
    # projection: the flag is then a no-op, which is cheaper than a signature
    # that varies per report and a `--help` where the option comes and goes.
    params.append(
        inspect.Parameter(
            "wide",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=wide_option,
            annotation=bool,
        )
    )
    return inspect.Signature(params)


def report_note_lines(
    result: CatalogReportResult,
    *,
    quiet: bool = False,
    symbols: TerminalSymbols | None = None,
) -> tuple[str, ...]:
    """Build report fidelity disclosures for one complete terminal answer."""
    if symbols is None:
        symbols = get_terminal_policy().symbols
    lines: list[str] = []
    if not result.records:
        # Requirement 6: an empty table says nothing on its own. State what was
        # searched before any hint below, so "nothing matched the filter" and
        # "nothing exists at all" are never the same silence.
        lines.append(_empty_result_scope(result))
    if conversion_note := applied_rates_note(
        result.applied_rates, result.display_currency
    ):
        lines.append(conversion_note)
    if result.degraded and result.degraded_reason:
        lines.append(f"{symbols.attention} {result.degraded_reason}")
    if result.truncated:
        lines.append(
            f"{symbols.attention} Showing the first {len(result.records):,} rows; more exist. "
            "Raise --limit or narrow the report to see the rest."
        )
    if not quiet:
        lines.extend(
            f"{symbols.action} {_next_step_line(action)}" for action in result.actions
        )
    return tuple(lines)


def _empty_result_scope(result: CatalogReportResult) -> str:
    """Requirement 6's line for zero rows: what was searched, not just that nothing came back.

    Names the effective parameters and period the caller already knows rather
    than distinguishing "no records at all" from "no matches for this filter"
    — no report here can tell the two apart without a second, unfiltered
    query. ``getattr`` covers a test double built from the base
    ``ReportResult`` (no ``report_id``/``parameters``), which never reaches a
    real terminal.
    """
    report_label = getattr(result, "report_id", "report").split(":", 1)[-1]
    parameters: Mapping[str, Any] = getattr(result, "parameters", {})
    scope = [
        f"{name}={value}" for name, value in parameters.items() if value is not None
    ]
    if result.period:
        scope.insert(0, result.period)
    if not scope:
        return f"No rows matched {report_label}."
    return f"No rows matched {report_label} ({', '.join(scope)})."


def _next_step_line(action: NextStep | str) -> str:
    """Render one next-step hint for the terminal: reason, then a runnable command.

    A ``NextStep`` renders its own ``cli`` argv so the command is last on the
    line, per requirement 7 of ``cli-human-experience.md``. A legacy string
    (a dedup caveat authored outside this module's allotment) prints as-is.
    """
    if not isinstance(action, NextStep):
        return action
    reason = action.reason[:1].upper() + action.reason[1:] if action.reason else ""
    return f"{reason}: {generated_cli_command(*action.cli)}"


def echo_report_notes(result: CatalogReportResult, *, quiet: bool = False) -> None:
    """Echo the envelope metadata the text path would otherwise drop.

    ``quiet`` reaches the next-step hints and nothing else. Requirement 4 makes
    ``-q`` the switch for an informational status line, and a hint is one: it
    suggests a command to run next. The conversion disclosure and the two
    warnings are not — they state how far the numbers above can be trusted, and
    a flag asking for less chatter is not a claim that masking, truncation, or
    a currency conversion stopped happening. Silencing those is the failure the
    rest of this docstring describes, arriving through ``-q`` instead of
    through the surface that skipped them.

    ``render_or_json`` renders the envelope on the JSON path only, so every
    text renderer of a report result has to say these three things itself.
    Shared rather than copied: a hand-written report renderer that skips them
    would let a conversion that fell back to per-currency segmentation show
    segmented positions and never say why — the silent masking these echoes
    exist to prevent, reappearing on the surface that skipped them.

    All of it goes to stderr (``cli.md`` "Exit Codes & stderr"): these are
    diagnostics about the answer, not the answer, and redirecting a report to a
    file or a downstream parser must not append prose to the data stream.
    """
    symbols = get_terminal_policy().symbols
    for line in report_note_lines(result, quiet=quiet, symbols=symbols):
        render_note(line, warn=line.startswith(f"{symbols.attention} "))


class ColumnView(NamedTuple):
    """What the text branch renders, and whether the renderer fits it."""

    columns: tuple[str, ...]
    fit: bool


def column_view(
    spec: ReportSpec,
    result_columns: Sequence[str],
    *,
    parameters: Mapping[str, Any],
    wide: bool,
    output: OutputFormat,
) -> ColumnView:
    """One report's whole text-branch column decision (requirements 6, 7).

    Both CLI paths reach the same renderer — a built-in's generated command and
    ``reports run``, which serves every tier — so resolving the narrowed set and
    the fit flag separately in each is two copies of one decision. They were,
    and the tested copy was not the one `run` used.

    ``output`` is here only for the counter below. Both callers resolve the
    view before :func:`render_report_result` branches on the format, so this
    function — unlike ``render.py``'s namesake, which its callers reach only
    after their own JSON branch has returned — runs on the JSON path too.
    """
    if wide and output != OutputFormat.JSON:
        # Counted here rather than in the generated command body, for the same
        # reason the narrowing itself is: `reports run` reaches this function
        # and not that body, so counting there would report a rate for the
        # built-ins alone and read as a rate for reports.
        #
        # Not counted for JSON, which returns the whole projection either way:
        # `--wide` asks for nothing there, and no omission can be counted
        # against it, so counting the request alone would inflate one half of
        # the ratio requirement 7 reads and leave the other at zero.
        count_wide_request()
    return ColumnView(
        visible_columns(spec, result_columns, parameters=parameters, wide=wide),
        # Only a report that named no columns of its own. `--wide` is a request
        # for the whole projection, not for a fitted one.
        fit=spec.default_columns is None and not wide,
    )


def visible_columns(
    spec: ReportSpec,
    result_columns: Sequence[str],
    *,
    parameters: Mapping[str, Any],
    wide: bool,
) -> tuple[str, ...]:
    """The columns this result renders as text (requirements 6, 7).

    Resolves the report's own declaration against the columns the result
    actually carries. The declaration orders the table — an author putting the
    identifying column first must not have to reorder the SQL projection, which
    ``--wide``, ``--output json``, and every MCP caller also read.

    A declared name absent from ``result_columns`` is dropped rather than
    rendered empty: a `cash_flow` grouped by account returns no ``category``,
    and the result is the authority on what exists. A declaration matching
    nothing at all fails open to the whole result — requirement 6 makes that a
    spec violation, caught by the width contract test rather than at a user's
    terminal, and an empty table under ``0 of 9 columns shown`` reads as a
    report that returned nothing.
    """
    if wide:
        return tuple(result_columns)
    names = resolve_default_columns(spec, parameters)
    available = set(result_columns)
    visible = tuple(name for name in names if name in available)
    if not visible:
        return tuple(result_columns)
    # `original_currency_code` is attached at runtime by display conversion, so
    # no declaration can name it and the intersection above would drop it. It
    # survives on its own terms: conversion relabels every amount into the
    # target currency, so without it the table states what a row is worth and
    # loses what it was — and `echo_applied_rates`, the only other disclosure,
    # goes to stderr, which `> report.txt` does not capture. Requirement 9's
    # 80-column bar is measured on declared projections; an explicit
    # `--display-currency` may exceed it rather than drop the provenance.
    # `not in visible` because nothing checks a callable declaration —
    # `validate_default_columns` takes one on trust — so one that names this
    # column itself would otherwise pick it up here a second time.
    if (
        ORIGINAL_CURRENCY_COLUMN in available
        and ORIGINAL_CURRENCY_COLUMN not in visible
    ):
        # Placed beside the currency it records, for the reason
        # `execute.py::_original_currency_position` gives about the projection:
        # both columns are `DataClass.CURRENCY`, so appending would end the
        # table on provenance rather than on the headline measure. `visible` is
        # already intersected against the result, so the currency column can be
        # absent from it even when the result carries one — that falls back to
        # the append this replaced.
        currency = spec.semantics.currency
        at = (
            visible.index(currency) + 1
            if currency is not None and currency in visible
            else len(visible)
        )
        visible = (*visible[:at], ORIGINAL_CURRENCY_COLUMN, *visible[at:])
    return visible


def resolve_default_columns(
    spec: ReportSpec, parameters: Mapping[str, Any]
) -> tuple[str, ...]:
    """The names a report's declaration resolves to, before any intersection.

    Split out of ``visible_columns`` so requirement 9's contract test can check
    what a declaration *names* rather than what survived intersection with a
    result. Intersecting first makes every surviving name trivially present,
    which is how a typo in a callable declaration would otherwise stay
    invisible — ``validate_default_columns`` cannot check a callable, so this
    is the only place a wrong name is catchable.
    """
    declared = spec.default_columns
    if declared is None:
        # The whole projection, left to the renderer to fit against the real
        # terminal. A fixed count here would be a worse answer at both ends —
        # under-filling a wide window and still overflowing a narrow one —
        # and it cannot know a column's display width, which depends on the
        # values. `render_rows(fit=True)` measures those and keeps the first
        # and last columns, as DuckDB and pandas do.
        return tuple(column.name for column in spec.columns)
    if callable(declared):
        # The same rule `validate_default_columns` applies to a static tuple at
        # construction, applied here because this is the first moment a
        # callable's answer exists. Refused rather than de-duplicated: a quiet
        # correction would render a projection nobody declared and leave the
        # mistake in the report indefinitely.
        resolved = tuple(declared(parameters))
        reject_repeated_default_columns(resolved)
        return resolved
    return tuple(declared)


def money_columns(spec: ReportSpec) -> dict[str, Money]:
    """Build the renderer's money declarations from a report's own columns.

    Requirement 12: the kind is declared where the meaning is known — beside
    the column, by its author — and the renderer never infers it from the
    number. A report that declares nothing renders its amounts as plain text,
    which is what an extension report does until it opts in.
    """
    return {
        column.name: Money(column.money_kind, column.polarity)
        for column in spec.columns
        if column.money_kind is not None
    }


def numeric_columns(spec: ReportSpec) -> tuple[str, ...]:
    """Columns declaring a bare number (a count, a score) for `build_rows(numeric=...)`.

    Mirrors :func:`money_columns`: the declaration lives beside the column, and
    a report that declares nothing renders its numbers left-aligned exactly as
    it did before this field existed.
    """
    return tuple(column.name for column in spec.columns if column.numeric)


def render_report_result(
    result: CatalogReportResult,
    output: OutputFormat,
    *,
    cli_actor: str,
    money: Mapping[str, Money] | None = None,
    numeric: Sequence[str] | None = None,
    quiet: bool = False,
    columns: Sequence[str] | None = None,
    fit: bool = False,
    no_pager: bool = False,
) -> None:
    """Render one report result as a table or the JSON envelope.

    Shared by every CLI report path — a built-in's generated command and
    ``reports run`` alike — so a saved report's output is not merely similar to a
    built-in's but produced by the same code.

    ``money`` carries the report's own column declarations, from
    :func:`money_columns`. Both callers resolve it from the spec, so one report
    renders its amounts identically whichever command ran it.

    ``numeric`` carries the report's bare-number declarations, from
    :func:`numeric_columns` — requirement 9's right-alignment for a column
    that is a count or a score rather than an amount.

    ``columns`` is the text branch's narrowed view, from
    :func:`visible_columns`. It never reaches the JSON envelope — requirement 8
    keeps that projection whole, filtered only by ``--json-fields``.

    ``fit`` asks the renderer to measure the result against the terminal and
    drop middle columns to fit. It is for a report that declares no default
    set: a declared set is a curated answer to "what does this report say",
    contract-tested at 80 characters, and re-deciding it from column widths
    would discard the author's judgement.
    """
    visible = list(result.columns if columns is None else columns)

    if output == OutputFormat.JSON:
        render_or_json(
            result.to_envelope(),
            output,
            cli_actor=cli_actor,
            classes_returned=result.classes_returned,
        )
        return
    policy = get_terminal_policy(no_pager=no_pager)
    disclosures = report_note_lines(result, quiet=quiet, symbols=policy.symbols)
    if result.records:
        from rich.console import Group
        from rich.text import Text

        human = build_rows(
            visible,
            [
                tuple(record.get(column) for column in visible)
                for record in result.records
            ],
            money=money,
            numeric=numeric,
            total_columns=len(result.columns),
            fit=fit,
            terminal=policy,
        )
        if disclosures:
            human = Group(human, Text("\n" + "\n".join(disclosures)))
        emit_human_result(
            human,
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )
    elif disclosures:
        from rich.text import Text

        emit_human_result(
            Text("\n".join(disclosures)),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )


def build_cli_command(spec: ReportSpec) -> Callable[..., None]:
    """Build the Typer command callback for ``spec`` with an explicit signature."""

    def _impl(**kwargs: Any) -> None:
        # Deferred so importing this module (at CLI command registration) does
        # not pull execute → sql_lineage → sqlglot into the CLI cold-start path.
        from moneybin.reports._framework.catalog import (
            get_report_catalog,
            profile_home_currency,
        )

        output: OutputFormat = kwargs.pop("output")
        # Reaches the next-step hints in `echo_report_notes` and nothing else —
        # the table is data (requirement 5) and JSON output has no notes.
        quiet: bool = bool(kwargs.pop("quiet", False))
        # Popped for the same reason as `display_currency`: a framework option
        # the runner never declared and would reject as unknown.
        wide: bool = bool(kwargs.pop("wide", False))
        no_pager: bool = bool(kwargs.pop("no_pager", False))
        # Popped before `kwargs` becomes `parameters`: display conversion is the
        # framework's, not the runner's, so a runner would reject it as unknown.
        display_currency: str | None = kwargs.pop("display_currency", None)
        cli_actor = f"reports_{spec.name}"
        with handle_cli_errors(cli_actor=cli_actor):
            # Runner enum/validation errors raise bare ValueError; let it
            # propagate to handle_cli_errors, which classifies ValueError →
            # INFRA_INVALID_INPUT and emits the JSON error envelope under
            # --output json (and a clean ❌ line otherwise). Catching it here to
            # raise typer.BadParameter would bypass that envelope (Typer prints
            # plain text, exit 2) — breaking the JSON contract for agents.
            with get_database(read_only=True) as db:
                # No `db` to the catalog: this command resolves the one fixed
                # built-in ID it was generated from, so the user tier would add
                # a per-row spec build that nothing here can reach.
                result = get_report_catalog().execute(
                    db,
                    report_id=spec.report_id,
                    parameters=kwargs,
                    limit=CLI_MAX_ROWS,
                    display_currency=display_currency,
                    home_currency=profile_home_currency(db),
                )
            view = column_view(
                spec, result.columns, parameters=kwargs, wide=wide, output=output
            )
            render_report_result(
                result,
                output,
                cli_actor=cli_actor,
                money=money_columns(spec),
                numeric=numeric_columns(spec),
                quiet=quiet,
                columns=view.columns,
                fit=view.fit,
                no_pager=no_pager,
            )

    _impl.__name__ = spec.name
    _impl.__qualname__ = spec.name
    _impl.__doc__ = spec.description
    _impl.__signature__ = _cli_signature(spec)  # type: ignore[attr-defined]
    return _impl


def register_report_cli(spec: ReportSpec, app: typer.Typer) -> None:
    """Register ``spec`` as a ``<cli_name>`` Typer command on ``app``."""
    app.command(spec.cli_name)(build_cli_command(spec))
