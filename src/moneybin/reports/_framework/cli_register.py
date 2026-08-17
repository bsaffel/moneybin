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
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import typer

from moneybin.cli.output import (
    CLI_MAX_ROWS,
    OutputFormat,
    display_currency_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors, render_rich_table
from moneybin.database import get_database
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.reports._framework.contract import ReportSpec

if TYPE_CHECKING:
    # Type-only: importing `execute` here would pull sql_lineage → sqlglot into
    # the CLI cold-start path, which this module exists to keep clear.
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
    return inspect.Signature(params)


def echo_report_notes(result: CatalogReportResult) -> None:
    """Echo the envelope metadata the text path would otherwise drop.

    ``render_or_json`` renders the envelope on the JSON path only, so every
    text renderer of a report result has to say these three things itself.
    Shared rather than copied: the report commands with hand-written renderers
    (``reports networth`` and ``networth-history``) printed none of them, so a
    conversion that fell back to per-currency segmentation showed segmented
    positions and never said why — the silent masking these echoes exist to
    prevent, reappearing on the surface that skipped them.

    All of it goes to stderr (``cli.md`` "Exit Codes & stderr"): these are
    diagnostics about the answer, not the answer, and redirecting a report to a
    file or a downstream parser must not append prose to the data stream.
    """
    # Requirement 10 on the surface that cannot read `summary.applied_rates`.
    # Summarized rather than enumerated: a twelve-month rollup in three
    # currencies applies thirty-six rates, and thirty-six lines under a table is
    # a wall nobody reads. The exact rate prints when there is exactly one —
    # the common case, and the only one a single line can state without
    # choosing which rate to favour. The full set rides the JSON envelope.
    if result.applied_rates:
        if len(result.applied_rates) == 1:
            rate = result.applied_rates[0]
            priced_on = (
                f"{rate.rate_date}"
                if rate.rate_date == rate.requested_date
                # A weekend or holiday prices at the previous published day, and
                # Requirement 10 wants that visible rather than smoothed over.
                else f"{rate.rate_date}, for {rate.requested_date}"
            )
            typer.echo(
                f"💱 Converted from {rate.from_currency} at {rate.rate} "
                f"({priced_on}, {rate.source})",
                err=True,
            )
        else:
            sources = sorted({rate.from_currency for rate in result.applied_rates})
            typer.echo(
                f"💱 Converted from {', '.join(sources)} using "
                f"{len(result.applied_rates)} stored rates; run "
                f"'moneybin fx rate <from> {result.display_currency} <date>' "
                "for one of them, or --output json for all",
                err=True,
            )
    # R4's verdict, on the surface that cannot see the envelope. JSON and MCP
    # callers read `summary.degraded_reason`; without this a drifted report
    # printed `*****` and said nothing — the silent masking that teaches a
    # reader to skip the warning that matters.
    if result.degraded and result.degraded_reason:
        typer.echo(f"⚠️  {result.degraded_reason}", err=True)
    # Same gap, same surface: `truncated` rides the envelope to JSON and MCP
    # callers, so without this the text path renders a capped table that
    # reads as the whole answer — worse here than a masked cell, because
    # nothing about the rows themselves looks unusual.
    #
    # The count of what was *not* shown is deliberately absent. A truncated
    # execution fetches `limit + 1` rows and reports that as `total_count`,
    # so it is a lower bound — "1,000,000 of 1,000,001" would read as one
    # row missing when millions are, which is a more confident lie than
    # saying nothing. `mcp.md` calls this a lower-bound total for the same
    # reason; counting the rest means running the query again without a cap.
    if result.truncated:
        typer.echo(
            f"⚠️  Showing the first {len(result.records):,} rows; more exist. "
            "Raise --limit or narrow the report to see the rest.",
            err=True,
        )
    # Third instance of the same asymmetry, and the one that inverted its own
    # intent: `inspection_hint` deliberately names a CLI command — "Run
    # `moneybin reports explain …`" — so the surfaces that cannot run it were
    # told to while the terminal printed `*****` and stopped. Every action is
    # rendered, not just that hint: a runner's own `actions` are next steps for
    # whoever called it, and the text path is a caller.
    for action in result.actions:
        typer.echo(f"💡 {action}", err=True)


def render_report_result(
    result: CatalogReportResult, output: OutputFormat, *, cli_actor: str
) -> None:
    """Render one report result as a table or the JSON envelope.

    Shared by every CLI report path — a built-in's generated command and
    ``reports run`` alike — so a saved report's output is not merely similar to a
    built-in's but produced by the same code.
    """

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        if result.records:
            rows: list[tuple[object, ...]] = [
                tuple(record.get(column) for column in result.columns)
                for record in result.records
            ]
            render_rich_table(result.columns, rows)
        echo_report_notes(result)

    render_or_json(
        result.to_envelope(),
        output,
        render_fn=_render_text,
        cli_actor=cli_actor,
        # Bare-list payload + lineage-derived classes: pass them explicitly so
        # the privacy.log audit event records the real data classes instead of an
        # empty set (same as `sql query`).
        classes_returned=result.classes_returned,
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
        # quiet has nothing to silence here: the text renderer emits only the
        # results table (no status chatter) and JSON output ignores it.
        kwargs.pop("quiet", None)
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
            render_report_result(result, output, cli_actor=cli_actor)

    _impl.__name__ = spec.name
    _impl.__qualname__ = spec.name
    _impl.__doc__ = spec.description
    _impl.__signature__ = _cli_signature(spec)  # type: ignore[attr-defined]
    return _impl


def register_report_cli(spec: ReportSpec, app: typer.Typer) -> None:
    """Register ``spec`` as a ``<cli_name>`` Typer command on ``app``."""
    app.command(spec.cli_name)(build_cli_command(spec))
