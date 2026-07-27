"""Generate and register a Typer command from a report spec.

Builds a command whose ``__signature__`` carries the report's params (each as a
``typer.Option``, flag auto-derived from the name) plus the shared
``--output`` / ``--quiet`` options, then runs the stable report ID through the
shared catalog and renders text or a JSON envelope via ``render_or_json``.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import typer

from moneybin.cli.output import (
    OutputFormat,
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

# The CLI is an operator/agent surface; result size is bounded by the runner's
# own LIMIT params (top, etc.), so the framing cap is effectively off.
_CLI_MAX_ROWS = 1_000_000


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
        # R4's verdict, on the surface that cannot see the envelope. JSON and MCP
        # callers read `summary.degraded_reason`; `render_or_json` renders no
        # envelope metadata on the text path, so without this a drifted report
        # printed `*****` and said nothing — the silent masking that teaches a
        # reader to skip the warning that matters.
        if result.degraded and result.degraded_reason:
            typer.echo(f"⚠️  {result.degraded_reason}")

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
        from moneybin.reports._framework.catalog import get_report_catalog

        output: OutputFormat = kwargs.pop("output")
        # quiet has nothing to silence here: the text renderer emits only the
        # results table (no status chatter) and JSON output ignores it.
        kwargs.pop("quiet", None)
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
                    limit=_CLI_MAX_ROWS,
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
