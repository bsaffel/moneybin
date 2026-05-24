"""Generate and register a Typer command from a report spec.

Builds a command whose ``__signature__`` carries the report's params (each as a
``typer.Option``, flag auto-derived from the name) plus the shared
``--output`` / ``--quiet`` options, then runs the report through ``run_report``
and renders text or a JSON envelope via the shared ``render_or_json`` helper.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

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


def build_cli_command(spec: ReportSpec) -> Callable[..., None]:
    """Build the Typer command callback for ``spec`` with an explicit signature."""

    def _impl(**kwargs: Any) -> None:
        # Deferred so importing this module (at CLI command registration) does
        # not pull execute → sql_lineage → sqlglot into the CLI cold-start path.
        from moneybin.reports._framework.execute import run_report

        output: OutputFormat = kwargs.pop("output")
        kwargs.pop("quiet", None)
        with handle_cli_errors(cli_actor=spec.mcp_tool_name):
            try:
                with get_database(read_only=True) as db:
                    result = run_report(spec, db, max_rows=_CLI_MAX_ROWS, **kwargs)
            except ValueError as exc:
                # Runner enum/validation errors → clean CLI exit, not a traceback.
                raise typer.BadParameter(str(exc)) from exc

            def _render_text(_: ResponseEnvelope[Any]) -> None:
                if result.records:
                    rows: list[tuple[object, ...]] = [
                        tuple(r.get(c) for c in result.columns) for r in result.records
                    ]
                    render_rich_table(result.columns, rows)

            render_or_json(
                result.to_envelope(),
                output,
                render_fn=_render_text,
                cli_actor=spec.mcp_tool_name,
            )

    _impl.__name__ = spec.name
    _impl.__qualname__ = spec.name
    _impl.__doc__ = spec.description
    _impl.__signature__ = _cli_signature(spec)  # type: ignore[attr-defined]
    return _impl


def register_report_cli(spec: ReportSpec, app: typer.Typer) -> None:
    """Register ``spec`` as a ``<cli_name>`` Typer command on ``app``."""
    app.command(spec.cli_name)(build_cli_command(spec))
