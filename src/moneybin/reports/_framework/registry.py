"""Discover ``@report`` runners and register them on the MCP and CLI surfaces.

In-tree reports are collected from an explicit list (the ``definitions``
package's exports); ``discover_reports`` scans a module's members for the same
``_report_spec`` marker so a package (Plan 4) can contribute reports the same
way.

Cold-start: the per-surface registrars are imported lazily inside each function
so importing this module from the CLI path never pulls ``fastmcp`` (via
``mcp_register``); ``cli_register`` in turn defers ``execute``/``sqlglot``.
"""

from __future__ import annotations

from collections.abc import Iterable
from types import ModuleType
from typing import TYPE_CHECKING, cast

from moneybin.reports._framework.contract import ReportSpec, Runner

if TYPE_CHECKING:
    import typer
    from fastmcp import FastMCP


def spec_of(runner: Runner) -> ReportSpec:
    spec = getattr(runner, "_report_spec", None)
    if not isinstance(spec, ReportSpec):
        name = getattr(runner, "__name__", repr(runner))
        raise ValueError(f"{name} is not a @report runner (missing _report_spec).")
    return spec


def register_report(runner: Runner, mcp: FastMCP, app: typer.Typer) -> ReportSpec:
    """Register one ``@report`` runner on both the MCP and CLI surfaces."""
    from moneybin.reports._framework.cli_register import register_report_cli
    from moneybin.reports._framework.mcp_register import register_report_mcp

    spec = spec_of(runner)
    register_report_mcp(spec, mcp)
    register_report_cli(spec, app)
    return spec


def register_reports(
    runners: Iterable[Runner], mcp: FastMCP, app: typer.Typer
) -> list[ReportSpec]:
    """Register every runner in ``runners`` on both surfaces; return their specs."""
    return [register_report(runner, mcp, app) for runner in runners]


def register_reports_mcp(runners: Iterable[Runner], mcp: FastMCP) -> list[ReportSpec]:
    """Register ``runners`` on the MCP surface only (the server wires MCP alone)."""
    from moneybin.reports._framework.mcp_register import register_report_mcp

    specs = [spec_of(r) for r in runners]
    for spec in specs:
        register_report_mcp(spec, mcp)
    return specs


def register_reports_cli(
    runners: Iterable[Runner], app: typer.Typer
) -> list[ReportSpec]:
    """Register ``runners`` on the CLI surface only (the CLI wires Typer alone)."""
    from moneybin.reports._framework.cli_register import register_report_cli

    specs = [spec_of(r) for r in runners]
    for spec in specs:
        register_report_cli(spec, app)
    return specs


def discover_reports(module: ModuleType) -> list[Runner]:
    """Return the ``@report`` runners exported by ``module``, in definition order.

    Scans the module's attributes for the ``_report_spec`` marker; de-duplicates
    while preserving first-seen order (a runner re-exported under two names
    registers once).
    """
    seen: set[int] = set()
    runners: list[Runner] = []
    for obj in vars(module).values():
        if callable(obj) and hasattr(obj, "_report_spec") and id(obj) not in seen:
            seen.add(id(obj))
            runners.append(cast("Runner", obj))
    return runners
