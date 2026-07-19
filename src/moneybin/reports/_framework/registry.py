"""Discover ``@report`` runners and register them on the CLI and catalog.

In-tree reports are collected from an explicit list (the ``definitions``
package's exports); ``discover_reports`` scans a module's members for the same
``_report_spec`` marker. Extensions join the process-local catalog and their
own Typer app through ``register_extension_reports`` without touching MCP. The
MCP surface has one generic catalog/runner independent of report count.

Cold-start: ``cli_register`` defers ``execute``/``sqlglot``.
"""

from __future__ import annotations

from collections.abc import Iterable
from types import ModuleType
from typing import TYPE_CHECKING, cast

from moneybin.reports._framework.contract import ReportSpec, Runner

if TYPE_CHECKING:
    import typer
    from fastmcp import FastMCP

_extension_reports: dict[str, ReportSpec] = {}


def register_extension_report(spec: ReportSpec) -> None:
    """Add one discovered SQL-backed extension report by stable full ID."""
    if spec.report_id in _extension_reports:
        raise ValueError(f"duplicate extension report_id: {spec.report_id}")
    _extension_reports[spec.report_id] = spec


def register_extension_reports(
    runners: Iterable[Runner], app: typer.Typer
) -> list[ReportSpec]:
    """Register extension runners in the process catalog and their own CLI app."""
    from moneybin.reports._framework.catalog import get_report_catalog
    from moneybin.reports._framework.cli_register import register_report_cli

    specs = [spec_of(runner) for runner in runners]
    batch_ids = [spec.report_id for spec in specs]
    duplicate_ids = {
        report_id for report_id in batch_ids if batch_ids.count(report_id) > 1
    }
    current_ids = {report.report_id for report in get_report_catalog().list()}
    duplicate_ids.update(current_ids.intersection(batch_ids))
    if duplicate_ids:
        raise ValueError(
            f"duplicate extension report_id: {', '.join(sorted(duplicate_ids))}"
        )

    for spec in specs:
        register_extension_report(spec)
    for spec in specs:
        register_report_cli(spec, app)
    return specs


def extension_report_specs() -> tuple[ReportSpec, ...]:
    """Return explicitly registered extension reports in deterministic ID order."""
    return tuple(_extension_reports[key] for key in sorted(_extension_reports))


def spec_of(runner: Runner) -> ReportSpec:
    spec = getattr(runner, "_report_spec", None)
    if not isinstance(spec, ReportSpec):
        name = getattr(runner, "__name__", repr(runner))
        raise ValueError(f"{name} is not a @report runner (missing _report_spec).")
    return spec


def register_reports_cli(
    runners: Iterable[Runner], app: typer.Typer
) -> list[ReportSpec]:
    """Register ``runners`` on the CLI surface only (the CLI wires Typer alone)."""
    from moneybin.reports._framework.cli_register import register_report_cli

    specs = [spec_of(r) for r in runners]
    for spec in specs:
        register_report_cli(spec, app)
    return specs


def register_generic_reports_tool(mcp: FastMCP) -> None:
    """Register only the dormant generic ``reports`` MCP contract."""
    from moneybin.mcp._registration import register
    from moneybin.mcp.tools.reports import reports

    register(
        mcp,
        reports,
        "reports",
        "Browse registered financial reports or run one by stable report ID. "
        "Omit `report_id` to return catalog metadata; supply it to execute a "
        "registered read-only report. This tool never accepts SQL; use "
        "`sql_query` separately for arbitrary read-only SQL.",
    )


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
