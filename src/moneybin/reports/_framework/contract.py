"""The runner-first report contract.

A report is a decorated runner ``(db, **params) -> ReportQuery``. The runner
owns parameter validation, free-text→id resolution, and SQL construction; the
framework introspects it into a :class:`ReportSpec` and generates the MCP tool,
CLI command, and ``TableRef`` wiring from that single definition. See
``docs/specs/extension-contracts.md`` §"Report contract".
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from moneybin.tables import TableRef

# A runner takes an open Database plus keyword-only params and returns the
# parameterized SELECT to run. ``Any`` for the first arg avoids importing
# Database here purely for a type alias.
Runner = Callable[..., "ReportQuery"]


@dataclass(frozen=True, slots=True)
class ReportQuery:
    """A parameterized read-only SELECT that a report runner returns.

    ``actions`` and ``period`` let the runner declare its own envelope
    enrichment — next-step hints and the human-readable window — since those
    are report-specific and the runner is the report's single definition.
    """

    sql: str
    params: Sequence[object] = ()
    actions: Sequence[str] = ()
    period: str | None = None


@dataclass(frozen=True, slots=True)
class ParamSpec:
    """One keyword-only runner parameter, introspected from the signature.

    Maps 1:1 to an MCP-tool argument, a Typer ``--flag``, and the runner
    keyword argument. ``annotation`` is the resolved Python type;
    ``required`` is true when the parameter has no default.
    """

    name: str
    annotation: Any
    default: Any
    required: bool
    help: str


@dataclass(frozen=True, slots=True)
class ReportSpec:
    """Introspected metadata for one report, built from its runner.

    Carries everything the dynamic registrars need to build the MCP tool, CLI
    command, and ``TableRef`` wiring without re-reading the source. The derived
    per-column class map is computed elsewhere (``classify``) and cached there,
    keeping this a pure, frozen description.
    """

    name: str
    description: str
    view: TableRef
    runner: Runner
    params: tuple[ParamSpec, ...] = ()
    examples: tuple[str, ...] = ()
    domain: str | None = None

    @property
    def mcp_tool_name(self) -> str:
        """FastMCP tool name, e.g. ``reports_large_transactions``."""
        return f"reports_{self.name}"

    @property
    def cli_name(self) -> str:
        """Typer command name, e.g. ``large-transactions``."""
        return self.name.replace("_", "-")


def report(
    *, name: str, view: TableRef, domain: str | None = None
) -> Callable[[Runner], Runner]:
    """Mark a runner as a report and attach its introspected :class:`ReportSpec`.

    The spec is stored on the function as ``_report_spec`` for later discovery;
    the runner itself is returned unchanged so it stays directly callable.

    Args:
        name: Canonical report name (underscore form). The MCP tool is
            ``reports_<name>`` and the CLI command is ``<name>`` with
            underscores rendered as hyphens.
        view: The ``reports.*`` ``TableRef`` the runner reads.
        domain: Optional MCP namespace tag.
    """
    # Imported lazily to avoid a contract<->introspect import cycle.
    from moneybin.reports._framework.introspect import build_spec

    def decorate(fn: Runner) -> Runner:
        fn._report_spec = build_spec(fn, name=name, view=view, domain=domain)  # type: ignore[attr-defined]
        return fn

    return decorate
