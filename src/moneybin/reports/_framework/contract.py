"""The runner-first report contract.

A report is a decorated runner ``(db, **params) -> ReportQuery``. The runner
owns parameter validation, free-text→id resolution, and SQL construction; the
framework introspects it into a :class:`ReportSpec` and generates the MCP tool,
CLI command, and ``TableRef`` wiring from that single definition. See
``docs/specs/extension-contracts.md`` §"Report contract".
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import TableRef

# A runner takes an open Database plus keyword-only params and returns the
# parameterized SELECT to run. ``Any`` for the first arg avoids importing
# Database here purely for a type alias.
Runner = Callable[..., "ReportQuery"]
_REPORT_ID = re.compile(r"[a-z][a-z0-9_-]*:[a-z][a-z0-9_-]*")


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
class OutputColumn:
    """One named report output with its meaning and privacy class."""

    name: str
    description: str
    data_class: DataClass


@dataclass(frozen=True, slots=True)
class ReportSemantics:
    """Financial interpretation metadata for a report's metrics."""

    unit: str
    currency: str | None
    sign: str
    kind: Literal["position", "flow", "ratio", "count"]
    valuation_basis: str | None
    fx_basis: str | None
    time_basis: str
    denominator: str | None
    comparison_window: str | None
    exclusions: tuple[str, ...]
    provenance: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReportSpec:
    """Introspected metadata for one report, built from its runner.

    Carries everything the dynamic registrars need to build the MCP tool, CLI
    command, and ``TableRef`` wiring without re-reading the source.

    ``classes`` is the report's **declared** column→DataClass map — the privacy
    contract. It is declared, not derived: SQLMesh deploys each report view as a
    ``SELECT * FROM <internal physical table>`` pointer, so lineage on the view
    body can't classify it (see ADR-013). ``redaction`` masks output columns by
    this map; any column absent from it fails closed (see ``classify``).
    """

    report_id: str
    name: str
    description: str
    view: TableRef
    runner: Runner
    classes: Mapping[str, DataClass]
    columns: tuple[OutputColumn, ...]
    semantics: ReportSemantics
    params: tuple[ParamSpec, ...] = ()
    examples: tuple[str, ...] = ()
    domain: str | None = None

    def __post_init__(self) -> None:
        if _REPORT_ID.fullmatch(self.report_id) is None:
            raise ValueError("report_id must use namespace:name")
        declared = {column.name: column.data_class for column in self.columns}
        if len(declared) != len(self.columns) or declared != dict(self.classes):
            raise ValueError(
                "columns and classes must declare the same output fields "
                "with identical privacy classes"
            )

    @property
    def mcp_tool_name(self) -> str:
        """Transitional legacy FastMCP name, e.g. ``reports_large_transactions``."""
        return f"reports_{self.name}"

    @property
    def cli_name(self) -> str:
        """Typer command name, e.g. ``large-transactions``."""
        return self.name.replace("_", "-")


def report(
    *,
    report_id: str,
    name: str,
    view: TableRef,
    classes: Mapping[str, DataClass],
    columns: tuple[OutputColumn, ...],
    semantics: ReportSemantics,
    domain: str | None = None,
) -> Callable[[Runner], Runner]:
    """Mark a runner as a report and attach its introspected :class:`ReportSpec`.

    The spec is stored on the function as ``_report_spec`` for later discovery;
    the runner itself is returned unchanged so it stays directly callable.

    Args:
        report_id: Stable namespaced report identifier.
        name: Canonical report name (underscore form). The MCP tool is
            ``reports_<name>`` and the CLI command is ``<name>`` with
            underscores rendered as hyphens.
        view: The ``reports.*`` ``TableRef`` the runner reads.
        classes: The declared output-column→DataClass map — the report's
            privacy contract. Must cover every column the view exposes; an
            undeclared column fails closed at redaction time. Declared (not
            lineage-derived) because the deployed SQLMesh view is a
            ``SELECT *`` pointer lineage can't classify (ADR-013).
        columns: Ordered output column descriptions and privacy classes.
        semantics: Financial interpretation metadata for the report metrics.
        domain: Optional MCP namespace tag.
    """
    # Imported lazily to avoid a contract<->introspect import cycle.
    from moneybin.reports._framework.introspect import build_spec

    def decorate(fn: Runner) -> Runner:
        fn._report_spec = build_spec(  # type: ignore[attr-defined]
            fn,
            report_id=report_id,
            name=name,
            view=view,
            classes=classes,
            columns=columns,
            semantics=semantics,
            domain=domain,
        )
        return fn

    return decorate
