"""Runner-first report framework: one decorated runner generates every surface.

A contributor writes a runner ``(db, **params) -> ReportQuery``; the framework
introspects it into a ``ReportSpec`` and generates the MCP tool, CLI command,
and ``TableRef`` wiring. See ``docs/specs/extension-contracts.md``
§"Report contract".
"""

from moneybin.reports._framework.contract import (
    ParamSpec,
    ReportQuery,
    ReportSpec,
    report,
)

__all__ = [
    "ParamSpec",
    "ReportQuery",
    "ReportSpec",
    "report",
]
