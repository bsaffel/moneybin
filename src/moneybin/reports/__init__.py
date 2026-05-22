"""Auto-generate the report registration trinity from one SQL file.

A contributor writes ONE SQL file with @-block structured comments; the
framework generates the MCP tool, CLI command, service method, and TableRef.
See docs/specs/extension-contracts.md §"Report contract".
"""

from moneybin.reports._framework.spec import ParamSpec, ReportSpec

__all__ = [
    "ParamSpec",
    "ReportSpec",
]
