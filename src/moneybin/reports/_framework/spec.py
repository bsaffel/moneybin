"""Typed representation of what was parsed from a report SQL file.

A ReportSpec is the framework's contract with the dynamic registrars —
it carries enough metadata to build the MCP tool signature, the CLI
command, the service method, and the TableRef constant without
re-reading the source file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ParamSpec:
    """One parameter declared via an @param structured comment.

    Maps 1:1 to MCP-tool argument, Typer --flag, and service-method
    keyword argument. The type_hint is the SQL-layer type (INTEGER,
    TEXT, TEXT[], DATE, DECIMAL); the framework maps to Python types
    when building tool signatures.
    """

    name: str
    type_hint: str  # SQL type: INTEGER, TEXT, TEXT[], DATE, DECIMAL, BOOLEAN
    optional: bool
    default: Any  # parsed literal (None, int, str, list, etc.)
    doc: str


@dataclass(frozen=True, slots=True)
class ReportSpec:
    """Parsed structured-comment metadata from a report SQL file.

    Built by parse_report_sql(); consumed by the dynamic registrars in
    mcp_register / cli_register / service_register / tableref_register.
    """

    name: str
    description: str
    params: list[ParamSpec] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)
    source_path: Path | None = None  # SQL file the spec was parsed from

    def param(self, name: str) -> ParamSpec:
        """Lookup a parameter by name; raises KeyError if absent."""
        for p in self.params:
            if p.name == name:
                return p
        raise KeyError(f"No parameter named {name!r} on report {self.name!r}")
