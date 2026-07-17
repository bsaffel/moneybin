"""Generate and register a FastMCP tool from a report spec.

Builds a function whose ``__signature__`` carries the report's params (so
FastMCP introspects the right input schema), forwards them to ``run_report``,
and wraps the result in the standard envelope. The tool runs in
``dynamic_classification`` mode: sensitivity and ``classes_returned`` are set
per call from the lineage-derived tier, and ``run_report`` has already masked
CRITICAL columns via ``redact_records`` (the decorator's trust contract).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import get_max_rows
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.reports._framework.contract import ReportSpec
from moneybin.reports._framework.execute import ReportRowsPayload, run_report


def _build_signature(spec: ReportSpec) -> inspect.Signature:
    params = [
        inspect.Parameter(
            p.name,
            inspect.Parameter.KEYWORD_ONLY,
            default=inspect.Parameter.empty if p.required else p.default,
            annotation=p.annotation if p.annotation is not None else Any,
        )
        for p in spec.params
    ]
    return inspect.Signature(
        params, return_annotation=ResponseEnvelope[ReportRowsPayload]
    )


def make_tool_fn(
    spec: ReportSpec,
) -> Callable[..., ResponseEnvelope[ReportRowsPayload]]:
    """Build the MCP tool implementation for ``spec`` with an explicit signature."""

    def _impl(**params: Any) -> ResponseEnvelope[ReportRowsPayload]:
        with get_database(read_only=True) as db:
            return run_report(spec, db, max_rows=get_max_rows(), **params).to_envelope()

    _impl.__name__ = spec.mcp_tool_name
    _impl.__qualname__ = spec.mcp_tool_name
    _impl.__doc__ = inspect.getdoc(spec.runner)
    _impl.__signature__ = _build_signature(spec)  # type: ignore[attr-defined]
    _impl.__annotations__ = {
        p.name: (p.annotation if p.annotation is not None else Any) for p in spec.params
    }
    _impl.__annotations__["return"] = ResponseEnvelope[ReportRowsPayload]
    return _impl


def register_report_mcp(spec: ReportSpec, mcp: FastMCP) -> None:
    """Register ``spec`` as a ``reports_<name>`` FastMCP tool."""
    fn = make_tool_fn(spec)
    decorated = mcp_tool(dynamic_classification=True, domain=spec.domain)(fn)
    # Summary only — the full docstring's Args block names `db`, which is not a
    # passable param (it is stripped from the signature), so dumping it into the
    # agent-facing description invites a rejected call. Per-param help still
    # reaches the schema via _impl.__doc__ (FastMCP parses it from there).
    register(mcp, decorated, spec.mcp_tool_name, spec.description)
