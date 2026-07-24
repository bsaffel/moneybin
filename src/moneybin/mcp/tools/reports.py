"""Generic catalog and runner for registered read-only reports."""

from __future__ import annotations

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field, JsonValue

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity, get_max_rows, tier_to_sensitivity
from moneybin.privacy.payloads.reports import ReportsPayload
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.reports._framework.catalog import (
    catalog_to_payload,
    get_report_catalog,
    result_to_payload,
)


@mcp_tool(
    dynamic_classification=True,
    maximum_sensitivity=Sensitivity.CRITICAL,
    domain="reports",
)
def reports(
    report_id: str | None = None,
    parameters: dict[str, JsonValue] | None = None,
    limit: Annotated[int, Field(strict=True, ge=1)] | None = None,
) -> ResponseEnvelope[ReportsPayload]:
    """Browse the report catalog or execute one registered read-only report."""
    catalog = get_report_catalog()
    if report_id is None:
        if parameters is not None or limit is not None:
            raise UserError(
                "parameters and limit require report_id",
                code="REPORT_ID_REQUIRED",
            )
        payload = catalog_to_payload(catalog)
        return build_envelope(
            data=payload,
            sensitivity="low",
            total_count=len(payload.reports),
            returned_count=len(payload.reports),
            classes_returned=["aggregate"],
        )

    if limit is not None and limit < 1:
        raise UserError(
            "limit must be at least 1",
            code="REPORT_LIMIT_INVALID",
        )

    session_max = get_max_rows()
    max_rows = session_max if limit is None else min(limit, session_max)
    with get_database(read_only=True) as db:
        result = catalog.execute(
            db,
            report_id=report_id,
            parameters=parameters or {},
            limit=max_rows,
        )
    payload = result_to_payload(result)
    return build_envelope(
        data=payload,
        sensitivity=tier_to_sensitivity(result.tier).value,
        total_count=result.total_count,
        returned_count=len(payload.rows),
        classes_returned=result.classes_returned,
        actions=result.actions or None,
        period=result.period,
        display_currency=result.display_currency,
    )


def register_reports_tools(mcp: FastMCP) -> None:
    """Register the single standard report catalog and runner."""
    from moneybin.reports._framework.registry import register_generic_reports_tool

    register_generic_reports_tool(mcp)
