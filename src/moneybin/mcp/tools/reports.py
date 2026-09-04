"""Generic catalog and runner for registered read-only reports."""

from __future__ import annotations

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field, JsonValue

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.reports import ReportsPayload
from moneybin.privacy.sensitivity import Sensitivity, get_max_rows, tier_to_sensitivity
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.reports._framework.catalog import (
    catalog_classes_returned,
    catalog_sensitivity,
    catalog_to_payload,
    get_report_catalog,
    open_report_catalog,
    profile_home_currency,
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
    display_currency: str | None = None,
) -> ResponseEnvelope[ReportsPayload]:
    """Browse the report catalog or execute one registered read-only report."""
    if report_id is None:
        if parameters is not None or limit is not None or display_currency is not None:
            raise UserError(
                "parameters, limit, and display_currency require report_id",
                code=error_codes.REPORT_ID_REQUIRED,
            )
        # The catalog spans all three tiers and the user tier lives in the
        # database, so listing opens one where it previously needed none — but it
        # degrades to the packaged tiers on a profile that has none rather than
        # turning "what reports exist?" into a db-init error.
        #
        # Active reports only. An `include_archived` parameter would mirror the
        # CLI flag, but it changes the serialized tool metadata that ADR-016's
        # carrying-weight evidence and a dated comparison record pin — a cost to
        # spend deliberately, not as a side effect of a listing tweak. An
        # archived report still runs, exports, and explains by id here.
        with open_report_catalog() as (catalog, _):
            payload = catalog_to_payload(catalog)
            sensitivity = catalog_sensitivity(payload.reports)
        return build_envelope(
            data=payload,
            sensitivity=sensitivity,
            total_count=len(payload.reports),
            returned_count=len(payload.reports),
            classes_returned=catalog_classes_returned(sensitivity),
        )

    if limit is not None and limit < 1:
        raise UserError(
            "limit must be at least 1",
            code=error_codes.REPORT_LIMIT_INVALID,
        )

    session_max = get_max_rows()
    max_rows = session_max if limit is None else min(limit, session_max)
    with get_database(read_only=True) as db:
        result = get_report_catalog(db).execute(
            db,
            report_id=report_id,
            parameters=parameters or {},
            limit=max_rows,
            display_currency=display_currency,
            home_currency=profile_home_currency(db),
        )
    payload = result_to_payload(result)
    # Built by hand rather than through `ReportResult.to_envelope`, which sends
    # raw records where this surface owes a typed payload and its own
    # `returned_count`. Every other field it carries must therefore be repeated
    # here — `applied_rates` included, or the one report-reading surface an
    # agent actually calls silently drops the provenance its own tool
    # description promises.
    return build_envelope(
        data=payload,
        sensitivity=tier_to_sensitivity(result.tier).value,
        total_count=result.total_count,
        returned_count=len(payload.rows),
        classes_returned=result.classes_returned,
        actions=result.actions or None,
        period=result.period,
        display_currency=result.display_currency,
        degraded=result.degraded,
        degraded_reason=result.degraded_reason,
        applied_rates=[rate.as_provenance() for rate in result.applied_rates] or None,
    )


def register_reports_tools(mcp: FastMCP) -> None:
    """Register the single standard report catalog and runner."""
    from moneybin.reports._framework.registry import register_generic_reports_tool

    register_generic_reports_tool(mcp)
