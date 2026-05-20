# src/moneybin/mcp/tools/tax.py
"""Tax namespace tools -- W-2 data retrieval.

Tools:
    - tax_w2 — Retrieve W-2 form data (high sensitivity)
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.tax_service import TaxService, W2Result


@mcp_tool(domain="tax")
def tax_w2(tax_year: int | None = None) -> ResponseEnvelope[W2Result]:
    """Retrieve W-2 form data for a tax year.

    Returns wages, federal income tax, social security, medicare, and
    state/local tax information. PII fields (SSN, EIN) are never
    included in the response.

    Args:
        tax_year: Filter to a specific tax year. Returns all years when omitted.
    """
    with get_database(read_only=True) as db:
        result = TaxService(db).w2(tax_year=tax_year)
    return build_envelope(
        data=result, actions=["Use reports_spending for spending overview"]
    )


def register_tax_tools(mcp: FastMCP) -> None:
    """Register all tax namespace tools with the FastMCP server."""
    register(
        mcp,
        tax_w2,
        "tax_w2",
        "Retrieve W-2 form data (wages, taxes, deductions). "
        "Amounts are in the currency named by `summary.display_currency`. "
        "PII fields (SSN, EIN) are excluded.",
    )
