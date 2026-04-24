# src/moneybin/mcp/tools/tax.py
"""Tax namespace tools -- W-2 data retrieval.

Tools:
    - tax.w2 — Retrieve W-2 form data (high sensitivity)
"""

from __future__ import annotations

import logging

from moneybin.database import get_database
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.envelope import ResponseEnvelope
from moneybin.mcp.namespaces import NamespaceRegistry, ToolDefinition
from moneybin.services.tax_service import TaxService

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="high")
def tax_w2(
    tax_year: int | None = None,
) -> ResponseEnvelope:
    """Retrieve W-2 form data for a tax year.

    Returns wages, federal income tax, social security, medicare, and
    state/local tax information. PII fields (SSN, EIN) are never
    included in the response.

    Args:
        tax_year: Filter to a specific tax year. Returns all years when omitted.
    """
    service = TaxService(get_database())
    result = service.w2(tax_year=tax_year)
    return result.to_envelope()


def register_tax_tools(registry: NamespaceRegistry) -> list[ToolDefinition]:
    """Register all tax namespace tools with the registry."""
    tools = [
        ToolDefinition(
            name="tax.w2",
            description=(
                "Retrieve W-2 form data (wages, taxes, deductions). "
                "PII fields (SSN, EIN) are excluded."
            ),
            fn=tax_w2,
        ),
    ]
    for tool in tools:
        registry.register(tool)
    return tools
