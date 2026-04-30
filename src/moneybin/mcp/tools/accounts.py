# src/moneybin/mcp/tools/accounts.py
"""Accounts namespace tools — account listing and balances.

Tools:
    - accounts.list — List all accounts (low sensitivity)
    - accounts.balances — Get latest account balances (medium sensitivity)
"""

from __future__ import annotations

import logging

from moneybin.database import get_database
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.namespaces import NamespaceRegistry, ToolDefinition
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.account_service import AccountService

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low")
def accounts_list() -> ResponseEnvelope:
    """List all accounts in MoneyBin.

    Returns account ID, type, institution name, and source type for
    each account. Use this to discover available accounts before
    querying balances or filtering transactions.
    """
    service = AccountService(get_database())
    result = service.list_accounts()
    return result.to_envelope()


@mcp_tool(sensitivity="medium")
def accounts_balances(
    account_id: str | None = None,
) -> ResponseEnvelope:
    """Get the latest balance snapshot for each account.

    Returns ledger balance, available balance, and as-of date.
    Optionally filter to a single account.

    Args:
        account_id: Filter to a specific account ID.
    """
    service = AccountService(get_database())
    result = service.balances(account_id=account_id)
    return result.to_envelope()


def register_accounts_tools(registry: NamespaceRegistry) -> list[ToolDefinition]:
    """Register all accounts namespace tools with the registry."""
    tools = [
        ToolDefinition(
            name="accounts.list",
            description=(
                "List all accounts in MoneyBin with type, institution, "
                "and source information."
            ),
            fn=accounts_list,
        ),
        ToolDefinition(
            name="accounts.balances",
            description=(
                "Get latest balance snapshot for each account. "
                "Optionally filter by account ID."
            ),
            fn=accounts_balances,
        ),
    ]
    for tool in tools:
        registry.register(tool)
    return tools
