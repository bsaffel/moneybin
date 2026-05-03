# src/moneybin/mcp/tools/accounts.py
"""Accounts namespace tools — account listing and balances.

Tools:
    - accounts_list — List all accounts (medium sensitivity)
    - accounts_balances — Get latest account balances (medium sensitivity)
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.account_service import AccountService


@mcp_tool(sensitivity="medium")
def accounts_list() -> ResponseEnvelope:
    """List all accounts in MoneyBin (default sensitivity: medium).

    Returns the canonical resolved view including display_name, account_type,
    institution_name, last_four, credit_limit, archived flag, and net-worth
    inclusion flag. Default response carries last_four and credit_limit, which
    require the medium tier.

    Phase 8 will add include_archived, type_filter, and redacted parameters
    plus the full v2 tool surface.
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


def register_accounts_tools(mcp: FastMCP) -> None:
    """Register all accounts namespace tools with the FastMCP server."""
    register(
        mcp,
        accounts_list,
        "accounts_list",
        "List all accounts in MoneyBin with type, institution, and source information.",
    )
    register(
        mcp,
        accounts_balances,
        "accounts_balances",
        "Get latest balance snapshot for each account. "
        "Optionally filter by account ID.",
    )
