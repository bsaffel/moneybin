# src/moneybin/mcp/tools/accounts.py
"""Accounts namespace tools — account listing and balances.

Tools:
    - accounts.list — List all accounts (low sensitivity)
    - accounts.balances — Get latest account balances (medium sensitivity)
"""

from __future__ import annotations

import logging

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
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


def register_accounts_tools(mcp: FastMCP) -> None:
    """Register all accounts namespace tools with the FastMCP server."""
    register(
        mcp,
        accounts_list,
        "accounts.list",
        "List all accounts in MoneyBin with type, institution, and source information.",
    )
    register(
        mcp,
        accounts_balances,
        "accounts.balances",
        "Get latest balance snapshot for each account. "
        "Optionally filter by account ID.",
    )
