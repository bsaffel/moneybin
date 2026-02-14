"""MCP prompt templates for MoneyBin.

Prompts are pre-built templates that guide AI assistants through common
financial analysis workflows. Each prompt is registered with the FastMCP
server via the @mcp.prompt() decorator.

Documentation: https://modelcontextprotocol.github.io/python-sdk/servers/prompts/
"""

import logging

from .server import mcp

logger = logging.getLogger(__name__)


@mcp.prompt()
def analyze_spending(period: str = "last 30 days") -> str:
    """Analyze spending patterns and identify top categories.

    Args:
        period: Time period to analyze (e.g. 'last 30 days', 'January 2025').
    """
    return (
        f"Analyze my spending for {period}.\n\n"
        "Steps:\n"
        "1. Use the query_transactions tool to get transactions for the period\n"
        "2. Group spending by payee/merchant to find top spending categories\n"
        "3. Calculate total spending, average transaction size, and count\n"
        "4. Identify any unusually large transactions\n"
        "5. Summarize findings with actionable insights\n\n"
        "If categorized data is not available, work with raw payee/merchant names."
    )


@mcp.prompt()
def find_anomalies(days: str = "30") -> str:
    """Look for unusual or suspicious transactions.

    Args:
        days: Number of days to look back (default: 30).
    """
    return (
        f"Look for unusual transactions in the last {days} days.\n\n"
        "Steps:\n"
        "1. Use query_transactions to get recent transactions\n"
        "2. Identify transactions that are significantly larger than typical\n"
        "3. Look for unfamiliar payees or merchants\n"
        "4. Check for duplicate charges (same amount, same payee, close dates)\n"
        "5. Flag any transactions that seem out of pattern\n\n"
        "Present findings as a list with the reason each was flagged."
    )


@mcp.prompt()
def tax_preparation(tax_year: str = "2024") -> str:
    """Gather tax-related information for a specific year.

    Args:
        tax_year: The tax year to prepare for.
    """
    return (
        f"Help me prepare tax information for {tax_year}.\n\n"
        "Steps:\n"
        "1. Use get_w2_summary to retrieve W-2 data for the year\n"
        "2. Summarize total wages, federal tax withheld, and state taxes\n"
        "3. If multiple W-2s exist, show a combined summary\n"
        "4. Use query_transactions to look for potentially deductible expenses\n"
        "5. Highlight any data gaps (missing forms, incomplete data)\n\n"
        "Note: This is informational only — not tax advice. "
        "Consult a tax professional for filing decisions."
    )


@mcp.prompt()
def account_overview() -> str:
    """Get a comprehensive overview of all accounts."""
    return (
        "Give me a comprehensive overview of all my financial accounts.\n\n"
        "Steps:\n"
        "1. Use list_accounts to see all connected accounts\n"
        "2. Use get_account_balances to get current balances\n"
        "3. Use list_institutions to see which banks are connected\n"
        "4. Summarize by institution and account type\n"
        "5. Calculate total balance across all accounts\n\n"
        "Present as a clear summary table."
    )


@mcp.prompt()
def transaction_search(description: str = "") -> str:
    """Help find specific transactions matching a description.

    Args:
        description: What to search for (e.g. 'Amazon purchases', 'rent payments').
    """
    search_context = (
        f"Help me find transactions matching: {description}"
        if description
        else "Help me find specific transactions"
    )

    return (
        f"{search_context}\n\n"
        "Steps:\n"
        "1. Convert the description into appropriate search filters\n"
        "2. Use query_transactions with payee_pattern, date range, "
        "or amount filters\n"
        "3. If no exact matches, try broader patterns (e.g. '%AMZN%' "
        "for Amazon)\n"
        "4. Present results sorted by date with key details\n"
        "5. Calculate totals if multiple matches found\n\n"
        "Tip: Use SQL ILIKE patterns with % wildcards for flexible matching."
    )
