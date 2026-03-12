"""MCP prompt templates for MoneyBin.

Prompts are pre-built templates that guide AI assistants through common
financial workflows. Each prompt is registered with the FastMCP server
via the @mcp.prompt() decorator.

Documentation: https://modelcontextprotocol.github.io/python-sdk/servers/prompts/
"""

import logging

from .server import mcp

logger = logging.getLogger(__name__)


@mcp.prompt()
def import_data() -> str:
    """Help the user import financial data files."""
    return (
        "Help me import financial data into MoneyBin.\n\n"
        "Steps:\n"
        "1. Ask the user for the file path to import\n"
        "2. Use the import_file tool with the provided path\n"
        "3. Review the import summary to confirm what was loaded\n"
        "4. Use list_accounts and query_transactions to verify the data\n\n"
        "Supported file types:\n"
        "- .ofx/.qfx — bank statements (OFX/Quicken format)\n"
        "- .pdf — W-2 tax forms"
    )


@mcp.prompt()
def categorize_transactions() -> str:
    """Help categorize uncategorized transactions."""
    return (
        "Help me categorize my transactions.\n\n"
        "Steps:\n"
        "1. Use get_categorization_stats to see how many are uncategorized\n"
        "2. Use list_categories to see the available taxonomy\n"
        "   - If no categories exist, use seed_categories first\n"
        "3. Use get_uncategorized_transactions to find transactions without categories\n"
        "4. Review each transaction's description and amount\n"
        "5. Suggest appropriate categories based on the payee/description\n"
        "6. Use categorize_transaction to assign categories\n"
        "7. Offer to create categorization rules for recurring patterns\n\n"
        "Category priority: user manual > user rules > plaid > AI.\n"
        "Ask the user to confirm before categorizing."
    )


@mcp.prompt()
def auto_categorize_transactions() -> str:
    """Guide the LLM through automatic transaction categorization."""
    return (
        "Automatically categorize uncategorized transactions.\n\n"
        "Steps:\n"
        "1. Use get_categorization_stats to check current coverage\n"
        "2. Use list_categories to verify taxonomy is seeded\n"
        "   - If no categories exist, use seed_categories first\n"
        "3. Use auto_categorize with dry_run=true to preview the scope\n"
        "4. Ask user to confirm, then run auto_categorize with dry_run=false\n"
        "5. Review results and use get_categorization_stats to see new coverage\n"
        "6. For any low-confidence results, offer to review and adjust\n"
        "7. Suggest creating rules for frequently occurring merchants\n\n"
        "The auto_categorize tool uses MCP sampling to classify transactions "
        "using your connected LLM. High-confidence results also create "
        "merchant mappings for future automatic categorization."
    )


@mcp.prompt()
def setup_budget() -> str:
    """Help set up monthly budgets by category."""
    return (
        "Help me set up a monthly budget.\n\n"
        "Steps:\n"
        "1. Use get_monthly_summary to understand current spending patterns\n"
        "2. Use get_spending_by_category to see where money is going\n"
        "3. Discuss reasonable budget amounts for each category\n"
        "4. Use set_budget to create budgets for each category\n"
        "5. Use get_budget_status to review the budget vs actual spending\n\n"
        "Tip: Start with the biggest spending categories first."
    )


@mcp.prompt()
def monthly_review(month: str = "") -> str:
    """Conduct a monthly financial review.

    Args:
        month: Month to review (YYYY-MM). Leave empty for current month.
    """
    month_text = f"for {month}" if month else "for the current month"
    return (
        f"Conduct a monthly financial review {month_text}.\n\n"
        "Steps:\n"
        "1. Use get_monthly_summary to see income vs expenses\n"
        "2. Use get_spending_by_category to analyze spending breakdown\n"
        "3. Use get_budget_status to check budget compliance\n"
        "4. Use find_recurring_transactions to identify subscriptions\n"
        "5. Highlight any unusual spending or trends\n"
        "6. Suggest areas for improvement\n\n"
        "Present a clear, concise summary with actionable insights."
    )


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
        "If categorized data is available, use get_spending_by_category."
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
