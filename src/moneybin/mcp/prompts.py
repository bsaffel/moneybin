"""MCP prompt templates for MoneyBin.

Prompts are pre-built templates that guide AI assistants through common
financial workflows. Each prompt is registered with the FastMCP server
via the @mcp.prompt() decorator.

Documentation: https://modelcontextprotocol.github.io/python-sdk/servers/prompts/
"""

import logging
import textwrap

from .server import mcp

logger = logging.getLogger(__name__)


def _dedent(text: str) -> str:
    """Strip leading indentation from a triple-quoted string."""
    return textwrap.dedent(text).strip()


@mcp.prompt()
def import_data() -> str:
    """Help the user import financial data files."""
    return _dedent("""
        Help me import financial data into MoneyBin.

        Steps:
        1. Ask the user for the file path to import
        2. Call import_file — it detects the format and handles the rest
        3. Review the import summary to confirm what was loaded
        4. Use list_accounts and query_transactions to verify the data
        5. Offer to run categorize_transactions if any transactions are uncategorized
    """)


@mcp.prompt()
def categorize_transactions() -> str:
    """Help categorize uncategorized transactions."""
    return _dedent("""
        Help me categorize my transactions.

        Steps:
        1. Use get_categorization_stats to see how many are uncategorized
        2. Use list_categories to see the available taxonomy
           - If no categories exist, use seed_categories first
        3. Use get_uncategorized_transactions to fetch a batch of transactions
        4. Review all transactions and decide categories for each
        5. Use bulk_categorize to assign categories to all transactions at once
        6. For recurring patterns, use bulk_create_categorization_rules to
           create rules so future imports are categorized automatically
        7. Optionally use bulk_create_merchant_mappings to normalize
           merchant names and associate them with categories
        8. Repeat from step 3 if more uncategorized transactions remain

        IMPORTANT: Always use the bulk tools (bulk_categorize,
        bulk_create_categorization_rules, bulk_create_merchant_mappings)
        instead of their single-item equivalents to avoid tool-call limits.

        Category priority: user manual > user rules > plaid > AI.
        Ask the user to confirm before categorizing.
    """)


@mcp.prompt()
def setup_budget() -> str:
    """Help set up monthly budgets by category."""
    return _dedent("""
        Help me set up a monthly budget.

        Steps:
        1. Use get_monthly_summary to understand current spending patterns
        2. Use get_spending_by_category to see where money is going
        3. Discuss reasonable budget amounts for each category
        4. Use set_budget to create budgets for each category
        5. Use get_budget_status to review the budget vs actual spending

        Tip: Start with the biggest spending categories first.
    """)


@mcp.prompt()
def monthly_review(month: str = "") -> str:
    """Conduct a monthly financial review.

    Args:
        month: Month to review (YYYY-MM). Leave empty for current month.
    """
    month_text = f"for {month}" if month else "for the current month"
    return _dedent(f"""
        Conduct a monthly financial review {month_text}.

        Steps:
        1. Use get_monthly_summary to see income vs expenses
        2. Use get_spending_by_category to analyze spending breakdown
        3. Use get_budget_status to check budget compliance
        4. Use find_recurring_transactions to identify subscriptions
        5. Highlight any unusual spending or trends
        6. Suggest areas for improvement

        Present a clear, concise summary with actionable insights.
    """)


@mcp.prompt()
def analyze_spending(period: str = "last 30 days") -> str:
    """Analyze spending patterns and identify top categories.

    Args:
        period: Time period to analyze (e.g. 'last 30 days', 'January 2025').
    """
    return _dedent(f"""
        Analyze my spending for {period}.

        Steps:
        1. Use the query_transactions tool to get transactions for the period
        2. Group spending by payee/merchant to find top spending categories
        3. Calculate total spending, average transaction size, and count
        4. Identify any unusually large transactions
        5. Summarize findings with actionable insights

        If categorized data is available, use get_spending_by_category.
    """)


@mcp.prompt()
def find_anomalies(days: str = "30") -> str:
    """Look for unusual or suspicious transactions.

    Args:
        days: Number of days to look back (default: 30).
    """
    return _dedent(f"""
        Look for unusual transactions in the last {days} days.

        Steps:
        1. Use query_transactions to get recent transactions
        2. Identify transactions that are significantly larger than typical
        3. Look for unfamiliar payees or merchants
        4. Check for duplicate charges (same amount, same payee, close dates)
        5. Flag any transactions that seem out of pattern

        Present findings as a list with the reason each was flagged.
    """)


@mcp.prompt()
def tax_preparation(tax_year: str = "2024") -> str:
    """Gather tax-related information for a specific year.

    Args:
        tax_year: The tax year to prepare for.
    """
    return _dedent(f"""
        Help me prepare tax information for {tax_year}.

        Steps:
        1. Use get_w2_summary to retrieve W-2 data for the year
        2. Summarize total wages, federal tax withheld, and state taxes
        3. If multiple W-2s exist, show a combined summary
        4. Use query_transactions to look for potentially deductible expenses
        5. Highlight any data gaps (missing forms, incomplete data)

        Note: This is informational only — not tax advice.
        Consult a tax professional for filing decisions.
    """)


@mcp.prompt()
def account_overview() -> str:
    """Get a comprehensive overview of all accounts."""
    return _dedent("""
        Give me a comprehensive overview of all my financial accounts.

        Steps:
        1. Use list_accounts to see all connected accounts
        2. Use get_account_balances to get current balances
        3. Use list_institutions to see which banks are connected
        4. Summarize by institution and account type
        5. Calculate total balance across all accounts

        Present as a clear summary table.
    """)


@mcp.prompt()
def transaction_search(description: str = "") -> str:
    """Help find specific transactions matching a description.

    Args:
        description: What to search for (e.g. 'Amazon purchases', 'rent payments').
    """
    header = (
        f"Help me find transactions matching: {description}"
        if description
        else "Help me find specific transactions"
    )
    return _dedent(f"""
        {header}

        Steps:
        1. Convert the description into appropriate search filters
        2. Use query_transactions with payee_pattern, date range, or amount filters
        3. If no exact matches, try broader patterns (e.g. '%AMZN%' for Amazon)
        4. Present results sorted by date with key details
        5. Calculate totals if multiple matches found

        Tip: Use SQL ILIKE patterns with % wildcards for flexible matching.
    """)
