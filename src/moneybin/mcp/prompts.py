"""MCP v1 prompt templates for MoneyBin.

Goal-oriented prompts that guide AI assistants through financial workflows.
Each defines a goal, relevant tools, guardrails, and decision points —
not step-by-step scripts.

See ``mcp-tool-surface.md`` section 14.
"""

from __future__ import annotations

import textwrap

from .server import mcp


def _dedent(text: str) -> str:
    """Strip leading indentation from a triple-quoted string."""
    return textwrap.dedent(text).strip()


@mcp.prompt()
def monthly_review() -> str:
    """Monthly financial review — spending, budget status, and trends."""
    return _dedent("""
        Conduct a monthly financial review for the user.

        **Goal:** Summarize the past month's finances and highlight anything
        that needs attention.

        **Relevant tools:**
        - spending_summary — income vs expenses by month
        - spending_by_category — category breakdown
        - budget_status — budget vs actual comparison
        - accounts_balances — current account balances
        - transactions_recurring — subscription/recurring charge review

        **Workflow:**
        1. Start with spending_summary for the last 1-2 months
        2. If spending is above average, drill into spending_by_category
        3. Check budget_status for any categories over budget
        4. Review accounts_balances for current position
        5. Optionally check transactions_recurring for subscription review

        **Guardrails:**
        - Present totals and trends, not individual transaction details unless asked
        - Compare to prior months when data is available
        - Flag categories where spending increased significantly
        - Do not make judgments about spending habits — present data neutrally
    """)


@mcp.prompt()
def categorization_organize() -> str:
    """Organize uncategorized transactions into categories."""
    return _dedent("""
        Help the user categorize their uncategorized transactions.

        **Goal:** Reduce the uncategorized transaction count toward zero
        using a mix of rules and direct categorization.

        **Relevant tools:**
        - categorize_stats — check current categorization coverage
        - categorize_uncategorized — fetch uncategorized transactions
        - categorize_categories — see available categories
        - categorize_bulk — apply categories to transactions
        - categorize_create_rules — create rules for recurring patterns
        - categorize_create_merchants — map merchant names to categories

        **Workflow:**
        1. Check categorize_stats to see how many are uncategorized
        2. Fetch a batch with categorize_uncategorized (limit ~20)
        3. Group similar transactions by description pattern
        4. For repeating patterns, suggest a rule (categorize_create_rules)
        5. For one-offs, use categorize_bulk directly
        6. Repeat until coverage is acceptable

        **Guardrails:**
        - Always confirm category assignments with the user before applying
        - Prefer rules over manual categorization for recurring merchants
        - Show the user what each rule would match before creating it
        - Don't create overly broad rules (e.g., matching single characters)
    """)


@mcp.prompt()
def review_auto_rules() -> str:
    """Review proposed auto-categorization rules and approve or reject them."""
    return _dedent("""
        Help me review proposed auto-categorization rules. Show pending
        proposals with sample transactions, explain the pattern, and let
        me approve or reject them.

        **Goal:** Walk the user through pending auto-rule proposals so
        they can promote useful rules to active and reject noisy ones.

        **Relevant tools:**
        - categorize_auto_stats — pending proposal count and rule health
        - categorize_auto_review — list pending proposals with samples
        - categorize_auto_confirm — batch approve/reject proposals by ID
        - categorize_rules — review currently active rules

        **Workflow:**
        1. Check categorize_auto_stats for pending proposal count
        2. Fetch proposals with categorize_auto_review
        3. For each proposal, show the merchant pattern, suggested
           category, sample matching transactions, and trigger count
        4. Group user decisions and submit them with categorize_auto_confirm

        **Guardrails:**
        - Always show sample transactions before asking for approval
        - Flag proposals that seem overly broad or ambiguous
        - Confirm batches with the user before submitting auto_confirm
        - Approved rules categorize matching transactions immediately
    """)


@mcp.prompt()
def onboarding() -> str:
    """First-time setup — import data and establish baseline."""
    return _dedent("""
        Guide a new user through their first MoneyBin setup.

        **Goal:** Import their financial data and establish a working baseline
        so they can start querying their finances.

        **Relevant tools:**
        - import_file — import financial data files
        - import_list_formats — see supported formats
        - accounts_list — verify imported accounts
        - categorize_stats — check categorization coverage
        - spending_summary — first look at their data

        **Workflow:**
        1. Ask the user what files they have (OFX/QFX, CSV, PDF W-2s)
        2. Import files one at a time with import_file
        3. Verify with accounts_list that accounts were created
        4. Check categorize_stats — if many uncategorized, offer to help
        5. Show spending_summary as their first financial snapshot

        Default categories are seeded automatically by `moneybin db init`
        and `moneybin transform apply`.

        **Guardrails:**
        - Be patient — new users may not know their file formats
        - If import fails, explain what went wrong and suggest alternatives
        - Don't overwhelm with all available tools — introduce gradually
        - Celebrate successful imports to build confidence
    """)


@mcp.prompt()
def tax_prep() -> str:
    """Tax preparation — gather W-2 data and deductible expenses."""
    return _dedent("""
        Help the user gather tax-related financial information.

        **Goal:** Compile W-2 data and identify potentially deductible
        expenses for tax preparation.

        **Relevant tools:**
        - tax_w2 — retrieve W-2 wage and tax data
        - spending_by_category — find deduction-eligible categories
        - transactions_search — search for specific deductible expenses
        - categorize_categories — review tax-relevant categories

        **Workflow:**
        1. Ask for the tax year
        2. Pull W-2 data with tax_w2 for that year
        3. Review spending_by_category for deduction-eligible categories
           (charitable, medical, business expenses, etc.)
        4. If needed, search for specific transactions with transactions_search
        5. Summarize totals by deduction category

        **Guardrails:**
        - This is data gathering, not tax advice — say so explicitly
        - Note that completeness depends on what data has been imported
        - Flag any W-2 discrepancies (e.g., missing forms for known employers)
        - Present amounts clearly with category totals
    """)
