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
        - reports_spending_get — monthly spending trend with MoM/YoY deltas
        - reports_cashflow_get — inflow/outflow/net per account x category
        - reports_budget_status — budget vs actual comparison
        - accounts_balance_list — current account balances
        - reports_recurring_get — subscription/recurring charge review

        **Workflow:**
        1. Start with reports_spending_get for the last 1-2 months
        2. If spending is above average, drill into reports_spending_get with a
           specific ``category`` filter, or use reports_cashflow_get for an
           account-and-category breakdown
        3. Check reports_budget_status for any categories over budget
        4. Review accounts_balance_list for current position
        5. Optionally check reports_recurring_get for subscription review

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
        - transactions_categorize_stats — check current categorization coverage
        - transactions_categorize_pending_list — fetch uncategorized transactions
        - categories_list — see available categories
        - transactions_categorize_apply — apply categories to transactions
        - transactions_categorize_rules_create — create rules for recurring patterns
        - merchants_create — map merchant names to categories

        **Workflow:**
        1. Check transactions_categorize_stats to see how many are uncategorized
        2. Fetch a batch with transactions_categorize_pending_list (limit ~20)
        3. Group similar transactions by description pattern
        4. For repeating patterns, suggest a rule (transactions_categorize_rules_create)
        5. For one-offs, use transactions_categorize_apply directly
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
        - transactions_categorize_auto_stats — pending proposal count and rule health
        - transactions_categorize_auto_review — list pending proposals with samples
        - transactions_categorize_auto_accept — batch approve/reject proposals by ID
        - transactions_categorize_rules_list — review currently active rules

        **Workflow:**
        1. Check transactions_categorize_auto_stats for pending proposal count
        2. Fetch proposals with transactions_categorize_auto_review
        3. For each proposal, show the merchant pattern, suggested
           category, sample matching transactions, and trigger count
        4. Group user decisions and submit them with transactions_categorize_auto_accept

        **Guardrails:**
        - Always show sample transactions before asking for approval
        - Flag proposals that seem overly broad or ambiguous
        - Confirm batches with the user before submitting auto_accept
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
        - import_files — import one or more financial data files
        - import_list_formats — see supported formats
        - accounts_list — verify imported accounts
        - transactions_categorize_stats — check categorization coverage
        - reports_spending_get — first look at their data

        **Workflow:**
        1. Ask the user what files they have (OFX/QFX, CSV, PDF W-2s)
        2. Import the user's files in one call to import_files (pass a list of paths)
        3. Verify with accounts_list that accounts were created
        4. Check transactions_categorize_stats — if many uncategorized, offer to help
        5. Show reports_spending_get as their first financial snapshot

        Default categories are seeded automatically by `moneybin db init`
        and `moneybin transform apply`.

        **Guardrails:**
        - Be patient — new users may not know their file formats
        - If import fails, explain what went wrong and suggest alternatives
        - Don't overwhelm with all available tools — introduce gradually
        - Celebrate successful imports to build confidence
    """)


@mcp.prompt()
def curate_recent_transactions() -> str:
    """Walk the user through curating recently-imported transactions."""
    return _dedent("""
        Help the user curate their most recent transactions — propose tags
        and an initial note for rows that lack curator context.

        **Goal:** Drain the un-noted, un-tagged tail of recent imports so the
        next analysis pass has consistent metadata.

        **Relevant tools:**
        - transactions_get — fetch recent rows; pair with system_audit_list to
          spot gaps (no note.add / tag.add events on a transaction_id).
        - transactions_tags_set — declarative tag replacement (idempotent).
        - transactions_notes_add — append a curator note.
        - system_audit_list — sanity check what already happened.

        **Workflow:**
        1. Call transactions_get with a recent date window (e.g., last 30 days,
           limit 50). Preserve transaction_id, description, amount, account_id.
        2. For each row, propose a small set of slug-pattern tags
           (^[a-z0-9_-]+(:[a-z0-9_-]+)?$) and an optional note. Keep tags short
           and reusable; reuse existing tags when possible (a prior
           system_audit_list with action_pattern='tag.%' helps here).
        3. Confirm the batch with the user before mutating.
        4. Apply: transactions_tags_set per row, transactions_notes_add for any
           row where a note adds context the description does not.

        **Guardrails:**
        - Tags are slugs — ASCII alnum + `_`/`-`, optional single namespace.
        - Notes max 2000 chars, must be non-empty.
        - Never invent transaction_ids; only act on rows returned by search.
        - Prefer fewer high-signal tags over many noisy ones.
    """)


@mcp.prompt()
def review_curation_history() -> str:
    """Summarize the last 7 days of curation activity from the audit log."""
    return _dedent("""
        Summarize what curation actions happened recently and surface anything
        unusual (high-volume tag renames, repeated noop edits, unfamiliar
        actors).

        **Goal:** Give the user a quick mental model of what changed in the
        last week without forcing them to read raw audit rows.

        **Relevant tools:**
        - system_audit_list — pull recent events. Call with limit=500 and
          filters['from'] set to seven days ago (ISO timestamp).
        - The result `data[]` already includes action, actor, target_table,
          target_id, before/after, parent_audit_id.

        **Workflow:**
        1. Call system_audit_list with from = (now - 7 days) and limit=500.
        2. Group by `action_pattern` prefix: note.*, tag.*, split.*,
           import_label.*, manual.*, category.*.
        3. Report counts per group, top 3 actors, and any noteworthy
           outliers (parent tag.rename events, large split.clear bursts).
        4. Offer drill-down via further system_audit_list calls
           (e.g., action_pattern='tag.rename') if the user asks.

        **Guardrails:**
        - Read-only — do not mutate state from this prompt.
        - Do not echo raw before/after values for high-sensitivity rows;
          summarize counts instead.
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
        - reports_spending_get — find deduction-eligible categories
        - transactions_get — search for specific deductible expenses
        - categories_list — review tax-relevant categories

        **Workflow:**
        1. Ask for the tax year
        2. Pull W-2 data with tax_w2 for that year
        3. Review reports_spending_get for deduction-eligible categories
           (charitable, medical, business expenses, etc.)
        4. If needed, search for specific transactions with transactions_get
        5. Summarize totals by deduction category

        **Guardrails:**
        - This is data gathering, not tax advice — say so explicitly
        - Note that completeness depends on what data has been imported
        - Flag any W-2 discrepancies (e.g., missing forms for known employers)
        - Present amounts clearly with category totals
    """)
