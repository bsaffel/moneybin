"""MCP v1 prompt templates for MoneyBin.

Goal-oriented prompts that guide AI assistants through financial workflows.
Each defines a goal, relevant tools, guardrails, and decision points —
not step-by-step scripts.

See ``docs/specs/moneybin-mcp.md`` for the current prompt and resource contract.
"""

from __future__ import annotations

import textwrap

from fastmcp import FastMCP


def _dedent(text: str) -> str:
    """Strip leading indentation from a triple-quoted string."""
    return textwrap.dedent(text).strip()


def monthly_review() -> str:
    """Monthly financial review — spending, budget status, and trends."""
    return _dedent("""
        Conduct a monthly financial review for the user.

        **Goal:** Summarize the past month's finances and highlight anything
        that needs attention.

        **Relevant tools:**
        - reports(report_id='core:spending') — monthly spending trend
        - reports(report_id='core:cashflow') — inflow/outflow/net
        - accounts_balances — current account balances
        - reports(report_id='core:recurring') — recurring charge review

        **Workflow:**
        1. Start with reports(report_id='core:spending') for the last 1-2 months
        2. If spending is above average, rerun reports with
           report_id='core:spending' and a category parameter, or use
           report_id='core:cashflow' for an account-and-category breakdown
        3. Review accounts_balances for current position
        4. Optionally run reports(report_id='core:recurring')

        **Guardrails:**
        - Present totals and trends, not individual transaction details unless asked
        - Compare to prior months when data is available
        - Flag categories where spending increased significantly
        - Do not make judgments about spending habits — present data neutrally
    """)


def categorization_organize() -> str:
    """Organize uncategorized transactions into categories."""
    return _dedent("""
        Help the user categorize their uncategorized transactions.

        **Goal:** Reduce the uncategorized transaction count toward zero
        using a mix of rules and direct categorization.

        **Relevant tools:**
        - system_status(sections=['categorization']) — check coverage
        - reviews(kind='categorization', status='pending') — fetch the queue
        - taxonomy(view='categories') — see available categories
        - transactions_categorize_commit — commit accepted categorizations
        - transactions_categorize_rules_set — create recurring rules
        - taxonomy_set — create or update merchant mappings

        **Workflow:**
        1. Check system_status(sections=['categorization'])
        2. Fetch a batch with reviews(kind='categorization',
           status='pending', limit=20)
        3. Group similar transactions by description pattern
        4. For repeating patterns, suggest a rule and submit its full target
           state with transactions_categorize_rules_set
        5. For one-offs, use transactions_categorize_commit directly
        6. Repeat until coverage is acceptable

        **Guardrails:**
        - Always confirm category assignments with the user before applying
        - Prefer rules over manual categorization for recurring merchants
        - Show the user what each rule would match before creating it
        - Don't create overly broad rules (e.g., matching single characters)
    """)


def review_auto_rules() -> str:
    """Review persisted categorization rules and apply confirmed state changes."""
    return _dedent("""
        Help me review the persisted categorization rules that MoneyBin can
        currently read and change.

        **Goal:** Audit active rule behavior and prior rule changes, then apply
        only the full target states the user confirms.

        **Relevant tools:**
        - system_status(sections=['categorization'], detail='full') — coverage
          and aggregate automatic-rule health
        - transactions_categorize_rules(view='active') — current active rules
        - transactions_categorize_rules(view='history') — prior rule state changes
        - transactions_categorize_rules_set — declare confirmed target states
        - transactions_categorize_run — apply active rules to uncategorized rows

        **Workflow:**
        1. Check system_status(sections=['categorization'], detail='full')
        2. Read transactions_categorize_rules(view='active')
        3. Use transactions_categorize_rules(view='history') when the user
           needs context about a rule's prior changes
        4. Explain each selected rule's matcher, category, priority, and state
        5. Confirm the batch, then submit full target states with
           transactions_categorize_rules_set
        6. If requested, call transactions_categorize_run(methods=['rules'])
           to apply the surviving active rules to uncategorized rows

        **Guardrails:**
        - Do not infer evidence that the rule read contract does not return
        - Flag matchers that look overly broad or ambiguous
        - Confirm every target-state batch before changing persisted rules
        - Creating a rule and running the categorizer are separate operations
    """)


def onboarding() -> str:
    """First-time setup — import data and establish baseline."""
    return _dedent("""
        Guide a new user through their first MoneyBin setup.

        **Goal:** Import their financial data and establish a working baseline
        so they can start querying their finances.

        **Relevant tools:**
        - import_files — import one or more financial data files
        - import_status(sections=['formats']) — see supported formats
        - accounts — verify imported accounts
        - system_status(sections=['categorization']) — check coverage
        - reports(report_id='core:spending') — first look at their data

        **Workflow:**
        1. Ask the user what files they have (OFX/QFX, CSV)
        2. Import the user's files in one call to import_files (pass a list of paths)
        3. Verify with accounts that accounts were created
        4. Check system_status(sections=['categorization']); if many are
           uncategorized, offer to help
        5. Show reports(report_id='core:spending') as the first snapshot

        Default categories are seeded automatically by `moneybin db init`
        and `moneybin transform apply`.

        **Guardrails:**
        - Be patient — new users may not know their file formats
        - If import fails, explain what went wrong and suggest alternatives
        - Don't overwhelm with all available tools — introduce gradually
        - Celebrate successful imports to build confidence
    """)


def curate_recent_transactions() -> str:
    """Walk the user through curating recently-imported transactions."""
    return _dedent("""
        Help the user curate their most recent transactions — propose tags
        and an initial note for rows that lack curator context.

        **Goal:** Drain the un-noted, un-tagged tail of recent imports so the
        next analysis pass has consistent metadata.

        **Relevant tools:**
        - transactions — fetch recent rows; pair with system_audit to
          spot gaps (no note.add / tag.add events on a transaction_id).
        - transactions_annotate — batch stable-ID note lifecycle and tag states.
        - system_audit — sanity check what already happened.

        **Workflow:**
        1. Call transactions with a recent date window (e.g., last 30 days,
           limit 50). Preserve transaction_id, description, amount, account_id.
        2. For each row, propose a small set of slug-pattern tags
           (^[a-z0-9_-]+(:[a-z0-9_-]+)?$) and an optional note. Keep tags short
           and reusable; reuse existing tags when possible (a prior
           system_audit(view='events', limit=500) helps identify existing tag
           activity; filter the returned events locally).
        3. Confirm the batch with the user before mutating.
        4. Apply one transactions_annotate batch containing the confirmed tag
           target states and notes.

        **Guardrails:**
        - Tags are slugs — ASCII alnum + `_`/`-`, optional single namespace.
        - Notes max 2000 chars, must be non-empty.
        - Never invent transaction_ids; only act on rows returned by search.
        - Prefer fewer high-signal tags over many noisy ones.
    """)


def review_curation_history() -> str:
    """Summarize the last 7 days of curation activity from the audit log."""
    return _dedent("""
        Summarize what curation actions happened recently and surface anything
        unusual (high-volume tag renames, repeated noop edits, unfamiliar
        actors).

        **Goal:** Give the user a quick mental model of what changed in the
        last week without forcing them to read raw audit rows.

        **Relevant tools:**
        - system_audit — pull recent events with
          system_audit(view='events', limit=500).
        - The result `data.events[]` already includes action, actor, target_table,
          target_id, before/after, parent_audit_id.

        **Workflow:**
        1. Call system_audit(view='events', limit=500), then filter returned
           events to the last seven days locally.
        2. Group returned event actions locally by prefix: note.*, tag.*, split.*,
           import.*, manual.*, category.*.
        3. Report counts per group, top 3 actors, and any noteworthy
           outliers (parent tag.rename events, large split.clear bursts).
        4. Offer drill-down with system_audit(view='detail', audit_id=...) or
           system_audit(view='detail', operation_id=...) if the user asks.

        **Guardrails:**
        - Read-only — do not mutate state from this prompt.
        - Do not echo raw before/after values for high-sensitivity rows;
          summarize counts instead.
    """)


def sync_review() -> str:
    """Review sync health and suggest the next action."""
    return _dedent("""
        Review my MoneyBin sync state and flag anything that needs attention.

        **Relevant tools:**
        - sync_status — list connected institutions with last-sync time, status,
          and error guidance.
        - reports(report_id='core:spending') — optional,
          aggregate context for recent transaction volume.

        **Workflow:**
        1. Call sync_status first.
        2. Use reports(report_id='core:spending') only when
           aggregate volume context would clarify an anomaly.
        3. Report errors, stale institutions (last sync older than seven days),
           and material volume anomalies. Quote the relevant action hint and
           recommend one next action, or say no action is needed.

        **Guardrails:**
        - Do not include account numbers, balances, individual transaction
          descriptions, or merchant names.
        - Use counts, dates, status codes, and institution names only.
    """)


PROMPT_FUNCTIONS = (
    monthly_review,
    categorization_organize,
    review_auto_rules,
    onboarding,
    curate_recent_transactions,
    review_curation_history,
    sync_review,
)


def register_prompts(mcp: FastMCP) -> None:
    """Register the complete central prompt set on one MCP server."""
    for prompt in PROMPT_FUNCTIONS:
        mcp.prompt()(prompt)


# tax_prep prompt removed alongside the W-2 extraction pipeline.
# Tax data ingestion will be re-designed in a future brainstorm.
