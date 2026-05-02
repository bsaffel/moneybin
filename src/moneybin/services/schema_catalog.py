"""Schema catalog service — produces the LLM-facing schema document.

Joins live DuckDB catalog metadata (table/column types and comments) with
hand-authored example queries, filtered to the curated interface tables
declared in `moneybin.tables`.
"""

from __future__ import annotations

from dataclasses import dataclass

CONVENTIONS: dict[str, str] = {
    "amount_sign": "negative = expense, positive = income",
    "currency": "DECIMAL(18,2); ISO 4217 codes in currency_code columns",
    "dates": "DATE type; transaction_date is the canonical posting date",
    "ids": (
        "Deterministic SHA-256 truncated to 16 hex chars; "
        "see core.fct_transactions.transaction_id"
    ),
}


@dataclass(frozen=True)
class Example:
    """A single example query for a table."""

    question: str
    sql: str


EXAMPLES: dict[str, list[Example]] = {
    "core.fct_transactions": [
        Example(
            question="Total spending by category last month",
            sql="""
                SELECT category, SUM(amount_absolute) AS total
                FROM core.fct_transactions
                WHERE transaction_direction = 'expense'
                  AND transaction_year_month = STRFTIME(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')
                GROUP BY category
                ORDER BY total DESC
            """,
        ),
        Example(
            question="Transactions for an account in a date range",
            sql="""
                SELECT transaction_date, description, amount, category
                FROM core.fct_transactions
                WHERE account_id = ?
                  AND transaction_date BETWEEN ? AND ?
                ORDER BY transaction_date DESC
            """,
        ),
        Example(
            question="Monthly spending trend (last 12 months)",
            sql="""
                SELECT transaction_year_month, SUM(amount_absolute) AS total_spent
                FROM core.fct_transactions
                WHERE transaction_direction = 'expense'
                  AND transaction_date >= CURRENT_DATE - INTERVAL 12 MONTH
                GROUP BY transaction_year_month
                ORDER BY transaction_year_month
            """,
        ),
    ],
    "core.dim_accounts": [
        Example(
            question="List all accounts with their institution",
            sql="""
                SELECT account_id, account_type, institution_name, source_type
                FROM core.dim_accounts
                ORDER BY institution_name, account_type
            """,
        ),
        Example(
            question="Join accounts to transactions to label by institution",
            sql="""
                SELECT a.institution_name, COUNT(*) AS txn_count,
                       SUM(t.amount_absolute) AS total_volume
                FROM core.fct_transactions t
                JOIN core.dim_accounts a USING (account_id)
                GROUP BY a.institution_name
                ORDER BY total_volume DESC
            """,
        ),
    ],
    "core.bridge_transfers": [
        Example(
            question="Confirmed transfer pairs with debit and credit transaction IDs",
            sql="""
                SELECT b.transfer_id, b.debit_transaction_id,
                       b.credit_transaction_id, b.amount, b.date_offset_days
                FROM core.bridge_transfers b
                ORDER BY b.transfer_id
            """,
        ),
    ],
    "app.categories": [
        Example(
            question="All active categories",
            sql="""
                SELECT category_id, category, subcategory, description
                FROM app.categories
                WHERE is_active
                ORDER BY category, subcategory
            """,
        ),
    ],
    "app.budgets": [
        Example(
            question="Active budgets with their target amounts",
            sql="""
                SELECT * FROM app.budgets ORDER BY category
            """,
        ),
    ],
    "app.transaction_notes": [
        Example(
            question="All notes on a specific transaction",
            sql="""
                SELECT transaction_id, note, created_at
                FROM app.transaction_notes
                WHERE transaction_id = ?
                ORDER BY created_at
            """,
        ),
    ],
    "app.merchants": [
        Example(
            question="Merchants with their canonical names",
            sql="""
                SELECT merchant_id, canonical_name, raw_pattern
                FROM app.merchants
                ORDER BY canonical_name
            """,
        ),
    ],
    "app.categorization_rules": [
        Example(
            question="Active categorization rules",
            sql="""
                SELECT rule_id, merchant_pattern, category, subcategory, priority
                FROM app.categorization_rules
                WHERE is_active
                ORDER BY priority DESC
            """,
        ),
    ],
    "app.transaction_categories": [
        Example(
            question="Per-transaction category assignments",
            sql="""
                SELECT transaction_id, category, subcategory, categorized_by
                FROM app.transaction_categories
                ORDER BY categorized_at DESC
                LIMIT 100
            """,
        ),
    ],
}
