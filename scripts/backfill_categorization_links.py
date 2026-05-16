#!/usr/bin/env python3
"""One-time backfill: set missing merchant_id and rule_id on transaction_categories.

Fixes records created before the bug fix that added merchant_id/rule_id to
categorize_transaction and bulk_categorize in write_tools.py.

Usage:
    uv run python scripts/backfill_categorization_links.py
"""

import logging
import re

import duckdb

from moneybin.database import Database, get_database
from moneybin.services.categorization import (
    CategorizationService,
    normalize_description,
)
from moneybin.tables import (
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _matches_pattern(text: str, pattern: str, match_type: str) -> bool:
    """Check if text matches a pattern using the specified match type."""
    text_lower = text.lower()
    pattern_lower = pattern.lower()

    if match_type == "exact":
        return text_lower == pattern_lower
    elif match_type == "contains":
        return pattern_lower in text_lower
    elif match_type == "regex":
        try:
            return bool(re.search(pattern, text, re.IGNORECASE))
        except re.error:
            return False
    return False


def backfill(db: Database) -> dict[str, int]:
    """Backfill missing merchant_id and rule_id."""
    service = CategorizationService(db)
    merchant_ids_set = 0
    rule_ids_set = 0

    # --- Backfill merchant_id ---
    try:
        rows = db.execute(
            f"""
            SELECT c.transaction_id, t.description
            FROM {TRANSACTION_CATEGORIES.full_name} c
            JOIN {FCT_TRANSACTIONS.full_name} t
                ON c.transaction_id = t.transaction_id
            WHERE c.merchant_id IS NULL
                AND t.description IS NOT NULL
                AND t.description != ''
            """,
        ).fetchall()
    except duckdb.CatalogException:
        rows = []

    for txn_id, description in rows:
        merchant = service.match_merchant(description)
        if merchant:
            db.execute(
                f"""
                UPDATE {TRANSACTION_CATEGORIES.full_name}
                SET merchant_id = ?
                WHERE transaction_id = ?
                """,
                [merchant["merchant_id"], txn_id],
            )
            merchant_ids_set += 1

    # --- Backfill rule_id ---
    try:
        rules = db.execute(
            f"""
            SELECT rule_id, merchant_pattern, match_type,
                   min_amount, max_amount, account_id,
                   category, subcategory
            FROM {CATEGORIZATION_RULES.full_name}
            WHERE is_active = true
            ORDER BY priority ASC, created_at ASC
            """,
        ).fetchall()
    except duckdb.CatalogException:
        rules = []

    if rules:
        try:
            categorized_no_rule = db.execute(
                f"""
                SELECT c.transaction_id, t.description, t.amount,
                       t.account_id, c.category, c.subcategory
                FROM {TRANSACTION_CATEGORIES.full_name} c
                JOIN {FCT_TRANSACTIONS.full_name} t
                    ON c.transaction_id = t.transaction_id
                WHERE c.rule_id IS NULL
                    AND t.description IS NOT NULL
                    AND t.description != ''
                """,
            ).fetchall()
        except duckdb.CatalogException:
            categorized_no_rule = []

        for txn_id, description, amount, account_id, cat, subcat in categorized_no_rule:
            normalized = normalize_description(description)
            for rule in rules:
                (
                    rule_id,
                    pattern,
                    match_type,
                    min_amount,
                    max_amount,
                    rule_account_id,
                    rule_category,
                    rule_subcategory,
                ) = rule

                # Rule must match the same category that was assigned
                if rule_category.lower() != cat.lower():
                    continue
                if (
                    rule_subcategory
                    and subcat
                    and rule_subcategory.lower() != subcat.lower()
                ):
                    continue

                if not (
                    _matches_pattern(description, pattern, match_type)
                    or _matches_pattern(normalized, pattern, match_type)
                ):
                    continue

                if min_amount is not None and amount < float(min_amount):
                    continue
                if max_amount is not None and amount > float(max_amount):
                    continue
                if rule_account_id is not None and account_id != rule_account_id:
                    continue

                db.execute(
                    f"""
                    UPDATE {TRANSACTION_CATEGORIES.full_name}
                    SET rule_id = ?
                    WHERE transaction_id = ?
                    """,
                    [rule_id, txn_id],
                )
                rule_ids_set += 1
                break

    return {"merchant_ids": merchant_ids_set, "rule_ids": rule_ids_set}


if __name__ == "__main__":
    db = get_database()
    logger.info("Backfilling categorization links")

    result = backfill(db)

    logger.info(
        f"Done: set {result['merchant_ids']} merchant_ids, "
        f"{result['rule_ids']} rule_ids"
    )
