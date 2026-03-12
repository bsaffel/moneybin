"""Transaction categorization service.

Handles merchant normalization, rule-based categorization, merchant matching,
and taxonomy management. Designed for deterministic operations — LLM-based
auto-categorization lives in the MCP layer (auto_categorize tool).
"""

import logging
import re
import uuid

import duckdb

from moneybin.tables import (
    CATEGORIES,
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)

# -- Merchant name normalization patterns --

# Common POS prefixes: Square, Toast, PayPal, etc.
_POS_PREFIXES = re.compile(
    r"^(SQ\s*\*|TST\s*\*|PP\s*\*|PAYPAL\s*\*|VENMO\s*\*|ZELLE\s*\*|CKE\s*\*)",
    re.IGNORECASE,
)

# Trailing location: city/state/zip patterns
_TRAILING_LOCATION = re.compile(
    r"\s+"
    r"(?:[A-Z]{2}\s+\d{5}(?:-\d{4})?$"  # ST 12345 or ST 12345-6789
    r"|[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,?\s+[A-Z]{2}$"  # City, ST (city must be 3+ chars)
    r"|\d{5}(?:-\d{4})?$"  # bare zip code
    r")"
)

# Trailing numbers: store IDs, reference numbers (3+ digits at end)
_TRAILING_NUMBERS = re.compile(r"\s+#?\d{3,}$")

# Multiple spaces to single
_MULTI_SPACE = re.compile(r"\s+")


def normalize_description(description: str) -> str:
    """Clean a raw transaction description for matching and display.

    Applies deterministic cleanup:
    1. Strip POS prefixes (SQ *, TST*, PP*, etc.)
    2. Strip trailing location info (city, state, zip)
    3. Strip trailing store IDs / reference numbers
    4. Normalize whitespace and trim

    Args:
        description: Raw transaction description.

    Returns:
        Cleaned description string.
    """
    if not description:
        return ""

    result = description.strip()
    result = _POS_PREFIXES.sub("", result)
    result = _TRAILING_LOCATION.sub("", result)
    result = _TRAILING_NUMBERS.sub("", result)
    result = _MULTI_SPACE.sub(" ", result).strip()

    return result


def _matches_pattern(text: str, pattern: str, match_type: str) -> bool:
    """Check if text matches a pattern using the specified match type.

    Args:
        text: Text to match against.
        pattern: Pattern to match.
        match_type: One of 'exact', 'contains', 'regex'.

    Returns:
        True if the text matches the pattern.
    """
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
            logger.warning("Invalid regex pattern: %s", pattern)
            return False
    else:
        logger.warning("Unknown match_type: %s", match_type)
        return False


def match_merchant(
    conn: duckdb.DuckDBPyConnection, description: str
) -> dict[str, str | None] | None:
    """Look up a merchant by raw description.

    Args:
        conn: DuckDB connection (read-only is fine).
        description: Transaction description to match.

    Returns:
        Dict with merchant_id, canonical_name, category, subcategory
        if found, otherwise None.
    """
    normalized = normalize_description(description)
    if not normalized:
        return None

    try:
        rows = conn.execute(
            f"""
            SELECT merchant_id, raw_pattern, match_type,
                   canonical_name, category, subcategory
            FROM {MERCHANTS.full_name}
            ORDER BY
                CASE match_type
                    WHEN 'exact' THEN 1
                    WHEN 'contains' THEN 2
                    WHEN 'regex' THEN 3
                END
            """,
        ).fetchall()
    except duckdb.CatalogException:
        return None

    for row in rows:
        merchant_id, raw_pattern, match_type, canonical_name, category, subcategory = (
            row
        )
        # Match against both raw description and normalized form
        if _matches_pattern(description, raw_pattern, match_type) or _matches_pattern(
            normalized, raw_pattern, match_type
        ):
            return {
                "merchant_id": merchant_id,
                "canonical_name": canonical_name,
                "category": category,
                "subcategory": subcategory,
            }

    return None


def create_merchant(
    conn: duckdb.DuckDBPyConnection,
    raw_pattern: str,
    canonical_name: str,
    *,
    match_type: str = "contains",
    category: str | None = None,
    subcategory: str | None = None,
    created_by: str = "ai",
) -> str:
    """Create a merchant mapping.

    Args:
        conn: DuckDB read-write connection.
        raw_pattern: Pattern to match in transaction descriptions.
        canonical_name: Clean merchant name for display.
        match_type: How to match: 'exact', 'contains', or 'regex'.
        category: Optional default category for this merchant.
        subcategory: Optional default subcategory.
        created_by: Who created the mapping ('user', 'ai', 'rule').

    Returns:
        The merchant_id of the created merchant.
    """
    merchant_id = str(uuid.uuid4())[:8]
    conn.execute(
        f"""
        INSERT INTO {MERCHANTS.full_name}
        (merchant_id, raw_pattern, match_type, canonical_name,
         category, subcategory, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [
            merchant_id,
            raw_pattern,
            match_type,
            canonical_name,
            category,
            subcategory,
            created_by,
        ],
    )
    logger.info("Created merchant mapping: %s -> %s", raw_pattern, canonical_name)
    return merchant_id


def apply_merchant_categories(
    conn: duckdb.DuckDBPyConnection,
) -> int:
    """Apply merchant-based categories to uncategorized transactions.

    For each uncategorized transaction, checks if a merchant mapping exists
    that matches the description and has a category assigned.

    Args:
        conn: DuckDB read-write connection.

    Returns:
        Number of transactions categorized.
    """
    try:
        uncategorized = conn.execute(
            f"""
            SELECT t.transaction_id, t.description
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            WHERE c.transaction_id IS NULL
                AND t.description IS NOT NULL
                AND t.description != ''
            """,
        ).fetchall()
    except duckdb.CatalogException:
        return 0

    if not uncategorized:
        return 0

    categorized_count = 0
    for txn_id, description in uncategorized:
        merchant = match_merchant(conn, description)
        if merchant and merchant.get("category"):
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory, categorized_at,
                 categorized_by, merchant_id, confidence)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'rule', ?, 1.0)
                """,
                [
                    txn_id,
                    merchant["category"],
                    merchant["subcategory"],
                    merchant["merchant_id"],
                ],
            )
            categorized_count += 1

    if categorized_count:
        logger.info("Merchant matching categorized %d transactions", categorized_count)
    return categorized_count


def apply_rules(
    conn: duckdb.DuckDBPyConnection,
) -> int:
    """Apply active categorization rules to uncategorized transactions.

    Rules are evaluated in priority order (lower number = higher priority).
    The first matching rule wins. Rules can filter by merchant pattern,
    amount range, and account ID.

    Args:
        conn: DuckDB read-write connection.

    Returns:
        Number of transactions categorized.
    """
    try:
        rules = conn.execute(
            f"""
            SELECT rule_id, name, merchant_pattern, match_type,
                   min_amount, max_amount, account_id,
                   category, subcategory
            FROM {CATEGORIZATION_RULES.full_name}
            WHERE is_active = true
            ORDER BY priority ASC, created_at ASC
            """,
        ).fetchall()
    except duckdb.CatalogException:
        return 0

    if not rules:
        return 0

    try:
        uncategorized = conn.execute(
            f"""
            SELECT t.transaction_id, t.description, t.amount, t.account_id
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            WHERE c.transaction_id IS NULL
                AND t.description IS NOT NULL
                AND t.description != ''
            """,
        ).fetchall()
    except duckdb.CatalogException:
        return 0

    if not uncategorized:
        return 0

    categorized_count = 0
    for txn_id, description, amount, account_id in uncategorized:
        normalized = normalize_description(description)
        for rule in rules:
            (
                rule_id,
                _name,
                pattern,
                match_type,
                min_amount,
                max_amount,
                rule_account_id,
                category,
                subcategory,
            ) = rule

            # Check pattern match (against both raw and normalized)
            if not (
                _matches_pattern(description, pattern, match_type)
                or _matches_pattern(normalized, pattern, match_type)
            ):
                continue

            # Check amount range
            if min_amount is not None and amount < float(min_amount):
                continue
            if max_amount is not None and amount > float(max_amount):
                continue

            # Check account filter
            if rule_account_id is not None and account_id != rule_account_id:
                continue

            # Rule matches — apply it
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory, categorized_at,
                 categorized_by, rule_id, confidence)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'rule', ?, 1.0)
                """,
                [txn_id, category, subcategory, rule_id],
            )
            categorized_count += 1
            break  # First matching rule wins

    if categorized_count:
        logger.info("Rule engine categorized %d transactions", categorized_count)
    return categorized_count


def apply_deterministic_categorization(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    """Run all deterministic categorization: merchants first, then rules.

    Called after import/transform to automatically categorize new transactions
    without any LLM dependency.

    Args:
        conn: DuckDB read-write connection.

    Returns:
        Dict with counts: {'merchant': N, 'rule': N, 'total': N}.
    """
    merchant_count = apply_merchant_categories(conn)
    rule_count = apply_rules(conn)
    total = merchant_count + rule_count

    if total:
        logger.info(
            "Deterministic categorization: %d merchant, %d rule, %d total",
            merchant_count,
            rule_count,
            total,
        )

    return {
        "merchant": merchant_count,
        "rule": rule_count,
        "total": total,
    }


def seed_categories(conn: duckdb.DuckDBPyConnection) -> int:
    """Populate user.categories from the SQLMesh seed table.

    Copies default categories from seeds.seed_categories into user.categories,
    skipping any that already exist. Safe to run multiple times.

    Args:
        conn: DuckDB read-write connection.

    Returns:
        Number of categories inserted.
    """
    count_before = 0
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {CATEGORIES.full_name}").fetchone()
        count_before = result[0] if result else 0
    except duckdb.CatalogException:
        pass

    conn.execute(
        f"""
        INSERT OR IGNORE INTO {CATEGORIES.full_name}
        (category_id, category, subcategory, description, is_default,
         is_active, plaid_detailed, created_at)
        SELECT
            category_id,
            category,
            subcategory,
            description,
            true AS is_default,
            true AS is_active,
            plaid_detailed,
            CURRENT_TIMESTAMP
        FROM seeds.seed_categories
        """
    )

    result = conn.execute(f"SELECT COUNT(*) FROM {CATEGORIES.full_name}").fetchone()
    count_after = result[0] if result else 0

    inserted = count_after - count_before
    logger.info("Seeded %d categories (%d total)", inserted, count_after)
    return inserted


def get_active_categories(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict[str, str | bool | None]]:
    """Get all active categories.

    Args:
        conn: DuckDB connection (read-only is fine).

    Returns:
        List of category dicts.
    """
    try:
        rows = conn.execute(
            f"""
            SELECT category_id, category, subcategory, description,
                   is_default, plaid_detailed
            FROM {CATEGORIES.full_name}
            WHERE is_active = true
            ORDER BY category, subcategory
            """
        ).fetchall()
    except duckdb.CatalogException:
        return []

    return [
        {
            "category_id": r[0],
            "category": r[1],
            "subcategory": r[2],
            "description": r[3],
            "is_default": r[4],
            "plaid_detailed": r[5],
        }
        for r in rows
    ]


def get_categorization_stats(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, int | float]:
    """Get summary statistics about categorization coverage.

    Args:
        conn: DuckDB connection (read-only is fine).

    Returns:
        Dict with total, categorized, uncategorized counts and
        breakdown by categorized_by source.
    """
    try:
        total_result = conn.execute(
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"
        ).fetchone()
        total = total_result[0] if total_result else 0
    except duckdb.CatalogException:
        return {"total": 0, "categorized": 0, "uncategorized": 0, "pct_categorized": 0}

    try:
        categorized_result = conn.execute(
            f"SELECT COUNT(*) FROM {TRANSACTION_CATEGORIES.full_name}"
        ).fetchone()
        categorized = categorized_result[0] if categorized_result else 0
    except duckdb.CatalogException:
        categorized = 0

    uncategorized = total - categorized
    pct = round((categorized / total * 100), 1) if total > 0 else 0.0

    stats: dict[str, int | float] = {
        "total": total,
        "categorized": categorized,
        "uncategorized": uncategorized,
        "pct_categorized": pct,
    }

    # Breakdown by source
    try:
        source_rows = conn.execute(
            f"""
            SELECT categorized_by, COUNT(*) AS cnt
            FROM {TRANSACTION_CATEGORIES.full_name}
            GROUP BY categorized_by
            ORDER BY cnt DESC
            """
        ).fetchall()
        for source, count in source_rows:
            stats[f"by_{source}"] = count
    except duckdb.CatalogException:
        pass

    return stats


def build_categorization_prompt(
    categories: list[dict[str, str | bool | None]],
    descriptions: list[str],
) -> str:
    """Build a prompt for LLM-based transaction categorization.

    Args:
        categories: Active category list from get_active_categories().
        descriptions: Unique normalized transaction descriptions to classify.

    Returns:
        Prompt string suitable for any LLM.
    """
    # Build compact taxonomy
    cat_lines: list[str] = []
    current_primary = ""
    for cat in categories:
        primary = str(cat["category"])
        sub = cat.get("subcategory")
        if primary != current_primary:
            current_primary = primary
            cat_lines.append(f"\n{primary}:")
        if sub:
            cat_lines.append(f"  - {sub}")

    taxonomy = "\n".join(cat_lines)

    # Build description list
    desc_list = "\n".join(f"- {d}" for d in descriptions)

    return (
        "Categorize these transaction descriptions. "
        "Reply with ONLY a JSON array, no other text.\n\n"
        f"Categories:\n{taxonomy}\n\n"
        f"Transactions:\n{desc_list}\n\n"
        "For each transaction, return:\n"
        '[{"description": "...", "category": "...", "subcategory": "...", '
        '"confidence": 0.0-1.0, "merchant_name": "..."}]\n\n'
        "Rules:\n"
        "- Use exact category/subcategory names from the list above\n"
        "- confidence: 1.0 = certain, 0.5 = guess\n"
        "- merchant_name: clean merchant name (e.g., 'Starbucks' not "
        "'SQ *STARBUCKS #1234 SEATTLE WA')\n"
        "- If unsure, use category 'Other' subcategory 'Uncategorized' "
        "with low confidence"
    )


def parse_categorization_response(
    response: str,
) -> list[dict[str, str | float]]:
    """Parse LLM response into categorization results.

    Handles common LLM response quirks: markdown code fences, trailing
    commas, extra text before/after JSON.

    Args:
        response: Raw LLM response text.

    Returns:
        List of parsed categorization dicts. Empty list if parsing fails.
    """
    import json

    # Strip markdown code fences
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```json or ```) and last line (```)
        lines = [line for line in lines if not line.strip().startswith("```")]
        text = "\n".join(lines)

    # Find JSON array in the text
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        logger.warning("No JSON array found in LLM response")
        return []

    json_text = text[start : end + 1]

    try:
        results = json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse LLM categorization response: %s", e)
        return []

    if not isinstance(results, list):
        logger.warning("LLM response is not a list")
        return []

    # Validate and normalize each result
    valid: list[dict[str, str | float]] = []
    for raw_item in results:
        if not isinstance(raw_item, dict):
            continue
        item: dict[str, object] = raw_item  # type: ignore[reportUnknownVariableType]
        if "description" not in item or "category" not in item:
            continue

        valid.append({
            "description": str(item["description"]),
            "category": str(item["category"]),
            "subcategory": str(item.get("subcategory", "")),
            "confidence": float(item.get("confidence", 0.5)),  # type: ignore[reportArgumentType]
            "merchant_name": str(item.get("merchant_name", "")),
        })

    return valid
