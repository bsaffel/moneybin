"""MCP write tool implementations for MoneyBin.

These tools allow the AI to modify data: importing files, categorizing
transactions, and managing budgets. All writes go through the service
layer with privacy validation.
"""

import json
import logging
import typing
import uuid
from pathlib import Path

import duckdb

from moneybin.database import get_database
from moneybin.services.categorization_service import (
    MatchType,
    create_merchant,
    match_merchant,
    normalize_description,
)
from moneybin.services.categorization_service import (
    seed_categories as seed_categories_svc,
)
from moneybin.services.import_service import import_file as run_import
from moneybin.tables import (
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
)

from .server import mcp, table_exists

logger = logging.getLogger(__name__)

_VALID_MATCH_TYPES: frozenset[MatchType] = frozenset(typing.get_args(MatchType))


def _validate_match_type(match_type: str) -> MatchType:
    """Validate and narrow a match_type string at the MCP boundary.

    Args:
        match_type: Raw string from MCP tool input.

    Returns:
        Validated MatchType literal.

    Raises:
        ValueError: If match_type is not one of the valid values.
    """
    if match_type not in _VALID_MATCH_TYPES:
        raise ValueError(
            f"Invalid match_type: '{match_type}'. "
            f"Must be one of: {', '.join(sorted(_VALID_MATCH_TYPES))}"
        )
    return match_type  # type: ignore[return-value]  # validated above


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------


@mcp.tool()
def import_file(
    file_path: str,
    account_id: str | None = None,
    account_name: str | None = None,
    institution: str | None = None,
    format_name: str | None = None,
) -> str:
    """Import a financial data file into MoneyBin.

    Supported formats (detected automatically by extension):
      - .ofx / .qfx — OFX/Quicken bank statements
      - .pdf        — W-2 tax forms
      - .csv / .tsv / .xlsx / .parquet / .feather — tabular transaction exports

    Tabular files (CSV, TSV, Excel, Parquet, Feather) go through a 5-stage pipeline:
      1. Format detection (encoding, delimiter, file type)
      2. File reading (header detection, trailing row removal)
      3. Column mapping (header aliases + content validation)
      4. Transform (date parsing, amount normalization, ID generation)
      5. Load (raw table write with import batch tracking)

    For single-account files, provide ``account_name`` (e.g. 'Chase Checking').
    Multi-account files (Tiller, Mint, etc.) are detected automatically.

    If auto-detection fails, use ``format_name`` to specify a known format
    (see ``list_formats``), or use the CLI with ``--override`` flags.

    Args:
        file_path: Absolute path to the file to import.
        account_id: Explicit account identifier (bypasses name matching).
        account_name: Account name for single-account tabular files.
        institution: Institution name (OFX only).
        format_name: Use a specific named format (bypass auto-detection).
    """
    file_name = Path(file_path).name
    logger.info(f"Tool called: import_file({file_name!r})")

    # Expand ~ and resolve to canonical path (collapses '..' and follows
    # symlinks), then verify the result stays within the user's home directory.
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.is_relative_to(Path.home()):
        return (
            "Error: file_path must be within the user's home directory. "
            "Path traversal and symlinks that escape the home directory are not allowed."
        )

    try:
        result = run_import(
            get_database(),
            str(resolved),
            account_id=account_id,
            account_name=account_name,
            institution=institution,
            format_name=format_name,
        )
        return result.summary()
    except FileNotFoundError as e:
        return f"Error: {e}"
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception(f"Import failed: {file_name!r}")
        return f"Import failed: {e}"


@mcp.tool()
def import_preview(file_path: str) -> str:
    """Preview a file's structure and detected column mapping without importing.

    Runs the first 3 stages of the tabular pipeline (detect, read, map)
    and returns format info, column mapping, and sample rows. Use this
    to understand an unknown file before importing.

    Args:
        file_path: Absolute path to the file to preview.
    """
    file_name = Path(file_path).name
    logger.info(f"Tool called: import_preview({file_name!r})")

    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import read_file

    resolved = Path(file_path).expanduser().resolve()
    if not resolved.is_relative_to(Path.home()):
        return (
            "Error: file_path must be within the user's home directory. "
            "Path traversal and symlinks that escape the home directory are not allowed."
        )

    try:
        # Stage 1: Format detection
        format_info = detect_format(resolved)

        # Stage 2: Read file (first rows only for preview)
        read_result = read_file(resolved, format_info)

        # Stage 3: Column mapping
        mapping_result = map_columns(read_result.df)

        preview: dict[str, object] = {
            "file": resolved.name,
            "format": {
                "file_type": format_info.file_type,
                "delimiter": format_info.delimiter,
                "encoding": format_info.encoding,
                "file_size_bytes": format_info.file_size,
            },
            "columns": {
                "mapping": mapping_result.field_mapping,
                "confidence": mapping_result.confidence,
                "date_format": mapping_result.date_format,
                "number_format": mapping_result.number_format,
                "sign_convention": mapping_result.sign_convention,
                "is_multi_account": mapping_result.is_multi_account,
                "unmapped_columns": mapping_result.unmapped_columns,
                "flagged_fields": mapping_result.flagged_fields,
            },
            "sample_values": mapping_result.sample_values,
            "rows_read": len(read_result.df),
            "rows_skipped_trailing": read_result.rows_skipped_trailing,
        }
        return json.dumps(preview, indent=2, default=str)
    except FileNotFoundError as e:
        return f"Error: {e}"
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception(f"Preview failed: {file_name!r}")
        return f"Preview failed: {e}"


@mcp.tool()
def import_history(limit: int = 20, import_id: str | None = None) -> str:
    """List past imports with batch details.

    Returns import ID, source file, status, row counts, and detection
    confidence for each completed import batch.

    Args:
        limit: Maximum number of records to return (default 20).
        import_id: Filter to a specific import ID for full details.
    """
    logger.info("Tool called: import_history")

    try:
        from moneybin.loaders.tabular_loader import TabularLoader

        db = get_database()
        loader = TabularLoader(db)
        records = loader.get_import_history(
            limit=min(limit, 200),
            import_id=import_id,
        )
        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("import_history failed")
        return json.dumps({"error": str(e)})


@mcp.tool()
def import_revert(import_id: str) -> str:
    """Undo an import batch — deletes all rows loaded in that batch.

    Removes transactions and accounts from the specified import batch
    and marks it as reverted in the import log.

    Args:
        import_id: The UUID of the import batch to revert.
    """
    logger.info(f"Tool called: import_revert({import_id})")

    try:
        from moneybin.loaders.tabular_loader import TabularLoader

        db = get_database()
        loader = TabularLoader(db)
        result = loader.revert_import(import_id)

        status = result.get("status")
        if status == "not_found":
            return f"Error: {result.get('reason', 'Import not found')}"
        if status == "already_reverted":
            return f"Import {import_id} was already reverted."
        if status == "superseded":
            return (
                f"Error: {result.get('reason', 'Import was superseded by a re-import')}"
            )
        rows_deleted = result.get("rows_deleted", 0)
        return f"Reverted import {import_id}: {rows_deleted} rows deleted."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception(f"import_revert failed: {import_id}")
        return f"Revert failed: {e}"


@mcp.tool()
def list_formats() -> str:
    """List all available tabular import formats (built-in + user-saved).

    Returns format name, institution, sign convention, and date format
    for each format. Use ``import_preview`` to test a format against
    a specific file.
    """
    logger.info("Tool called: list_formats")

    try:
        from moneybin.extractors.tabular.formats import load_builtin_formats

        formats = load_builtin_formats()
        format_list = [
            {
                "name": fmt.name,
                "institution_name": fmt.institution_name,
                "file_type": fmt.file_type,
                "sign_convention": fmt.sign_convention,
                "date_format": fmt.date_format,
                "number_format": fmt.number_format,
                "multi_account": fmt.multi_account,
                "header_signature": fmt.header_signature,
            }
            for fmt in sorted(formats.values(), key=lambda f: f.name)
        ]
        return json.dumps({"formats": format_list, "total": len(format_list)}, indent=2)
    except Exception as e:
        logger.exception("list_formats failed")
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Transaction categorization
# ---------------------------------------------------------------------------


@mcp.tool()
def categorize_transaction(
    transaction_id: str,
    category: str,
    subcategory: str | None = None,
    categorized_by: str = "user",
) -> str:
    """Assign a category to a transaction.

    Also auto-creates a merchant mapping if one doesn't exist for the
    transaction's description, so future similar transactions are
    categorized automatically.

    Args:
        transaction_id: The transaction ID to categorize.
        category: Category name (e.g. 'Food & Drink', 'Shopping').
        subcategory: Optional subcategory (e.g. 'Groceries', 'Restaurants').
        categorized_by: Who is categorizing: 'user' (default), 'ai', 'rule', 'plaid'.
    """
    logger.info(f"Tool called: categorize_transaction({transaction_id}, {category})")

    try:
        db = get_database()
        # Resolve merchant_id before inserting the category record
        merchant_id = None
        try:
            txn = db.execute(
                f"""
                SELECT description FROM {FCT_TRANSACTIONS.full_name}
                WHERE transaction_id = ?
                """,
                [transaction_id],
            ).fetchone()

            if txn and txn[0]:
                description = txn[0]
                existing = match_merchant(db, description)
                if existing:
                    merchant_id = existing["merchant_id"]
                else:
                    normalized = normalize_description(description)
                    if normalized:
                        merchant_id = create_merchant(
                            db,
                            normalized,
                            normalized,
                            match_type="contains",
                            category=category,
                            subcategory=subcategory,
                            created_by=categorized_by,
                        )
        except Exception:
            logger.debug(
                "Could not resolve merchant mapping",
                exc_info=True,
            )

        db.execute(
            f"""
            INSERT OR REPLACE INTO {TRANSACTION_CATEGORIES.full_name}
            (transaction_id, category, subcategory,
             categorized_at, categorized_by, merchant_id)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            """,
            [transaction_id, category, subcategory, categorized_by, merchant_id],
        )

        sub = f" / {subcategory}" if subcategory else ""
        return f"Transaction {transaction_id} categorized as: {category}{sub}"
    except Exception as e:
        logger.exception("Categorization failed")
        return f"Error categorizing transaction: {e}"


@mcp.tool()
def get_uncategorized_transactions(limit: int = 50) -> str:
    """Find transactions that have not been categorized yet.

    Args:
        limit: Maximum number of results (default 50).
    """
    logger.info("Tool called: get_uncategorized_transactions")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    db = get_database()
    limit = min(limit, 1000)

    try:
        result = db.execute(
            f"""
            SELECT t.transaction_id, t.transaction_date, t.amount,
                   t.description, t.memo, t.account_id
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            WHERE c.transaction_id IS NULL
            ORDER BY t.transaction_date DESC
            LIMIT ?
            """,
            [limit],
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]
        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Query failed")
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Category taxonomy management
# ---------------------------------------------------------------------------


@mcp.tool()
def seed_categories() -> str:
    """Initialize default categories from the Plaid PFCv2 taxonomy.

    Copies ~100 default categories from the SQLMesh seed table into
    user.categories. Safe to call multiple times — existing categories
    are not overwritten.

    Requires SQLMesh transforms to have been run at least once so the
    seeds.categories table exists.
    """
    logger.info("Tool called: seed_categories")

    try:
        db = get_database()
        count = seed_categories_svc(db)
        return f"Seeded {count} new categories."
    except Exception as e:
        logger.exception("Failed to seed categories")
        return f"Error seeding categories: {e}"


@mcp.tool()
def toggle_category(category_id: str, is_active: bool) -> str:
    """Enable or disable a category.

    Disabled categories are hidden from the taxonomy but existing
    categorizations using them are preserved.

    Args:
        category_id: The category ID to toggle (e.g. 'FND-COF').
        is_active: True to enable, False to disable.
    """
    logger.info(f"Tool called: toggle_category({category_id}, {is_active})")

    try:
        db = get_database()
        row = db.execute(
            f"""
            UPDATE {CATEGORIES.full_name}
            SET is_active = ?
            WHERE category_id = ?
            RETURNING category_id
            """,
            [is_active, category_id],
        ).fetchone()
        if row is not None:
            action = "enabled" if is_active else "disabled"
            return f"Category {category_id} {action}."
        return f"Category {category_id} not found."
    except Exception as e:
        logger.exception("Toggle category failed")
        return f"Error: {e}"


@mcp.tool()
def create_category(
    category: str,
    subcategory: str | None = None,
    description: str | None = None,
) -> str:
    """Create a custom category or subcategory.

    Args:
        category: Primary category name (e.g. 'Childcare').
        subcategory: Optional subcategory (e.g. 'Daycare', 'Babysitter').
        description: Optional description of this category.
    """
    logger.info(f"Tool called: create_category({category}, {subcategory})")

    cat_id = uuid.uuid4().hex[:12]

    try:
        db = get_database()
        db.execute(
            f"""
            INSERT INTO {CATEGORIES.full_name}
            (category_id, category, subcategory, description,
             is_default, is_active, created_at)
            VALUES (?, ?, ?, ?, false, true, CURRENT_TIMESTAMP)
            """,
            [cat_id, category, subcategory, description],
        )
        sub = f" / {subcategory}" if subcategory else ""
        return f"Created category: {category}{sub} (ID: {cat_id})"
    except duckdb.ConstraintException:
        sub = f" / {subcategory}" if subcategory else ""
        return f"Category already exists: {category}{sub}"
    except Exception as e:
        logger.exception("Create category failed")
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Merchant management
# ---------------------------------------------------------------------------


@mcp.tool()
def create_merchant_mapping(
    raw_pattern: str,
    canonical_name: str,
    match_type: str = "contains",
    category: str | None = None,
    subcategory: str | None = None,
) -> str:
    """Create a merchant name mapping and optional category default.

    Merchant mappings normalize messy transaction descriptions (e.g.
    'SQ *STARBUCKS #1234 SEATTLE WA' -> 'Starbucks') and cache the
    merchant-to-category association for future auto-categorization.

    Args:
        raw_pattern: Pattern to match in descriptions.
        canonical_name: Clean merchant name for display.
        match_type: How to match: 'exact', 'contains' (default), or 'regex'.
        category: Optional default category for this merchant.
        subcategory: Optional default subcategory.
    """
    logger.info(f"Tool called: create_merchant_mapping({canonical_name})")

    try:
        validated_match_type = _validate_match_type(match_type)
    except ValueError as e:
        return f"Error: {e}"

    try:
        db = get_database()
        merchant_id = create_merchant(
            db,
            raw_pattern,
            canonical_name,
            match_type=validated_match_type,
            category=category,
            subcategory=subcategory,
            created_by="user",
        )
        cat_info = f" -> {category}" if category else ""
        return f"Created merchant: {canonical_name} (pattern: '{raw_pattern}'{cat_info}, ID: {merchant_id})"
    except Exception as e:
        logger.exception("Create merchant failed")
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Categorization rules
# ---------------------------------------------------------------------------


@mcp.tool()
def create_categorization_rule(
    name: str,
    merchant_pattern: str,
    category: str,
    subcategory: str | None = None,
    match_type: str = "contains",
    min_amount: float | None = None,
    max_amount: float | None = None,
    account_id: str | None = None,
    priority: int = 100,
) -> str:
    """Create a rule for automatic transaction categorization.

    Rules match transactions by description pattern and optional conditions
    (amount range, account). They are applied in priority order during import.

    Args:
        name: Human-readable rule name (e.g. 'Starbucks -> Coffee').
        merchant_pattern: Pattern to match in transaction descriptions.
        category: Category to assign (e.g. 'Food & Drink').
        subcategory: Subcategory to assign (e.g. 'Coffee Shops').
        match_type: How to match: 'contains' (default), 'exact', or 'regex'.
        min_amount: Optional minimum amount filter (use negative for expenses).
        max_amount: Optional maximum amount filter (use negative for expenses).
        account_id: Optional account ID filter.
        priority: Rule priority (lower = higher priority, default 100).
    """
    logger.info(f"Tool called: create_categorization_rule({name})")

    try:
        validated_match_type = _validate_match_type(match_type)
    except ValueError as e:
        return f"Error: {e}"

    rule_id = uuid.uuid4().hex[:12]

    try:
        db = get_database()
        db.execute(
            f"""
            INSERT INTO {CATEGORIZATION_RULES.full_name}
            (rule_id, name, merchant_pattern, match_type,
             min_amount, max_amount, account_id,
             category, subcategory, priority, is_active,
             created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true,
                    'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
                rule_id,
                name,
                merchant_pattern,
                validated_match_type,
                min_amount,
                max_amount,
                account_id,
                category,
                subcategory,
                priority,
            ],
        )
        return f"Created rule '{name}' (ID: {rule_id}, priority: {priority})"
    except Exception as e:
        logger.exception("Create rule failed")
        return f"Error: {e}"


@mcp.tool()
def delete_categorization_rule(rule_id: str) -> str:
    """Delete a categorization rule.

    Args:
        rule_id: The rule ID to delete.
    """
    logger.info(f"Tool called: delete_categorization_rule({rule_id})")

    try:
        db = get_database()
        db.execute(
            f"""
            DELETE FROM {CATEGORIZATION_RULES.full_name}
            WHERE rule_id = ?
            """,
            [rule_id],
        )
        return f"Deleted rule {rule_id}."
    except Exception as e:
        logger.exception("Delete rule failed")
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Bulk categorization
# ---------------------------------------------------------------------------


@mcp.tool()
def bulk_categorize(
    categorizations: list[dict[str, str]],
    create_merchant_mappings: bool = True,
) -> str:
    """Apply categorizations to multiple transactions in a single call.

    Use this after calling get_uncategorized_transactions: review the list,
    decide categories for each, then submit them all here in one call.
    High-confidence assignments also create merchant mappings so future
    imports are categorized automatically by the rule engine.

    Args:
        categorizations: List of dicts, each with:
            - transaction_id: the transaction to categorize
            - category: category name (e.g. 'Food & Drink')
            - subcategory: optional subcategory (e.g. 'Coffee Shops')
            - merchant_name: optional clean merchant name for mapping
        create_merchant_mappings: If True (default), auto-create merchant
            mappings from transaction descriptions so future similar
            transactions are categorized automatically.
    """
    logger.info(f"Tool called: bulk_categorize({len(categorizations)} items)")

    if not categorizations:
        return "No categorizations provided."

    categorized_count = 0
    merchant_count = 0
    errors: list[str] = []

    try:
        db = get_database()
        for item in categorizations:
            txn_id = item.get("transaction_id", "").strip()
            category = item.get("category", "").strip()
            if not txn_id or not category:
                errors.append(
                    f"Skipped item missing transaction_id or category: {item}"
                )
                continue

            subcategory = item.get("subcategory", "").strip() or None
            merchant_name = item.get("merchant_name", "").strip() or None

            # Resolve merchant_id before inserting
            merchant_id = None
            try:
                txn = db.execute(
                    f"""
                    SELECT description FROM {FCT_TRANSACTIONS.full_name}
                    WHERE transaction_id = ?
                    """,
                    [txn_id],
                ).fetchone()
                if txn and txn[0]:
                    existing = match_merchant(db, txn[0])
                    if existing:
                        merchant_id = existing["merchant_id"]
                    elif create_merchant_mappings and merchant_name:
                        normalized = normalize_description(txn[0])
                        if normalized:
                            merchant_id = create_merchant(
                                db,
                                normalized,
                                merchant_name,
                                match_type="contains",
                                category=category,
                                subcategory=subcategory,
                                created_by="ai",
                            )
                            merchant_count += 1
            except Exception:
                logger.debug(
                    f"Could not resolve merchant mapping for {txn_id}",
                    exc_info=True,
                )

            db.execute(
                f"""
                INSERT OR REPLACE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory,
                 categorized_at, categorized_by, merchant_id)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'ai', ?)
                """,
                [txn_id, category, subcategory, merchant_id],
            )
            categorized_count += 1

    except Exception as e:
        logger.exception("bulk_categorize failed")
        return f"Error: {e}"

    summary = f"Categorized {categorized_count} transactions"
    if merchant_count:
        summary += f", created {merchant_count} merchant mappings"
    if errors:
        summary += "\nWarnings:\n" + "\n".join(errors)
    return summary + "."


# ---------------------------------------------------------------------------
# Bulk rule & merchant creation
# ---------------------------------------------------------------------------


@mcp.tool()
def bulk_create_categorization_rules(
    rules: list[dict[str, str | float | int | None]],
) -> str:
    """Create multiple categorization rules in a single call.

    Use this after reviewing uncategorized transactions to set up rules
    that will automatically categorize future imports. Each rule matches
    transactions by description pattern and assigns a category.

    Args:
        rules: List of dicts, each with:
            - name: Human-readable rule name (e.g. 'Starbucks -> Coffee')
            - merchant_pattern: Pattern to match in descriptions
            - category: Category to assign
            - subcategory: optional subcategory
            - match_type: 'contains' (default), 'exact', or 'regex'
            - min_amount: optional minimum amount filter
            - max_amount: optional maximum amount filter
            - account_id: optional account ID filter
            - priority: rule priority (default 100, lower = higher priority)
    """
    logger.info(f"Tool called: bulk_create_categorization_rules({len(rules)} items)")

    if not rules:
        return "No rules provided."

    created_count = 0
    errors: list[str] = []

    try:
        db = get_database()
        for item in rules:
            name = str(item.get("name", "")).strip()
            pattern = str(item.get("merchant_pattern", "")).strip()
            category = str(item.get("category", "")).strip()
            if not name or not pattern or not category:
                errors.append(
                    f"Skipped rule missing name, merchant_pattern, or category: {item}"
                )
                continue

            subcategory = str(item.get("subcategory", "")).strip() or None
            raw_match_type = str(item.get("match_type", "contains")).strip()
            try:
                match_type = _validate_match_type(raw_match_type)
            except ValueError:
                errors.append(
                    f"Skipped rule with invalid match_type '{raw_match_type}': {item}"
                )
                continue
            min_amount = item.get("min_amount")
            max_amount = item.get("max_amount")
            account_id = item.get("account_id")
            priority = int(item.get("priority", 100) or 100)

            rule_id = uuid.uuid4().hex[:12]
            db.execute(
                f"""
                INSERT INTO {CATEGORIZATION_RULES.full_name}
                (rule_id, name, merchant_pattern, match_type,
                 min_amount, max_amount, account_id,
                 category, subcategory, priority, is_active,
                 created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true,
                        'ai', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    rule_id,
                    name,
                    pattern,
                    match_type,
                    min_amount,
                    max_amount,
                    account_id,
                    category,
                    subcategory,
                    priority,
                ],
            )
            created_count += 1

    except Exception as e:
        logger.exception("bulk_create_categorization_rules failed")
        return f"Error: {e}"

    summary = f"Created {created_count} categorization rules"
    if errors:
        summary += "\nWarnings:\n" + "\n".join(errors)
    return summary + "."


@mcp.tool()
def bulk_create_merchant_mappings(
    mappings: list[dict[str, str | None]],
) -> str:
    """Create multiple merchant name mappings in a single call.

    Merchant mappings normalize messy transaction descriptions and cache
    the merchant-to-category association for automatic categorization of
    future imports. Use this after reviewing transactions to set up
    merchant recognition in bulk.

    Args:
        mappings: List of dicts, each with:
            - raw_pattern: Pattern to match in descriptions
            - canonical_name: Clean merchant name for display
            - match_type: 'contains' (default), 'exact', or 'regex'
            - category: optional default category
            - subcategory: optional default subcategory
    """
    logger.info(f"Tool called: bulk_create_merchant_mappings({len(mappings)} items)")

    if not mappings:
        return "No mappings provided."

    created_count = 0
    errors: list[str] = []

    try:
        db = get_database()
        for item in mappings:
            raw_pattern = str(item.get("raw_pattern", "")).strip()
            canonical_name = str(item.get("canonical_name", "")).strip()
            if not raw_pattern or not canonical_name:
                errors.append(
                    f"Skipped mapping missing raw_pattern or canonical_name: {item}"
                )
                continue

            raw_match_type = str(item.get("match_type", "contains")).strip()
            try:
                match_type = _validate_match_type(raw_match_type)
            except ValueError:
                errors.append(
                    f"Skipped mapping with invalid match_type '{raw_match_type}': {item}"
                )
                continue
            category = str(item.get("category", "")).strip() or None
            subcategory = str(item.get("subcategory", "")).strip() or None

            try:
                create_merchant(
                    db,
                    raw_pattern,
                    canonical_name,
                    match_type=match_type,
                    category=category,
                    subcategory=subcategory,
                    created_by="ai",
                )
                created_count += 1
            except Exception as e:
                errors.append(f"Failed to create mapping '{canonical_name}': {e}")

    except Exception as e:
        logger.exception("bulk_create_merchant_mappings failed")
        return f"Error: {e}"

    summary = f"Created {created_count} merchant mappings"
    if errors:
        summary += "\nWarnings:\n" + "\n".join(errors)
    return summary + "."


# ---------------------------------------------------------------------------
# Budget management
# ---------------------------------------------------------------------------


@mcp.tool()
def set_budget(
    category: str,
    monthly_amount: float,
    start_month: str | None = None,
) -> str:
    """Create or update a monthly budget for a category.

    Args:
        category: Budget category (should match transaction categories).
        monthly_amount: Monthly budget amount in dollars.
        start_month: Starting month (YYYY-MM). Defaults to current month.
    """
    logger.info(f"Tool called: set_budget({category})")

    db = get_database()

    if start_month is None:
        start_month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index] — fetchone() returns a row here, not None

    try:
        # Check if budget already exists for this category
        existing = db.execute(
            """
            SELECT budget_id FROM app.budgets
            WHERE category = ? AND (end_month IS NULL OR end_month >= ?)
            """,
            [category, start_month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of mixed types
        ).fetchone()

        if existing:
            db.execute(
                """
                UPDATE app.budgets
                SET monthly_amount = ?, updated_at = CURRENT_TIMESTAMP
                WHERE budget_id = ?
                """,
                [monthly_amount, existing[0]],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of mixed types
            )
            return f"Updated budget for '{category}': ${monthly_amount:.2f}/month"
        else:
            budget_id = uuid.uuid4().hex[:12]
            db.execute(
                """
                INSERT INTO app.budgets
                (budget_id, category, monthly_amount, start_month, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [budget_id, category, monthly_amount, start_month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of mixed types
            )
            return f"Created budget for '{category}': ${monthly_amount:.2f}/month starting {start_month}"
    except Exception as e:
        logger.exception("Budget operation failed")
        return f"Error setting budget: {e}"


@mcp.tool()
def get_budget_status(month: str | None = None) -> str:
    """Get budget vs actual spending comparison for a month.

    Args:
        month: Month to check (YYYY-MM). Defaults to current month.
    """
    logger.info("Tool called: get_budget_status")
    db = get_database()

    if not table_exists(BUDGETS):
        return "No budgets set yet. Use set_budget to create one."

    if month is None:
        month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index] — fetchone() returns a row here, not None

    try:
        result = db.execute(
            f"""
            WITH spending AS (
                SELECT
                    c.category,
                    SUM(ABS(t.amount)) AS total_spent
                FROM {FCT_TRANSACTIONS.full_name} t
                JOIN app.transaction_categories c
                    ON t.transaction_id = c.transaction_id
                WHERE t.transaction_year_month = ?
                    AND t.amount < 0
                GROUP BY c.category
            )
            SELECT
                b.category,
                b.monthly_amount AS budget,
                COALESCE(s.total_spent, 0) AS spent,
                b.monthly_amount - COALESCE(s.total_spent, 0) AS remaining,
                CASE
                    WHEN COALESCE(s.total_spent, 0) > b.monthly_amount THEN 'OVER'
                    WHEN COALESCE(s.total_spent, 0) > b.monthly_amount * 0.9 THEN 'WARNING'
                    ELSE 'OK'
                END AS status
            FROM app.budgets b
            LEFT JOIN spending s ON b.category = s.category
            WHERE b.start_month <= ?
                AND (b.end_month IS NULL OR b.end_month >= ?)
            ORDER BY b.category
            """,
            [month, month, month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of str params
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]

        if not records:
            return f"No active budgets found for {month}."

        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Budget status query failed")
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Analytical tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_monthly_summary(months: int = 6) -> str:
    """Get income vs expenses summary by month.

    Args:
        months: Number of recent months to include (default 6).
    """
    logger.info("Tool called: get_monthly_summary")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    db = get_database()

    try:
        result = db.execute(
            f"""
            SELECT
                transaction_year_month,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS expenses,
                SUM(amount) AS net,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name}
            GROUP BY transaction_year_month
            ORDER BY transaction_year_month DESC
            LIMIT ?
            """,
            [months],
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]
        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Monthly summary query failed")
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_spending_by_category(month: str | None = None) -> str:
    """Get spending breakdown by category for a month.

    Requires transactions to be categorized first using categorize_transaction.

    Args:
        month: Month to analyze (YYYY-MM). Defaults to current month.
    """
    logger.info("Tool called: get_spending_by_category")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    if not table_exists(TRANSACTION_CATEGORIES):
        return "No categorized transactions. Use categorize_transaction first."

    db = get_database()

    if month is None:
        month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index] — fetchone() returns a row here, not None

    try:
        result = db.execute(
            f"""
            SELECT
                c.category,
                c.subcategory,
                SUM(ABS(t.amount)) AS total_spent,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name} t
            JOIN app.transaction_categories c
                ON t.transaction_id = c.transaction_id
            WHERE t.transaction_year_month = ?
                AND t.amount < 0
            GROUP BY c.category, c.subcategory
            ORDER BY total_spent DESC
            """,
            [month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of str params
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]

        if not records:
            return f"No categorized spending found for {month}."

        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Spending by category query failed")
        return json.dumps({"error": str(e)})


@mcp.tool()
def find_recurring_transactions(min_occurrences: int = 3) -> str:
    """Identify recurring transactions like subscriptions and regular charges.

    Looks for transactions with the same payee and similar amounts that
    appear multiple times.

    Args:
        min_occurrences: Minimum number of times a transaction must appear (default 3).
    """
    logger.info("Tool called: find_recurring_transactions")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    db = get_database()

    try:
        result = db.execute(
            f"""
            WITH payee_groups AS (
                SELECT
                    description,
                    ROUND(ABS(amount), 0) AS rounded_amount,
                    COUNT(*) AS occurrence_count,
                    MIN(transaction_date) AS first_seen,
                    MAX(transaction_date) AS last_seen,
                    ROUND(AVG(amount), 2) AS avg_amount
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE amount < 0
                    AND description IS NOT NULL
                    AND description != ''
                GROUP BY description, ROUND(ABS(amount), 0)
                HAVING COUNT(*) >= ?
            )
            SELECT
                description,
                avg_amount,
                occurrence_count,
                first_seen,
                last_seen
            FROM payee_groups
            ORDER BY occurrence_count DESC, avg_amount DESC
            LIMIT 50
            """,
            [min_occurrences],
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]

        if not records:
            return (
                f"No recurring transactions found with {min_occurrences}+ occurrences."
            )

        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Recurring transactions query failed")
        return json.dumps({"error": str(e)})
