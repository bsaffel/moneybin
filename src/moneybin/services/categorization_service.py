"""Transaction categorization service.

Handles merchant normalization, rule-based categorization, merchant matching,
and taxonomy management. Designed for deterministic operations — LLM-based
auto-categorization lives in the MCP layer (auto_categorize tool).

The public API is the ``CategorizationService`` class. The companion
``AutoRuleService`` (``auto_rule_service.py``) handles the auto-rule
proposal/approval/deactivation lifecycle and depends on this module's
``find_matching_rule`` and ``normalize_description``.
"""

import logging
import re
import uuid
from dataclasses import dataclass
from typing import Any, Literal

import duckdb

from moneybin.database import Database, sqlmesh_context
from moneybin.mcp.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import (
    CATEGORIES,
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    SEED_CATEGORIES,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)

MatchType = Literal["exact", "contains", "regex"]


@dataclass(slots=True)
class CategorizationStats:
    """Typed result for categorization statistics."""

    total: int
    categorized: int
    uncategorized: int
    percent_categorized: float
    by_source: dict[str, int]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this categorization stats result."""
        data: dict[str, Any] = {
            "total_transactions": self.total,
            "categorized": self.categorized,
            "uncategorized": self.uncategorized,
            "percent_categorized": self.percent_categorized,
            "by_source": self.by_source,
        }
        return build_envelope(
            data=data,
            sensitivity="low",
            actions=["Use categorize.uncategorized to see uncategorized transactions"],
        )


@dataclass(slots=True)
class BulkCategorizationResult:
    """Typed result for bulk categorization operations."""

    applied: int
    skipped: int
    errors: int
    error_details: list[dict[str, str]]
    merchants_created: int = 0

    def to_envelope(self, input_count: int) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this bulk categorization result."""
        return build_envelope(
            data={
                "applied": self.applied,
                "skipped": self.skipped,
                "errors": self.errors,
                "error_details": self.error_details,
                "merchants_created": self.merchants_created,
            },
            sensitivity="medium",
            total_count=input_count,
            actions=[
                "Use categorize.rules to review auto-created rules",
                "Use categorize.uncategorized to fetch the next batch",
            ],
        )


@dataclass(slots=True)
class SeedResult:
    """Typed result for category seeding."""

    seeded_count: int

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this seed result."""
        return build_envelope(
            data={"seeded_count": self.seeded_count},
            sensitivity="low",
        )


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


def matches_pattern(text: str, pattern: str, match_type: str) -> bool:
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
            logger.warning("Invalid regex pattern in merchant rule")
            return False
    else:
        logger.warning(f"Unknown match_type: {match_type}")
        return False


def _fetch_merchants(
    db: Database,
) -> list[tuple[str, str, str, str, str, str | None]] | None:
    """Fetch all merchant mappings ordered by match priority.

    Args:
        db: Database instance (read-only access is sufficient).

    Returns:
        List of merchant rows, or None if the table doesn't exist.
    """
    try:
        return db.execute(
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


def _match_description(
    description: str,
    merchants: list[tuple[str, str, str, str, str, str | None]],
) -> dict[str, str | None] | None:
    """Match a description against a pre-fetched merchant list.

    Args:
        description: Transaction description to match.
        merchants: Pre-fetched merchant rows from ``_fetch_merchants()``.

    Returns:
        Dict with merchant_id, canonical_name, category, subcategory
        if found, otherwise None.
    """
    normalized = normalize_description(description)
    if not normalized:
        return None

    for row in merchants:
        merchant_id, raw_pattern, match_type, canonical_name, category, subcategory = (
            row
        )
        if matches_pattern(description, raw_pattern, match_type) or matches_pattern(
            normalized, raw_pattern, match_type
        ):
            return {
                "merchant_id": merchant_id,
                "canonical_name": canonical_name,
                "category": category,
                "subcategory": subcategory,
            }

    return None


class CategorizationService:
    """Canonical categorization surface — merchants, rules, taxonomy, auto-rules.

    All categorization operations route through this class. The MCP tools, CLI
    commands, and import service share this single entry point so caller-visible
    behavior is consistent across surfaces.
    """

    def __init__(self, db: Database) -> None:
        """Bind the service to a database connection."""
        self._db = db

    # -- Merchant lookup / management --

    def match_merchant(self, description: str) -> dict[str, str | None] | None:
        """Look up a merchant by raw description.

        Args:
            description: Transaction description to match.

        Returns:
            Dict with merchant_id, canonical_name, category, subcategory
            if found, otherwise None.
        """
        merchants = _fetch_merchants(self._db)
        if merchants is None:
            return None
        return _match_description(description, merchants)

    def create_merchant(
        self,
        raw_pattern: str,
        canonical_name: str,
        *,
        match_type: MatchType = "contains",
        category: str | None = None,
        subcategory: str | None = None,
        created_by: str = "ai",
    ) -> str:
        """Create a merchant mapping.

        Args:
            raw_pattern: Pattern to match in transaction descriptions.
            canonical_name: Clean merchant name for display.
            match_type: How to match: 'exact', 'contains', or 'regex'.
            category: Optional default category for this merchant.
            subcategory: Optional default subcategory.
            created_by: Who created the mapping ('user', 'ai', 'rule').

        Returns:
            The merchant_id of the created merchant.
        """
        merchant_id = uuid.uuid4().hex[:12]
        self._db.execute(
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
        logger.info(f"Created merchant mapping {merchant_id}")
        return merchant_id

    # -- Categorization core --

    def bulk_categorize(self, items: list[dict[str, str]]) -> BulkCategorizationResult:
        """Assign categories to multiple transactions with merchant auto-creation.

        For each item, looks up the transaction description, resolves or creates
        a merchant mapping, then inserts/replaces the category assignment.
        Merchant resolution is best-effort — failures do not prevent categorization.

        Read-side cost is O(1) in the number of items: one batch description
        fetch and one merchant-table fetch, regardless of input size.

        Args:
            items: List of dicts with transaction_id, category, and optional subcategory.

        Returns:
            BulkCategorizationResult with applied/skipped/error counts.
        """
        applied = 0
        skipped = 0
        errors = 0
        merchants_created = 0
        error_details: list[dict[str, str]] = []

        # Phase 1 — validate and partition input
        valid_items: list[tuple[str, str, str | None]] = []
        for item in items:
            txn_id = item.get("transaction_id", "").strip()
            category = item.get("category", "").strip()
            if not txn_id or not category:
                skipped += 1
                error_details.append({
                    "transaction_id": txn_id or "(missing)",
                    "reason": "Missing transaction_id or category",
                })
                continue
            subcategory = item.get("subcategory", "").strip() or None
            valid_items.append((txn_id, category, subcategory))

        if not valid_items:
            return BulkCategorizationResult(
                applied=applied,
                skipped=skipped,
                errors=errors,
                error_details=error_details,
                merchants_created=merchants_created,
            )

        # Phase 2 — batch-fetch descriptions
        txn_ids = [v[0] for v in valid_items]
        placeholders = ",".join(["?"] * len(txn_ids))
        descriptions: dict[str, str | None] = {}
        try:
            rows = self._db.execute(
                f"""
                SELECT transaction_id, description
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE transaction_id IN ({placeholders})
                """,  # noqa: S608 — FCT_TRANSACTIONS is a compile-time TableRef constant; values are parameterized
                txn_ids,
            ).fetchall()
            descriptions = {row[0]: row[1] for row in rows}
        except Exception:  # noqa: BLE001 — best-effort; degrades to no merchant resolution
            logger.warning("Could not batch-fetch descriptions", exc_info=True)

        # Phase 3 — fetch merchants once, then match in memory.
        # Guard against any non-CatalogException (schema drift, binder errors, etc.)
        # so a merchant-table failure doesn't block all category writes for the batch.
        try:
            cached_merchants = _fetch_merchants(self._db)
        except Exception:  # noqa: BLE001 — best-effort; degrades to no merchant resolution
            logger.warning("Could not batch-fetch merchants", exc_info=True)
            cached_merchants = None

        # Phase 4 — per-item categorization (writes only)
        for txn_id, category, subcategory in valid_items:
            try:
                # Record before merchant resolution: bulk_categorize creates a merchant
                # mapping below that would otherwise short-circuit auto-rule proposals.
                # Lazy import keeps the module-level dependency one-way
                # (auto_rule_service → categorization_service).
                try:
                    from moneybin.services.auto_rule_service import AutoRuleService

                    AutoRuleService(self._db).record_categorization(
                        txn_id, category, subcategory=subcategory
                    )
                except Exception:  # noqa: BLE001 — auto-rule learning is best-effort
                    logger.warning("auto-rule recording failed", exc_info=True)

                merchant_id: str | None = None
                description = descriptions.get(txn_id)
                if description and cached_merchants is not None:
                    try:
                        existing = _match_description(description, cached_merchants)
                        if existing:
                            merchant_id = existing["merchant_id"]
                        else:
                            normalized = normalize_description(description)
                            if normalized:
                                merchant_id = self.create_merchant(
                                    normalized,
                                    normalized,
                                    match_type="contains",
                                    category=category,
                                    subcategory=subcategory,
                                    created_by="ai",
                                )
                                merchants_created += 1
                                # Insert into cache preserving _fetch_merchants() ordering
                                # (exact → contains → regex) so subsequent items in this
                                # batch match the just-created contains rule before any
                                # pre-existing regex rule.
                                new_row = (
                                    merchant_id,
                                    normalized,
                                    "contains",
                                    normalized,
                                    category,
                                    subcategory,
                                )
                                insert_at = next(
                                    (
                                        i
                                        for i, m in enumerate(cached_merchants)
                                        if m[2] == "regex"
                                    ),
                                    len(cached_merchants),
                                )
                                cached_merchants.insert(insert_at, new_row)
                    except Exception:  # noqa: BLE001 — merchant resolution is best-effort; categorization proceeds without it
                        logger.debug(
                            f"Could not resolve merchant for {txn_id}",
                            exc_info=True,
                        )

                self._db.execute(
                    f"""
                    INSERT OR REPLACE INTO {TRANSACTION_CATEGORIES.full_name}
                    (transaction_id, category, subcategory,
                     categorized_at, categorized_by, merchant_id)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'ai', ?)
                    """,
                    [txn_id, category, subcategory, merchant_id],
                )
                applied += 1
            except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
                errors += 1
                logger.exception(f"bulk_categorize failed for transaction {txn_id!r}")
                error_details.append({
                    "transaction_id": txn_id,
                    "reason": "Failed to apply category — check logs for details.",
                })

        # Best-effort override check: deactivates auto-rules whose categories
        # have been corrected past the configured threshold. Runs once per
        # batch so cost is independent of batch size.
        if applied:
            try:
                from moneybin.services.auto_rule_service import AutoRuleService

                AutoRuleService(self._db).check_overrides()
            except Exception:  # noqa: BLE001 — override check is best-effort
                logger.debug("auto-rule override check failed", exc_info=True)

        return BulkCategorizationResult(
            applied=applied,
            skipped=skipped,
            errors=errors,
            error_details=error_details,
            merchants_created=merchants_created,
        )

    def apply_merchant_categories(self) -> int:
        """Apply merchant-based categories to uncategorized transactions.

        Fetches all merchants once, then matches each uncategorized transaction
        in Python — avoids a per-transaction DB query.

        Returns:
            Number of transactions categorized.
        """
        merchants = _fetch_merchants(self._db)
        if not merchants:
            return 0

        try:
            uncategorized = self._db.execute(
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
            merchant = _match_description(description, merchants)
            if merchant and merchant.get("category"):
                self._db.execute(
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
            logger.info(
                f"Merchant matching categorized {categorized_count} transactions"
            )
        return categorized_count

    def _fetch_active_rules(self) -> list[tuple[Any, ...]]:
        """Return all active rules in priority order (priority ASC, created_at ASC)."""
        try:
            return self._db.execute(
                f"""
                SELECT rule_id, merchant_pattern, match_type,
                       min_amount, max_amount, account_id,
                       category, subcategory, created_by
                FROM {CATEGORIZATION_RULES.full_name}
                WHERE is_active = true
                ORDER BY priority ASC, created_at ASC
                """
            ).fetchall()
        except duckdb.CatalogException:
            return []

    @staticmethod
    def _match_first_rule(
        rules: list[tuple[Any, ...]],
        description: str,
        amount: float | None,
        account_id: str | None,
    ) -> tuple[str, str, str | None, str] | None:
        """Return ``(rule_id, category, subcategory, created_by)`` for the first rule that matches.

        Evaluates pattern (against both raw and normalized description),
        amount bounds, and account filter. Mirrors the rule engine semantics
        so callers can ask "would any rule match this transaction?" without
        duplicating logic. Returns ``None`` when no rule matches.
        """
        normalized = normalize_description(description)
        for rule in rules:
            (
                rule_id,
                pattern,
                match_type,
                min_amount,
                max_amount,
                rule_account_id,
                category,
                subcategory,
                created_by,
            ) = rule
            if not (
                matches_pattern(description, pattern, match_type)
                or matches_pattern(normalized, pattern, match_type)
            ):
                continue
            if (
                min_amount is not None
                and amount is not None
                and amount < float(min_amount)
            ):
                continue
            if (
                max_amount is not None
                and amount is not None
                and amount > float(max_amount)
            ):
                continue
            if rule_account_id is not None and account_id != rule_account_id:
                continue
            return rule_id, category, subcategory, created_by
        return None

    def find_matching_rule(
        self, transaction_id: str
    ) -> tuple[str, str, str | None, str] | None:
        """Return the first active rule matching this transaction, or ``None``.

        Result tuple is ``(rule_id, category, subcategory, created_by)``.
        Single-transaction variant of :meth:`apply_rules`; lets callers (e.g.,
        the auto-rule proposal pipeline) ask "is this transaction already
        covered by an existing rule?" using the canonical match semantics
        instead of re-implementing them.
        """
        try:
            txn_row = self._db.execute(
                f"SELECT description, amount, account_id "
                f"FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
                [transaction_id],
            ).fetchone()
        except duckdb.CatalogException:
            return None
        if not txn_row or not txn_row[0]:
            return None
        description, amount, account_id = txn_row
        rules = self._fetch_active_rules()
        if not rules:
            return None
        return self._match_first_rule(
            rules,
            str(description),
            float(amount) if amount is not None else None,
            str(account_id) if account_id is not None else None,
        )

    def apply_rules(self) -> int:
        """Apply active categorization rules to uncategorized transactions.

        Runs before merchant mapping in :meth:`apply_deterministic` so that
        explicit rules take priority. Rules are evaluated in priority order
        (lower number = higher priority); the first matching rule wins. Rules
        can filter by merchant pattern, amount range, and account ID.

        Provenance: when the matched rule was created by the auto-rule
        pipeline (``created_by='auto_rule'``), the resulting categorization
        is written with ``categorized_by='auto_rule'`` so downstream stats
        can identify auto-rule-driven assignments without joining through
        ``rule_id``. All other rules write ``categorized_by='rule'``.

        Returns:
            Number of transactions categorized.
        """
        rules = self._fetch_active_rules()
        if not rules:
            return 0

        try:
            uncategorized = self._db.execute(
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
            match = self._match_first_rule(
                rules,
                str(description),
                float(amount) if amount is not None else None,
                str(account_id) if account_id is not None else None,
            )
            if match is None:
                continue
            rule_id, category, subcategory, created_by = match
            categorized_by = "auto_rule" if created_by == "auto_rule" else "rule"
            self._db.execute(
                f"""
                INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory, categorized_at,
                 categorized_by, rule_id, confidence)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, 1.0)
                """,
                [txn_id, category, subcategory, categorized_by, rule_id],
            )
            categorized_count += 1

        if categorized_count:
            logger.info(f"Rule engine categorized {categorized_count} transactions")
        return categorized_count

    def apply_deterministic(self) -> dict[str, int]:
        """Run all deterministic categorization: rules first, then merchant fallback.

        Rules run first in priority order so explicit user-defined rules (which can
        filter by amount, account, and pattern) take precedence over generic merchant
        mappings. Merchant mappings apply only to transactions not matched by any rule.

        Returns:
            Dict with counts: {'merchant': N, 'rule': N, 'total': N}.
        """
        rule_count = self.apply_rules()
        merchant_count = self.apply_merchant_categories()
        total = merchant_count + rule_count

        if total:
            logger.info(
                f"Deterministic categorization: {merchant_count} merchant, "
                f"{rule_count} rule, {total} total"
            )

        return {
            "merchant": merchant_count,
            "rule": rule_count,
            "total": total,
        }

    # -- Taxonomy / seed --

    def ensure_seed_table(self) -> None:
        """Materialize the SQLMesh seed table if it doesn't exist yet.

        Runs a targeted ``sqlmesh plan --auto-apply`` scoped to just the
        ``seeds.categories`` model so the MCP server works without a
        prior CLI invocation of ``sqlmesh apply``.
        """
        result = self._db.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [SEED_CATEGORIES.schema, SEED_CATEGORIES.name],
        ).fetchone()
        if result and result[0] > 0:
            return  # already exists

        logger.info("Seed table missing — running targeted SQLMesh apply")

        with sqlmesh_context() as ctx:
            ctx.plan(
                auto_apply=True,
                no_prompts=True,
                select_models=[SEED_CATEGORIES.full_name],
            )
        logger.info("SQLMesh seed apply completed")

    def seed(self) -> int:
        """Populate ``app.categories`` from the SQLMesh seed table.

        If the seed table does not yet exist, a targeted SQLMesh apply is
        run automatically to materialize it. Skips any categories that
        already exist. Safe to run multiple times.

        Returns:
            Number of categories inserted.
        """
        self.ensure_seed_table()

        count_before = 0
        try:
            result = self._db.execute(
                f"SELECT COUNT(*) FROM {CATEGORIES.full_name}"
            ).fetchone()
            count_before = result[0] if result else 0
        except duckdb.CatalogException:
            pass

        self._db.execute(
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
            FROM {SEED_CATEGORIES.full_name}
            """
        )

        result = self._db.execute(
            f"SELECT COUNT(*) FROM {CATEGORIES.full_name}"
        ).fetchone()
        count_after = result[0] if result else 0

        inserted = count_after - count_before
        logger.info(f"Seeded {inserted} categories ({count_after} total)")
        return inserted

    def get_active_categories(self) -> list[dict[str, str | bool | None]]:
        """Get all active categories.

        Returns:
            List of category dicts.
        """
        try:
            rows = self._db.execute(
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

    # -- Stats --

    def categorization_stats(self) -> dict[str, int | float]:
        """Get summary statistics about categorization coverage.

        Returns:
            Dict with total, categorized, uncategorized counts and
            breakdown by categorized_by source.
        """
        try:
            total_result = self._db.execute(
                f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"
            ).fetchone()
            total = total_result[0] if total_result else 0
        except duckdb.CatalogException:
            return {
                "total": 0,
                "categorized": 0,
                "uncategorized": 0,
                "pct_categorized": 0,
            }

        try:
            categorized_result = self._db.execute(
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
            source_rows = self._db.execute(
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

    def stats(self) -> CategorizationStats:
        """Get categorization stats as a typed result.

        Wrapper around :meth:`categorization_stats` that returns a typed object.
        """
        raw = self.categorization_stats()
        by_source = {
            k.removeprefix("by_"): v
            for k, v in raw.items()
            if k.startswith("by_") and isinstance(v, int)
        }
        return CategorizationStats(
            total=int(raw["total"]),
            categorized=int(raw["categorized"]),
            uncategorized=int(raw["uncategorized"]),
            percent_categorized=float(raw["pct_categorized"]),
            by_source=by_source,
        )
