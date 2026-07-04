"""Read-only queries for the categorization surface.

Reporting-shaped reads (taxonomy listings, rule and merchant catalogs,
uncategorized inventory, coverage stats) consumed by the CLI and MCP
surface. Distinct from ``matcher.py``'s read paths — those serve the
matching loop and return matcher-internal shapes; these return
presentation-ready dicts and typed payloads for the user-facing tools.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import duckdb

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.payloads.categories import (
    CategoriesPayload,
    CategoryRow,
    MerchantRow,
    MerchantsPayload,
)
from moneybin.privacy.payloads.categorize import (
    CategorizeRulesPayload,
    CategorizeStatsPayload,
    RuleRow,
)
from moneybin.tables import (
    CATEGORIES,
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    REPORTS_UNCATEGORIZED_QUEUE,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CategorizationStats:
    """Typed result for categorization statistics."""

    total: int
    categorized: int
    uncategorized: int
    percent_categorized: float
    by_source: dict[str, int]

    def to_payload(self) -> CategorizeStatsPayload:
        """Return a typed payload for the MCP/CLI envelope boundary."""
        return CategorizeStatsPayload(
            total_transactions=self.total,
            categorized=self.categorized,
            uncategorized=self.uncategorized,
            percent_categorized=self.percent_categorized,
            by_source=self.by_source,
        )


class CategorizationQueries:
    """Read-only reporting queries against the categorization tables."""

    def __init__(self, db: Database) -> None:
        """Bind the queries collaborator to a database connection."""
        self._db = db

    def _fct_transactions_exists(self) -> bool:
        """Return True if core.fct_transactions is queryable.

        Used to distinguish pre-first-import state (no fact table yet) from
        schema drift (fact table present, derived views missing).
        """
        try:
            self._db.execute(
                f"SELECT 1 FROM {FCT_TRANSACTIONS.full_name} LIMIT 0"  # noqa: S608  # TableRef constant
            )
        except duckdb.CatalogException:
            return False
        return True

    def get_active_categories(self) -> list[dict[str, str | bool | None]]:
        """Get all active categories."""
        try:
            rows = self._db.execute(
                f"""
                SELECT category_id, category, subcategory, description,
                       class, is_default
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
                "class": r[4],
                "is_default": r[5],
            }
            for r in rows
        ]

    def get_all_categories(self, *, include_inactive: bool) -> CategoriesPayload:
        """Get categories with consistent field shape including is_active.

        Active-only views can use ``get_active_categories()`` to omit
        ``is_active`` from each row; this method always includes it so the
        MCP tool surface is consumer-friendly when toggling the include flag.
        """
        where = "" if include_inactive else "WHERE is_active = true"
        try:
            rows = self._db.execute(
                f"""
                SELECT category_id, category, subcategory, description,
                       is_default, is_active
                FROM {CATEGORIES.full_name}
                {where}
                ORDER BY category, subcategory
                """  # noqa: S608  # constant clause, not user input
            ).fetchall()
        except duckdb.CatalogException:
            return CategoriesPayload(categories=[])

        return CategoriesPayload(
            categories=[
                CategoryRow(
                    category_id=r[0],
                    category=r[1],
                    subcategory=r[2],
                    description=r[3],
                    is_default=bool(r[4]) if r[4] is not None else None,
                    is_active=bool(r[5]) if r[5] is not None else None,
                )
                for r in rows
            ]
        )

    def list_rules(self) -> CategorizeRulesPayload:
        """List all categorization rules (active and inactive) ordered by priority."""
        try:
            rows = self._db.execute(
                f"""
                SELECT rule_id, name, merchant_pattern, match_type,
                       min_amount, max_amount, account_id,
                       category, subcategory, priority, is_active
                FROM {CATEGORIZATION_RULES.full_name}
                ORDER BY priority ASC, created_at ASC
                """
            ).fetchall()
        except duckdb.CatalogException:
            return CategorizeRulesPayload(rules=[])

        return CategorizeRulesPayload(
            rules=[
                RuleRow(
                    rule_id=r[0],
                    name=r[1],
                    merchant_pattern=r[2],
                    match_type=r[3],
                    min_amount=float(r[4]) if r[4] is not None else None,
                    max_amount=float(r[5]) if r[5] is not None else None,
                    account_id=r[6],
                    category=r[7],
                    subcategory=r[8],
                    priority=int(r[9]) if r[9] is not None else None,
                    is_active=bool(r[10]) if r[10] is not None else None,
                )
                for r in rows
            ]
        )

    def list_merchants(self) -> MerchantsPayload:
        """List all merchant name mappings ordered by canonical name."""
        try:
            rows = self._db.execute(
                f"""
                SELECT merchant_id, raw_pattern, match_type,
                       canonical_name, category, subcategory
                FROM {MERCHANTS.full_name}
                ORDER BY canonical_name
                """
            ).fetchall()
        except duckdb.CatalogException:
            return MerchantsPayload(merchants=[])

        return MerchantsPayload(
            merchants=[
                MerchantRow(
                    merchant_id=r[0],
                    raw_pattern=r[1],
                    match_type=r[2],
                    canonical_name=r[3],
                    category=r[4],
                    subcategory=r[5],
                )
                for r in rows
            ]
        )

    def list_uncategorized_transactions(
        self,
        *,
        limit: int,
        sort: Literal["date", "impact"] = "date",
        min_amount: Decimal = Decimal("0"),
        account_id: str | None = None,
    ) -> list[dict[str, Any]] | None:
        """List uncategorized transactions from the curator-impact view.

        Uses ``reports.uncategorized_queue`` which already excludes transfer
        pairs and archived accounts and provides pre-computed ``age_days`` and
        ``priority_score`` columns needed for impact-sort.

        ``sort`` controls the ORDER BY:
        - ``"date"``   — ``txn_date DESC`` (most recent first, default)
        - ``"impact"`` — ``priority_score DESC`` (ABS(amount) * age_days, largest first)

        Returns ``None`` only when the underlying fact table doesn't exist
        yet (pre-first-import). When the fact table exists but the queue
        view is missing — schema drift or unapplied refresh — raises
        ``UserError(code="schema_out_of_date")`` so callers surface a
        ``refresh_run`` remediation rather than misreporting "no data".
        """
        if sort not in {"date", "impact"}:
            raise ValueError(f"Unknown sort: {sort!r}; expected 'date' or 'impact'")

        order = "priority_score DESC" if sort == "impact" else "txn_date DESC"
        sql = f"""
            SELECT transaction_id, account_id, account_name, txn_date, amount,
                   description, merchant_id, merchant_normalized, age_days,
                   priority_score, source_type, source_id
            FROM {REPORTS_UNCATEGORIZED_QUEUE.full_name}
            WHERE ABS(amount) >= ?
        """  # noqa: S608  # TableRef constant + allowlisted sort literal
        params: list[object] = [min_amount]
        if account_id is not None:
            sql += " AND account_id = ?"
            params.append(account_id)
        sql += f" ORDER BY {order} LIMIT ?"  # noqa: S608  # order from allowlisted set
        params.append(limit)

        try:
            result = self._db.execute(sql, params)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        except duckdb.CatalogException as e:
            if not self._fct_transactions_exists():
                return None
            raise UserError(
                "Uncategorized-queue view is missing. The schema is out of "
                "date — run `refresh_run` (MCP) or `moneybin refresh` (CLI) "
                "to rebuild derived views.",
                code="schema_out_of_date",
                hint="refresh_run",
                details={"missing_object": REPORTS_UNCATEGORIZED_QUEUE.full_name},
            ) from e

        return [dict(zip(columns, row, strict=False)) for row in rows]

    def count_uncategorized(self) -> int:
        """Return the number of transactions without a category assignment."""
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                """  # noqa: S608  # TableRef constants, no user input interpolated
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:  # noqa: BLE001 — tables may not exist before first import
            return 0

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
