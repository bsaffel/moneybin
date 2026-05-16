r"""Pattern matching, rule evaluation, and merchant lookup.

Read-only against ``core.fct_transactions``, ``app.user_merchants`` (via the
``core.dim_merchants`` view), and ``app.categorization_rules``. Holds no
write logic — the ``MatchApplier`` collaborator owns every write to
``app.*``. The facade composes both.

Two-stage merchant lookup: oneOf exemplar membership first (most specific),
then pattern-based exact / contains / regex per-field. Per-field fallback is
required because ``exact`` and anchored-``regex`` shapes can't span the
``description + "\n" + memo`` concatenation boundary. See
``docs/specs/categorization-matching-mechanics.md`` §Matcher algorithm.
"""

from __future__ import annotations

import logging
from typing import Any

import duckdb

from moneybin.database import Database
from moneybin.metrics.registry import CATEGORIZE_MATCH_OUTCOME_TOTAL
from moneybin.services._text import build_match_inputs
from moneybin.services.categorization._shared import (
    Merchant,
    match_shape_case_sql,
    matches_pattern,
)
from moneybin.tables import (
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)


def _match_shape_label(description_present: bool, memo_present: bool) -> str:
    """Return the ``shape`` metric label for a matcher call.

    Both ``_match_exemplar`` and ``_match_text`` fire
    ``CATEGORIZE_MATCH_OUTCOME_TOTAL`` with a label describing which input
    signals were present. Extracted so the mapping lives in one place.
    """
    if description_present and memo_present:
        return "both"
    if memo_present:
        return "memo_only"
    return "description_only"


def _match_exemplar(
    match_text: str,
    merchants: list[Merchant],
    *,
    description_present: bool = True,
    memo_present: bool = False,
) -> dict[str, str | None] | None:
    """Match match_text against merchants' oneOf exemplar sets (set membership).

    Returns the first merchant whose exemplars contain ``match_text`` exactly.
    Iteration order is the same as ``CategorizationMatcher.fetch_merchants``
    (oneOf first), so exact-string membership fires before pattern-based
    shapes. Records ``outcome='exemplar'`` on a hit.
    """
    shape = _match_shape_label(description_present, memo_present)

    if not match_text:
        return None

    for m in merchants:
        if m.match_type != "oneOf":
            continue
        if m.exemplars and match_text in m.exemplars:
            CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(outcome="exemplar", shape=shape).inc()
            return {
                "merchant_id": m.merchant_id,
                "canonical_name": m.canonical_name,
                "category": m.category,
                "subcategory": m.subcategory,
            }
    return None


def _match_text(
    match_text: str,
    merchants: list[Merchant],
    *,
    normalized_description: str = "",
    normalized_memo: str = "",
    description_present: bool = True,
    memo_present: bool = False,
) -> dict[str, str | None] | None:
    r"""Match a pre-fetched merchant list against the per-field candidate texts.

    ``match_text`` is ``description + "\n" + memo`` (per ``build_match_text``);
    ``normalized_description`` and ``normalized_memo`` are the individual
    normalized fields. Patterns are tried against each non-empty candidate so
    ``exact`` and anchored-``regex`` shapes (which can't span the concatenation
    boundary) still match the original field, while ``contains`` and unanchored
    ``regex`` shapes can still hit cross-boundary substrings via ``match_text``.

    description_present and memo_present control the "shape" label on the
    match-outcome metric so callers can attribute matches by signal source.

    Exemplar-only merchants (match_type='oneOf' with raw_pattern=None) are
    skipped — exemplar lookup is handled by :func:`_match_exemplar`, which
    callers invoke first.
    """
    shape = _match_shape_label(description_present, memo_present)

    candidates: list[str] = []
    for text in (match_text, normalized_description, normalized_memo):
        if text and text not in candidates:
            candidates.append(text)

    if not candidates:
        CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(outcome="none", shape=shape).inc()
        return None

    for m in merchants:
        if m.match_type == "oneOf" or not m.raw_pattern:
            # Exemplar-only merchants are handled by _match_exemplar.
            continue
        if any(matches_pattern(c, m.raw_pattern, m.match_type) for c in candidates):
            CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(
                outcome=m.match_type or "contains", shape=shape
            ).inc()
            return {
                "merchant_id": m.merchant_id,
                "canonical_name": m.canonical_name,
                "category": m.category,
                "subcategory": m.subcategory,
            }

    CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(outcome="none", shape=shape).inc()
    return None


def match_merchants(
    match_text: str,
    merchants: list[Merchant],
    *,
    normalized_description: str = "",
    normalized_memo: str = "",
    description_present: bool = True,
    memo_present: bool = False,
) -> dict[str, str | None] | None:
    """Resolve a merchant against the cached merchant list.

    Two-stage lookup per categorization-matching-mechanics.md §Matcher
    algorithm: oneOf exemplar membership against ``match_text`` first (most
    specific shape), then pattern-based (exact / contains / regex) per-field
    fallback (see :func:`_match_text` for why per-field is required).
    """
    hit = _match_exemplar(
        match_text,
        merchants,
        description_present=description_present,
        memo_present=memo_present,
    )
    if hit is not None:
        return hit
    return _match_text(
        match_text,
        merchants,
        normalized_description=normalized_description,
        normalized_memo=normalized_memo,
        description_present=description_present,
        memo_present=memo_present,
    )


class CategorizationMatcher:
    """Read-only matching: merchants, rules, and uncategorized transaction scans.

    Holds a ``Database`` handle and serves both the in-memory merchant cache
    and rule evaluation. No writes — the facade pairs this with
    ``MatchApplier`` when a match must produce a categorization.
    """

    def __init__(self, db: Database) -> None:
        """Bind the matcher to a database connection."""
        self._db = db

    def fetch_merchants(self) -> list[Merchant] | None:
        """Fetch all merchant mappings ordered for lookup precedence.

        Ordering:
        1. match-type OP_SCORE DESC — oneOf/exact (10) outrank contains/regex (0).
           CASE expression generated from ``_MATCH_SHAPE_SCORES`` via
           :func:`match_shape_case_sql` so SQL and Python ladders cannot drift.
        2. created_at ASC — deterministic tie-break among same-score merchants.

        Returns:
            List of merchant rows, or None if the table doesn't exist.
        """
        try:
            rows = self._db.execute(
                f"""
                SELECT merchant_id, raw_pattern, match_type,
                       canonical_name, category, subcategory, exemplars
                FROM {MERCHANTS.full_name}
                ORDER BY
                    {match_shape_case_sql("match_type")} DESC,
                    created_at ASC
                """,  # noqa: S608  # MERCHANTS is a TableRef constant; CASE generated from _MATCH_SHAPE_SCORES
            ).fetchall()
        except duckdb.CatalogException:
            return None
        return [Merchant.from_row(r) for r in rows]

    def match_merchant(
        self, description: str, memo: str | None = None
    ) -> dict[str, str | None] | None:
        """Look up a merchant by raw description (and optional memo).

        Two-stage lookup: oneOf exemplar membership first (most-specific shape),
        then pattern-based exact/contains/regex (per
        categorization-matching-mechanics.md §Matcher algorithm).

        For OFX-sourced and aggregator-style transactions, memo carries the
        wrapped merchant identity and is essential for accurate matching.
        """
        merchants = self.fetch_merchants()
        if merchants is None:
            return None
        match_text, norm_desc, norm_memo = build_match_inputs(description, memo)
        return match_merchants(
            match_text,
            merchants,
            normalized_description=norm_desc,
            normalized_memo=norm_memo,
            description_present=bool(description and description.strip()),
            memo_present=bool(memo and memo.strip()),
        )

    def fetch_uncategorized_rows(self) -> list[tuple[Any, ...]] | None:
        """Return rows for uncategorized transactions with a non-empty description or memo.

        Single scan shared between the facade's :meth:`apply_rules` and
        :meth:`apply_merchant_categories` when called from
        :meth:`categorize_pending`. Returns ``None`` if the source tables don't
        exist (DB pre-migration); returns ``[]`` when there are no pending rows.

        Columns: ``(transaction_id, description, amount, account_id, memo)`` —
        the superset of what either consumer needs; ``apply_merchant_categories``
        ignores ``amount`` and ``account_id``.
        """
        try:
            return self._db.execute(
                f"""
                SELECT t.transaction_id, t.description, t.amount, t.account_id, t.memo
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                    AND (
                        (t.description IS NOT NULL AND t.description != '')
                        OR (t.memo IS NOT NULL AND t.memo != '')
                    )
                """,
            ).fetchall()
        except duckdb.CatalogException:
            return None

    def fetch_active_rules(self) -> list[tuple[Any, ...]]:
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
    def match_first_rule(
        rules: list[tuple[Any, ...]],
        description: str,
        amount: float | None,
        account_id: str | None,
        memo: str | None = None,
    ) -> tuple[str, str, str | None, str] | None:
        """Return ``(rule_id, category, subcategory, created_by)`` for the first rule that matches.

        Evaluates each pattern against the canonical ``match_text``
        (``build_match_text(description, memo)``) plus the individual normalized
        fields, so ``contains`` / unanchored ``regex`` patterns can hit
        cross-boundary substrings while ``exact`` and anchored-``regex``
        patterns still match the original field they were authored against.
        Amount bounds and account filter are applied as before. Returns
        ``None`` when no rule matches.
        """
        match_text, norm_desc, norm_memo = build_match_inputs(description, memo)
        candidates: list[str] = []
        for text in (match_text, norm_desc, norm_memo):
            if text and text not in candidates:
                candidates.append(text)
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
            if not any(matches_pattern(c, pattern, match_type) for c in candidates):
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
        self,
        transaction_id: str,
        *,
        rules_override: list[tuple[Any, ...]] | None = None,
        txn_row_override: tuple[str, float | None, str | None, str | None]
        | None = None,
    ) -> tuple[str, str, str | None, str] | None:
        """Return the first active rule matching this transaction, or ``None``.

        Result tuple is ``(rule_id, category, subcategory, created_by)``.
        Single-transaction variant of the facade's :meth:`apply_rules`; lets
        callers (e.g., the auto-rule proposal pipeline) ask "is this transaction
        already covered by an existing rule?" using the canonical match
        semantics instead of re-implementing them.

        The batch path supplies pre-loaded rule rows and txn metadata via
        ``rules_override`` and ``txn_row_override`` so this function issues no
        queries during a batch loop. Both default to ``None`` for non-batch callers.
        ``txn_row_override`` is ``(description, amount, account_id, memo)``.
        """
        description: str
        amount: float | None
        account_id: str | None
        memo: str | None
        if txn_row_override is not None:
            description, amount, account_id, memo = txn_row_override
        else:
            try:
                txn_row = self._db.execute(
                    f"SELECT description, amount, account_id, memo "
                    f"FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
                    [transaction_id],
                ).fetchone()
            except duckdb.CatalogException:
                return None
            if not txn_row:
                return None
            # DuckDB row values are dynamically typed; normalize to the shapes
            # match_first_rule expects.
            raw_desc, raw_amt, raw_acct, raw_memo = txn_row
            description = str(raw_desc) if raw_desc else ""
            amount = float(raw_amt) if raw_amt is not None else None
            account_id = str(raw_acct) if raw_acct is not None else None
            memo = str(raw_memo) if raw_memo else None
        if not description and not memo:
            return None
        rules = (
            rules_override if rules_override is not None else self.fetch_active_rules()
        )
        if not rules:
            return None
        return self.match_first_rule(rules, description, amount, account_id, memo)
