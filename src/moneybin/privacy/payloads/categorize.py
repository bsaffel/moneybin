"""Typed payload dataclasses for the categorize surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Tier derivation summary:
  - ``CategorizeRunPayload``       → Tier.LOW  (AGGREGATE only — counts)
  - ``ImproveAiPayload``           → Tier.LOW  (AGGREGATE only — counts)
  - ``CategorizeStatsPayload``     → Tier.LOW  (AGGREGATE only — counts)
  - ``CategorizeCommitPayload``    → Tier.LOW  (AGGREGATE only — counts)
  - ``RuleRow``                    → Tier.HIGH (TXN_AMOUNT via min/max_amount;
                                    account_id = RECORD_ID per spec D6)
  - ``CategorizeRulesPayload``     → Tier.HIGH (via RuleRow)
  - ``RulesCreatePayload``         → Tier.LOW  (AGGREGATE only — counts + IDs)
  - ``RulesDeletePayload``         → Tier.LOW  (RECORD_ID — rule_id only)
  - ``PendingTxnRow``              → Tier.HIGH (TXN_AMOUNT via amount;
                                    account_id = RECORD_ID per spec D6)
  - ``CatPendingPayload``          → Tier.HIGH (via PendingTxnRow)
  - ``AutoReviewProposalRow``      → Tier.MEDIUM (merchant_pattern = MERCHANT_NAME)
  - ``AutoReviewPayload``          → Tier.MEDIUM (via AutoReviewProposalRow)
  - ``AutoAcceptPayload``          → Tier.LOW  (AGGREGATE only — counts + IDs)
  - ``AutoStatsPayload``           → Tier.LOW  (AGGREGATE only — counts)
  - ``AssistRow``                  → Tier.MEDIUM (description_redacted = DESCRIPTION)
  - ``CatAssistPayload``           → Tier.MEDIUM (via AssistRow)

``transactions_categorize_assist`` deliberately redacts amounts and dates.
``account_id`` is RECORD_ID (spec D6) so it passes through without masking.
``AssistRow`` mirrors that shape — no TXN_AMOUNT or TXN_DATE field. The
middleware must not mask further.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# transactions_categorize_rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RuleRow:
    """One row from app.categorization_rules (transactions_categorize_rules result)."""

    rule_id: Annotated[str, DataClass.RECORD_ID]
    name: Annotated[str | None, DataClass.USER_NOTE]
    merchant_pattern: Annotated[str | None, DataClass.MERCHANT_NAME]
    match_type: Annotated[str | None, DataClass.TXN_TYPE]
    min_amount: Annotated[float | None, DataClass.TXN_AMOUNT]
    max_amount: Annotated[float | None, DataClass.TXN_AMOUNT]
    # RECORD_ID (spec D6): opaque canonical surrogate, not PII; passes through.
    account_id: Annotated[str | None, DataClass.RECORD_ID]
    category: Annotated[str | None, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    priority: Annotated[int | None, DataClass.AGGREGATE]
    is_active: Annotated[bool | None, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class CategorizeRulesPayload:
    """Payload for transactions_categorize_rules."""

    rules: list[RuleRow]


# ---------------------------------------------------------------------------
# transactions_categorize_stats
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategorizeStatsPayload:
    """Payload for transactions_categorize_stats — aggregate counts only."""

    total_transactions: Annotated[int, DataClass.AGGREGATE]
    categorized: Annotated[int, DataClass.AGGREGATE]
    uncategorized: Annotated[int, DataClass.AGGREGATE]
    percent_categorized: Annotated[float, DataClass.AGGREGATE]
    by_source: Annotated[dict[str, int], DataClass.AGGREGATE]
    # None when the Plaid staging view isn't materialized yet (no Plaid data
    # ever loaded) — mirrors the omit-not-zero convention in by_source.
    plaid_unmapped: Annotated[int | None, DataClass.AGGREGATE] = None


@dataclass(frozen=True, slots=True)
class CategorizeStatsWithAutoPayload:
    """Payload for transactions_categorize_stats(include_auto=True).

    Composite of the overall coverage stats plus auto-rule health metrics.
    A distinct type (not a bare dict) so the declared return annotation
    matches the runtime shape and the privacy middleware derives the tier
    from real fields.
    """

    overall: CategorizeStatsPayload
    auto: AutoStatsPayload


# ---------------------------------------------------------------------------
# transactions_categorize_pending
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PendingTxnRow:
    """One uncategorized transaction row (transactions_categorize_pending result)."""

    transaction_id: Annotated[str, DataClass.RECORD_ID]
    transaction_date: Annotated[str | None, DataClass.TXN_DATE]
    amount: Annotated[float | None, DataClass.TXN_AMOUNT]
    description: Annotated[str | None, DataClass.DESCRIPTION]
    memo: Annotated[str | None, DataClass.DESCRIPTION]
    # RECORD_ID (spec D6): opaque canonical surrogate, not PII; passes through.
    account_id: Annotated[str | None, DataClass.RECORD_ID]
    age_days: Annotated[int | None, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class CatPendingPayload:
    """Payload for transactions_categorize_pending."""

    transactions: list[PendingTxnRow]


# ---------------------------------------------------------------------------
# transactions_categorize_commit
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategorizeCommitPayload:
    """Payload for transactions_categorize_commit — aggregate result counts."""

    applied: Annotated[int, DataClass.AGGREGATE]
    skipped: Annotated[int, DataClass.AGGREGATE]
    errors: Annotated[int, DataClass.AGGREGATE]
    merchants_created: Annotated[int, DataClass.AGGREGATE]
    error_details: Annotated[list[dict[str, object]], DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# transactions_categorize_rules_create
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RulesCreatePayload:
    """Payload for transactions_categorize_rules_create — creation result."""

    created: Annotated[int, DataClass.AGGREGATE]
    existing: Annotated[int, DataClass.AGGREGATE]
    skipped: Annotated[int, DataClass.AGGREGATE]
    rule_ids: Annotated[list[str], DataClass.RECORD_ID]
    error_details: Annotated[list[dict[str, str]], DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# transactions_categorize_rules_delete
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RulesDeletePayload:
    """Payload for transactions_categorize_rules_delete."""

    rule_id: Annotated[str, DataClass.RECORD_ID]
    action: Annotated[str, DataClass.TXN_TYPE]


# ---------------------------------------------------------------------------
# transactions_categorize_auto_review
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AutoReviewProposalRow:
    """One pending auto-rule proposal row."""

    proposed_rule_id: Annotated[str, DataClass.RECORD_ID]
    # MERCHANT_NAME — drives AutoReviewPayload to Tier.MEDIUM
    merchant_pattern: Annotated[str | None, DataClass.MERCHANT_NAME]
    match_type: Annotated[str | None, DataClass.TXN_TYPE]
    category: Annotated[str | None, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    trigger_count: Annotated[int, DataClass.AGGREGATE]
    sample_txn_ids: Annotated[list[str], DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class AutoReviewPayload:
    """Payload for transactions_categorize_auto_review."""

    proposals: list[AutoReviewProposalRow]


# ---------------------------------------------------------------------------
# transactions_categorize_auto_accept
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AutoAcceptPayload:
    """Payload for transactions_categorize_auto_accept — aggregate counts."""

    approved: Annotated[int, DataClass.AGGREGATE]
    rejected: Annotated[int, DataClass.AGGREGATE]
    skipped: Annotated[int, DataClass.AGGREGATE]
    newly_categorized: Annotated[int, DataClass.AGGREGATE]
    rule_ids: Annotated[list[str], DataClass.RECORD_ID]


# ---------------------------------------------------------------------------
# transactions_categorize_auto_stats
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AutoStatsPayload:
    """Payload for transactions_categorize_auto_stats — health metrics."""

    active_auto_rules: Annotated[int, DataClass.AGGREGATE]
    pending_proposals: Annotated[int, DataClass.AGGREGATE]
    transactions_categorized: Annotated[int, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# transactions_categorize_run
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategorizeRunPayload:
    """Payload for transactions_categorize_run — engine cascade result."""

    applied_by_method: Annotated[dict[str, int], DataClass.AGGREGATE]
    total_applied: Annotated[int, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# transactions_categorize_improve_ai
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImproveAiPayload:
    """Payload for transactions_categorize_improve_ai — AI-to-provider upgrade count."""

    upgraded_count: Annotated[int, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# transactions_categorize_assist
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AssistRow:
    """One LLM-safe redacted transaction row.

    Deliberately mirrors ``RedactedTransaction``'s privacy contract:
    no full amount (``amount_sign`` only), no date, no account_id.
    The Phase 6 middleware must not mask further — this shape is already
    the redacted projection.  Adding any ACCOUNT_IDENTIFIER, TXN_AMOUNT,
    or TXN_DATE field here would regress that guarantee.
    """

    transaction_id: Annotated[str, DataClass.RECORD_ID]
    # DESCRIPTION — drives CatAssistPayload to Tier.MEDIUM
    description_redacted: Annotated[str, DataClass.DESCRIPTION]
    memo_redacted: Annotated[str, DataClass.DESCRIPTION]
    source_type: Annotated[str, DataClass.TXN_TYPE]
    transaction_type: Annotated[str | None, DataClass.TXN_TYPE]
    check_number: Annotated[str | None, DataClass.DESCRIPTION]
    is_transfer: Annotated[bool, DataClass.TXN_TYPE]
    transfer_pair_id: Annotated[str | None, DataClass.RECORD_ID]
    payment_channel: Annotated[str | None, DataClass.TXN_TYPE]
    # Sign only — deliberately not TXN_AMOUNT; amount itself is never sent.
    amount_sign: Annotated[Literal["+", "-", "0"], DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class CatAssistPayload:
    """Payload for transactions_categorize_assist."""

    transactions: list[AssistRow]
