"""Adapter functions that convert auto-rule service results into ResponseEnvelopes.

Keeps the service layer free of transport-layer imports by centralising the
``build_envelope`` calls here. Each function is pure: no I/O, no side-effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from moneybin.privacy.payloads.categorize import (
    AutoAcceptPayload,
    AutoReviewPayload,
    AutoReviewProposalRow,
    AutoStatsPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

if TYPE_CHECKING:
    from moneybin.services.auto_rule_service import (
        AutoConfirmResult,
        AutoReviewResult,
        AutoStatsResult,
    )


def auto_review_envelope(
    result: AutoReviewResult,
) -> ResponseEnvelope[AutoReviewPayload]:
    """Build a ResponseEnvelope for the transactions_categorize_auto_review tool."""
    payload = AutoReviewPayload(
        proposals=[
            AutoReviewProposalRow(
                proposed_rule_id=p["proposed_rule_id"],
                merchant_pattern=p.get("merchant_pattern"),
                match_type=p.get("match_type"),
                category=p.get("category"),
                subcategory=p.get("subcategory"),
                trigger_count=int(p.get("trigger_count", 0)),
                sample_txn_ids=list(p.get("sample_txn_ids") or []),
            )
            for p in result.proposals
        ]
    )
    return build_envelope(
        data=payload,
        sensitivity="medium",
        total_count=result.total_count,
        actions=[
            "Use transactions_categorize_auto_accept to accept or reject proposals",
        ],
    )


def auto_accept_envelope(
    result: AutoConfirmResult,
) -> ResponseEnvelope[AutoAcceptPayload]:
    """Build a ResponseEnvelope for the transactions_categorize_auto_accept tool."""
    payload = AutoAcceptPayload(
        approved=result.approved,
        rejected=result.rejected,
        skipped=result.skipped,
        newly_categorized=result.newly_categorized,
        rule_ids=list(result.rule_ids),
    )
    return build_envelope(
        data=payload,
        sensitivity="medium",
    )


def auto_stats_envelope(result: AutoStatsResult) -> ResponseEnvelope[AutoStatsPayload]:
    """Build a ResponseEnvelope for the transactions_categorize_auto_stats tool."""
    payload = AutoStatsPayload(
        active_auto_rules=result.active_auto_rules,
        pending_proposals=result.pending_proposals,
        transactions_categorized=result.transactions_categorized,
    )
    return build_envelope(
        data=payload,
        sensitivity="low",
    )
