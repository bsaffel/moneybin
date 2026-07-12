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
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

if TYPE_CHECKING:
    from moneybin.services.auto_rule_service import (
        AutoConfirmResult,
        AutoReviewResult,
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
                estimated_match_count=int(p.get("estimated_match_count", 0)),
                is_broad=bool(p.get("is_broad", False)),
            )
            for p in result.proposals
        ]
    )
    broad = [p for p in result.proposals if p.get("is_broad")]
    actions = [
        "Use transactions_categorize_auto_accept to accept or reject proposals",
    ]
    if broad:
        actions.append(
            f"{len(broad)} proposal(s) are flagged is_broad — each would "
            "recategorize far more transactions than its evidence supports. "
            "Check estimated_match_count; accepting one requires allow_broad=true."
        )
    return build_envelope(
        data=payload,
        sensitivity="medium",
        total_count=result.total_count,
        actions=actions,
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
