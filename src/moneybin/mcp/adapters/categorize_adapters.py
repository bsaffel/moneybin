"""Adapter functions that convert auto-rule service results into ResponseEnvelopes.

Keeps the service layer free of transport-layer imports by centralising the
``build_envelope`` calls here. Each function is pure: no I/O, no side-effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

if TYPE_CHECKING:
    from moneybin.services.auto_rule_service import (
        AutoConfirmResult,
        AutoReviewResult,
        AutoStatsResult,
    )


def auto_review_envelope(result: AutoReviewResult) -> ResponseEnvelope:
    """Build a ResponseEnvelope for the categorize_auto_review tool."""
    return build_envelope(
        data=result.proposals,
        sensitivity="medium",
        total_count=result.total_count,
        actions=[
            "Use categorize_auto_confirm to approve or reject proposals",
        ],
    )


def auto_confirm_envelope(result: AutoConfirmResult) -> ResponseEnvelope:
    """Build a ResponseEnvelope for the categorize_auto_confirm tool."""
    return build_envelope(
        data={
            "approved": result.approved,
            "rejected": result.rejected,
            "skipped": result.skipped,
            "newly_categorized": result.newly_categorized,
            "rule_ids": result.rule_ids,
        },
        sensitivity="medium",
    )


def auto_stats_envelope(result: AutoStatsResult) -> ResponseEnvelope:
    """Build a ResponseEnvelope for the categorize_auto_stats tool."""
    return build_envelope(
        data={
            "active_auto_rules": result.active_auto_rules,
            "pending_proposals": result.pending_proposals,
            "transactions_categorized": result.transactions_categorized,
        },
        sensitivity="low",
    )
