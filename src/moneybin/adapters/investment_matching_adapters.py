"""Shared durable investment review views for CLI and MCP."""

from moneybin.database import Database
from moneybin.privacy.payloads.reviews import (
    InvestmentMatchDetails,
    InvestmentMatchReviewRow,
    ReviewsInvestmentMatchesView,
    ReviewStatus,
)
from moneybin.services.investment_matching_service import InvestmentMatchingService


def investment_review_view(
    db: Database, status: ReviewStatus
) -> ReviewsInvestmentMatchesView:
    """Read issued evidence without recomputing the Proposal."""
    service = InvestmentMatchingService(db)
    records = service.pending() if status == "pending" else service.history()
    return ReviewsInvestmentMatchesView(
        status=status,
        rows=[
            InvestmentMatchReviewRow(
                decision_id=row["proposal_id"],
                status=row["status"],
                created_at=str(row["created_at"])
                if row["created_at"] is not None
                else None,
                summary="Competing investment Proposal"
                if row["is_competing"]
                else "Investment Proposal",
                details=InvestmentMatchDetails.model_validate(row),
            )
            for row in records
        ],
    )
