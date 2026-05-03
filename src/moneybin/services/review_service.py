"""Service composing match + categorize review queue counts."""

from __future__ import annotations

from dataclasses import dataclass

from moneybin.services.categorization_service import CategorizationService
from moneybin.services.matching_service import MatchingService


@dataclass(frozen=True)
class ReviewStatus:
    """Counts for both review queues at a point in time."""

    matches_pending: int
    categorize_pending: int

    @property
    def total(self) -> int:
        """Sum of both queues."""
        return self.matches_pending + self.categorize_pending


class ReviewService:
    """Composes MatchingService + CategorizationService for unified review queue counts."""

    def __init__(
        self, match_service: MatchingService, categorize_service: CategorizationService
    ) -> None:
        """Bind to an existing MatchingService and CategorizationService."""
        self._match_service = match_service
        self._categorize_service = categorize_service

    def status(self) -> ReviewStatus:
        """Return current queue counts for both review queues."""
        return ReviewStatus(
            matches_pending=self._match_service.count_pending(),
            categorize_pending=self._categorize_service.count_uncategorized(),
        )
