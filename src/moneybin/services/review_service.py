"""Service composing match + categorize + account-links review queue counts."""

from __future__ import annotations

from dataclasses import dataclass

from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.categorization import CategorizationService
from moneybin.services.matching_service import MatchingService


@dataclass(frozen=True)
class ReviewStatus:
    """Counts for all three review queues at a point in time."""

    matches_pending: int
    categorize_pending: int
    account_links_pending: int

    @property
    def total(self) -> int:
        """Sum of all three queues."""
        return (
            self.matches_pending + self.categorize_pending + self.account_links_pending
        )


class ReviewService:
    """Composes MatchingService + CategorizationService + AccountLinksService for unified review queue counts."""

    def __init__(
        self,
        match_service: MatchingService,
        categorize_service: CategorizationService,
        account_links_service: AccountLinksService,
    ) -> None:
        """Bind to existing services."""
        self._match_service = match_service
        self._categorize_service = categorize_service
        self._account_links_service = account_links_service

    def status(self) -> ReviewStatus:
        """Return current queue counts for all three review queues."""
        return ReviewStatus(
            matches_pending=self._match_service.count_pending(),
            categorize_pending=self._categorize_service.count_uncategorized(),
            account_links_pending=self._account_links_service.count_pending(),
        )
