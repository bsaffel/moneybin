"""Service composing match + categorize + account-links + merchant-links review queue counts."""

from __future__ import annotations

from dataclasses import dataclass

from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.categorization import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.services.merchant_links_service import MerchantLinksService


@dataclass(frozen=True)
class ReviewStatus:
    """Counts for all four review queues at a point in time."""

    matches_pending: int
    categorize_pending: int
    account_links_pending: int
    merchant_links_pending: int

    @property
    def total(self) -> int:
        """Sum of all four queues."""
        return (
            self.matches_pending
            + self.categorize_pending
            + self.account_links_pending
            + self.merchant_links_pending
        )


class ReviewService:
    """Composes MatchingService + CategorizationService + AccountLinksService + MerchantLinksService for unified review queue counts."""

    def __init__(
        self,
        match_service: MatchingService,
        categorize_service: CategorizationService,
        account_links_service: AccountLinksService,
        merchant_links_service: MerchantLinksService,
    ) -> None:
        """Bind to existing services."""
        self._match_service = match_service
        self._categorize_service = categorize_service
        self._account_links_service = account_links_service
        self._merchant_links_service = merchant_links_service

    def status(self) -> ReviewStatus:
        """Return current queue counts for all four review queues."""
        return ReviewStatus(
            matches_pending=self._match_service.count_pending(),
            categorize_pending=self._categorize_service.count_uncategorized(),
            account_links_pending=self._account_links_service.count_pending(),
            merchant_links_pending=self._merchant_links_service.count_pending(),
        )
