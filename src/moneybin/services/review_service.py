"""Compose cash, investment, categorization, and identity review counts."""

from __future__ import annotations

from dataclasses import dataclass

from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.categorization import CategorizationService
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.matching_service import MatchingService
from moneybin.services.merchant_links_service import MerchantLinksService
from moneybin.services.security_links_service import SecurityLinksService


@dataclass(frozen=True)
class ReviewStatus:
    """Counts for six review queues at a point in time."""

    matches_pending: int
    categorize_pending: int
    account_links_pending: int
    merchant_links_pending: int
    security_links_pending: int
    investment_matches_pending: int = 0

    @property
    def total(self) -> int:
        """Sum of all review queues."""
        return (
            self.matches_pending
            + self.categorize_pending
            + self.account_links_pending
            + self.merchant_links_pending
            + self.security_links_pending
            + self.investment_matches_pending
        )


class ReviewService:
    """Compose the existing domain services into unified review counts."""

    def __init__(
        self,
        match_service: MatchingService,
        categorize_service: CategorizationService,
        account_links_service: AccountLinksService,
        merchant_links_service: MerchantLinksService,
        security_links_service: SecurityLinksService,
        investment_matching_service: InvestmentMatchingService,
    ) -> None:
        """Bind to existing services."""
        self._match_service = match_service
        self._categorize_service = categorize_service
        self._account_links_service = account_links_service
        self._merchant_links_service = merchant_links_service
        self._security_links_service = security_links_service
        self._investment_matching_service = investment_matching_service

    def status(self) -> ReviewStatus:
        """Return current queue counts for all review domains."""
        return ReviewStatus(
            matches_pending=self._match_service.count_pending(),
            categorize_pending=self._categorize_service.count_uncategorized(),
            account_links_pending=self._account_links_service.count_pending(),
            merchant_links_pending=self._merchant_links_service.count_pending(),
            security_links_pending=self._security_links_service.count_pending(),
            investment_matches_pending=self._investment_matching_service.count_pending(),
        )
