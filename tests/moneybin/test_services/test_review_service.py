"""Tests for ReviewService — unified queue counts."""

from unittest.mock import MagicMock

from moneybin.services.review_service import ReviewService, ReviewStatus


def _make_review_service(
    matches: int = 0,
    categorize: int = 0,
    account_links: int = 0,
    merchant_links: int = 0,
) -> ReviewService:
    """Factory that wires four mock services into a ReviewService."""
    match_service = MagicMock()
    match_service.count_pending.return_value = matches
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = categorize
    links_service = MagicMock()
    links_service.count_pending.return_value = account_links
    merchant_links_service = MagicMock()
    merchant_links_service.count_pending.return_value = merchant_links

    return ReviewService(
        match_service=match_service,
        categorize_service=cat_service,
        account_links_service=links_service,
        merchant_links_service=merchant_links_service,
    )


def test_review_status_counts_all_queues() -> None:
    svc = _make_review_service(
        matches=3, categorize=12, account_links=0, merchant_links=0
    )
    status = svc.status()

    assert isinstance(status, ReviewStatus)
    assert status.matches_pending == 3
    assert status.categorize_pending == 12
    assert status.account_links_pending == 0
    assert status.merchant_links_pending == 0
    assert status.total == 15


def test_review_status_zero_queues() -> None:
    svc = _make_review_service()
    status = svc.status()

    assert status.total == 0
    assert status.matches_pending == 0
    assert status.categorize_pending == 0
    assert status.account_links_pending == 0
    assert status.merchant_links_pending == 0


def test_review_status_delegates_to_services() -> None:
    """Each service method is called exactly once."""
    match_service = MagicMock()
    match_service.count_pending.return_value = 5
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = 7
    links_service = MagicMock()
    links_service.count_pending.return_value = 2
    merchant_links_service = MagicMock()
    merchant_links_service.count_pending.return_value = 1

    svc = ReviewService(
        match_service=match_service,
        categorize_service=cat_service,
        account_links_service=links_service,
        merchant_links_service=merchant_links_service,
    )
    svc.status()

    match_service.count_pending.assert_called_once()
    cat_service.count_uncategorized.assert_called_once()
    links_service.count_pending.assert_called_once()
    merchant_links_service.count_pending.assert_called_once()


def test_review_status_includes_account_links_pending() -> None:
    """account_links_pending is counted and included in total."""
    svc = _make_review_service(account_links=4)
    status = svc.status()

    assert status.account_links_pending == 4
    assert status.total == 4


def test_review_status_includes_merchant_links_pending() -> None:
    """merchant_links_pending is counted and included in total."""
    svc = _make_review_service(merchant_links=7)
    status = svc.status()

    assert status.merchant_links_pending == 7
    assert status.total == 7


def test_review_status_total_sums_all_four_queues() -> None:
    """Total = matches_pending + categorize_pending + account_links_pending + merchant_links_pending."""
    svc = _make_review_service(
        matches=2, categorize=3, account_links=5, merchant_links=4
    )
    status = svc.status()

    assert status.total == 14  # 2 + 3 + 5 + 4
