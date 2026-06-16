"""Tests for ReviewService — unified queue counts."""

from unittest.mock import MagicMock

from moneybin.services.review_service import ReviewService, ReviewStatus


def test_review_status_counts_both_queues() -> None:
    match_service = MagicMock()
    match_service.count_pending.return_value = 3
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = 12
    links_service = MagicMock()
    links_service.count_pending.return_value = 0

    svc = ReviewService(
        match_service=match_service,
        categorize_service=cat_service,
        account_links_service=links_service,
    )
    status = svc.status()

    assert isinstance(status, ReviewStatus)
    assert status.matches_pending == 3
    assert status.categorize_pending == 12
    assert status.account_links_pending == 0
    assert status.total == 15


def test_review_status_zero_queues() -> None:
    match_service = MagicMock()
    match_service.count_pending.return_value = 0
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = 0
    links_service = MagicMock()
    links_service.count_pending.return_value = 0

    svc = ReviewService(
        match_service=match_service,
        categorize_service=cat_service,
        account_links_service=links_service,
    )
    status = svc.status()

    assert status.total == 0
    assert status.matches_pending == 0
    assert status.categorize_pending == 0
    assert status.account_links_pending == 0


def test_review_status_delegates_to_services() -> None:
    """Each service method is called exactly once."""
    match_service = MagicMock()
    match_service.count_pending.return_value = 5
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = 7
    links_service = MagicMock()
    links_service.count_pending.return_value = 2

    svc = ReviewService(
        match_service=match_service,
        categorize_service=cat_service,
        account_links_service=links_service,
    )
    svc.status()

    match_service.count_pending.assert_called_once()
    cat_service.count_uncategorized.assert_called_once()
    links_service.count_pending.assert_called_once()


def test_review_status_includes_account_links_pending() -> None:
    """account_links_pending is counted and included in total."""
    match_service = MagicMock()
    match_service.count_pending.return_value = 0
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = 0
    links_service = MagicMock()
    links_service.count_pending.return_value = 4

    svc = ReviewService(
        match_service=match_service,
        categorize_service=cat_service,
        account_links_service=links_service,
    )
    status = svc.status()

    assert status.account_links_pending == 4
    assert status.total == 4


def test_review_status_total_sums_all_three_queues() -> None:
    """Total = matches_pending + categorize_pending + account_links_pending."""
    match_service = MagicMock()
    match_service.count_pending.return_value = 2
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = 3
    links_service = MagicMock()
    links_service.count_pending.return_value = 5

    svc = ReviewService(
        match_service=match_service,
        categorize_service=cat_service,
        account_links_service=links_service,
    )
    status = svc.status()

    assert status.total == 10  # 2 + 3 + 5
