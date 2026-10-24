"""History retrieval rejects future dates before any broker operation."""

from datetime import UTC, date, datetime, tzinfo

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services import sync_service
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture
)


@pytest.fixture
def utc_today(monkeypatch: pytest.MonkeyPatch) -> date:
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            return datetime(2026, 10, 1, 0, 30, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(sync_service, "datetime", FrozenDatetime)
    return date(2026, 10, 1)


@pytest.mark.parametrize("history_start", ["2026-10-02", "2099-01-01"])
def test_future_history_selector_is_invalid_input(
    utc_today: date, history_start: str
) -> None:
    with pytest.raises(UserError) as caught:
        sync_service.validate_pull_selectors(history_start=history_start)
    assert caught.value.code == error_codes.MUTATION_INVALID_INPUT


def test_today_history_selector_is_allowed(utc_today: date) -> None:
    assert (
        sync_service.validate_pull_selectors(history_start=utc_today.isoformat())
        == utc_today
    )


@pytest.mark.parametrize("institution", [None, "Synthetic Bank"])
def test_future_history_pull_fails_before_remote_calls(
    db: Database, utc_today: date, institution: str | None
) -> None:
    client = _client(_envelope())
    with pytest.raises(UserError) as caught:
        _service(db, client).pull(
            institution=institution, history_start=date(2099, 1, 1), refresh=False
        )
    assert caught.value.code == error_codes.MUTATION_INVALID_INPUT
    assert not client.mock_calls
