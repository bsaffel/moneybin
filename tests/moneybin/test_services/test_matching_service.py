"""Tests for the MatchingService facade."""

from unittest.mock import MagicMock, patch

from moneybin.services.matching_service import MatchingService


def test_run_delegates_to_transaction_matcher() -> None:
    """MatchingService.run() should delegate to TransactionMatcher.run()."""
    db = MagicMock()
    fake_result = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
    ):
        matcher_cls.return_value.run.return_value = fake_result
        svc = MatchingService(db)
        result = svc.run()
    matcher_cls.assert_called_once()
    matcher_cls.return_value.run.assert_called_once()
    assert result is fake_result


def test_uses_default_settings_when_omitted() -> None:
    """When settings is omitted, MatchingService should use get_settings().matching."""
    db = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as cls,
        patch("moneybin.services.matching_service.get_settings") as gs,
        patch("moneybin.services.matching_service.seed_source_priority") as ssp,
    ):
        gs.return_value.matching = "MATCHING_SETTINGS"
        MatchingService(db).run()
    args, kwargs = cls.call_args
    assert "MATCHING_SETTINGS" in args or kwargs.get("settings") == "MATCHING_SETTINGS"
    ssp.assert_called_once_with(db, "MATCHING_SETTINGS")
