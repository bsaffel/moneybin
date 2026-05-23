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
    # actor forwards to the matcher (defaults to the 'system' surface).
    assert matcher_cls.call_args.kwargs.get("actor") == "system"
    matcher_cls.return_value.run.assert_called_once()
    assert result is fake_result


def test_auto_accept_transfers_passed_through() -> None:
    """auto_accept_transfers=True must reach TransactionMatcher.run()."""
    db = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
    ):
        MatchingService(db).run(auto_accept_transfers=True)
    matcher_cls.return_value.run.assert_called_once_with(auto_accept_transfers=True)


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


def test_undo_delegates_to_match_decisions_repo() -> None:
    """MatchingService.undo() should delegate to MatchDecisionsRepo.reverse()."""
    db = MagicMock()
    with patch(
        "moneybin.repositories.match_decisions_repo.MatchDecisionsRepo"
    ) as repo_cls:
        MatchingService(db).undo("match-123", reversed_by="user", actor="cli")
    repo_cls.assert_called_once_with(db)
    repo_cls.return_value.reverse.assert_called_once_with(
        "match-123", reversed_by="user", actor="cli"
    )


def test_undo_default_reversed_by_and_actor() -> None:
    """reversed_by defaults to 'user'; actor defaults to the 'system' surface."""
    db = MagicMock()
    with patch(
        "moneybin.repositories.match_decisions_repo.MatchDecisionsRepo"
    ) as repo_cls:
        MatchingService(db).undo("match-123")
    repo_cls.return_value.reverse.assert_called_once_with(
        "match-123", reversed_by="user", actor="system"
    )


def test_get_log_delegates_to_get_match_log() -> None:
    """MatchingService.get_log() should delegate to persistence.get_match_log()."""
    db = MagicMock()
    expected = [{"match_id": "m1"}]
    with patch(
        "moneybin.services.matching_service.get_match_log", return_value=expected
    ) as fn:
        result = MatchingService(db).get_log(limit=10, match_type="dedup")
    fn.assert_called_once_with(db, limit=10, match_type="dedup")
    assert result == expected


def test_seed_priority_delegates_to_seed_source_priority() -> None:
    """MatchingService.seed_priority() runs the seed step in isolation."""
    db = MagicMock()
    with patch("moneybin.services.matching_service.seed_source_priority") as fn:
        MatchingService(db).seed_priority()
    fn.assert_called_once()
    assert fn.call_args.args[0] is db
