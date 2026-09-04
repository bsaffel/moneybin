"""Tests for the MatchingService facade."""

from unittest.mock import MagicMock, patch

import pytest

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.matching.engine import MatchResult, MatchRunError
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


@pytest.mark.parametrize(
    "result",
    [MatchResult(transfers_retired=1), MatchResult(accepted_transfers=1)],
)
def test_run_restates_fx_after_committed_transfer_changes(result: MatchResult) -> None:
    """A clean matcher run restates FX after accepting or retiring a transfer."""
    db = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch(
            "moneybin.services.fx_accounting_refresh."
            "restate_fx_accounting_after_match_run"
        ) as restate,
    ):
        matcher_cls.return_value.run.return_value = result

        assert MatchingService(db).run(auto_accept_transfers=True) is result

    restate.assert_called_once_with(db, result)


def test_run_restates_fx_after_partial_transfer_failure() -> None:
    """A failed matcher run restates FX when its partial effects committed."""
    db = MagicMock()
    partial = MatchResult(transfers_retired=1)
    failure = MatchRunError(RuntimeError("tier 4 boom"), partial=partial)
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch(
            "moneybin.services.fx_accounting_refresh."
            "restate_fx_accounting_after_match_run"
        ) as restate,
    ):
        matcher_cls.return_value.run.side_effect = failure

        with pytest.raises(MatchRunError) as caught:
            MatchingService(db).run()

    assert caught.value is failure
    restate.assert_called_once_with(db, partial)


def test_run_preserves_partial_outcome_when_restatement_also_fails() -> None:
    """A dual failure keeps committed counts and both underlying failures."""
    db = MagicMock()
    partial = MatchResult(transfers_retired=1)
    match_failure = RuntimeError("tier 4 boom")
    failure = MatchRunError(match_failure, partial=partial)
    restatement_failure = UserError(
        "FX restatement failed", code=error_codes.REFRESH_MODEL_FAILED
    )
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch(
            "moneybin.services.fx_accounting_refresh."
            "restate_fx_accounting_after_match_run",
            side_effect=restatement_failure,
        ),
    ):
        matcher_cls.return_value.run.side_effect = failure

        with pytest.raises(MatchRunError) as caught:
            MatchingService(db).run()

    assert caught.value is failure
    assert caught.value.partial is partial
    assert caught.value.cause is match_failure
    assert caught.value.restatement_error is restatement_failure
    assert caught.value.__cause__ is restatement_failure


def test_run_skips_fx_restatement_without_transfer_changes() -> None:
    """Dedup-only matcher effects do not rebuild FX accounting."""
    db = MagicMock()
    result = MatchResult(auto_merged=1)
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch(
            "moneybin.services.fx_accounting_refresh."
            "restate_fx_accounting_after_match_run"
        ) as restate,
    ):
        matcher_cls.return_value.run.return_value = result

        assert MatchingService(db).run() is result

    restate.assert_called_once_with(db, result)


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
