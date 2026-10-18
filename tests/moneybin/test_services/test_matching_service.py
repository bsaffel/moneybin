"""Tests for the MatchingService facade."""

from unittest.mock import MagicMock, patch

import pytest

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.matching.engine import MatchResult, MatchRunError
from moneybin.services.matching_service import MatchingService
from moneybin.tables import MATCH_DECISIONS


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
def test_run_restates_after_committed_decision_changes(result: MatchResult) -> None:
    """A clean matcher run hands its result to the match-run restate."""
    db = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch("moneybin.services.restate.restate_after_match_run") as restate,
    ):
        matcher_cls.return_value.run.return_value = result

        assert MatchingService(db).run(auto_accept_transfers=True) is result

    restate.assert_called_once_with(db, result)


def test_run_restates_after_partial_failure() -> None:
    """A failed matcher run restates the partial effects that committed."""
    db = MagicMock()
    partial = MatchResult(transfers_retired=1)
    failure = MatchRunError(RuntimeError("tier 4 boom"), partial=partial)
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch("moneybin.services.restate.restate_after_match_run") as restate,
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
        "restatement failed", code=error_codes.REFRESH_MODEL_FAILED
    )
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch(
            "moneybin.services.restate.restate_after_match_run",
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


@pytest.mark.parametrize(
    ("result", "restated"),
    [
        (MatchResult(auto_merged=1), True),
        (MatchResult(accepted_transfers=1), True),
        (MatchResult(transfers_retired=1), True),
        (MatchResult(pending_review=1, pending_transfers=1), False),
    ],
    ids=["dedup-merge", "accepted-transfer", "retired-transfer", "pending-only"],
)
def test_run_restates_match_decisions_only_after_an_accept_or_retire(
    result: MatchResult, restated: bool
) -> None:
    """A dedup-only run restates too: merged transactions re-key the ledger."""
    db = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch("moneybin.services.restate.restate_after_write") as restate,
    ):
        matcher_cls.return_value.run.return_value = result

        assert MatchingService(db).run() is result

    if restated:
        restate.assert_called_once_with(
            db, [MATCH_DECISIONS], committed_change="match decision"
        )
    else:
        restate.assert_not_called()


def test_run_skips_the_restate_when_the_caller_transforms_next() -> None:
    """``restate=False`` leaves the rebuild to the caller's own transform."""
    db = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls,
        patch("moneybin.services.matching_service.seed_source_priority"),
        patch("moneybin.services.restate.restate_after_write") as restate,
    ):
        matcher_cls.return_value.run.return_value = MatchResult(auto_merged=1)

        MatchingService(db).run(restate=False)

    restate.assert_not_called()


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
    """MatchingService.undo() delegates to the repo, then restates the decisions."""
    db = MagicMock()
    with (
        patch(
            "moneybin.repositories.match_decisions_repo.MatchDecisionsRepo"
        ) as repo_cls,
        patch("moneybin.services.restate.restate_after_write") as restate,
    ):
        MatchingService(db).undo("match-123", reversed_by="user", actor="cli")
    repo_cls.assert_called_once_with(db)
    # ``in_outer_txn`` because the reversal and the curation restore that follows
    # it are one transaction, opened here rather than inside the repo.
    repo_cls.return_value.reverse.assert_called_once_with(
        "match-123", reversed_by="user", actor="cli", in_outer_txn=True
    )
    db.begin.assert_called_once_with()
    db.commit.assert_called_once_with()
    restate.assert_called_once_with(db, [MATCH_DECISIONS], committed_change="undo")


def test_undo_rolls_back_when_the_reversal_raises() -> None:
    """A refused reversal must not leave the transaction it opened dangling."""
    db = MagicMock()
    with (
        patch(
            "moneybin.repositories.match_decisions_repo.MatchDecisionsRepo"
        ) as repo_cls,
        patch("moneybin.services.restate.restate_after_write") as restate,
    ):
        repo_cls.return_value.reverse.side_effect = ValueError("already reversed")
        with pytest.raises(ValueError, match="already reversed"):
            MatchingService(db).undo("match-123")
    db.rollback.assert_called_once_with()
    db.commit.assert_not_called()
    restate.assert_not_called()


def test_undo_default_reversed_by_and_actor() -> None:
    """reversed_by defaults to 'user'; actor defaults to the 'system' surface."""
    db = MagicMock()
    with (
        patch(
            "moneybin.repositories.match_decisions_repo.MatchDecisionsRepo"
        ) as repo_cls,
        patch("moneybin.services.restate.restate_after_write"),
    ):
        MatchingService(db).undo("match-123")
    repo_cls.return_value.reverse.assert_called_once_with(
        "match-123", reversed_by="user", actor="system", in_outer_txn=True
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
