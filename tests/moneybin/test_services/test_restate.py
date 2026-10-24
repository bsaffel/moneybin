"""Restating materialized models after the app writes they read."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.application import MatchApplicationEffects, MatchStatusChange
from moneybin.matching.engine import MatchResult
from moneybin.metrics.registry import MATERIALIZED_RESTATE_TOTAL
from moneybin.services.restate import (
    CommittedChange,
    restate_after_match_effects,
    restate_after_match_run,
    restate_after_write,
)
from moneybin.services.transform_service import ApplyResult, TransformService
from moneybin.sqlmesh_registry import ModelPresence
from moneybin.tables import (
    ACCOUNT_SETTINGS,
    EXCHANGE_RATES,
    GSHEET_SEEDS,
    MATCH_DECISIONS,
    PDF_SEEDS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
    TRANSACTION_NOTES,
    USER_REPORTS,
)

pytestmark = pytest.mark.unit


def _presence(*, never_built: bool = False) -> ModelPresence:
    return ModelPresence(missing=(), built_beyond_init_count=0 if never_built else 1)


def _built(_db: Database) -> ModelPresence:
    return _presence()


def _unbuilt(_db: Database) -> ModelPresence:
    return _presence(never_built=True)


def _promoted(_db: Database) -> bool:
    return True


@pytest.fixture()
def restate_models(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """A built warehouse whose restate succeeds; returns the restate spy."""
    monkeypatch.setattr(
        "moneybin.services.restate.sqlmesh_registry.model_presence",
        _built,
    )
    monkeypatch.setattr("moneybin.services.restate._has_prod_environment", _promoted)
    spy = MagicMock(return_value=ApplyResult(applied=True, duration_seconds=0.0))
    monkeypatch.setattr(TransformService, "restate_models", spy)
    return spy


def test_restates_from_the_written_tables(
    db: Database, restate_models: MagicMock
) -> None:
    restate_after_write(
        db, [MATCH_DECISIONS, EXCHANGE_RATES], committed_change="match decision"
    )
    restate_models.assert_called_once_with([
        "app.match_decisions",
        "raw.exchange_rates",
    ])


def test_read_through_and_unread_tables_restate_nothing(
    db: Database, restate_models: MagicMock
) -> None:
    restate_after_write(
        db, [TRANSACTION_NOTES, USER_REPORTS], committed_change="setting"
    )
    restate_models.assert_not_called()


def test_a_mixed_set_restates_only_its_restating_tables(
    db: Database, restate_models: MagicMock
) -> None:
    restate_after_write(
        db, [TRANSACTION_NOTES, ACCOUNT_SETTINGS], committed_change="setting"
    )
    restate_models.assert_called_once_with(["app.account_settings"])


def test_a_table_no_external_model_declares_restates_nothing(
    db: Database, restate_models: MagicMock
) -> None:
    """Only a declared external model can be a root; naming another fails the plan."""
    restate_after_write(db, [GSHEET_SEEDS, PDF_SEEDS], committed_change="purge")
    restate_models.assert_not_called()


def test_a_revert_restates_only_its_declared_external_models(
    db: Database, restate_models: MagicMock
) -> None:
    restate_after_write(
        db,
        [PDF_SEEDS, TABULAR_TRANSACTIONS, TABULAR_ACCOUNTS],
        committed_change="import revert",
    )
    restate_models.assert_called_once_with([
        "raw.tabular_transactions",
        "raw.tabular_accounts",
    ])


def test_an_unbuilt_warehouse_has_nothing_to_restate(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "moneybin.services.restate.sqlmesh_registry.model_presence",
        _unbuilt,
    )
    spy = MagicMock()
    monkeypatch.setattr(TransformService, "restate_models", spy)

    restate_after_write(db, [MATCH_DECISIONS], committed_change="match decision")

    spy.assert_not_called()


def test_relations_sqlmesh_never_promoted_have_nothing_to_restate(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tables built outside SQLMesh look built in the catalog but carry no state."""
    monkeypatch.setattr(
        "moneybin.services.restate.sqlmesh_registry.model_presence", _built
    )
    spy = MagicMock()
    monkeypatch.setattr(TransformService, "restate_models", spy)

    restate_after_write(db, [MATCH_DECISIONS], committed_change="match decision")

    spy.assert_not_called()


@pytest.mark.parametrize(
    ("change", "wording"),
    [
        ("setting", "setting was saved"),
        ("undo", "undo was committed"),
        ("balance assertion", "balance assertion was committed"),
    ],
)
def test_a_failed_restate_reports_the_committed_write(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    change: CommittedChange,
    wording: str,
) -> None:
    monkeypatch.setattr(
        "moneybin.services.restate.sqlmesh_registry.model_presence",
        _built,
    )
    monkeypatch.setattr("moneybin.services.restate._has_prod_environment", _promoted)
    monkeypatch.setattr(
        TransformService,
        "restate_models",
        MagicMock(
            return_value=ApplyResult(
                applied=False, duration_seconds=0.0, error="PlanError"
            )
        ),
    )

    with pytest.raises(UserError) as caught:
        restate_after_write(db, [MATCH_DECISIONS], committed_change=change)

    assert caught.value.code == error_codes.REFRESH_MODEL_FAILED
    assert wording in str(caught.value).lower()
    assert caught.value.hint is not None
    assert "moneybin refresh" in caught.value.hint


def test_a_catalog_failure_keeps_the_committed_change(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _fail(_db: Database) -> ModelPresence:
        raise UserError("catalog unreadable", code=error_codes.REFRESH_MODEL_FAILED)

    monkeypatch.setattr(
        "moneybin.services.restate.sqlmesh_registry.model_presence", _fail
    )

    with pytest.raises(UserError) as caught:
        restate_after_write(db, [MATCH_DECISIONS], committed_change="undo")

    assert "undo was committed" in str(caught.value).lower()


def test_outcomes_are_counted_per_input(
    db: Database, restate_models: MagicMock
) -> None:
    before = MATERIALIZED_RESTATE_TOTAL.labels(
        input="app.match_decisions", outcome="applied"
    )._value.get()  # type: ignore[reportPrivateUsage]  # reading prometheus internals

    restate_after_write(db, [MATCH_DECISIONS], committed_change="match decision")

    after = MATERIALIZED_RESTATE_TOTAL.labels(
        input="app.match_decisions", outcome="applied"
    )._value.get()  # type: ignore[reportPrivateUsage]  # reading prometheus internals
    assert after == before + 1


@pytest.mark.parametrize(
    ("result", "should_restate"),
    [
        (MatchResult(auto_merged=1), True),
        (MatchResult(accepted_transfers=1), True),
        (MatchResult(transfers_retired=1), True),
        (MatchResult(pending_review=1), False),
        (MatchResult(pending_transfers=1), False),
    ],
)
def test_a_match_run_restates_when_it_accepted_or_retired_a_decision(
    db: Database, restate_models: MagicMock, result: MatchResult, should_restate: bool
) -> None:
    restate_after_match_run(db, result)

    assert restate_models.called is should_restate


def _change(prior: str, effective: str, *, changed: bool = True) -> MatchStatusChange:
    return MatchStatusChange(
        match_id="m1",
        requested_status="accepted" if effective == "accepted" else "rejected",
        prior_status=prior,
        effective_status=effective,
        changed=changed,
    )


@pytest.mark.parametrize(
    ("change", "should_restate"),
    [
        (_change("pending", "accepted"), True),
        (_change("accepted", "rejected"), True),
        (_change("pending", "rejected"), False),
        (_change("accepted", "accepted", changed=False), False),
    ],
)
def test_match_effects_restate_only_when_acceptance_moved(
    db: Database,
    restate_models: MagicMock,
    change: MatchStatusChange,
    should_restate: bool,
) -> None:
    effects = MatchApplicationEffects(changes=(change,), reconciliation_reversals=None)

    restate_after_match_effects(db, effects)

    assert restate_models.called is should_restate
