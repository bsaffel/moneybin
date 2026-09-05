"""Targeted restatement after mutable FX-accounting inputs change."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.engine import MatchResult
from moneybin.services.fx_accounting_refresh import (
    CommittedChange,
    restate_fx_accounting,
    restate_fx_accounting_after_match_run,
)
from moneybin.services.transform_service import ApplyResult, TransformService

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("result", "should_restate"),
    [
        (MatchResult(accepted_transfers=1), True),
        (MatchResult(transfers_retired=1), True),
        (MatchResult(auto_merged=1), False),
        (MatchResult(pending_transfers=1), False),
    ],
)
def test_match_run_restates_only_for_committed_transfer_changes(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    result: MatchResult,
    should_restate: bool,
) -> None:
    restate = MagicMock()
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting", restate
    )

    restate_fx_accounting_after_match_run(db, result)

    if should_restate:
        restate.assert_called_once_with(db, committed_change="match decision")
    else:
        restate.assert_not_called()


def test_restate_fx_accounting_restates_only_the_root_model(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    def model_presence(_db: Database) -> MagicMock:
        return MagicMock(missing=())

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.sqlmesh_registry.model_presence",
        model_presence,
    )
    restate = MagicMock(return_value=ApplyResult(applied=True, duration_seconds=0.1))
    monkeypatch.setattr(TransformService, "restate_models", restate)

    restate_fx_accounting(db)

    restate.assert_called_once_with(["core.bridge_currency_conversions"])


def test_restate_fx_accounting_is_a_noop_before_the_models_exist(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    def model_presence(_db: Database) -> MagicMock:
        return MagicMock(missing=("core.bridge_currency_conversions",))

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.sqlmesh_registry.model_presence",
        model_presence,
    )
    restate = MagicMock()
    monkeypatch.setattr(TransformService, "restate_models", restate)

    restate_fx_accounting(db)

    restate.assert_not_called()


def test_account_currency_change_restates_from_the_account_dimension(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    def model_presence(_db: Database) -> MagicMock:
        return MagicMock(missing=())

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.sqlmesh_registry.model_presence",
        model_presence,
    )
    restate = MagicMock(return_value=ApplyResult(applied=True, duration_seconds=0.1))
    monkeypatch.setattr(TransformService, "restate_models", restate)

    restate_fx_accounting(db, account_currency_changed=True)

    restate.assert_called_once_with(["core.dim_accounts"])


def test_restate_fx_accounting_reports_committed_write_when_restate_fails(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    def model_presence(_db: Database) -> MagicMock:
        return MagicMock(missing=())

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.sqlmesh_registry.model_presence",
        model_presence,
    )

    def fail_restatement(_self: TransformService, _models: list[str]) -> ApplyResult:
        return ApplyResult(applied=False, duration_seconds=0.1, error="PlanError")

    monkeypatch.setattr(
        TransformService,
        "restate_models",
        fail_restatement,
    )

    with pytest.raises(UserError) as caught:
        restate_fx_accounting(db)

    assert caught.value.code == error_codes.REFRESH_MODEL_FAILED
    assert "setting was saved" in str(caught.value).lower()


def test_restate_failure_reports_that_an_undo_was_committed(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    def model_presence(_db: Database) -> MagicMock:
        return MagicMock(missing=())

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.sqlmesh_registry.model_presence",
        model_presence,
    )

    def fail_restatement(_self: TransformService, _models: list[str]) -> ApplyResult:
        return ApplyResult(applied=False, duration_seconds=0.1, error="PlanError")

    monkeypatch.setattr(TransformService, "restate_models", fail_restatement)

    with pytest.raises(UserError) as caught:
        restate_fx_accounting(db, committed_change="undo")

    assert caught.value.code == error_codes.REFRESH_MODEL_FAILED
    assert "undo was committed" in str(caught.value).lower()


@pytest.mark.parametrize(
    ("committed_change", "wording"),
    [
        ("setting", "setting was saved"),
        ("match decision", "match decision was committed"),
        ("undo", "undo was committed"),
    ],
)
def test_catalog_failure_preserves_the_committed_change_context(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    committed_change: CommittedChange,
    wording: str,
) -> None:
    def fail_catalog(_db: Database) -> MagicMock:
        raise UserError(
            "Could not read the database model catalog.",
            code=error_codes.INFRA_CATALOG_UNAVAILABLE,
        )

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.sqlmesh_registry.model_presence",
        fail_catalog,
    )

    with pytest.raises(UserError) as caught:
        restate_fx_accounting(db, committed_change=committed_change)

    assert caught.value.code == error_codes.REFRESH_MODEL_FAILED
    assert wording in str(caught.value).lower()
