"""Account eligibility changes finish local processing after durable mutations."""

from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.orchestration.account_mutations import (
    undo_operation,
    update_account_settings,
)
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services.account_service import AccountService
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.refresh_outcome import StageOutcome
from moneybin.services.transform_service import ApplyResult
from moneybin.sqlmesh_registry import ModelPresence
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture
def account_db(db: Database, monkeypatch: pytest.MonkeyPatch) -> Database:
    def model_presence(_db: Database) -> ModelPresence:
        return ModelPresence(missing=(), built_beyond_init_count=1)

    create_core_tables(db)
    db.execute("""INSERT INTO core.dim_accounts
        (account_id, account_type, institution_name, source_type)
        VALUES ('account', 'depository', 'Synthetic', 'ofx')""")
    monkeypatch.setattr(
        "moneybin.sqlmesh_registry.model_presence",
        model_presence,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.restate_models",
        MagicMock(return_value=ApplyResult(applied=True, duration_seconds=0.1)),
    )
    return db


@pytest.mark.parametrize("actor", ["cli", "mcp"])
def test_refresh_audit_belongs_to_setting_and_undo_operations(
    account_db: Database, monkeypatch: pytest.MonkeyPatch, actor: str
) -> None:
    def refresh(db: Database, *, steps: list[str], actor: str) -> RefreshResult:
        assert steps == ["match", "transform", "categorize"]
        AuditService(db).record_audit_event(
            action="refresh.test_marker",
            target=(None, None, None),
            before=None,
            after=None,
            actor=actor,
        )
        return RefreshResult(
            applied=True,
            duration_seconds=0.1,
            stages=tuple(
                StageOutcome(step=step, ran=True)
                for step in ("match", "investment_match", "transform", "categorize")
            ),
        )

    monkeypatch.setattr("moneybin.orchestration.account_mutations.refresh", refresh)
    with operation() as change_op:
        update_account_settings(
            account_db, "account", account_type="credit", actor=actor
        )
    result = undo_operation(account_db, change_op, actor=actor)
    for operation_id in (change_op, result.undo_operation_id):
        events = AuditService(account_db).events_for_operation(operation_id)
        assert {event.actor for event in events} == {actor}
        assert sum(event.action == "refresh.test_marker" for event in events) == 1
        assert any(event.target_table == "account_settings" for event in events)


@pytest.mark.parametrize(
    "failure", ["skipped", "stage_error", "apply_failed", "raised"]
)
@pytest.mark.parametrize("undo", [False, True], ids=["setting", "undo"])
def test_refresh_failure_reports_the_durable_classification_change(
    account_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    undo: bool,
) -> None:
    with operation() as change_op:
        AccountService(account_db).settings_update(
            "account", account_type="depository", actor="cli"
        )
    result = RefreshResult(
        applied=failure != "apply_failed",
        duration_seconds=0.1,
        stages=(
            StageOutcome(step="match", ran=failure != "skipped"),
            StageOutcome(step="investment_match", ran=True),
            StageOutcome(step="transform", ran=failure != "apply_failed"),
            StageOutcome(
                step="categorize",
                ran=True,
                error="failed" if failure == "stage_error" else None,
            ),
        ),
    )
    monkeypatch.setattr(
        "moneybin.orchestration.account_mutations.refresh",
        MagicMock(
            return_value=result,
            side_effect=RuntimeError("refresh failed") if failure == "raised" else None,
        ),
    )
    with pytest.raises(UserError, match="committed" if undo else "saved") as error:
        if undo:
            undo_operation(account_db, change_op, actor="cli")
        else:
            update_account_settings(
                account_db, "account", account_type="credit", actor="cli"
            )
    assert error.value.code == "refresh_model_failed"
    assert account_db.execute(
        "SELECT account_type FROM app.account_settings WHERE account_id = 'account'"
    ).fetchone() == (None if undo else ("credit",))


@pytest.mark.parametrize("kind", ["unchanged", "display_only", "never_built"])
def test_settings_without_new_warehouse_eligibility_do_not_refresh(
    account_db: Database, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    AccountService(account_db).settings_update(
        "account", account_type="credit", actor="cli"
    )
    if kind == "never_built":

        def model_presence(_db: Database) -> ModelPresence:
            return ModelPresence(missing=(), built_beyond_init_count=0)

        monkeypatch.setattr(
            "moneybin.sqlmesh_registry.model_presence",
            model_presence,
        )
    monkeypatch.setattr(
        "moneybin.orchestration.account_mutations.refresh",
        MagicMock(side_effect=AssertionError("unnecessary pipeline execution")),
    )
    settings, _ = update_account_settings(
        account_db,
        "account",
        actor="cli",
        account_type="depository" if kind == "never_built" else "credit",
        display_name="New name" if kind == "display_only" else None,
    )
    assert settings.account_type == (
        "depository" if kind == "never_built" else "credit"
    )
    if kind == "display_only":
        assert settings.display_name == "New name"


@pytest.mark.parametrize("actor", ["cli", "mcp"])
def test_settings_wrapper_persists_and_clears_investment_source(
    account_db: Database, actor: str
) -> None:
    from moneybin.services.account_service import CLEAR

    settings, warnings = update_account_settings(
        account_db, "account", actor=actor, investment_source_type="manual"
    )
    assert settings.investment_source_type == "manual"
    assert settings.investment_source_type_changed_at is not None
    assert warnings == []
    cleared, _ = update_account_settings(
        account_db, "account", actor=actor, investment_source_type=CLEAR
    )
    assert cleared.investment_source_type is None
    assert cleared.investment_source_type_changed_at is not None
    assert (
        cleared.investment_source_type_changed_at
        >= settings.investment_source_type_changed_at
    )
