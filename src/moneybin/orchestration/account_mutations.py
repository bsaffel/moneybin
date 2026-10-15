"""Finish local processing when account mutations change evidence eligibility."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from moneybin import error_codes, sqlmesh_registry
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.orchestration.refresh import refresh
from moneybin.services.account_service import AccountService, AccountSettings
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import current_operation_id, operation
from moneybin.services.undo_service import UndoResult, UndoService


def _finish_classification(
    db: Database,
    operation_id: str,
    *,
    actor: str,
    committed_change: Literal["setting", "undo"],
    prior_audit_ids: frozenset[str] = frozenset(),
) -> None:
    try:
        events = AuditService(db).events_for_operation(operation_id)
        changed = any(
            event.audit_id not in prior_audit_ids
            and event.target_schema == "app"
            and event.target_table == "account_settings"
            and any(
                (event.before_value or {}).get(field)
                != (event.after_value or {}).get(field)
                for field in ("account_type", "account_subtype")
            )
            for event in events
        )
        if not changed or sqlmesh_registry.model_presence(db).never_built:
            return
        # The service has already restated dim_accounts, so matching now sees
        # the changed eligibility. Keep this cascade local to the warehouse.
        result = refresh(db, steps=["match", "transform", "categorize"], actor=actor)
        stages = [
            result.stage(step)
            for step in ("match", "investment_match", "transform", "categorize")
        ]
        if result.applied and all(
            stage is not None and stage.ran and stage.error is None for stage in stages
        ):
            return
    except Exception as exc:
        raise _committed_error(committed_change) from exc
    raise _committed_error(committed_change)


def _committed_error(committed_change: Literal["setting", "undo"]) -> UserError:
    verb = "was saved" if committed_change == "setting" else "was committed"
    return UserError(
        f"The {committed_change} {verb}, but account processing could not be completed.",
        code=error_codes.REFRESH_MODEL_FAILED,
        hint="Run 'moneybin refresh' before relying on account classifications or totals.",
    )


def update_account_settings(
    db: Database,
    account_id: str,
    *,
    actor: str,
    official_name: str | None | object = None,
    last_four: str | None | object = None,
    account_subtype: str | None | object = None,
    account_type: str | None | object = None,
    holder_category: str | None | object = None,
    currency_code: str | None | object = None,
    credit_limit: Decimal | None | object = None,
    display_name: str | None | object = None,
    default_cost_basis_method: str | None | object = None,
    include_in_net_worth: bool | None = None,
    archived: bool | None = None,
) -> tuple[AccountSettings, list[dict[str, str]]]:
    """Save settings and process newly eligible or excluded account evidence."""
    with operation(current_operation_id()) as operation_id:
        prior_audit_ids = frozenset(
            event.audit_id
            for event in AuditService(db).events_for_operation(operation_id)
        )
        result = AccountService(db).settings_update(
            account_id,
            actor=actor,
            official_name=official_name,
            last_four=last_four,
            account_subtype=account_subtype,
            account_type=account_type,
            holder_category=holder_category,
            currency_code=currency_code,
            credit_limit=credit_limit,
            display_name=display_name,
            default_cost_basis_method=default_cost_basis_method,
            include_in_net_worth=include_in_net_worth,
            archived=archived,
        )
        _finish_classification(
            db,
            operation_id,
            actor=actor,
            committed_change="setting",
            prior_audit_ids=prior_audit_ids,
        )
        return result


def undo_operation(
    db: Database,
    operation_id: str,
    *,
    actor: str,
    expected_audit_ids: tuple[str, ...] | None = None,
) -> UndoResult:
    """Reverse an operation and process any resulting account eligibility change."""
    result = UndoService(db).undo(
        operation_id, actor=actor, expected_audit_ids=expected_audit_ids
    )
    # Derived writes belong to the reversal, so undoing it also reverses them.
    with operation(result.undo_operation_id):
        _finish_classification(
            db, result.undo_operation_id, actor=actor, committed_change="undo"
        )
    return result
