"""Keep materialized FX accounting coherent with mutable App inputs."""

from __future__ import annotations

from moneybin import error_codes, sqlmesh_registry
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.transform_service import TransformService

_FX_ACCOUNTING_ROOT_MODEL = "core.bridge_currency_conversions"
_ACCOUNT_ROOT_MODEL = "core.dim_accounts"


def _committed_refresh_error(*, undo_committed: bool) -> UserError:
    committed_change = "undo was committed" if undo_committed else "setting was saved"
    return UserError(
        f"The {committed_change}, but derived FX accounting could not be rebuilt.",
        code=error_codes.REFRESH_MODEL_FAILED,
        hint="Run 'moneybin refresh' before relying on FX lots or gains.",
    )


def restate_fx_accounting(
    db: Database,
    *,
    account_currency_changed: bool = False,
    undo_committed: bool = False,
) -> None:
    """Rebuild FX accounting after a relevant committed App mutation."""
    root_model = (
        _ACCOUNT_ROOT_MODEL if account_currency_changed else _FX_ACCOUNTING_ROOT_MODEL
    )
    try:
        missing_models = sqlmesh_registry.model_presence(db).missing
    except UserError as exc:
        raise _committed_refresh_error(undo_committed=undo_committed) from exc
    if root_model in missing_models:
        return

    result = TransformService(db).restate_models([root_model])
    if result.applied:
        return

    raise _committed_refresh_error(undo_committed=undo_committed)
