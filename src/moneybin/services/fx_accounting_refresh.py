"""Keep materialized FX accounting coherent with mutable App inputs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

from moneybin import error_codes, sqlmesh_registry
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.transform_service import TransformService
from moneybin.tables import MATCH_DECISIONS

if TYPE_CHECKING:
    from moneybin.matching.application import MatchApplicationEffects
    from moneybin.matching.engine import MatchResult

_FX_ACCOUNTING_ROOT_MODEL = "core.bridge_currency_conversions"
_ACCOUNT_ROOT_MODEL = "core.dim_accounts"


CommittedChange = Literal["setting", "match decision", "undo"]


def _committed_refresh_error(*, committed_change: CommittedChange) -> UserError:
    verb = "was saved" if committed_change == "setting" else "was committed"
    return UserError(
        f"The {committed_change} {verb}, but derived FX accounting could not be rebuilt.",
        code=error_codes.REFRESH_MODEL_FAILED,
        hint="Run 'moneybin refresh' before relying on FX lots or gains.",
    )


def restate_fx_accounting(
    db: Database,
    *,
    account_currency_changed: bool = False,
    committed_change: CommittedChange = "setting",
) -> None:
    """Rebuild FX accounting after a relevant committed App mutation."""
    root_model = (
        _ACCOUNT_ROOT_MODEL if account_currency_changed else _FX_ACCOUNTING_ROOT_MODEL
    )
    try:
        missing_models = sqlmesh_registry.model_presence(db).missing
    except UserError as exc:
        raise _committed_refresh_error(committed_change=committed_change) from exc
    if root_model in missing_models:
        return

    result = TransformService(db).restate_models([root_model])
    if result.applied:
        return

    raise _committed_refresh_error(committed_change=committed_change)


def _has_transfer_decision(db: Database, match_ids: Sequence[str]) -> bool:
    """Return whether any named decision is a Transfer Decision."""
    if not match_ids:
        return False
    placeholders = ", ".join("?" for _match_id in match_ids)
    row = db.execute(
        f"""
        SELECT 1
        FROM {MATCH_DECISIONS.full_name}
        WHERE match_id IN ({placeholders}) AND match_type = 'transfer'
        LIMIT 1
        """,  # noqa: S608  # placeholders and TableRef are code-supplied
        list(match_ids),
    ).fetchone()
    return row is not None


def restate_fx_accounting_after_match_effects(
    db: Database, effects: MatchApplicationEffects
) -> None:
    """Rebuild FX rows when committed match effects changed trusted evidence."""
    accepted_ids = tuple(
        change.match_id
        for change in effects.changes
        if change.changed and change.effective_status == "accepted"
    )
    if effects.standing_transfers_retired or _has_transfer_decision(db, accepted_ids):
        restate_fx_accounting(db, committed_change="match decision")


def restate_fx_accounting_after_match_undo(db: Database, match_id: str) -> None:
    """Rebuild FX rows after a committed direct Transfer Decision reversal."""
    if _has_transfer_decision(db, (match_id,)):
        restate_fx_accounting(db, committed_change="undo")


def restate_fx_accounting_after_match_run(db: Database, result: MatchResult) -> None:
    """Rebuild FX rows when a matcher run accepted or retired Transfers."""
    if result.accepted_transfers or result.transfers_retired:
        restate_fx_accounting(db, committed_change="match decision")
