"""Keep materialized models coherent with the ``app.*`` writes they read.

Every writer of an input :mod:`moneybin.sqlmesh_freshness` classifies as
restating calls :func:`restate_after_write` once its transaction commits.
SQLMesh accepts an external model as a restate root and rebuilds every model
downstream of it, so no writer names a model, and a new materialized model is
covered the day it lands. See docs/specs/materialization-freshness-contract.md.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Literal

from moneybin import error_codes, sqlmesh_registry
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import MATERIALIZED_RESTATE_TOTAL
from moneybin.sqlmesh_freshness import restates
from moneybin.tables import MATCH_DECISIONS, TableRef

if TYPE_CHECKING:
    from moneybin.matching.application import MatchApplicationEffects
    from moneybin.matching.engine import MatchResult

logger = logging.getLogger(__name__)

CommittedChange = Literal[
    "setting",
    "exchange rate",
    "match decision",
    "undo",
    "account link",
    "balance assertion",
    "security",
    "price",
    "lot selection",
    "manual transaction",
    "purge",
    "import revert",
]


def _committed_refresh_error(committed_change: CommittedChange) -> UserError:
    verb = (
        "was saved"
        if committed_change in ("setting", "exchange rate", "price")
        else "was committed"
    )
    return UserError(
        f"The {committed_change} {verb}, but the reports that read it could not "
        "be rebuilt.",
        code=error_codes.REFRESH_MODEL_FAILED,
        hint="Run 'moneybin refresh' before relying on balances, holdings, "
        "lots or gains.",
    )


def restate_after_write(
    db: Database,
    inputs: Collection[TableRef],
    *,
    committed_change: CommittedChange,
) -> None:
    """Rebuild every materialized model that reads one of *inputs*.

    Read-through inputs are dropped, and so is a table no external model
    declares (``raw.gsheet_seeds``, ``raw.pdf_seeds``), so a caller that wrote
    a mixed set may pass all of it. A warehouse that was never built has
    nothing to rebuild. Raises the coded ``REFRESH_MODEL_FAILED`` error when
    the rebuild fails; the write itself has already committed.
    """
    roots = list(
        dict.fromkeys(
            t.full_name for t in inputs if restates(t) and _is_external_model(t)
        )
    )
    if not roots:
        return
    label = ",".join(sorted(roots))
    try:
        presence = sqlmesh_registry.model_presence(db)
    except UserError as exc:
        _record(roots, "failed")
        raise _committed_refresh_error(committed_change) from exc
    if presence.never_built or not _has_prod_environment(db):
        _record(roots, "unbuilt")
        return

    # Function-local: the transform stack imports SQLMesh.
    from moneybin.services.transform_service import TransformService

    logger.info(f"Restating models downstream of {label}")
    if TransformService(db).restate_models(roots).applied:
        _record(roots, "applied")
        return
    _record(roots, "failed")
    raise _committed_refresh_error(committed_change)


def _is_external_model(table: TableRef) -> bool:
    """Whether SQLMesh can take *table* as a restate root: only a declared one."""
    return table.full_name in sqlmesh_registry.external_model_names()


def _has_prod_environment(db: Database) -> bool:
    """Whether SQLMesh has ever promoted a build; without one there is no model to restate.

    The catalog alone cannot say: relations created outside SQLMesh (a profile
    rebuilt from DDL, a test fixture) look built but carry no SQLMesh state.
    """
    exists = db.execute(
        """
        SELECT 1 FROM duckdb_tables()
        WHERE database_name = 'moneybin' AND schema_name = 'sqlmesh'
          AND table_name = '_environments'
        """
    ).fetchone()
    if exists is None:
        return False
    return (
        db.execute(
            "SELECT 1 FROM sqlmesh._environments WHERE name = 'prod' LIMIT 1"
        ).fetchone()
        is not None
    )


def _record(roots: Sequence[str], outcome: str) -> None:
    for root in roots:
        try:
            MATERIALIZED_RESTATE_TOTAL.labels(input=root, outcome=outcome).inc()
        except Exception as exc:  # telemetry must not mask the restate's outcome
            logger.warning(f"Could not record the restate: {type(exc).__name__}")


def restate_after_match_effects(db: Database, effects: MatchApplicationEffects) -> None:
    """Rebuild after committed effects that moved a decision into or out of accepted.

    Models read only accepted, unreversed decisions, so a pending decision
    rejected, or one left as it was, changes nothing downstream.
    """
    if effects.standing_transfers_retired or any(
        change.changed and "accepted" in (change.prior_status, change.effective_status)
        for change in effects.changes
    ):
        restate_after_write(db, [MATCH_DECISIONS], committed_change="match decision")


def restate_after_match_run(db: Database, result: MatchResult) -> None:
    """Rebuild after a matcher run that accepted or retired any decision."""
    if result.auto_merged or result.accepted_transfers or result.transfers_retired:
        restate_after_write(db, [MATCH_DECISIONS], committed_change="match decision")
