"""Post-load refresh pipeline — a top-level MoneyBin domain concept.

"Refresh" means: update everything in the data warehouse based on the
latest new data that loaders wrote to ``raw.*``. It is the operational
verb that wraps three source-agnostic steps:

1. **Cross-source matching** — :class:`TransactionMatcher` resolves
   identity across `source_type='ofx' | 'csv' | 'plaid' | ...` so the
   same transaction observed by multiple loaders collapses to one row.
2. **SQLMesh apply** — :class:`TransformService` rebuilds derived
   ``core.*`` and ``reports.*`` models from current raw state. This is
   the only step that surfaces a structured error in the result.
3. **Deterministic categorization** — :class:`CategorizationService`
   applies user rules + merchant exemplars to uncategorized rows, with
   source-precedence enforcement so user-manual categories are never
   overwritten.

Matching and categorization are best-effort: failures are logged and
swallowed so a partial pipeline still leaves raw rows durable and core
tables rebuilt. Only SQLMesh failures propagate via
``RefreshResult.error``.

Invoked by any service whose loaders wrote to ``raw.*``:
``ImportService`` (file imports), ``InboxService`` (inbox drain),
``SyncService`` (Plaid pull). Mutations to ``app.*`` outside loaders
(annotations, rules, budgets, sync-connection state) do NOT invoke
refresh — they don't change the data-warehouse state refresh rebuilds.

Performance: dominated by SQLMesh apply (typically 5–30s; the
``sqlmesh.Context`` init alone is 2–5s). Matching and categorization
add tens-to-hundreds of milliseconds combined. High-frequency callers
(scheduled syncs, webhooks) should pass ``refresh=False`` to their
loader entry point and run refresh on a separate schedule. See
``docs/specs/sync-plaid.md`` Req 10.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from moneybin.database import Database
from moneybin.services.transform_service import TransformService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of a :func:`refresh` call.

    Fields describe the SQLMesh apply step specifically — the only step
    that can hard-fail. Matching and categorization are best-effort and
    log-only on failure; their outcomes are not surfaced here.
    """

    applied: bool
    duration_seconds: float | None
    error: str | None = None


_CANONICAL_STEPS: tuple[str, ...] = ("match", "transform", "categorize")


def refresh(db: Database, *, steps: list[str] | None = None) -> RefreshResult:
    """Run the post-load pipeline: matching → SQLMesh apply → categorization.

    When ``steps`` is None (default), the full cascade runs — same behavior
    as the pre-``steps`` signature, preserved for all existing callers.

    When ``steps`` is provided, only the named steps execute, in canonical
    order (``match`` → ``transform`` → ``categorize``) regardless of the
    input list's order. Dependencies enforce the order: categorize reads
    SQLMesh-built views, so running it after transform is mandatory; the
    parameter cannot reorder this.

    Skipping ``transform`` returns ``RefreshResult(applied=False,
    duration_seconds=None)`` without invoking the SQLMesh apply path —
    callers reading ``applied`` get an unambiguous "no apply happened"
    signal rather than a half-truthful "apply succeeded."

    Args:
        db: Database handle to run against.
        steps: Subset of ``("match", "transform", "categorize")`` to run.
            Defaults to all three when None.

    Raises:
        UserError(code="UNKNOWN_REFRESH_STEP"): if any element of ``steps``
            is not in the canonical set.

    See module docstring for the conceptual contract. Soft-fail variant:
    SQLMesh errors are returned in the result rather than raised, so
    callers can preserve already-loaded raw rows and surface the failure
    in their response envelope.
    """
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.errors import UserError  # noqa: PLC0415
    from moneybin.matching.engine import TransactionMatcher  # noqa: PLC0415
    from moneybin.matching.priority import seed_source_priority  # noqa: PLC0415

    if steps is not None:
        unknown = [s for s in steps if s not in _CANONICAL_STEPS]
        if unknown:
            raise UserError(
                f"Unknown refresh step(s): {', '.join(unknown)}",
                code="UNKNOWN_REFRESH_STEP",
                hint=f"known steps: {', '.join(_CANONICAL_STEPS)}",
            )

    requested: frozenset[str] = (
        frozenset(_CANONICAL_STEPS) if steps is None else frozenset(steps)
    )

    if "match" in requested:
        try:
            settings = get_settings().matching
            seed_source_priority(db, settings)
            matcher = TransactionMatcher(db, settings)
            match_result = matcher.run()
            if match_result.has_matches:
                logger.info(f"Matching: {match_result.summary()}")
                if match_result.has_pending:
                    logger.info(
                        "Run 'moneybin transactions review --type matches' when ready"
                    )
        except Exception:  # noqa: BLE001 — best-effort; first load may precede SQLMesh views
            logger.debug("Matching skipped (views may not exist yet)", exc_info=True)

    if "transform" not in requested:
        # Caller asked for a partial cascade that omits transform. Return
        # an "apply did not run" result so the envelope's applied=False
        # signal is honest. Categorize, if also requested, still runs
        # against whatever SQLMesh-built views are already on disk.
        if "categorize" in requested:
            _run_categorize_step(db)
        return RefreshResult(applied=False, duration_seconds=None)

    apply_result = TransformService(db).apply()
    if not apply_result.applied:
        return RefreshResult(
            applied=False,
            duration_seconds=apply_result.duration_seconds,
            error=apply_result.error,
        )

    if "categorize" in requested:
        _run_categorize_step(db)

    return RefreshResult(
        applied=True,
        duration_seconds=apply_result.duration_seconds,
    )


def _run_categorize_step(db: Database) -> None:
    """Best-effort categorization step. Failures log-only — never propagated."""
    from moneybin.services.auto_rule_service import AutoRuleService  # noqa: PLC0415
    from moneybin.services.categorization import CategorizationService  # noqa: PLC0415

    cat_start = time.monotonic()
    try:
        service = CategorizationService(db)
        stats = service.categorize_pending()
        if stats["total"] > 0:
            logger.info(
                f"Auto-categorized {stats['total']} transactions "
                f"({stats['merchant']} merchant, {stats['rule']} rule)"
            )
        pending = AutoRuleService(db).stats().pending_proposals
        if pending:
            logger.info(f"  {pending} new auto-rule proposals")
            logger.info(
                "  💡 Run 'moneybin transactions categorize auto review' "
                "to review proposed rules"
            )
    except Exception:  # noqa: BLE001 — best-effort; surfaces in logs only
        logger.debug("Categorization skipped (tables may not exist yet)", exc_info=True)
    logger.debug(f"Categorization step finished in {time.monotonic() - cat_start:.2f}s")
