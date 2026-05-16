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


def refresh(db: Database) -> RefreshResult:
    """Run the post-load pipeline: matching → SQLMesh apply → categorization.

    See module docstring for the conceptual contract. Soft-fail variant:
    SQLMesh errors are returned in the result rather than raised, so
    callers can preserve already-loaded raw rows and surface the failure
    in their response envelope. Callers needing fail-loud semantics
    (single-file imports preserving the legacy exit-code contract)
    check ``result.error`` and raise themselves.
    """
    # Imports deferred to avoid pulling matching/categorization stacks
    # into cold-start paths that don't run refresh.
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.matching.engine import TransactionMatcher  # noqa: PLC0415
    from moneybin.matching.priority import seed_source_priority  # noqa: PLC0415
    from moneybin.services.auto_rule_service import AutoRuleService  # noqa: PLC0415
    from moneybin.services.categorization import (  # noqa: PLC0415
        CategorizationService,
    )

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

    apply_result = TransformService(db).apply()
    if not apply_result.applied:
        # Stop here: categorization runs against the same SQLMesh-built
        # views the apply was supposed to rebuild. Running it on a
        # half-built core would either no-op or produce inconsistent state.
        return RefreshResult(
            applied=False,
            duration_seconds=apply_result.duration_seconds,
            error=apply_result.error,
        )

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

    return RefreshResult(
        applied=True,
        duration_seconds=apply_result.duration_seconds,
    )
