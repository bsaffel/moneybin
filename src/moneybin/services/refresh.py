"""Post-load refresh pipeline — a top-level MoneyBin domain concept.

"Refresh" means: update everything in the data warehouse based on the
latest new data that loaders wrote to ``raw.*``. It is the operational
verb that wraps three source-agnostic steps:

1. **Cross-source matching** — :class:`TransactionMatcher` resolves
   identity across `source_type='ofx' | 'csv' | 'plaid' | ...` so the
   same transaction observed by multiple loaders collapses to one row.
2. **SQLMesh apply** — :class:`TransformService` rebuilds derived
   ``core.*`` and ``reports.*`` models from current raw state. This is
   the only step that can hard-fail the call (``RefreshResult.error``);
   the others surface crashes without aborting (see below).
3. **Deterministic categorization** — :class:`CategorizationService`
   applies user rules + merchant exemplars to uncategorized rows, with
   source-precedence enforcement so user-manual categories are never
   overwritten.

Matching and categorization are best-effort: a stage failure never aborts
the pipeline, so a partial run still leaves raw rows durable and core
tables rebuilt. A real crash in either is surfaced (logged at ERROR and
returned in ``RefreshResult.matching_error`` / ``categorization_error``);
a missing-view precondition on first load is logged at DEBUG and not
surfaced. Only SQLMesh apply failures set ``RefreshResult.error``.

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
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import duckdb

from moneybin.database import Database
from moneybin.services.transform_service import TransformService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SelfHealRecord:
    """One self-heal recipe execution during refresh.

    Populated by the self-heal safelist (M2D PR 7, not yet implemented);
    the carrier ships here so ``RefreshResult``'s shape is stable for
    agents before the safelist lands.
    """

    recipe_id: str
    rows_affected: int
    operation_id: str
    timestamp: str


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of a :func:`refresh` call.

    ``error`` describes the SQLMesh apply step — the only step that can
    hard-fail. ``matching_error`` / ``categorization_error`` surface real
    crashes in the best-effort matcher / categorizer steps; a missing-view
    precondition on first load (before SQLMesh apply built the views) is
    NOT a crash and leaves them ``None``. ``self_heal_actions`` lists
    self-heal recipes that ran (empty until the M2D self-heal safelist
    lands).
    """

    applied: bool
    duration_seconds: float | None
    error: str | None = None
    matching_error: str | None = None
    categorization_error: str | None = None
    # tuple, not list: frozen=True blocks reassignment but not in-place
    # mutation of a list field — a tuple keeps the result carrier truly immutable.
    self_heal_actions: tuple[SelfHealRecord, ...] = field(default_factory=tuple)


RefreshStep = Literal["gsheet", "match", "transform", "categorize"]
CANONICAL_STEPS: tuple[RefreshStep, ...] = (
    "gsheet",
    "match",
    "transform",
    "categorize",
)


def expand_steps(steps: Sequence[str] | None) -> frozenset[str]:
    """Resolve a steps list (or None) to the canonical frozenset.

    None expands to all canonical steps; a list narrows to its elements.
    Used by surfaces to decide which follow-up hints to emit without
    re-deriving the membership rule from the service's internal logic.
    """
    return frozenset(CANONICAL_STEPS) if steps is None else frozenset(steps)


def refresh(db: Database, *, steps: list[str] | None = None) -> RefreshResult:
    """Run the post-load pipeline: gsheet pull → matching → SQLMesh apply → categorization.

    When ``steps`` is None (default), the full cascade runs — same behavior
    as the pre-``steps`` signature, preserved for all existing callers.

    When ``steps`` is provided, only the named steps execute, in canonical
    order (``gsheet`` → ``match`` → ``transform`` → ``categorize``) regardless of the
    input list's order. Dependencies enforce the order: categorize reads
    SQLMesh-built views, so running it after transform is mandatory; the
    parameter cannot reorder this.

    Skipping ``transform`` returns ``RefreshResult(applied=False,
    duration_seconds=None)`` without invoking the SQLMesh apply path —
    callers reading ``applied`` get an unambiguous "no apply happened"
    signal rather than a half-truthful "apply succeeded."

    Args:
        db: Database handle to run against.
        steps: Subset of ``("gsheet", "match", "transform", "categorize")`` to run.
            Defaults to all four when None.

    Raises:
        UserError(code="UNKNOWN_REFRESH_STEP"): if any element of ``steps``
            is not in the canonical set.

    See module docstring for the conceptual contract. Soft-fail variant:
    SQLMesh errors are returned in the result rather than raised, so
    callers can preserve already-loaded raw rows and surface the failure
    in their response envelope.
    """
    from moneybin.errors import UserError  # noqa: PLC0415
    from moneybin.services.matching_service import MatchingService  # noqa: PLC0415

    if steps is not None:
        unknown = [s for s in steps if s not in CANONICAL_STEPS]
        if unknown:
            raise UserError(
                f"Unknown refresh step(s): {', '.join(unknown)}",
                code="UNKNOWN_REFRESH_STEP",
                hint=f"known steps: {', '.join(CANONICAL_STEPS)}",
            )

    requested = expand_steps(steps)

    if "gsheet" in requested:
        # _run_gsheet_step catches all exceptions internally and always
        # returns a list — no outer try/except needed here.
        pull_results = _run_gsheet_step(db)
        if pull_results:
            completed = [r for r in pull_results if r.status == "complete"]
            non_complete = [r for r in pull_results if r.status != "complete"]
            if completed:
                total_rows = sum(
                    r.load_result.rows_inserted + r.load_result.rows_upserted
                    for r in completed
                    if r.load_result
                )
                logger.info(
                    f"GSheet pull: {len(completed)} completed, {total_rows} total rows"
                )
            if non_complete:
                # Surface non-success statuses at WARNING so refresh_run
                # callers (CLI users / agents) see degraded gsheet pulls
                # instead of a nominally-successful refresh hiding stale
                # data. pull_all_healthy isolates per-connection failures
                # — they reach us here as PullResult(status=...), not raises.
                status_counts: dict[str, int] = {}
                for r in non_complete:
                    status_counts[r.status] = status_counts.get(r.status, 0) + 1
                summary = ", ".join(
                    f"{count} {status}"
                    for status, count in sorted(status_counts.items())
                )
                logger.warning(
                    f"GSheet pull: {len(non_complete)} non-complete result(s) "
                    f"({summary}); see gsheet_status for per-connection detail"
                )

    matching_error: str | None = None
    categorization_error: str | None = None
    if "match" in requested:
        try:
            match_result = MatchingService(db).run()
            if match_result.has_matches:
                logger.info(f"Matching: {match_result.summary()}")
                if match_result.has_pending:
                    logger.info(
                        "Run 'moneybin transactions review --type matches' when ready"
                    )
        except (duckdb.CatalogException, duckdb.BinderException):
            # Views not built yet (first load precedes SQLMesh apply) — an
            # expected precondition, not a crash. Stay quiet; no error surfaced
            # so a fresh DB's first refresh doesn't report a false failure.
            logger.debug("Matching skipped (views may not exist yet)", exc_info=True)
        except Exception as exc:  # noqa: BLE001 — surface a real crash; never abort the pipeline
            matching_error = str(exc)
            logger.error(f"Matching failed during refresh: {exc}", exc_info=True)

    if "transform" not in requested:
        # Caller asked for a partial cascade that omits transform. Return
        # an "apply did not run" result so the envelope's applied=False
        # signal is honest. Categorize, if also requested, still runs
        # against whatever SQLMesh-built views are already on disk.
        if "categorize" in requested:
            categorization_error = _run_categorize_step(db)
        return RefreshResult(
            applied=False,
            duration_seconds=None,
            matching_error=matching_error,
            categorization_error=categorization_error,
        )

    apply_result = TransformService(db).apply()
    if not apply_result.applied:
        # categorize is not attempted when apply fails (it reads SQLMesh-built
        # views), so categorization_error stays None here — "not attempted",
        # not "succeeded". The caller distinguishes via applied=False + error.
        return RefreshResult(
            applied=False,
            duration_seconds=apply_result.duration_seconds,
            error=apply_result.error,
            matching_error=matching_error,
        )

    if "categorize" in requested:
        categorization_error = _run_categorize_step(db)

    return RefreshResult(
        applied=True,
        duration_seconds=apply_result.duration_seconds,
        matching_error=matching_error,
        categorization_error=categorization_error,
    )


def _run_gsheet_step(db: Database) -> list[Any]:
    """Best-effort GSheet pull step. Failures log-only — never propagated."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.gsheet.oauth_client import (
        GoogleOAuthClient,  # noqa: PLC0415
    )
    from moneybin.connectors.gsheet.pull_service import (
        GSheetPullService,  # noqa: PLC0415
    )
    from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
    from moneybin.repositories.gsheet_connections_repo import (  # noqa: PLC0415
        GSheetConnectionsRepo,
    )
    from moneybin.secrets import SecretStore  # noqa: PLC0415

    gsheet_start = time.monotonic()
    try:
        secret_store = SecretStore()
        settings = get_settings()
        oauth_client = GoogleOAuthClient(secrets=secret_store, settings=settings)
        sheets_client = SheetsClient(oauth=oauth_client)
        service = GSheetPullService(
            db=db, sheets_client=sheets_client, oauth_client=oauth_client
        )
        results = service.pull_all_healthy()
        return results
    except Exception:  # noqa: BLE001 — best-effort; surfaces in logs only
        # Distinguish "no connections → nothing to do" (debug) from
        # "connections exist but setup broke" (warning). A configured-but-
        # broken environment otherwise silently skips every scheduled pull
        # with no signal to the user.
        try:
            has_connections = bool(GSheetConnectionsRepo(db).list_healthy())
        except Exception:  # noqa: BLE001 — repo probe is itself best-effort
            has_connections = False
        if has_connections:
            logger.warning(
                "GSheet pull failed during setup despite healthy connections "
                "— scheduled pulls did not run; see exception detail",
                exc_info=True,
            )
        else:
            logger.debug(
                "GSheet pull skipped (no connections or setup incomplete)",
                exc_info=True,
            )
        return []
    finally:
        logger.debug(
            f"GSheet pull step finished in {time.monotonic() - gsheet_start:.2f}s"
        )


def _run_categorize_step(db: Database) -> str | None:
    """Best-effort categorization step.

    Returns the error string on a real crash, else ``None``. A missing-view
    precondition (first load before SQLMesh apply built the views) returns
    ``None`` and logs DEBUG — it is expected, not a failure. A genuine crash
    logs ERROR and returns its message so ``refresh`` can surface it in
    ``RefreshResult.categorization_error``.
    """
    from moneybin.services.auto_rule_service import AutoRuleService  # noqa: PLC0415
    from moneybin.services.categorization import CategorizationService  # noqa: PLC0415

    cat_start = time.monotonic()
    # Only the categorization write itself decides categorization_error. The
    # post-step auto-rule proposal read below is informational — a crash there
    # must NOT be reported as a categorization failure (categorize succeeded).
    try:
        stats = CategorizationService(db).categorize_pending()
    except (duckdb.CatalogException, duckdb.BinderException):
        # Tables/views not built yet (first load precedes SQLMesh apply) —
        # an expected precondition, not a crash. No error surfaced.
        logger.debug("Categorization skipped (tables may not exist yet)", exc_info=True)
        return None
    except Exception as exc:  # noqa: BLE001 — surface a real crash; never abort the pipeline
        logger.error(f"Categorization failed during refresh: {exc}", exc_info=True)
        return str(exc)
    finally:
        # "attempted", not "finished": this fires on every exit path,
        # including the missing-table skip, where the step didn't complete.
        logger.debug(
            f"Categorization step attempted in {time.monotonic() - cat_start:.2f}s"
        )

    if stats["total"] > 0:
        logger.info(
            f"Auto-categorized {stats['total']} transactions "
            f"({stats['merchant']} merchant, {stats['rule']} rule, "
            f"{stats['plaid']} plaid)"
        )
    # Informational only — never surfaces as categorization_error.
    try:
        pending = AutoRuleService(db).stats().pending_proposals
        if pending:
            logger.info(f"  {pending} new auto-rule proposals")
            logger.info(
                "  💡 Run 'moneybin transactions categorize auto review' "
                "to review proposed rules"
            )
    except Exception:  # noqa: BLE001 — informational post-step read; never fail refresh
        logger.debug("Auto-rule proposal stats unavailable", exc_info=True)
    return None
