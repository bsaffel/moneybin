"""Post-load refresh pipeline — a top-level MoneyBin domain concept.

"Refresh" means: update everything in the data warehouse based on the
latest new data that loaders wrote to ``raw.*``. It is the operational
verb that wraps five source-agnostic steps:

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
4. **Identity backfill** — :class:`AccountLinksService` and
   :class:`MerchantLinksService` generate reviewable identity proposals.
5. **Exchange-rate backfill** — :func:`~moneybin.services.rate_backfill.
   run_rate_backfill` caches the reference rates this profile's own rows
   imply, so a later report converts without reaching the network. It runs
   here rather than lazily on the read path because refresh already holds the
   exclusive writer lock a cache write needs; a report that fetched would take
   that lock behind a read-only-looking command and fail whenever a sync held
   it. It runs last because nothing downstream reads its output, so a provider
   outage costs the run nothing that had already succeeded.

Matching, categorization, identity backfill, and the rate gather are best-effort: a stage
failure never aborts the pipeline, so a partial run still leaves raw rows
durable and core tables rebuilt. Matcher/categorizer crashes surface their
error strings; identity failures surface only their fixed domain labels in
``RefreshResult.identity_errors``. A missing-view precondition on first load
is logged at DEBUG and not surfaced. Only SQLMesh apply failures set
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

**Why the step bodies defer their imports.** Not import cycles — there are
none, and the annotations that used to claim one were wrong. This module is on
the CLI's cold-start path (``import_service`` imports it at module level, and
``moneybin import inbox`` imports that), so every ``moneybin --help`` and every
shell completion pays for whatever it imports eagerly. Each step therefore
imports its own collaborators inside its body. Measured marginal cost of
hoisting, from this module's own import: rates ``run_rate_backfill`` +348
modules and polars, ``GSheetPullService`` +252 and polars, the Frankfurter
adapter +121, categorize +77, identity +44…49, the two repos +43…45. The
cheap-looking members of a step's block stay with their expensive siblings so
the block reads as one decision.
``test_orchestration_layering.test_orchestrator_import_stays_light`` fails if
any of it is hoisted.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError, classify_user_error, exception_origin
from moneybin.matching.engine import MatchRunError
from moneybin.services.matching_service import PENDING_MATCHES_HINT, MatchingService
from moneybin.services.refresh_outcome import RefreshStepOutcome
from moneybin.services.transform_service import TransformService

if TYPE_CHECKING:
    # Type-only: importing this at runtime would pull polars into the CLI
    # cold-start path through currency_service, which every `moneybin --help`
    # and every shell completion would then pay for.
    from moneybin.services.rate_backfill import RateBackfillResult

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
    hard-fail. ``matching_error`` / ``categorization_error`` /
    ``rate_backfill_error`` surface real crashes in the best-effort matcher /
    categorizer / rates steps. ``identity_errors`` holds only failed identity
    domain labels. A missing-view precondition on first load (before SQLMesh
    apply built the views) is NOT a crash and leaves those errors ``None``.
    ``self_heal_actions`` lists self-heal recipes that ran (empty until the M2D
    self-heal safelist lands).
    """

    applied: bool
    duration_seconds: float | None
    error: str | None = None
    matching_error: str | None = None
    categorization_error: str | None = None
    identity_errors: tuple[str, ...] = field(default_factory=tuple)
    # None means the step did not run — not that it ran and found nothing.
    rate_backfill: RateBackfillResult | None = None
    # The rates step ran and crashed. Its own carrier above cannot say so: a
    # crash and a step that correctly declined to run are both a null backfill,
    # so without this the caller sees `rates_written=null` with empty pair
    # lists either way and has no reason to act on the failure.
    rate_backfill_error: str | None = None
    # What the match step found, for callers that must report it rather than
    # leave it in the log. A pass can auto-merge without asking (see
    # engine._classify_pair), so a caller who *triggered* the pass — the
    # post-merge re-match — owes the user that number. Zero when the step
    # did not run: "found nothing" and "was skipped" are distinguished by
    # whether the caller asked for the step, not by a null count.
    matches_auto_merged: int = 0
    matches_pending_review: int = 0
    matches_pending_transfers: int = 0
    # The match step was asked for but could not run — its views were missing
    # or stale. Distinct from both a clean pass and a crash: the counts above
    # are zero because nothing was examined, not because nothing was found. A
    # caller reporting "no duplicates" off those zeros would be inventing a
    # result. Expected on a first load, where the views postdate SQLMesh apply.
    matching_skipped: bool = False
    # Accepted transfers reversed because the match step's dedup pass collapsed
    # their legs — the matcher reconciles them mid-run (see
    # matching.reconciliation.retire_transfers_invalidated_by_dedup), so every
    # trigger that reaches the match step reports them, not only the post-merge
    # re-match. That caller adds one more of its own: transfers whose two
    # *accounts* the merge collapsed, which happen inside `set`'s transaction
    # and never reach the matcher. Reported rather than left in the log: the
    # user accepted those transfers.
    transfers_retired: int = 0
    # tuple, not list: frozen=True blocks reassignment but not in-place
    # mutation of a list field — a tuple keeps the result carrier truly immutable.
    self_heal_actions: tuple[SelfHealRecord, ...] = field(default_factory=tuple)


RefreshStep = Literal["gsheet", "match", "transform", "categorize", "identity", "rates"]
CANONICAL_STEPS: tuple[RefreshStep, ...] = (
    "gsheet",
    "match",
    "transform",
    "categorize",
    "identity",
    "rates",
)


def step_outcome(result: RefreshResult) -> RefreshStepOutcome:
    """Flatten the best-effort step outcomes a caller has to pass on.

    Lives here rather than on :class:`RefreshStepOutcome` so that carrier stays
    a stdlib-only leaf the Pydantic result models can embed without importing
    this module.
    """
    rates = result.rate_backfill
    return RefreshStepOutcome(
        matching_error=result.matching_error,
        categorization_error=result.categorization_error,
        identity_errors=tuple(result.identity_errors),
        rates_written=None if rates is None else rates.rates_written,
        rate_pairs_failed=() if rates is None else tuple(rates.pairs_failed),
        rate_pairs_unsupported=() if rates is None else tuple(rates.pairs_unsupported),
        rate_pairs_discarded=() if rates is None else tuple(rates.pairs_discarded),
        rate_backfill_error=result.rate_backfill_error,
    )


def expand_steps(steps: Sequence[str] | None) -> frozenset[str]:
    """Resolve a steps list (or None) to the canonical frozenset.

    None expands to all canonical steps; a list narrows to its elements.
    Used by surfaces to decide which follow-up hints to emit without
    re-deriving the membership rule from the service's internal logic.
    """
    return frozenset(CANONICAL_STEPS) if steps is None else frozenset(steps)


def refresh(
    db: Database, *, steps: list[str] | None = None, actor: str = "system"
) -> RefreshResult:
    """Run the post-load pipeline through the exchange-rate gather.

    When ``steps`` is None (default), the full cascade runs — same behavior
    as the pre-``steps`` signature, preserved for all existing callers.

    When ``steps`` is provided, only the named steps execute, in canonical
    order (``gsheet`` → ``match`` → ``transform`` → ``categorize`` →
    ``identity`` → ``rates``) regardless of the input list's order.
    Dependencies enforce the order: categorize reads SQLMesh-built views and
    rates derives its currency pairs from ``core.*``, so running both after
    transform is mandatory; the parameter cannot reorder this.

    Skipping ``transform`` returns ``RefreshResult(applied=False,
    duration_seconds=None)`` without invoking the SQLMesh apply path —
    callers reading ``applied`` get an unambiguous "no apply happened"
    signal rather than a half-truthful "apply succeeded."

    Args:
        db: Database handle to run against.
        steps: Subset of ``("gsheet", "match", "transform", "categorize",
            "identity", "rates")`` to run. Defaults to every stage when None.
        actor: Audit actor for decisions the match step writes.
            ``app-integrity-invariant.md`` binds those to the surface that
            caused them, and a refresh has two kinds of caller: the automated
            ones it names (``moneybin refresh``, ``refresh_run``, the scenario
            runner), which stay ``"system"``, and a surface re-matching because
            a user just decided something — the post-merge re-match — whose
            decisions are that user's work and say so.

    Raises:
        UserError(code=error_codes.REFRESH_UNKNOWN_STEP): if any element of ``steps``
            is not in the canonical set.

    See module docstring for the conceptual contract. Soft-fail variant:
    SQLMesh errors are returned in the result rather than raised, so
    callers can preserve already-loaded raw rows and surface the failure
    in their response envelope.
    """
    if steps is not None:
        unknown = [s for s in steps if s not in CANONICAL_STEPS]
        if unknown:
            raise UserError(
                f"Unknown refresh step(s): {', '.join(unknown)}",
                code=error_codes.REFRESH_UNKNOWN_STEP,
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
                    f"({summary}); see gsheet for per-connection detail"
                )

    matching_error: str | None = None
    categorization_error: str | None = None
    identity_errors: tuple[str, ...] = ()
    auto_merged = 0
    pending_review = 0
    pending_transfers = 0
    transfers_retired = 0
    matching_skipped = False
    if "match" in requested:
        try:
            match_result = MatchingService(db).run(actor=actor)
            auto_merged = match_result.auto_merged
            pending_review = match_result.pending_review
            pending_transfers = match_result.pending_transfers
            transfers_retired = match_result.transfers_retired
            if match_result.has_matches:
                logger.info(f"Matching: {match_result.summary()}")
                if match_result.has_pending:
                    logger.info(PENDING_MATCHES_HINT)
        except MatchRunError as exc:
            # The run died after committing part of its work — tier decisions,
            # reversals, or both. Take every count off the exception: this
            # branch is the only place they still exist, and between them they
            # name merges now visible in the ledger and a transfer decision of
            # the user's that has been undone. Caught before the catalog branch
            # below on purpose — a *late* CatalogException reaches here wrapped,
            # and calling it a skipped step would claim nothing was examined
            # after the tiers had already written decisions.
            matching_error = _step_error(exc, step="Matching")
            auto_merged = exc.partial.auto_merged
            pending_review = exc.partial.pending_review
            pending_transfers = exc.partial.pending_transfers
            transfers_retired = exc.partial.transfers_retired
        except (duckdb.CatalogException, duckdb.BinderException):
            # Views not built yet (first load precedes SQLMesh apply) — an
            # expected precondition, not a crash. Stay quiet; no error surfaced
            # so a fresh DB's first refresh doesn't report a false failure.
            # It is still recorded, because "skipped" and "ran and found
            # nothing" are the same zero counts to a caller: a mature DB with a
            # stale view would otherwise be told no duplicates exist when
            # nothing was examined.
            matching_skipped = True
            logger.debug("Matching skipped (views may not exist yet)", exc_info=True)
        except Exception as exc:  # noqa: BLE001 — surface a real crash; never abort the pipeline
            matching_error = _step_error(exc, step="Matching")

    if "transform" not in requested:
        # Caller asked for a partial cascade that omits transform. Return
        # an "apply did not run" result so the envelope's applied=False
        # signal is honest. Categorize, if also requested, still runs
        # against whatever SQLMesh-built views are already on disk.
        if "categorize" in requested:
            categorization_error = _run_categorize_step(db)
        if "identity" in requested:
            identity_errors = _run_identity_step(db)
        rate_backfill, rate_backfill_error = (
            _run_rates_step(db) if "rates" in requested else (None, None)
        )
        return RefreshResult(
            applied=False,
            duration_seconds=None,
            matching_error=matching_error,
            categorization_error=categorization_error,
            identity_errors=identity_errors,
            rate_backfill=rate_backfill,
            rate_backfill_error=rate_backfill_error,
            matches_auto_merged=auto_merged,
            matches_pending_review=pending_review,
            matches_pending_transfers=pending_transfers,
            matching_skipped=matching_skipped,
            transfers_retired=transfers_retired,
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
            matches_auto_merged=auto_merged,
            matches_pending_review=pending_review,
            matches_pending_transfers=pending_transfers,
            matching_skipped=matching_skipped,
            transfers_retired=transfers_retired,
        )

    if "categorize" in requested:
        categorization_error = _run_categorize_step(db)
    if "identity" in requested:
        identity_errors = _run_identity_step(db)
    rate_backfill, rate_backfill_error = (
        _run_rates_step(db) if "rates" in requested else (None, None)
    )

    return RefreshResult(
        applied=True,
        duration_seconds=apply_result.duration_seconds,
        matching_error=matching_error,
        categorization_error=categorization_error,
        identity_errors=identity_errors,
        rate_backfill=rate_backfill,
        rate_backfill_error=rate_backfill_error,
        matches_auto_merged=auto_merged,
        matches_pending_review=pending_review,
        matches_pending_transfers=pending_transfers,
        matching_skipped=matching_skipped,
        transfers_retired=transfers_retired,
    )


def _step_error(exc: Exception, *, step: str) -> str:
    """What a crashed best-effort step is allowed to say, and log.

    ``matching_error``, ``categorization_error`` and ``rate_backfill_error`` are
    ``DataClass.DESCRIPTION`` fields on ``RefreshRunPayload``: they reach the
    model provider through
    ``refresh_run`` and land in CLI JSON. An exception's message is whatever
    raised it — DuckDB binder text, file paths, row values — and for
    ``MatchRunError`` it *is* the cause verbatim, because the carrier passes
    ``str(cause)`` to ``Exception``. So the returned string comes from
    ``classify_user_error``, the same boundary the direct matcher surfaces use,
    and a type it does not recognize says nothing beyond where to look.

    The log gets the frame chain rather than the message for the reason
    ``exception_origin`` documents: a traceback's last line is the message, and
    AGENTS.md's no-financial-data rule has no local-log carve-out.
    """
    logger.error(
        f"{step} failed during refresh at {exception_origin(exc.__cause__ or exc)}"
    )
    classified = classify_user_error(exc)
    if classified is not None:
        return classified.message
    return f"{step} failed — the cause is in the local log"


def _run_gsheet_step(db: Database) -> list[Any]:
    """Best-effort GSheet pull step. Failures log-only — never propagated."""
    # Deferred as one block: GSheetPullService reaches polars (+252 modules on
    # this module's import), and this module is on the CLI cold-start path.
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
    # Deferred: the categorization stack costs +77 modules on this module's
    # import, and this module is on the CLI cold-start path.
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
        return _step_error(exc, step="Categorization")
    finally:
        # "attempted", not "finished": this fires on every exit path,
        # including the missing-table skip, where the step didn't complete.
        logger.debug(
            f"Categorization step attempted in {time.monotonic() - cat_start:.2f}s"
        )

    if stats["total"] > 0:
        # The categorization run already reported this same total and
        # breakdown; at info the pipeline would echo it a second time.
        logger.debug(
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


def _run_rates_step(db: Database) -> tuple[RateBackfillResult | None, str | None]:
    """Gather the exchange rates this profile's own rows imply.

    Runs here rather than on the report read path. Display conversion prices
    every row at its own date, so a report spanning years needs a rate per day;
    fetching those lazily would put a network call and the exclusive writer lock
    behind a command that looks read-only, and it would fail outright whenever a
    sync already held that lock. Refresh holds the lock and is already slow.

    Returns ``(backfill, error)``. Both are None when there was nothing to
    gather — no home currency set means nothing is ever converted, so no rate is
    implied, and ``core.*`` not existing yet is a first-load precondition. The
    error is what separates those declines from a step that ran and crashed:
    the backfill is null on all three, so a caller reading only that would tell
    the user nothing happened when in fact something broke.
    """
    # Deferred as one block: run_rate_backfill reaches polars through
    # currency_service (+348 modules on this module's import) and the
    # Frankfurter adapter pulls httpx (+121); this module is on the CLI
    # cold-start path.
    from moneybin.connectors.rates.frankfurter import (  # noqa: PLC0415
        FrankfurterRateAdapter,
    )
    from moneybin.repositories.profile_settings_repo import (  # noqa: PLC0415
        ProfileSettingsRepo,
    )
    from moneybin.services.rate_backfill import (  # noqa: PLC0415
        RateBackfillNotReadyError,
        run_rate_backfill,
    )

    try:
        home_currency = ProfileSettingsRepo(db).get_home_currency()
    except Exception as exc:  # noqa: BLE001  # best-effort refresh stage
        return None, _step_error(exc, step="Rate backfill")
    if home_currency is None:
        logger.debug("Rate backfill skipped: no home currency is set")
        return None, None

    try:
        return run_rate_backfill(
            db,
            home_currency=home_currency,
            # The UTC day, not the host's: Frankfurter keys its series by UTC
            # date, so east of UTC a host-local `today` names a day the provider
            # has not published. Same reasoning, same shape, as
            # `PriceService.__init__`.
            through=datetime.now(UTC).date(),
            adapter=FrankfurterRateAdapter(),
        ), None
    except RateBackfillNotReadyError:
        # core.* not built yet — the same first-load precondition the matching
        # stage tolerates, not a failure worth reporting. Matched by name and
        # not by the DuckDB exception types it wraps, for the reason the
        # matching step catches MatchRunError ahead of its own catalog branch:
        # the store raises those same types on a late write failure, and
        # calling that a skipped step would claim nothing was attempted.
        logger.debug("Rate backfill skipped (core views may not exist yet)")
        return None, None
    except Exception as exc:  # noqa: BLE001  # best-effort refresh stage
        # Through _step_error like every sibling step, not the bare type name:
        # this string lands in CLI JSON and the MCP envelope, and a rates crash
        # can carry a provider URL with a currency pair in it.
        return None, _step_error(exc, step="Rate backfill")


def _run_identity_step(db: Database) -> tuple[str, ...]:
    """Generate account and merchant identity proposals without aborting refresh."""
    # Deferred: the two link services cost +44…49 modules on this module's
    # import, and this module is on the CLI cold-start path.
    from moneybin.services.account_links_service import (  # noqa: PLC0415
        AccountLinksService,
    )
    from moneybin.services.merchant_links_service import (  # noqa: PLC0415
        MerchantLinksService,
    )

    errors: list[str] = []
    for label, run in (
        ("accounts", lambda: AccountLinksService(db).run()),
        ("merchants", lambda: MerchantLinksService(db).run()),
    ):
        try:
            run()
        except Exception as exc:  # noqa: BLE001  # best-effort refresh stage
            logger.error(f"{label} identity backfill failed: {type(exc).__name__}")
            errors.append(label)
    return tuple(errors)
