"""Business logic for sync operations.

CLI and MCP layers are thin wrappers around this service. Owns:
- pull() orchestration (trigger → fetch → load)
- link() with optional auto-pull
- list_connections() with error-code → user-guidance mapping
- disconnect() with institution name → connection_id resolution

State source: server. No local connection-state mirror. See design Section 4.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

import duckdb

from moneybin import error_codes
from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    LinkInitiateResponse,
    LinkResult,
    PullResult,
    SyncConnectionView,
    SyncDataResponse,
)
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.account_identity import SourceAccount
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.investments.source_overlap import investment_source_overlap
from moneybin.metrics.registry import (
    ACCOUNT_LINK_OUTCOMES_TOTAL,
    SYNC_CONNECT_OUTCOMES,
    SYNC_DISCONNECT_OUTCOMES,
    SYNC_INSTITUTION_ERRORS_TOTAL,
    SYNC_PULL_DURATION_SECONDS,
    SYNC_PULL_OUTCOMES_TOTAL,
    SYNC_PULL_TRANSACTIONS_LOADED,
    SYNC_PURGE_ROWS_DELETED,
)
from moneybin.orchestration.refresh import refresh as _refresh
from moneybin.orchestration.refresh import step_outcome as _step_outcome
from moneybin.progress import ProgressEvent
from moneybin.repositories.account_link_decisions_repo import (
    AccountLinkDecisionsRepo,
)
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolver import (
    AccountResolver,
    fetch_core_display_names,
)
from moneybin.services.security_resolver import SecurityResolver
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    FCT_INVESTMENT_TRANSACTIONS,
    PLAID_ACCOUNTS,
    PLAID_BALANCES,
    PLAID_INVESTMENT_HOLDING_LOTS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS,
    PLAID_INVESTMENT_TRANSACTION_RECEIPTS,
    PLAID_INVESTMENT_TRANSACTIONS,
    PLAID_SECURITIES,
    PLAID_TRANSACTIONS,
    SECURITY_PRICES,
    TableRef,
)

logger = logging.getLogger(__name__)

_PROVIDER = "plaid"  # Phase 1: single provider; widen when SimpleFIN/MX land

# Every raw table a Plaid item writes, keyed by its provider_item_id in
# source_origin. A purge deletes from each; a table added to the loader has to
# be added here or a purge that says "everything this item pulled" lies.
_PURGE_TABLES = (
    PLAID_ACCOUNTS,
    PLAID_TRANSACTIONS,
    PLAID_BALANCES,
    PLAID_SECURITIES,
    PLAID_INVESTMENT_TRANSACTIONS,
    PLAID_INVESTMENT_TRANSACTION_RECEIPTS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_HOLDING_LOTS,
    PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS,
)


def _require_one_selector(
    institution: str | None, provider_item_id: str | None
) -> None:
    """Refuse a disconnect selector that names both targets or neither."""
    if institution is not None and provider_item_id is not None:
        raise UserError(
            "institution and provider_item_id are mutually exclusive — "
            "pass exactly one",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if institution is None and provider_item_id is None:
        raise UserError(
            "institution or provider_item_id is required to disconnect — "
            "run `moneybin sync status` to list connected banks",
            code=error_codes.SYNC_INSTITUTION_REQUIRED,
        )


@dataclass(frozen=True)
class SyncPurgePlan:
    """Exact live state a purging disconnect would remove.

    ``connection`` is ``None`` when the sync server no longer lists the item —
    a connection removed server-side before this command existed, or a purge
    whose local half failed after its remote half succeeded. Either way the
    local rows are still owned by ``provider_item_id`` and are what the purge
    is for.
    """

    provider_item_id: str
    connection: ConnectedInstitution | None
    table_counts: tuple[tuple[str, int], ...]
    account_link_ids: tuple[str, ...]
    decision_ids: tuple[str, ...]

    @property
    def rows_to_delete(self) -> int:
        """Every raw row the purge removes, summed across the tables it touches."""
        return sum(count for _, count in self.table_counts)

    @property
    def binding(self) -> tuple[object, ...]:
        """What a confirmation binds: the item, its connection id, and every count.

        Volatile connection metadata (``last_sync``, ``status``, ``error_code``)
        stays out, so a server-side sync completing while a prompt is open does
        not read as a changed target.
        """
        return (
            self.provider_item_id,
            None if self.connection is None else self.connection.id,
            self.table_counts,
            self.account_link_ids,
            self.decision_ids,
        )

    @property
    def blast_radius(self) -> dict[str, int]:
        """Per-table counts plus the app-state the purge flips, for confirmation."""
        radius = dict(self.table_counts)
        radius["total_rows"] = self.rows_to_delete
        radius["institutions"] = int(self.connection is not None)
        radius["account_links_reversed"] = len(self.account_link_ids)
        radius["proposals_withdrawn"] = len(self.decision_ids)
        return radius


_ERROR_GUIDANCE: dict[str, str] = {
    "ITEM_LOGIN_REQUIRED": "{institution} needs re-authentication — run `moneybin sync link --institution {institution}`",
    "ITEM_NOT_FOUND": "{institution} connection was revoked. Run `moneybin sync link` to reconnect.",
    "INSTITUTION_NOT_RESPONDING": "{institution} is temporarily unavailable. Try again later.",
    "INSTITUTION_DOWN": "{institution} is down for maintenance. Try again later.",
    "RATE_LIMIT_EXCEEDED": "Rate limit reached. Sync will resume on the next scheduled run.",
    "PRODUCTS_NOT_READY": "{institution} is still processing initial data. Try again in a few minutes.",
    "PRODUCT_NOT_READY": "{institution} is still processing investment data. Try again in a few minutes.",
    "PRODUCTS_NOT_SUPPORTED": "{institution} doesn't provide investment data through Plaid. Cash accounts still sync normally.",
    "INVALID_PRODUCT": "{institution} was linked before investment access — run `moneybin sync link` to re-consent.",
}


class SyncService:
    """Orchestrates Plaid sync operations: pull, link, list, disconnect."""

    def __init__(
        self,
        *,
        client: SyncClient,
        db: Database,
        loader: PlaidExtractor,
    ) -> None:
        """Bind the service to a SyncClient, open Database, and PlaidExtractor."""
        self.client = client
        self.db = db
        self.loader = loader

    # ------------------------------ Pull ------------------------------

    def pull(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        force: bool = False,
        refresh: bool = True,
        progress: Callable[[ProgressEvent], None] | None = None,
    ) -> PullResult:
        """Trigger a sync, fetch data, load into raw tables, return counts.

        When ``refresh`` is True (default) and the sync changed raw state
        (loaded new rows or processed removals), runs the post-load
        :func:`moneybin.orchestration.refresh.refresh` pipeline — matching,
        SQLMesh apply, and categorization — so derived ``core.*`` models
        reflect the new data before this call returns.

        Transform failures soft-fail: raw rows stay durable, and the result
        envelope reports ``transforms_applied=False`` with ``transforms_error``
        (matching and categorization are best-effort and log-only on failure).

        High-frequency callers (scheduled syncs, webhooks) should pass
        ``refresh=False`` and run refresh on a separate schedule; see
        ``docs/specs/sync-plaid.md`` Req 10 for the latency profile.
        """
        if institution is not None and provider_item_id is not None:
            raise UserError(
                "institution and provider_item_id are mutually exclusive — pass one or neither",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if provider_item_id is None and institution is not None:
            provider_item_id = self._resolve_institution(institution)
        with SYNC_PULL_DURATION_SECONDS.labels(provider=_PROVIDER).time():
            try:
                if progress is not None:
                    progress(ProgressEvent("Syncing institutions"))
                trigger_resp = self.client.trigger_sync(
                    provider_item_id=provider_item_id,
                    reset_cursor=force,
                )
                if trigger_resp.status != "completed":
                    # Server contract: /sync/trigger is synchronous and must return
                    # a terminal status. pending/running here means the server
                    # returned before the sync finished — proceeding to get_data
                    # would silently load nothing.
                    raise RuntimeError(
                        f"sync trigger returned non-terminal status '{trigger_resp.status}' "
                        f"for job_id={trigger_resp.job_id}; expected 'completed'"
                    )
                sync_data = self.client.get_data(trigger_resp.job_id)
                if progress is not None:
                    progress(ProgressEvent("Loading synced data"))
                removed_count = self.loader.handle_removed_transactions(
                    sync_data.removed_transactions,
                )
                load_result = self.loader.load(sync_data, trigger_resp.job_id)
            except Exception:
                SYNC_PULL_OUTCOMES_TOTAL.labels(
                    provider=_PROVIDER, status="failed"
                ).inc()
                raise
            SYNC_PULL_OUTCOMES_TOTAL.labels(provider=_PROVIDER, status="success").inc()
            SYNC_PULL_TRANSACTIONS_LOADED.labels(provider=_PROVIDER).inc(
                load_result.transactions_loaded
            )
            # Ack the broker so it advances its per-institution cursors. Done
            # unconditionally once the load is durable (even an empty completed
            # sync advanced cursors broker-side; acking persists them and frees
            # the held window). Best-effort: the data is already durable, so an
            # ack failure (network blip, broker 5xx/410) just leaves the cursor
            # un-advanced — the next pull re-pulls from it and the loader dedups,
            # loss-free. So a failure must not flip this pull's success.
            try:
                self.client.ack(trigger_resp.job_id)
            except Exception as e:  # best-effort post-load ack
                logger.warning(
                    f"Ack failed after pull (job_id={trigger_resp.job_id}): {e}"
                )
            # Populate app.account_links for each synced account (mirrors the
            # import path, A6/A7). Best-effort: raw rows are already durable and
            # this pull has been counted a success, so a resolver failure must
            # not flip that accounting — log and continue (refresh/auto-pull
            # soft-fail pattern). A subsequent pull re-resolves idempotently.
            try:
                self._resolve_accounts(sync_data)
            except Exception as e:  # best-effort post-load metadata
                logger.warning(f"Account resolution failed after pull: {e}")
            # Security identity resolution. Unlike account resolution above,
            # there is NO staging COALESCE fallback for securities — B1's
            # fallback (COALESCE(links.account_id, b.account_id)) exists only
            # for accounts, whose source-native id is still usable in core
            # even unresolved. security_id resolves ONLY through
            # app.security_links; a row this run couldn't bind reaches
            # core.fct_investment_transactions with security_id = NULL, and
            # the cost-basis engine silently `continue`s past every
            # NULL-security event (investments/cost_basis.py) — every buy/sell
            # on that security is dropped from lots and gains, understating
            # them, with no error surfaced anywhere else. Raw rows stay
            # durable and retrying is idempotent, so this must not raise or
            # roll back the pull's success accounting — but it MUST be
            # reported, not swallowed: security_resolution_error flows into
            # the CLI warning and exit code exactly like transforms_error.
            #
            # Runs on EVERY pull, not only one whose securities array is
            # non-empty: resolve_all() reads the whole raw.plaid_securities
            # table, so gating it on this pull's delta would strand securities
            # a previous pull loaded but failed to bind (the soft-fail path
            # right below) — permanently, since a later cash-only pull would
            # skip resolution again. That is the self-heal stg_plaid__securities
            # promises. The cost on a pull with no investments is one SELECT
            # over an empty table (resolve_all returns {} immediately).
            resolution: dict[str, int] = {}
            resolution_writes = 0
            security_resolution_error: str | None = None
            try:
                resolver = SecurityResolver(self.db, actor="system")
                resolution = resolver.resolve_all()
                resolution_writes = resolver.writes
            except Exception as e:  # reported via security_resolution_error, not raised
                security_resolution_error = str(e)
                logger.warning(f"Security resolution failed after pull: {e}")
            overlap = self._investment_source_overlap()
            if overlap:
                logger.warning(
                    f"{len(overlap)} account(s) carry BOTH manual and Plaid "
                    "investment rows; lots and gains will double-count until one "
                    "source is chosen per account (investment dedup is a future "
                    "matching child)"
                )
            for inst in sync_data.metadata.institutions:
                if inst.status == "failed" and inst.error_code:
                    SYNC_INSTITUTION_ERRORS_TOTAL.labels(
                        error_code=inst.error_code
                    ).inc()
        result = PullResult(
            job_id=trigger_resp.job_id,
            transactions_loaded=load_result.transactions_loaded,
            accounts_loaded=load_result.accounts_loaded,
            balances_loaded=load_result.balances_loaded,
            transactions_removed=removed_count,
            securities_loaded=load_result.securities_loaded,
            investment_transactions_loaded=load_result.investment_transactions_loaded,
            holdings_loaded=load_result.holdings_loaded,
            holding_lots_loaded=load_result.holding_lots_loaded,
            security_prices_loaded=load_result.security_prices_loaded,
            institutions=sync_data.metadata.institutions,
            investment_source_overlap_accounts=overlap,
            security_resolution=resolution,
            security_resolution_error=security_resolution_error,
        )
        # Every durable raw write counts, not just loaded entity rows.
        # Removals: a pure-removal sync deletes from raw.plaid_transactions and
        # the deletion must propagate through SQLMesh into core.fct_transactions,
        # or the deleted row stays visible in core.
        # Holdings-snapshot receipts: on a liquidated broker's pull the receipt
        # is the ONLY write — no holdings rows, by definition — and it is the
        # sole input core.dim_holdings reads to pick the newest snapshot. Skip
        # the refresh and dim_holdings keeps serving the previous, non-empty
        # snapshot, so the emptied broker still reads as holding its old
        # positions: the exact phantom the receipt exists to expose.
        # Price observations: raw.security_prices is append-only, so this counts
        # only the closes actually written. Today they ride along with a
        # securities upsert, but nothing structural guarantees that — a price
        # source that does not is a durable raw write whose rows would never
        # reach core.fct_security_prices without this term.
        # Security-resolution writes: resolve_all() sweeps the whole raw
        # securities table, not this pull's delta, so a pull that loads nothing
        # can still bind a security an earlier pull stranded. That binding is
        # what core reads for security_id — skip the refresh and the leg keeps
        # its NULL and the cost-basis engine goes on dropping it. Counted by
        # actual writes, not by outcome: a steady-state re-resolve adopts every
        # ref and writes nothing, and must not refresh.
        rows_changed = (
            load_result.transactions_loaded
            + load_result.accounts_loaded
            + load_result.balances_loaded
            + removed_count
            + load_result.securities_loaded
            + load_result.investment_transactions_loaded
            + load_result.holdings_loaded
            + load_result.holding_lots_loaded
            + load_result.holdings_snapshots_loaded
            + load_result.security_prices_loaded
            + resolution_writes
        )
        if refresh and rows_changed > 0:
            if progress is not None:
                progress(ProgressEvent("Refreshing reports"))
            refresh_result = (
                _refresh(self.db)
                if progress is None
                else _refresh(self.db, progress=progress)
            )
            result.transforms_applied = refresh_result.applied
            result.transforms_duration_seconds = refresh_result.duration_seconds
            result.transforms_error = refresh_result.error
            result.transfers_retired = refresh_result.transfers_retired
            result.refresh_steps = _step_outcome(refresh_result)
            if not refresh_result.error:
                result.opening_bootstrap_rows = self._count_bootstrap_rows()
        return result

    def _resolve_accounts(self, sync_data: SyncDataResponse) -> None:
        """Resolve each synced account to a canonical id, populating app.account_links.

        The Plaid native key is the source ``account_id`` token already stamped
        on ``raw.plaid_accounts`` (unchanged) — the resolver only ADDS the
        native->canonical mapping the B1 staging JOIN keys on. ``source_origin``
        is the account's ``provider_item_id``; we reuse the loader's
        account->item attribution so it is byte-identical to what raw recorded
        (a divergent scope would corrupt source_native uniqueness).

        ``persistent_token`` carries Plaid's ``persistent_account_id``, which
        survives a relink where ``account_id`` does not: reconnecting an item
        reissues every native id, so ``source_native`` alone cannot tell a
        returning card from a new one and the resolver mints a second canonical
        account for a ledger the user already has. The persistent id is the
        strong ref that confirms identity across that boundary.

        ``name`` leads the display fallback because ``official_name`` is the
        product's marketing label — sibling Chase cards report one
        ``Ultimate Rewards®`` between them, so it cannot discriminate on the
        name rung of the candidate pass. It is that rung's input only: both
        sides of a merge proposal are labelled from
        ``core.dim_accounts.display_name``, never from this string.
        """
        if not sync_data.accounts:
            return
        item_by_account = self.loader.build_account_to_item_map(sync_data)
        resolver = AccountResolver(self.db, actor="system")
        for acc in sync_data.accounts:
            resolved_account = resolver.resolve(
                SourceAccount(
                    source_type="plaid",
                    source_origin=item_by_account[acc.account_id],
                    source_account_key=acc.account_id,
                    account_name=(
                        acc.name
                        or acc.official_name
                        or (
                            f"{acc.institution_name} account"
                            if acc.institution_name
                            else acc.account_id
                        )
                    ),
                    # acc.name is the holder's own nickname (set at the
                    # institution or in Plaid); official_name and the two
                    # generated fallbacks below it are not -- see the
                    # docstring's note on why official_name can't discriminate.
                    account_name_is_user_set=bool(acc.name),
                    account_number=None,  # Plaid never exposes a full number
                    last_four=acc.mask,
                    institution=acc.institution_name,
                    persistent_token=acc.persistent_account_id,
                )
            )
            ACCOUNT_LINK_OUTCOMES_TOTAL.labels(result=resolved_account.outcome).inc()

    def _investment_source_overlap(self) -> list[str]:
        """Canonical accounts with manual and Plaid transaction or holdings evidence."""
        try:
            return investment_source_overlap(self.db)
        except duckdb.CatalogException:  # tables may not exist on fresh DBs
            return []

    def _count_bootstrap_rows(self) -> int:
        """Count synthetic opening-lot bootstrap rows (spec: counted in the sync envelope).

        Cumulative across all history, not scoped to this pull — bootstrap
        writes once per (account, source_origin), so a later sync with zero
        new bootstrap activity still reports every lot seeded on any earlier
        pull. The CLI wording must read as a standing count, not a per-pull
        delta (see sync.py's "opening lot(s) seeded" line).
        """
        try:
            row = self.db.execute(
                f"SELECT COUNT(*) FROM {FCT_INVESTMENT_TRANSACTIONS.full_name} "  # TableRef constant
                "WHERE subtype = 'opening_bootstrap'"
            ).fetchone()
        except duckdb.CatalogException:  # core view absent before first transform
            return 0
        return int(row[0]) if row else 0

    # ------------------------------ Link ------------------------------

    def initiate_link(
        self,
        *,
        institution: str | None = None,
        return_to: str | None = None,
    ) -> LinkInitiateResponse:
        """Resolve institution and start a Plaid Link session — does not poll.

        Used by JSON-mode CLI and MCP sync_link, where the caller surfaces
        link_url to the user and verifies completion via a separate
        sync_link_status call. The full link() path (resolve → initiate →
        poll → auto-pull) remains for text-mode CLI.

        Falls through to a new-connection request when institution is provided
        but matches no existing connection (per design Section 8). Raises on
        ambiguous matches via _find_institution.
        """
        provider_item_id: str | None = None
        if institution:
            inst = self._find_institution(institution)
            if inst is not None:
                provider_item_id = inst.provider_item_id
        initiate = self.client.initiate_link(
            provider_item_id=provider_item_id,
            return_to=return_to,
        )
        if initiate.link_type != "widget_flow":
            raise NotImplementedError(
                f"link_type '{initiate.link_type}' is not supported in this version"
            )
        return initiate

    def link(
        self,
        *,
        institution: str | None = None,
        auto_pull: bool = True,
        return_to: str | None = None,
        on_initiate: Callable[[LinkInitiateResponse], None] | None = None,
    ) -> LinkResult:
        """Link new institution OR re-authenticate existing one.

        When `institution` matches an existing connection, runs Plaid update mode
        against that item. When it matches none, falls through to a new-connection
        request (per design Section 8); the server's Link flow handles naming.
        Ambiguous matches (same name on multiple connections) raise.

        `on_initiate` is invoked synchronously with the LinkInitiateResponse before
        the service starts polling. The CLI uses this hook to display `link_url`
        and optionally open the user's browser. Without it, the service blocks on
        polling without surfacing the URL — only safe for callers that surface it
        themselves (MCP returns the URL in its envelope and never enters this path).
        """
        initiate = self.initiate_link(institution=institution, return_to=return_to)
        if on_initiate is not None:
            on_initiate(initiate)
        try:
            status = self.client.poll_link_status(initiate.session_id)
        except Exception:
            # poll_link_status raises SyncLinkError on terminal 'failed'
            # status, and SyncTimeoutError when the user abandons the browser.
            # Surface both as failed-link outcomes; the CLI/MCP layer
            # re-raises with the specific exception type.
            SYNC_CONNECT_OUTCOMES.labels(status="failed").inc()
            raise
        SYNC_CONNECT_OUTCOMES.labels(status=status.status or "linked").inc()
        pull_result: PullResult | None = None
        if auto_pull:
            try:
                pull_result = self.pull(provider_item_id=status.provider_item_id)
            except Exception as e:
                logger.warning(f"Auto-pull failed after link: {e}")
        return LinkResult(
            provider_item_id=status.provider_item_id or "",
            institution_name=status.institution_name,
            pull_result=pull_result,
        )

    # ------------------------------ Status and disconnect ------------------------------

    def list_connections(self) -> list[SyncConnectionView]:
        """Return enriched connection views with user-facing guidance for non-active statuses."""
        institutions = self.client.list_institutions()
        return [
            SyncConnectionView(
                id=i.id,
                provider_item_id=i.provider_item_id,
                institution_name=i.institution_name,
                provider=i.provider,
                status=i.status,
                last_sync=i.last_sync,
                created_at=i.created_at,
                error_code=i.error_code,
                guidance=self._guidance_for(
                    status=i.status,
                    error_code=i.error_code,
                    institution=i.institution_name or "this connection",
                ),
            )
            for i in institutions
        ]

    def disconnect(
        self, *, institution: str | None = None, provider_item_id: str | None = None
    ) -> ConnectedInstitution:
        """Resolve a target (name or exact item) to a connection and disconnect it."""
        inst = self.plan_disconnect(
            institution=institution, provider_item_id=provider_item_id
        )
        self.client.disconnect(inst.id)
        SYNC_DISCONNECT_OUTCOMES.labels(scope="remote").inc()
        return inst

    def plan_disconnect(
        self, *, institution: str | None = None, provider_item_id: str | None = None
    ) -> ConnectedInstitution:
        """Resolve the exact live connection that a disconnect targets.

        `provider_item_id` targets one connection exactly — set when an
        institution has more than one (e.g. after a relink), where
        `institution` alone is ambiguous and `_find_institution` refuses it.
        Mutually exclusive with `institution`, mirroring `pull()`'s guard.
        """
        _require_one_selector(institution, provider_item_id)
        if provider_item_id is not None:
            inst = self._find_institution_by_item(provider_item_id)
            if inst is None:
                raise UserError(
                    f"no connected institution with provider_item_id "
                    f"'{provider_item_id}' — run `moneybin sync status` to "
                    f"list connected banks",
                    code=error_codes.MUTATION_NOT_FOUND,
                )
            return inst
        inst = self._find_institution(institution or "")
        if inst is None:
            raise UserError(
                f"no connected institution matching '{institution}' — "
                f"run `moneybin sync status` to list connected banks",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        return inst

    def disconnect_confirmed(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        verify: Callable[[ConnectedInstitution], None],
    ) -> ConnectedInstitution:
        """Verify the live target immediately before deleting the connection."""
        inst = self.plan_disconnect(
            institution=institution, provider_item_id=provider_item_id
        )
        verify(inst)
        self.client.disconnect(inst.id)
        SYNC_DISCONNECT_OUTCOMES.labels(scope="remote").inc()
        return inst

    # ------------------------------ Purge ------------------------------

    def plan_purge(
        self, *, institution: str | None = None, provider_item_id: str | None = None
    ) -> SyncPurgePlan:
        """Resolve one item and count everything a purging disconnect removes.

        Read-only. ``institution`` resolves through the server exactly as
        ``plan_disconnect`` does. ``provider_item_id`` first tries the server
        and then falls back to the local rows alone, so an item the server
        already dropped — the orphan a plain disconnect leaves behind — is
        still reachable by its id. Both paths ask the server first, so the
        server has to answer even for an item it no longer lists.
        """
        _require_one_selector(institution, provider_item_id)
        if provider_item_id is None:
            connection = self._find_institution(institution or "")
            if connection is None:
                raise UserError(
                    f"no connected institution matching '{institution}' — "
                    f"run `moneybin sync status` to list connected banks",
                    code=error_codes.MUTATION_NOT_FOUND,
                    hint=(
                        "A connection the server no longer lists can still be "
                        "purged by its provider_item_id."
                    ),
                )
            provider_item_id = connection.provider_item_id
        else:
            connection = self._find_institution_by_item(provider_item_id)

        table_counts = tuple(
            (table.full_name, self._count_item_rows(table, provider_item_id))
            for table in _PURGE_TABLES
        ) + ((SECURITY_PRICES.full_name, self._count_item_prices(provider_item_id)),)
        link_ids = tuple(
            str(row[0])
            for row in self.db.execute(
                f"""
                SELECT link_id
                FROM {ACCOUNT_LINKS.full_name}
                WHERE status = 'accepted'
                  AND source_type = ?
                  AND source_origin = ?
                ORDER BY link_id
                """,  # TableRef + parameterized values
                [_PROVIDER, provider_item_id],
            ).fetchall()
        )
        decision_ids = self._orphaned_proposals(provider_item_id)
        plan = SyncPurgePlan(
            provider_item_id=provider_item_id,
            connection=connection,
            table_counts=table_counts,
            account_link_ids=link_ids,
            decision_ids=decision_ids,
        )
        if connection is None and plan.rows_to_delete == 0 and not link_ids:
            raise UserError(
                f"no connected institution with provider_item_id "
                f"'{provider_item_id}', and no locally pulled rows carry it — "
                f"run `moneybin sync status` to list connected banks",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        return plan

    def purge_confirmed(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        verify: Callable[[SyncPurgePlan], None],
        actor: str = "cli",
    ) -> SyncPurgePlan:
        """Re-plan, verify, disconnect on the server, then delete the local rows.

        The server call comes first because it is the half that cannot be rolled
        back: a remote failure leaves everything untouched, while a local failure
        after a remote success leaves exactly the orphan this method accepts by
        ``provider_item_id``, so the purge is re-runnable. The local half runs in
        one transaction: accepted ``app.account_links`` for the item are
        reversed, pending merge proposals that only this item's accounts still
        describe are rejected, and every ``raw.plaid_*`` row the item wrote is
        deleted.
        """
        self.db.begin()
        try:
            live = self.plan_purge(
                institution=institution, provider_item_id=provider_item_id
            )
            verify(live)
            if live.connection is not None:
                self.client.disconnect(live.connection.id)
            try:
                self._purge_local(live, actor)
                self.db.commit()
            except Exception as exc:
                if live.connection is None:
                    raise
                # The server half already landed and `sync status` no longer
                # lists the item, so the error has to carry the one id that
                # still reaches the orphan.
                raise UserError(
                    f"The server connection was removed, but the local purge of "
                    f"provider_item_id '{live.provider_item_id}' failed and was "
                    f"rolled back; its locally pulled rows are still here.",
                    code=error_codes.SYNC_ERROR,
                    hint=(
                        f"Run 'moneybin sync disconnect --provider-item-id "
                        f"{live.provider_item_id} --purge' to finish removing them."
                    ),
                ) from exc
        except BaseException:
            # BaseException: a KeyboardInterrupt between the deletes must still
            # roll back the open transaction (BaseRepo._transaction does the same).
            self.db.rollback()
            raise
        for table_name, count in live.table_counts:
            if count:
                SYNC_PURGE_ROWS_DELETED.labels(table=table_name).inc(count)
        SYNC_DISCONNECT_OUTCOMES.labels(
            scope="purged" if live.connection is not None else "purged_orphan"
        ).inc()
        logger.info(
            f"Purged sync item {live.provider_item_id}: {live.rows_to_delete} raw "
            f"rows deleted, {len(live.account_link_ids)} account links reversed, "
            f"{len(live.decision_ids)} proposals withdrawn"
        )
        return live

    def _purge_local(self, live: SyncPurgePlan, actor: str) -> None:
        """The local half of a purge, inside the caller's open transaction."""
        links = AccountLinksRepo(self.db)
        for link_id in live.account_link_ids:
            # A purge is only reachable through a confirmed user action, so
            # the domain actor is "user" whichever surface carried it.
            links.reverse(
                link_id=link_id,
                reversed_by="user",
                actor=actor,
                in_outer_txn=True,
            )
        self._reject_proposals(live.decision_ids, actor)
        for table in _PURGE_TABLES:
            self.db.execute(
                f"""
                DELETE FROM {table.full_name}
                WHERE source_origin = ?
                """,  # TableRef + parameterized value
                [live.provider_item_id],
            )
        self.db.execute(
            f"""
            DELETE FROM {SECURITY_PRICES.full_name}
            WHERE source_type = ? AND source_origin = ?
            """,  # TableRef + parameterized values
            [_PROVIDER, live.provider_item_id],
        )

    def _count_item_rows(self, table: TableRef, provider_item_id: str) -> int:
        row = self.db.execute(
            f"""
            SELECT COUNT(*) FROM {table.full_name}
            WHERE source_origin = ?
            """,  # TableRef + parameterized value
            [provider_item_id],
        ).fetchone()
        return int(row[0]) if row else 0

    def _count_item_prices(self, provider_item_id: str) -> int:
        # Shared across providers, so the provider scopes it as well as the item.
        row = self.db.execute(
            f"""
            SELECT COUNT(*) FROM {SECURITY_PRICES.full_name}
            WHERE source_type = ? AND source_origin = ?
            """,  # TableRef + parameterized values
            [_PROVIDER, provider_item_id],
        ).fetchone()
        return int(row[0]) if row else 0

    def _orphaned_proposals(self, provider_item_id: str) -> tuple[str, ...]:
        """Pending merge proposals that only this item's accounts still describe.

        An account this item linked is orphaned by the purge when no other
        accepted link — another item, an OFX statement, a CSV — feeds it. A
        proposal naming an orphaned account on either side would be reviewing a
        ledger that no longer exists, so it is withdrawn with the rows.
        """
        rows = self.db.execute(
            f"""
            WITH mine AS (
                SELECT account_id
                FROM {ACCOUNT_LINKS.full_name}
                WHERE status = 'accepted' AND source_type = ? AND source_origin = ?
            ),
            orphaned AS (
                SELECT m.account_id
                FROM mine AS m
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {ACCOUNT_LINKS.full_name} AS o
                    WHERE o.account_id = m.account_id
                      AND o.status = 'accepted'
                      AND NOT (o.source_type = ? AND o.source_origin = ?)
                )
            )
            SELECT d.decision_id
            FROM {ACCOUNT_LINK_DECISIONS.full_name} AS d
            WHERE d.status = 'pending'
              AND d.reversed_at IS NULL
              AND (
                  d.provisional_account_id IN (SELECT account_id FROM orphaned)
                  OR d.candidate_account_id IN (SELECT account_id FROM orphaned)
              )
            ORDER BY d.decision_id
            """,  # TableRef + parameterized values
            [_PROVIDER, provider_item_id, _PROVIDER, provider_item_id],
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def _reject_proposals(self, decision_ids: tuple[str, ...], actor: str) -> None:
        """Reject withdrawn proposals, freezing both names while they can be read."""
        if not decision_ids:
            return
        placeholders = ", ".join("?" for _ in decision_ids)
        pairs = self.db.execute(
            f"""
            SELECT decision_id, provisional_account_id, candidate_account_id
            FROM {ACCOUNT_LINK_DECISIONS.full_name}
            WHERE decision_id IN ({placeholders})
            """,  # TableRef + parameterized values
            list(decision_ids),
        ).fetchall()
        names = fetch_core_display_names(
            self.db, {str(row[1]) for row in pairs} | {str(row[2]) for row in pairs}
        )
        decisions = AccountLinkDecisionsRepo(self.db)
        for decision_id, provisional_id, candidate_id in pairs:
            decisions.update_status(
                str(decision_id),
                status="rejected",
                decided_by="user",
                actor=actor,
                provisional_display_name=names.get(str(provisional_id), ""),
                candidate_display_name=names.get(str(candidate_id), ""),
                in_outer_txn=True,
            )

    def _guidance_for(
        self, *, status: str, error_code: str | None, institution: str
    ) -> str | None:
        if status == "active":
            return None
        if status == "error":
            if error_code and error_code in _ERROR_GUIDANCE:
                return _ERROR_GUIDANCE[error_code].format(institution=institution)
            return (
                f"{institution} needs attention. "
                f"Run `moneybin sync link --institution {institution}` "
                f"to inspect and re-authenticate if needed."
            )
        if status == "revoked":
            return _ERROR_GUIDANCE["ITEM_NOT_FOUND"].format(institution=institution)
        return None

    # ------------------------------ Helpers ------------------------------

    def _find_institution(self, name: str) -> ConnectedInstitution | None:
        """Look up a connected institution by case-insensitive name match.

        Returns None when no connection matches (caller decides what to do).
        Raises UserError when multiple connections share the name — the name
        is ambiguous and must be disambiguated by the caller before any
        action runs.
        """
        institutions = self.client.list_institutions()
        matches = [
            inst
            for inst in institutions
            if inst.institution_name and inst.institution_name.lower() == name.lower()
        ]
        if len(matches) > 1:
            candidates = ", ".join(
                f"{m.provider_item_id} "
                f"(linked {m.created_at.strftime('%Y-%m-%d %H:%M UTC')})"
                for m in matches
            )
            raise UserError(
                f"multiple connected institutions match '{name}': {candidates}. "
                f"Only disconnect can target one of several same-named "
                f"connections: remove the extra one by provider_item_id "
                f"(`moneybin sync disconnect --provider-item-id <id>`, or "
                f"`sync_disconnect` over MCP), then retry.",
                code=error_codes.SYNC_INSTITUTION_AMBIGUOUS,
            )
        return matches[0] if matches else None

    def _find_institution_by_item(
        self, provider_item_id: str
    ) -> ConnectedInstitution | None:
        """Look up a connected institution by its exact provider_item_id.

        Unambiguous by construction — unlike institution name, the server
        assigns one item id per connection, so this never raises.
        """
        institutions = self.client.list_institutions()
        for inst in institutions:
            if inst.provider_item_id == provider_item_id:
                return inst
        return None

    def _resolve_institution(self, name: str) -> str:
        """Map a human-readable institution name to its provider_item_id.

        Strict — raises if no connection matches. Reads from GET /institutions.
        """
        inst = self._find_institution(name)
        if inst is None:
            raise UserError(
                f"no connected institution matching '{name}' — "
                f"run `moneybin sync status` to list connected banks",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        return inst.provider_item_id
