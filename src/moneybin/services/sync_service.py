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

import duckdb

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
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.metrics.registry import (
    ACCOUNT_LINK_OUTCOMES_TOTAL,
    SYNC_CONNECT_OUTCOMES,
    SYNC_INSTITUTION_ERRORS_TOTAL,
    SYNC_PULL_DURATION_SECONDS,
    SYNC_PULL_OUTCOMES_TOTAL,
    SYNC_PULL_TRANSACTIONS_LOADED,
)
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.refresh import refresh as _refresh
from moneybin.services.security_resolver import SecurityResolver
from moneybin.tables import (
    ACCOUNT_LINKS,
    FCT_INVESTMENT_TRANSACTIONS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    PLAID_INVESTMENT_TRANSACTIONS,
)

logger = logging.getLogger(__name__)

_PROVIDER = "plaid"  # Phase 1: single provider; widen when SimpleFIN/MX land


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
    ) -> PullResult:
        """Trigger a sync, fetch data, load into raw tables, return counts.

        When ``refresh`` is True (default) and the sync changed raw state
        (loaded new rows or processed removals), runs the post-load
        :func:`moneybin.services.refresh.refresh` pipeline — matching,
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
            raise ValueError(
                "institution and provider_item_id are mutually exclusive — pass one or neither"
            )
        if provider_item_id is None and institution is not None:
            provider_item_id = self._resolve_institution(institution)
        with SYNC_PULL_DURATION_SECONDS.labels(provider=_PROVIDER).time():
            try:
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
            except Exception as e:  # noqa: BLE001  # best-effort post-load ack
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
            except Exception as e:  # noqa: BLE001  # best-effort post-load metadata
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
            except Exception as e:  # noqa: BLE001  # reported via security_resolution_error, not raised
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
            refresh_result = _refresh(self.db)
            result.transforms_applied = refresh_result.applied
            result.transforms_duration_seconds = refresh_result.duration_seconds
            result.transforms_error = refresh_result.error
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

        ``persistent_token`` (Plaid ``persistent_account_id``) is the
        cross-connection strong ref, but the server's ``SyncAccount`` contract
        does not expose it today, so it is always None here — cross-connection
        identity for Plaid stays unwired until that field lands (followup).
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
                        acc.official_name
                        or (
                            f"{acc.institution_name} account"
                            if acc.institution_name
                            else acc.account_id
                        )
                    ),
                    account_number=None,  # Plaid never exposes a full number
                    last_four=acc.mask,
                    institution=acc.institution_name,
                    persistent_token=None,  # not in SyncAccount contract (followup)
                )
            )
            ACCOUNT_LINK_OUTCOMES_TOTAL.labels(result=resolved_account.outcome).inc()

    def _investment_source_overlap(self) -> list[str]:
        """Canonical account ids carrying BOTH manual and Plaid investment rows.

        Manual rows store canonical account ids; raw Plaid rows store
        provider-native ids — the join resolves the Plaid side through
        ``app.account_links`` first (falling back to the raw id when no link
        exists yet), since a raw-to-raw join would never match. The manual
        side is a ``WHERE EXISTS`` semi-join, not an inner join: this runs on
        every pull (including cash-only ones), and an inner join would cross
        N Plaid rows by M manual rows for the same account before the
        DISTINCT collapsed them back down — EXISTS only checks presence.
        """
        try:
            rows = self.db.execute(
                f"""
                SELECT DISTINCT COALESCE(al.account_id, p.account_id) AS account_id
                FROM {PLAID_INVESTMENT_TRANSACTIONS.full_name} AS p
                LEFT JOIN {ACCOUNT_LINKS.full_name} AS al
                  ON al.status = 'accepted' AND al.ref_kind = 'source_native'
                  AND al.source_type = 'plaid' AND al.source_origin = p.source_origin
                  AND al.ref_value = p.account_id
                WHERE EXISTS (
                  SELECT 1 FROM {MANUAL_INVESTMENT_TRANSACTIONS.full_name} AS m
                  WHERE m.account_id = COALESCE(al.account_id, p.account_id)
                )
                ORDER BY account_id
                """  # noqa: S608  # TableRef constants
            ).fetchall()
        except duckdb.CatalogException:  # tables may not exist on fresh DBs
            return []
        return [str(r[0]) for r in rows]

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
                f"SELECT COUNT(*) FROM {FCT_INVESTMENT_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant
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
                error_code=i.error_code,
                guidance=self._guidance_for(
                    status=i.status,
                    error_code=i.error_code,
                    institution=i.institution_name or "this connection",
                ),
            )
            for i in institutions
        ]

    def disconnect(self, *, institution: str) -> None:
        """Resolve institution name to connection id and call client.disconnect()."""
        inst = self._find_institution(institution)
        if inst is None:
            raise ValueError(
                f"no connected institution matching '{institution}' — "
                f"run `moneybin sync status` to list connected banks"
            )
        self.client.disconnect(inst.id)

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
        Raises ValueError when multiple connections share the name — the name is
        ambiguous and must be disambiguated by the caller before any action runs.
        """
        institutions = self.client.list_institutions()
        matches = [
            inst
            for inst in institutions
            if inst.institution_name and inst.institution_name.lower() == name.lower()
        ]
        if len(matches) > 1:
            ids = ", ".join(m.provider_item_id for m in matches)
            raise ValueError(
                f"multiple connected institutions match '{name}' ({ids}). "
                f"Run `moneybin sync status` to identify them; disambiguate "
                f"via the matching server-side connection id."
            )
        return matches[0] if matches else None

    def _resolve_institution(self, name: str) -> str:
        """Map a human-readable institution name to its provider_item_id.

        Strict — raises if no connection matches. Reads from GET /institutions.
        """
        inst = self._find_institution(name)
        if inst is None:
            raise ValueError(
                f"no connected institution matching '{name}' — "
                f"run `moneybin sync status` to list connected banks"
            )
        return inst.provider_item_id
