"""Business logic for sync operations.

CLI and MCP layers are thin wrappers around this service. Owns:
- pull() orchestration (trigger → fetch → load)
- connect() with optional auto-pull
- list_connections() with error-code → user-guidance mapping
- disconnect() with institution name → connection_id resolution

State source: server. No local connection-state mirror. See design Section 4.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    ConnectInitiateResponse,
    ConnectResult,
    PullResult,
    SyncConnectionView,
)
from moneybin.database import Database
from moneybin.loaders.plaid_loader import PlaidLoader
from moneybin.metrics.registry import (
    SYNC_CONNECT_OUTCOMES,
    SYNC_INSTITUTION_ERRORS_TOTAL,
    SYNC_PULL_DURATION_SECONDS,
    SYNC_PULL_OUTCOMES_TOTAL,
    SYNC_PULL_TRANSACTIONS_LOADED,
)
from moneybin.services.transform_service import TransformService

logger = logging.getLogger(__name__)

_PROVIDER = "plaid"  # Phase 1: single provider; widen when SimpleFIN/MX land


_ERROR_GUIDANCE: dict[str, str] = {
    "ITEM_LOGIN_REQUIRED": "{institution} needs re-authentication — run `moneybin sync connect --institution {institution}`",
    "ITEM_NOT_FOUND": "{institution} connection was revoked. Run `moneybin sync connect` to reconnect.",
    "INSTITUTION_NOT_RESPONDING": "{institution} is temporarily unavailable. Try again later.",
    "INSTITUTION_DOWN": "{institution} is down for maintenance. Try again later.",
    "RATE_LIMIT_EXCEEDED": "Rate limit reached. Sync will resume on the next scheduled run.",
    "PRODUCTS_NOT_READY": "{institution} is still processing initial data. Try again in a few minutes.",
}


class SyncService:
    """Orchestrates Plaid sync operations: pull, connect, list, disconnect."""

    def __init__(
        self,
        *,
        client: SyncClient,
        db: Database,
        loader: PlaidLoader,
    ) -> None:
        """Bind the service to a SyncClient, open Database, and PlaidLoader."""
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
        apply_transforms: bool = True,
    ) -> PullResult:
        """Trigger a sync, fetch data, load into raw tables, return counts.

        When ``apply_transforms`` is True (default) and the sync wrote at
        least one raw row, runs :meth:`TransformService.apply` once after
        the load so derived ``core.*`` models (especially ``dim_accounts``)
        reflect the new data before this call returns. Mirrors the
        end-of-batch contract documented in
        ``docs/specs/smart-import-transform.md`` for the import path.

        Transform failures soft-fail: raw rows stay durable, and the result
        envelope reports ``transforms_applied=False`` with ``transforms_error``.
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
            institutions=sync_data.metadata.institutions,
        )
        rows_landed = (
            load_result.transactions_loaded
            + load_result.accounts_loaded
            + load_result.balances_loaded
        )
        if apply_transforms and rows_landed > 0:
            apply_result = TransformService(self.db).apply()
            result.transforms_applied = apply_result.applied
            result.transforms_duration_seconds = apply_result.duration_seconds
            result.transforms_error = apply_result.error
        return result

    # ------------------------------ Connect ------------------------------

    def initiate_connect(
        self,
        *,
        institution: str | None = None,
        return_to: str | None = None,
    ) -> ConnectInitiateResponse:
        """Resolve institution and start a Plaid Link session — does not poll.

        Used by JSON-mode CLI and MCP sync_connect, where the caller surfaces
        link_url to the user and verifies completion via a separate
        sync_connect_status call. The full connect() path (resolve → initiate →
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
        initiate = self.client.initiate_connect(
            provider_item_id=provider_item_id,
            return_to=return_to,
        )
        if initiate.connect_type != "widget_flow":
            raise NotImplementedError(
                f"connect_type '{initiate.connect_type}' is not supported in this version"
            )
        return initiate

    def connect(
        self,
        *,
        institution: str | None = None,
        auto_pull: bool = True,
        return_to: str | None = None,
        on_initiate: Callable[[ConnectInitiateResponse], None] | None = None,
    ) -> ConnectResult:
        """Connect new institution OR re-authenticate existing one.

        When `institution` matches an existing connection, runs Plaid update mode
        against that item. When it matches none, falls through to a new-connection
        request (per design Section 8); the server's Link flow handles naming.
        Ambiguous matches (same name on multiple connections) raise.

        `on_initiate` is invoked synchronously with the ConnectInitiateResponse before
        the service starts polling. The CLI uses this hook to display `link_url`
        and optionally open the user's browser. Without it, the service blocks on
        polling without surfacing the URL — only safe for callers that surface it
        themselves (MCP returns the URL in its envelope and never enters this path).
        """
        initiate = self.initiate_connect(institution=institution, return_to=return_to)
        if on_initiate is not None:
            on_initiate(initiate)
        try:
            status = self.client.poll_connect_status(initiate.session_id)
        except Exception:
            # poll_connect_status raises SyncConnectError on terminal 'failed'
            # status, and SyncTimeoutError when the user abandons the browser.
            # Surface both as failed-connect outcomes; the CLI/MCP layer
            # re-raises with the specific exception type.
            SYNC_CONNECT_OUTCOMES.labels(status="failed").inc()
            raise
        SYNC_CONNECT_OUTCOMES.labels(status=status.status or "connected").inc()
        pull_result: PullResult | None = None
        if auto_pull:
            try:
                pull_result = self.pull(provider_item_id=status.provider_item_id)
            except Exception as e:
                logger.warning(f"Auto-pull failed after connect: {e}")
        return ConnectResult(
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
                f"Run `moneybin sync connect --institution {institution}` "
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
