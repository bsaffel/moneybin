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

logger = logging.getLogger(__name__)


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
    ) -> PullResult:
        """Trigger a sync, fetch data, load into raw tables, return counts."""
        if institution is not None and provider_item_id is not None:
            raise ValueError(
                "institution and provider_item_id are mutually exclusive — pass one or neither"
            )
        if provider_item_id is None and institution is not None:
            provider_item_id = self._resolve_institution(institution)
        trigger_resp = self.client.trigger_sync(
            provider_item_id=provider_item_id,
            reset_cursor=force,
        )
        if trigger_resp.status != "completed":
            # Server contract: /sync/trigger is synchronous and must return a terminal
            # status. pending/running here means the server returned before the sync
            # finished — proceeding to get_data would silently load nothing.
            raise RuntimeError(
                f"sync trigger returned non-terminal status '{trigger_resp.status}' "
                f"for job_id={trigger_resp.job_id}; expected 'completed'"
            )
        sync_data = self.client.get_data(trigger_resp.job_id)
        removed_count = self.loader.handle_removed_transactions(
            sync_data.removed_transactions,
        )
        load_result = self.loader.load(sync_data, trigger_resp.job_id)
        return PullResult(
            job_id=trigger_resp.job_id,
            transactions_loaded=load_result.transactions_loaded,
            accounts_loaded=load_result.accounts_loaded,
            balances_loaded=load_result.balances_loaded,
            transactions_removed=removed_count,
            institutions=sync_data.metadata.institutions,
        )

    # ------------------------------ Connect ------------------------------

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
        provider_item_id: str | None = None
        if institution:
            inst = self._find_institution(institution)
            if inst is not None:
                provider_item_id = inst.provider_item_id
            # else: institution name doesn't match any existing connection;
            # fall through to new-connection request per design Section 8.
        initiate = self.client.initiate_connect(
            provider_item_id=provider_item_id,
            return_to=return_to,
        )
        if initiate.connect_type != "widget_flow":
            raise NotImplementedError(
                f"connect_type '{initiate.connect_type}' is not supported in this version"
            )
        if on_initiate is not None:
            on_initiate(initiate)
        status = self.client.poll_connect_status(initiate.session_id)
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
                guidance=self._guidance_for(
                    status=i.status,
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

    def _guidance_for(self, *, status: str, institution: str) -> str | None:
        # Phase 1: ConnectedInstitution doesn't carry per-institution error_code,
        # so we can't map to the specific entry in _ERROR_GUIDANCE. Use a generic
        # message that doesn't lie about the root cause (e.g., claiming
        # ITEM_LOGIN_REQUIRED when the actual error is INSTITUTION_DOWN).
        if status == "active":
            return None
        if status == "error":
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
