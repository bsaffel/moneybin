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

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_models import (
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
    ) -> ConnectResult:
        """Connect new institution OR re-authenticate existing one.

        When `institution` is provided, resolve to provider_item_id and trigger
        Plaid update mode. When omitted, create a new connection.
        """
        provider_item_id = (
            self._resolve_institution(institution) if institution else None
        )
        initiate = self.client.initiate_connect(
            provider_item_id=provider_item_id,
            return_to=return_to,
        )
        if initiate.connect_type != "widget_flow":
            raise NotImplementedError(
                f"connect_type '{initiate.connect_type}' is not supported in this version"
            )
        # The CLI/MCP layer is responsible for surfacing initiate.link_url
        # to the user. The service blocks on polling.
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
        institutions = self.client.list_institutions()
        for inst in institutions:
            if (
                inst.institution_name
                and inst.institution_name.lower() == institution.lower()
            ):
                self.client.disconnect(inst.id)
                return
        raise ValueError(
            f"no connected institution matching '{institution}' — "
            f"run `moneybin sync status` to list connected banks"
        )

    def _guidance_for(self, *, status: str, institution: str) -> str | None:
        if status == "active":
            return None
        if status == "error":
            return _ERROR_GUIDANCE.get(
                "ITEM_LOGIN_REQUIRED",  # Phase 1: we don't track per-institution error_code
                "Connection error",
            ).format(institution=institution)
        if status == "revoked":
            return _ERROR_GUIDANCE["ITEM_NOT_FOUND"].format(institution=institution)
        return None

    # ------------------------------ Helpers ------------------------------

    def _resolve_institution(self, name: str) -> str:
        """Map a human-readable institution name to its provider_item_id.

        Reads from GET /institutions — server is the system of record.
        """
        institutions = self.client.list_institutions()
        for inst in institutions:
            if inst.institution_name and inst.institution_name.lower() == name.lower():
                return inst.provider_item_id
        raise ValueError(
            f"no connected institution matching '{name}' — "
            f"run `moneybin sync status` to list connected banks"
        )
