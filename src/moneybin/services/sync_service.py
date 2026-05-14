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
from moneybin.connectors.sync_models import PullResult
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
        force: bool = False,
    ) -> PullResult:
        """Trigger a sync, fetch data, load into raw tables, return counts."""
        provider_item_id = (
            self._resolve_institution(institution) if institution else None
        )
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
