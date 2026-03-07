"""Client connector for Plaid data synchronization via local server mode.

This connector handles Plaid sync in development/local mode where the
client calls the server-side Plaid extractor directly.
"""

import logging

import polars as pl

from moneybin.config import get_settings

logger = logging.getLogger(__name__)


class PlaidSyncConnector:
    """Plaid sync connector for local development mode.

    In local mode, the connector calls the server-side Plaid extractor
    directly. Plaid access tokens are read from environment variables.
    """

    def __init__(self):
        """Initialize the Plaid sync connector.

        Raises:
            ValueError: If sync is not enabled or configured.
        """
        settings = get_settings()

        if not settings.sync.enabled and not settings.sync.use_local_server:
            raise ValueError(
                "MoneyBin Sync is not enabled. Set:\n"
                "  MONEYBIN_SYNC__USE_LOCAL_SERVER=true"
            )

        self.settings = settings

        from moneybin_server.connectors.plaid.extractor import (
            PlaidExtractionConfig,
            PlaidExtractor,
        )

        config = PlaidExtractionConfig(
            raw_data_path=self.settings.data.raw_data_path / "plaid",
        )
        self._server_extractor = PlaidExtractor(
            config=config,
            database_path=self.settings.database.path,
        )
        logger.info("Using local server mode (development)")

    def sync_institutions(
        self,
        institution_ids: list[str] | None = None,
        force_full_sync: bool = False,
    ) -> dict[str, dict[str, pl.DataFrame]]:
        """Sync data from linked institutions.

        Args:
            institution_ids: Optional list of specific institutions to sync.
            force_full_sync: If True, fetch full history instead of incremental.

        Returns:
            Mapping of institution names to their synced data.
        """
        import os

        all_data: dict[str, dict[str, pl.DataFrame]] = {}

        plaid_tokens = {
            key.replace("PLAID_TOKEN_", "").lower().replace("_", " "): value
            for key, value in os.environ.items()
            if key.startswith("PLAID_TOKEN_")
        }

        if not plaid_tokens:
            logger.warning(
                "No linked institutions found. "
                "Add tokens: PLAID_TOKEN_INSTITUTION_NAME=access-token"
            )
            return all_data

        if institution_ids:
            plaid_tokens = {
                name: token
                for name, token in plaid_tokens.items()
                if name in institution_ids
            }

        logger.info("Syncing %d institutions", len(plaid_tokens))

        for institution_name, access_token in plaid_tokens.items():
            logger.info("Syncing %s", institution_name)
            try:
                institution_data = self._server_extractor.extract_all_data(
                    access_token=access_token,
                    institution_name=institution_name,
                    force_extraction=force_full_sync,
                )
                all_data[institution_name] = institution_data
                logger.info("Successfully synced %s", institution_name)
            except Exception as e:
                logger.error("Failed to sync %s: %s", institution_name, e)
                all_data[institution_name] = {
                    "accounts": pl.DataFrame(),
                    "transactions": pl.DataFrame(),
                }

        return all_data


class PlaidConnectionManager:
    """Manages Plaid sync operations."""

    def __init__(self):
        """Initialize connection manager with sync connector."""
        self.connector = PlaidSyncConnector()

    def extract_all_institutions(
        self, force_extraction: bool = False
    ) -> dict[str, dict[str, pl.DataFrame]]:
        """Sync data from all linked institutions.

        Args:
            force_extraction: If True, perform full sync instead of incremental.

        Returns:
            Mapping of institution names to their synced data.
        """
        return self.connector.sync_institutions(
            institution_ids=None,
            force_full_sync=force_extraction,
        )
