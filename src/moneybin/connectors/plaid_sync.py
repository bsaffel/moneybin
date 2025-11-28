"""Client connector for Plaid data synchronization via MoneyBin Sync.

This connector implements secure client-server communication where:
- Client authenticates to MoneyBin Sync server (OAuth - future)
- All Plaid access tokens are stored and managed server-side
- Client requests sync operations; server handles all Plaid API calls
- Server encrypts data end-to-end before transmission (future)
- Client decrypts data with keys derived from master password (future)
- No Plaid tokens ever touch the client

Security Model:
1. User links bank via Plaid Link (server-hosted, server stores access token)
2. Client authenticates to MoneyBin Sync API (OAuth2/Auth0 - future stub)
3. Client calls sync_institutions() with MoneyBin API token + session public key
4. Server uses stored Plaid tokens to fetch data from banks
5. Server encrypts Parquet data with client's session public key (E2E - future)
6. Server returns encrypted Parquet data to client
7. Client decrypts with private key derived from master password (future)
8. Client saves plaintext to local profile-specific directory

End-to-End Encryption (Future):
- See docs/architecture/e2e-encryption.md for full design
- Master password never leaves client device
- Server never has access to plaintext financial data
- Uses age encryption with Argon2 key derivation
- Zero-knowledge architecture like password managers
"""

import logging

import polars as pl

from moneybin.config import get_settings

logger = logging.getLogger(__name__)


class PlaidSyncConnector:
    """Secure client connector for Plaid synchronization via MoneyBin Sync service.

    This connector never handles Plaid tokens directly. All bank connections are
    managed server-side, and the client only requests sync operations using
    MoneyBin Sync API authentication.

    In development mode (use_local_server=True), it simulates the client-server
    architecture while using local server code for convenience.
    """

    def __init__(self):
        """Initialize the secure Plaid sync connector.

        Raises:
            ValueError: If MoneyBin Sync is not enabled or not configured
        """
        settings = get_settings()

        if not settings.sync.enabled and not settings.sync.use_local_server:
            raise ValueError(
                "MoneyBin Sync is not enabled. Either:\n"
                "1. Enable sync service: MONEYBIN_SYNC__ENABLED=true\n"
                "2. Use local server for dev: MONEYBIN_SYNC__USE_LOCAL_SERVER=true"
            )

        self.settings = settings
        self.use_local_server = settings.sync.use_local_server
        self.server_url = settings.sync.server_url
        self.api_key = settings.sync.api_key

        # OAuth authentication stub (future: Auth0 integration)
        self._auth_token: str | None = None
        self._authenticated = False

        if self.use_local_server:
            logger.info("Using local server mode (development)")
            # In local mode, we'll still separate concerns but use local code
            self._init_local_server()
        else:
            logger.info(f"Using MoneyBin Sync API at {self.server_url}")
            # Production mode would authenticate to hosted API
            self._init_remote_server()

    def _init_local_server(self) -> None:
        """Initialize local server mode (development only).

        In local mode, we simulate the proper architecture but use local
        server code for convenience. This helps develop the correct separation
        of concerns before deploying the hosted service.
        """
        from moneybin_server.connectors.plaid.extractor import (
            PlaidExtractionConfig,
            PlaidExtractor,
        )

        # Server-side configuration (would be on hosted service in production)
        config = PlaidExtractionConfig(
            raw_data_path=self.settings.data.raw_data_path / "plaid",
        )

        # In local mode, we create a server instance directly
        # In production, this would be running on hosted infrastructure
        self._server_extractor = PlaidExtractor(
            config=config,
            database_path=self.settings.database.path,
        )

        # Simulate successful authentication
        self._authenticated = True
        logger.debug("Local server initialized and authenticated")

    def _init_remote_server(self) -> None:
        """Initialize remote server connection (production).

        Stub for future implementation:
        - Authenticate using OAuth2/Auth0
        - Exchange for MoneyBin Sync API token
        - Store token for subsequent requests
        """
        raise NotImplementedError(
            "Remote MoneyBin Sync API not yet implemented.\n"
            "Current implementation:\n"
            "1. Set MONEYBIN_SYNC__USE_LOCAL_SERVER=true for development\n"
            "2. Run Plaid Link flow to get access tokens (server-side)\n"
            "3. Server stores tokens securely (never sent to client)\n"
            "\n"
            "Future implementation:\n"
            "1. Client authenticates via OAuth (Auth0)\n"
            "2. Client calls sync API with auth token\n"
            "3. Server handles all Plaid communication\n"
            "4. Client receives and saves standardized data"
        )

    def authenticate(
        self, username: str | None = None, password: str | None = None
    ) -> bool:
        """Authenticate to MoneyBin Sync service (OAuth stub).

        Future implementation will use OAuth2/Auth0 for authentication.
        The client will never see or handle Plaid tokens.

        OAuth Flow (Future):
        1. Client redirects to Auth0 login
        2. User authenticates with email/password or social login
        3. Auth0 returns OAuth token to client
        4. Client uses OAuth token for all MoneyBin Sync API calls

        Args:
            username: MoneyBin Sync username (stub - future: OAuth)
            password: MoneyBin Sync password (stub - future: OAuth)

        Returns:
            bool: True if authentication successful

        Raises:
            NotImplementedError: OAuth not yet implemented
        """
        if self.use_local_server:
            # Local mode: Already authenticated during init
            return self._authenticated

        # Future OAuth implementation
        raise NotImplementedError(
            "OAuth authentication not yet implemented.\n"
            "Future flow:\n"
            "1. Client → Auth0 login page\n"
            "2. User authenticates\n"
            "3. Auth0 → Client (OAuth token)\n"
            "4. Client uses token for MoneyBin Sync API"
        )

    def link_institution(self, institution_id: str) -> dict[str, str]:
        """Link a new bank institution (Plaid Link flow).

        This initiates the Plaid Link flow which happens entirely server-side.
        The client never sees the Plaid access token.

        Server-side Flow:
        1. Client requests link for institution
        2. Server generates Plaid Link token
        3. Client opens Plaid Link UI (hosted by Plaid)
        4. User authenticates with bank
        5. Plaid sends access token to SERVER (not client)
        6. Server stores access token securely
        7. Server confirms link to client

        Args:
            institution_id: Identifier for the institution to link

        Returns:
            dict: Link status with institution info

        Raises:
            NotImplementedError: Plaid Link not yet implemented
        """
        raise NotImplementedError(
            "Plaid Link flow not yet implemented.\n"
            "Future implementation:\n"
            "1. Client → MoneyBin Sync API (request link)\n"
            "2. Server → Plaid API (create link token)\n"
            "3. Server → Client (Plaid Link URL)\n"
            "4. Client opens Plaid Link UI\n"
            "5. User authenticates with bank\n"
            "6. Plaid → Server (access token) - NEVER sent to client\n"
            "7. Server stores token securely\n"
            "8. Server → Client (success confirmation)"
        )

    def sync_institutions(
        self,
        institution_ids: list[str] | None = None,
        force_full_sync: bool = False,
    ) -> dict[str, dict[str, pl.DataFrame]]:
        """Sync data from linked institutions (primary sync method).

        This is the main sync operation. The client requests sync, and the
        server handles all communication with Plaid using stored access tokens.

        Secure Flow (Current):
        1. Client → MoneyBin Sync API (sync request with auth token)
        2. Server retrieves user's linked institutions
        3. Server uses stored Plaid tokens to fetch data
        4. Server extracts accounts and transactions from Plaid
        5. Server → Client (standardized Parquet data)
        6. Client saves data to local profile directory

        Secure Flow (Future with E2E Encryption):
        1. Client generates ephemeral key pair for this session
        2. Client → MoneyBin Sync API (sync request with auth token + session public key)
        3. Server retrieves user's linked institutions
        4. Server uses stored Plaid tokens to fetch data from Plaid
        5. Server converts to Parquet format
        6. Server encrypts Parquet with client's session public key (age encryption)
        7. Server → Client (encrypted Parquet data)
        8. Client decrypts with session private key (derived from master password)
        9. Client saves plaintext data to local profile directory

        Note: See docs/architecture/e2e-encryption.md for full encryption design

        Args:
            institution_ids: Optional list of specific institutions to sync
                           If None, syncs all linked institutions
            force_full_sync: If True, fetch full history instead of incremental

        Returns:
            dict: Mapping of institution names to their synced data
                  Each institution contains: accounts, transactions DataFrames
                  (Future: Will decrypt encrypted data before returning)

        Raises:
            ValueError: If not authenticated
        """
        if not self._authenticated:
            raise ValueError(
                "Not authenticated to MoneyBin Sync service. Call authenticate() first."
            )

        if self.use_local_server:
            return self._sync_local(institution_ids, force_full_sync)
        else:
            return self._sync_remote(institution_ids, force_full_sync)

    def _sync_local(
        self,
        institution_ids: list[str] | None,
        force_full_sync: bool,
    ) -> dict[str, dict[str, pl.DataFrame]]:
        """Sync using local server code (development mode).

        Even in local mode, we maintain the proper security model:
        - Plaid tokens are read from server-side environment (not client)
        - Client calls sync method
        - Server-side code handles Plaid API calls
        - Results returned to client

        This simulates the production architecture while using local code.
        """
        import os

        all_data = {}

        # Server-side: Read Plaid tokens from environment
        # In production, these would be in server's secure storage
        plaid_tokens = {
            key.replace("PLAID_TOKEN_", "").lower().replace("_", " "): value
            for key, value in os.environ.items()
            if key.startswith("PLAID_TOKEN_")
        }

        if not plaid_tokens:
            logger.warning(
                "No linked institutions found. "
                "In production, institutions would be linked via Plaid Link flow."
            )
            logger.info(
                "For development, add tokens: PLAID_TOKEN_INSTITUTION_NAME=access-token"
            )
            return all_data

        # Filter by requested institutions if specified
        if institution_ids:
            plaid_tokens = {
                name: token
                for name, token in plaid_tokens.items()
                if name in institution_ids
            }

        logger.info(f"Syncing {len(plaid_tokens)} institutions")

        # Server-side: Extract data from Plaid using stored tokens
        for institution_name, access_token in plaid_tokens.items():
            logger.info(f"Syncing {institution_name}")
            try:
                # Server calls Plaid API (client never sees token)
                institution_data = self._server_extractor.extract_all_data(
                    access_token=access_token,
                    institution_name=institution_name,
                    force_extraction=force_full_sync,
                )

                # Server returns standardized data to client
                all_data[institution_name] = institution_data
                logger.info(f"✅ Successfully synced {institution_name}")

            except Exception as e:
                logger.error(f"❌ Failed to sync {institution_name}: {e}")
                all_data[institution_name] = {
                    "accounts": pl.DataFrame(),
                    "transactions": pl.DataFrame(),
                }

        return all_data

    def _sync_remote(
        self,
        institution_ids: list[str] | None,
        force_full_sync: bool,
    ) -> dict[str, dict[str, pl.DataFrame]]:
        """Sync using remote MoneyBin Sync API (production).

        Future implementation with E2E encryption:

        1. Generate ephemeral session key pair
           ```python
           from pyrage import x25519
           private_key, public_key = x25519.generate()
           ```

        2. POST /api/v1/sync with auth token and public key
           ```python
           response = requests.post(
               f"{self.server_url}/api/v1/sync",
               headers={
                   "Authorization": f"Bearer {self._auth_token}",
                   "X-Session-Public-Key": base64.b64encode(public_key),
               },
               json={
                   "institution_ids": institution_ids,
                   "force_full_sync": force_full_sync,
               }
           )
           ```

        3. Server fetches data from Plaid (using stored access tokens)

        4. Server encrypts Parquet data with session public key
           ```python
           # Server-side (in moneybin_server)
           from pyrage import encrypt
           parquet_bytes = df.to_parquet()
           encrypted_data = encrypt(parquet_bytes, [public_key])
           ```

        5. Client receives encrypted Parquet data
           ```python
           encrypted_response = response.json()
           # {"chase": {"accounts": "encrypted_b64", "transactions": "encrypted_b64"}}
           ```

        6. Client decrypts with session private key
           ```python
           from pyrage import decrypt
           encrypted_accounts = base64.b64decode(encrypted_response["chase"]["accounts"])
           parquet_bytes = decrypt(encrypted_accounts, [private_key])
           accounts_df = pl.read_parquet(BytesIO(parquet_bytes))
           ```

        7. Client saves plaintext Parquet to local directory

        Note: Full design in docs/architecture/e2e-encryption.md

        Raises:
            NotImplementedError: Remote API not yet implemented
        """
        raise NotImplementedError(
            "Remote sync with E2E encryption not yet implemented.\n"
            "\n"
            "Planned implementation:\n"
            "1. Generate ephemeral session key pair (age/x25519)\n"
            "2. POST /api/v1/sync with OAuth token + session public key\n"
            "3. Server encrypts Parquet data with session public key\n"
            "4. Client decrypts with session private key\n"
            "5. Client saves plaintext to local profile directory\n"
            "\n"
            "Security benefits:\n"
            "- Server never sees plaintext financial data\n"
            "- Even with server compromise, data is encrypted\n"
            "- Zero-knowledge architecture like password managers\n"
            "\n"
            "See: docs/architecture/e2e-encryption.md"
        )

    def list_linked_institutions(self) -> list[dict[str, str]]:
        """List all institutions linked to this MoneyBin account.

        Returns:
            list: List of linked institutions with metadata

        Raises:
            NotImplementedError: Remote API not yet implemented
        """
        if self.use_local_server:
            import os

            institutions: list[dict[str, str]] = []
            for key, _value in os.environ.items():
                if key.startswith("PLAID_TOKEN_"):
                    inst_name = (
                        key.replace("PLAID_TOKEN_", "").lower().replace("_", " ")
                    )
                    institutions.append({
                        "name": inst_name,
                        "status": "linked",
                        "last_sync": "unknown",  # Would be tracked in production
                    })
            return institutions

        raise NotImplementedError(
            "Remote API not yet implemented.\nFuture: GET /api/v1/institutions"
        )


class PlaidConnectionManager:
    """Manages Plaid sync operations through MoneyBin Sync service.

    This manager coordinates sync operations across multiple institutions
    while maintaining the secure server-side token management.
    """

    def __init__(self):
        """Initialize connection manager with sync connector."""
        self.connector = PlaidSyncConnector()

    def extract_all_institutions(
        self, force_extraction: bool = False
    ) -> dict[str, dict[str, pl.DataFrame]]:
        """Sync data from all linked institutions.

        This is the main entry point for CLI and other code. It delegates
        to the secure sync_institutions method.

        Args:
            force_extraction: If True, perform full sync instead of incremental

        Returns:
            dict: Mapping of institution names to their synced data
        """
        return self.connector.sync_institutions(
            institution_ids=None,  # Sync all
            force_full_sync=force_extraction,
        )
