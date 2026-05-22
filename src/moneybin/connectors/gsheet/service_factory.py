"""Shared service-wiring helpers for CLI + MCP surfaces.

Both the `moneybin gsheet` CLI subgroup and the `gsheet_*` MCP tools need to
construct the same service stack (OAuth client → Sheets client → connection
or pull service) per invocation. This module is the one place that wiring
lives — surfaces import from here instead of duplicating the factory body.

The `_pull_service_with_db` variant yields `(service, db)` because the CLI
chains `refresh_run()` on the same Database connection after the pull;
MCP callers use `_pull_service` and let the context manager close the DB.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from moneybin.connectors.gsheet.connection_service import GSheetConnectionService
    from moneybin.connectors.gsheet.pull_service import GSheetPullService
    from moneybin.database import Database


def build_oauth_client() -> Any:
    """Construct a GoogleOAuthClient from current settings + SecretStore."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.gsheet.oauth_client import (  # noqa: PLC0415
        GoogleOAuthClient,
    )
    from moneybin.secrets import SecretStore  # noqa: PLC0415

    return GoogleOAuthClient(secrets=SecretStore(), settings=get_settings())


@contextmanager
def build_connection_service() -> Generator[GSheetConnectionService, None, None]:
    """Yield a GSheetConnectionService with an active Database connection."""
    from moneybin.connectors.gsheet.connection_service import (  # noqa: PLC0415
        GSheetConnectionService,
    )
    from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415

    oauth_client = build_oauth_client()
    sheets_client = SheetsClient(oauth=oauth_client)
    with get_database(read_only=False) as db:
        yield GSheetConnectionService(
            db=db, sheets_client=sheets_client, oauth_client=oauth_client
        )


@contextmanager
def build_pull_service() -> Generator[GSheetPullService, None, None]:
    """Yield a GSheetPullService with an active Database connection."""
    from moneybin.connectors.gsheet.pull_service import (  # noqa: PLC0415
        GSheetPullService,
    )
    from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415

    oauth_client = build_oauth_client()
    sheets_client = SheetsClient(oauth=oauth_client)
    with get_database(read_only=False) as db:
        yield GSheetPullService(
            db=db, sheets_client=sheets_client, oauth_client=oauth_client
        )


@contextmanager
def build_pull_service_with_db() -> Generator[
    tuple[GSheetPullService, Database], None, None
]:
    """Yield ``(GSheetPullService, Database)`` so callers can reuse the same DB.

    The CLI uses this variant to chain `refresh_run()` on the post-pull DB
    handle without re-acquiring the write lock.
    """
    from moneybin.connectors.gsheet.pull_service import (  # noqa: PLC0415
        GSheetPullService,
    )
    from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415

    oauth_client = build_oauth_client()
    sheets_client = SheetsClient(oauth=oauth_client)
    with get_database(read_only=False) as db:
        service = GSheetPullService(
            db=db, sheets_client=sheets_client, oauth_client=oauth_client
        )
        yield service, db
