"""Google Sheets API v4 wrapper. Read-only.

Exposes a thin Protocol (`SheetsAPI`) implemented by both the production
`SheetsClient` (real google-api-python-client calls) and `TestSheetsClient`
(in-memory fake under `testing/`). All HTTP / library failures are mapped
to the project's typed exception hierarchy via `_map_error`.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Protocol

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetAuthError,
    GSheetRateLimitError,
    GSheetUnreachableError,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SheetInfo:
    """Metadata for a single tab within a workbook."""

    name: str
    gid: int
    row_count: int
    col_count: int


@dataclass(frozen=True)
class WorkbookMetadata:
    """Workbook-level metadata returned by `spreadsheets.get`."""

    title: str
    sheets: tuple[SheetInfo, ...]


class OAuthCredentialsProvider(Protocol):
    """Minimal interface SheetsClient needs from any OAuth client.

    Satisfied structurally by Task 11's `GoogleOAuthClient`.
    """

    def get_access_token(self) -> str:
        """Return a current OAuth access token, refreshing if needed."""
        ...


class SheetsAPI(Protocol):
    """Interface implemented by both `SheetsClient` and `TestSheetsClient`."""

    def get_workbook_metadata(self, spreadsheet_id: str) -> WorkbookMetadata:
        """Fetch workbook title and per-tab metadata."""
        ...

    def read_sheet_values(
        self, spreadsheet_id: str, sheet_name: str
    ) -> list[list[str]]:
        """Read all cell values from one tab as rows of strings."""
        ...


class SheetsClient:
    """Real wrapper around google-api-python-client.

    Raises typed `GSheet*` exceptions; never leaks Google library exceptions.
    """

    def __init__(self, oauth: OAuthCredentialsProvider) -> None:
        """Initialize with an OAuth credentials provider."""
        self._oauth = oauth

    def get_workbook_metadata(self, spreadsheet_id: str) -> WorkbookMetadata:
        """Fetch workbook title and per-tab metadata."""
        try:
            service = self._build_service()
            meta = (
                service
                .spreadsheets()
                .get(spreadsheetId=spreadsheet_id, includeGridData=False)
                .execute()
            )
        except Exception as exc:
            raise _map_error(exc) from exc

        sheets = tuple(
            SheetInfo(
                name=s["properties"]["title"],
                gid=s["properties"]["sheetId"],
                row_count=s["properties"]["gridProperties"]["rowCount"],
                col_count=s["properties"]["gridProperties"]["columnCount"],
            )
            for s in meta["sheets"]
        )
        return WorkbookMetadata(title=meta["properties"]["title"], sheets=sheets)

    def read_sheet_values(
        self, spreadsheet_id: str, sheet_name: str
    ) -> list[list[str]]:
        """Read all cell values from a single tab as rows of strings."""
        try:
            service = self._build_service()
            result = (
                service
                .spreadsheets()
                .values()
                .get(
                    spreadsheetId=spreadsheet_id,
                    range=_quote_a1_sheet_name(sheet_name),
                    valueRenderOption="UNFORMATTED_VALUE",
                    dateTimeRenderOption="FORMATTED_STRING",
                )
                .execute()
            )
            return [[str(cell) for cell in row] for row in result.get("values", [])]
        except Exception as exc:
            raise _map_error(exc) from exc

    def _build_service(self) -> Any:
        """Build the Google Sheets v4 service with the current access token."""
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        creds = Credentials(token=self._oauth.get_access_token())
        return build("sheets", "v4", credentials=creds, cache_discovery=False)


_A1_BARE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_a1_sheet_name(sheet_name: str) -> str:
    """Wrap a sheet/tab name for safe use as an A1 range.

    Google Sheets A1 notation requires single-quoting tab names that
    contain anything other than letters / digits / underscore (and that
    don't start with a digit). Embedded single quotes double up.

    Names that are already safe pass through unchanged so the on-wire
    range matches the bare form Google docs use in examples.
    """
    if _A1_BARE_RE.fullmatch(sheet_name):
        return sheet_name
    escaped = sheet_name.replace("'", "''")
    return f"'{escaped}'"


def _map_error(exc: Exception) -> Exception:
    """Map google-api-python-client / network exceptions to project exceptions.

    google-api-python-client wraps API responses in
    ``googleapiclient.errors.HttpError`` and surfaces transport-level
    failures (DNS, TCP, TLS, socket timeouts) as ``OSError`` subclasses
    via httplib2. Branch on those two; everything else falls through to
    GSheetAPIError so the caller still gets a typed surface.

    Exception messages stay status-code-only or generic. ``str(exc)`` on
    a Google ``HttpError`` includes the full request URL (with any API
    key query param) and response body — those must not flow into the
    typed exception text where downstream ``logger.warning(str(e))`` or
    error-envelope construction would leak them. Full detail is logged
    internally via ``logger.debug(..., exc_info=True)``.
    """
    from googleapiclient.errors import HttpError

    if isinstance(exc, HttpError):
        status: int = exc.resp.status  # type: ignore[reportUnknownMemberType]
        logger.debug("Google Sheets HttpError mapped", exc_info=exc)
        if status == 401:
            return GSheetAuthError(f"Google Sheets HTTP {status}")
        if status == 429:
            return GSheetRateLimitError(f"Google Sheets HTTP {status}")
        if status in (403, 404):
            return GSheetUnreachableError(f"Google Sheets HTTP {status}")
        return GSheetAPIError(f"Google Sheets HTTP {status}")
    if isinstance(exc, OSError):
        # DNS failures, refused connections, socket timeouts — all OSError
        # subclasses via httplib2 / urllib3 under google-api-python-client.
        logger.debug("Google Sheets transport error mapped", exc_info=exc)
        return GSheetUnreachableError("Network unreachable")
    logger.debug("Google Sheets unmapped error", exc_info=exc)
    return GSheetAPIError("Unexpected Google Sheets API error")
