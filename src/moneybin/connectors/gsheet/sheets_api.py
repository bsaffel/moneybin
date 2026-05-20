"""Google Sheets API v4 wrapper. Read-only.

Exposes a thin Protocol (`SheetsAPI`) implemented by both the production
`SheetsClient` (real google-api-python-client calls) and `TestSheetsClient`
(in-memory fake under `testing/`). All HTTP / library failures are mapped
to the project's typed exception hierarchy via `_map_error`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetAuthError,
    GSheetRateLimitError,
    GSheetUnreachableError,
)


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
                    range=sheet_name,
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


def _map_error(exc: Exception) -> Exception:
    """Map google-api-python-client / network exceptions to project exceptions."""
    from googleapiclient.errors import HttpError

    if isinstance(exc, HttpError):
        status: int = exc.resp.status  # type: ignore[reportUnknownMemberType]
        if status == 401:
            return GSheetAuthError(str(exc))
        if status == 429:
            return GSheetRateLimitError(str(exc))
        if status in (403, 404):
            return GSheetUnreachableError(str(exc))
        return GSheetAPIError(f"Google API HTTP {status}: {exc}")
    if isinstance(exc, httpx.NetworkError | httpx.TimeoutException):
        return GSheetUnreachableError(str(exc))
    return GSheetAPIError(str(exc))
