"""Typed Google Sheets API v4 wrapper.

Exposes a thin Protocol (`SheetsAPI`) implemented by both the production
`SheetsClient` (real google-api-python-client calls) and `TestSheetsClient`
(in-memory fake under `testing/`). All HTTP / library failures are mapped
to the project's typed exception hierarchy via `_map_error`.
"""

from __future__ import annotations

import logging
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


@dataclass(frozen=True, slots=True)
class SheetIdentity:
    """Stable identity of one tab, safe across title changes."""

    name: str
    gid: int


@dataclass(frozen=True, slots=True)
class SheetCreate:
    """One grid tab to create in a structural batch."""

    name: str
    row_count: int
    col_count: int


@dataclass(frozen=True, slots=True)
class SheetValueWrite:
    """One rectangular value grid targeting a known sheet identity."""

    sheet: SheetIdentity
    values: tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class SheetRename:
    """One stable sheet identity and its promoted title."""

    sheet: SheetIdentity
    new_name: str


class OAuthCredentialsProvider(Protocol):
    """Minimal interface SheetsClient needs from any OAuth client.

    Satisfied structurally by Task 11's `GoogleOAuthClient`.
    """

    def get_access_token(self, *, require_write: bool = False) -> str:
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

    def create_sheets(
        self, spreadsheet_id: str, sheets: tuple[SheetCreate, ...]
    ) -> tuple[SheetIdentity, ...]:
        """Create grid tabs atomically and return their stable identities."""
        ...

    def write_sheet_values(
        self, spreadsheet_id: str, writes: tuple[SheetValueWrite, ...]
    ) -> None:
        """Write rectangular raw value grids to named tabs in one batch."""
        ...

    def promote_sheets(
        self,
        spreadsheet_id: str,
        *,
        managed_prefix: str,
        renames: tuple[SheetRename, ...],
        deletes: tuple[SheetIdentity, ...],
    ) -> None:
        """Atomically delete exact old identities and rename staged tabs."""
        ...


class SheetsClient:
    """Real wrapper around google-api-python-client.

    Raises typed `GSheet*` exceptions; never leaks Google library exceptions.
    """

    def __init__(
        self,
        oauth: OAuthCredentialsProvider,
        *,
        timeout_seconds: float | None = None,
    ) -> None:
        """Initialize with an OAuth credentials provider.

        ``timeout_seconds`` caps every HTTP request the underlying
        googleapiclient makes (transport-level, applied to the httplib2
        Http object). None resolves to ``GSheetSettings.api_timeout_seconds``
        at first build, so callers can override per-instance for tests.
        """
        self._oauth = oauth
        self._timeout_seconds = timeout_seconds
        # Cache the built service across calls, keyed by the access token.
        # build() parses the API discovery spec each time — for
        # pull_all_healthy with N connections that's 2N rebuilds per
        # refresh. Rebuild only when the token rotates (the only input to
        # _build_service that changes between calls).
        self._cached_service: Any = None
        self._cached_token: str | None = None

    def get_workbook_metadata(self, spreadsheet_id: str) -> WorkbookMetadata:
        """Fetch workbook title and per-tab metadata."""
        try:
            service = self._build_service(require_write=False)
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
            service = self._build_service(require_write=False)
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

    def create_sheets(
        self, spreadsheet_id: str, sheets: tuple[SheetCreate, ...]
    ) -> tuple[SheetIdentity, ...]:
        """Create grid tabs atomically and return their API-assigned IDs."""
        if not sheets:
            return ()
        if any(sheet.row_count < 1 or sheet.col_count < 1 for sheet in sheets):
            raise ValueError("created sheets require positive grid dimensions")
        body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet.name,
                            "gridProperties": {
                                "rowCount": sheet.row_count,
                                "columnCount": sheet.col_count,
                            },
                        }
                    }
                }
                for sheet in sheets
            ]
        }
        try:
            service = self._build_service(require_write=True)
            # Official request contracts (request bodies kept exact here):
            # https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets/batchUpdate
            # create: {"requests":[{"addSheet":{"properties":{"title":...,
            #   "gridProperties":{"rowCount":...,"columnCount":...}}}}]}
            # promote: {"requests":[{"deleteSheet":{"sheetId":...}},
            #   {"updateSheetProperties":{"properties":{"sheetId":...,
            #   "title":...},"fields":"title"}}]}
            # https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate
            # values: {"valueInputOption":"RAW","data":[{"range":...,
            #   "majorDimension":"ROWS","values":[[...]]}]}
            # Sheet IDs are immutable even when titles change:
            # https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets/sheets#SheetProperties
            response = (
                service
                .spreadsheets()
                .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
                .execute()
            )
        except Exception as exc:
            raise _map_error(exc) from exc
        replies = response.get("replies", [])
        if len(replies) != len(sheets):
            raise GSheetAPIError("Google Sheets returned an invalid create response")
        try:
            return tuple(
                SheetIdentity(
                    name=str(reply["addSheet"]["properties"]["title"]),
                    gid=int(reply["addSheet"]["properties"]["sheetId"]),
                )
                for reply in replies
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise GSheetAPIError(
                "Google Sheets returned an invalid create response"
            ) from exc

    def write_sheet_values(
        self, spreadsheet_id: str, writes: tuple[SheetValueWrite, ...]
    ) -> None:
        """Write one or more rectangular grids using RAW value semantics."""
        if not writes:
            return
        data: list[dict[str, object]] = []
        for write in writes:
            width = _rectangular_width(write.values)
            if width == 0:
                raise ValueError("sheet value writes require at least one column")
            row_count = len(write.values)
            data.append({
                "range": (
                    f"{_quote_a1_sheet_name(write.sheet.name)}!"
                    f"A1:{_column_name(width)}{row_count}"
                ),
                "majorDimension": "ROWS",
                "values": [list(row) for row in write.values],
            })
        try:
            service = self._build_service(require_write=True)
            (
                service
                .spreadsheets()
                .values()
                .batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"valueInputOption": "RAW", "data": data},
                )
                .execute()
            )
        except Exception as exc:
            raise _map_error(exc) from exc

    def promote_sheets(
        self,
        spreadsheet_id: str,
        *,
        managed_prefix: str,
        renames: tuple[SheetRename, ...],
        deletes: tuple[SheetIdentity, ...],
    ) -> None:
        """Atomically replace exact old identities with staged identities."""
        namespace = f"{managed_prefix} "
        identities = (*deletes, *(rename.sheet for rename in renames))
        promoted_names = tuple(rename.new_name for rename in renames)
        if not managed_prefix or any(
            not sheet.name.startswith(namespace) for sheet in identities
        ):
            raise ValueError("promotion identities must be in the managed namespace")
        if any(not name.startswith(namespace) for name in promoted_names):
            raise ValueError("promoted titles must be in the managed namespace")
        requests: list[dict[str, object]] = [
            {"deleteSheet": {"sheetId": sheet.gid}} for sheet in deletes
        ]
        requests.extend(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": rename.sheet.gid,
                        "title": rename.new_name,
                    },
                    "fields": "title",
                }
            }
            for rename in renames
        )
        if not requests:
            return
        try:
            service = self._build_service(require_write=True)
            (
                service
                .spreadsheets()
                .batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": requests},
                )
                .execute()
            )
        except Exception as exc:
            raise _map_error(exc) from exc

    def _build_service(self, *, require_write: bool = False) -> Any:
        """Build (or reuse) the Google Sheets v4 service.

        Reuses the cached service while the access token is unchanged;
        a token rotation invalidates the cache and rebuilds.
        """
        import google_auth_httplib2  # noqa: PLC0415
        import httplib2  # noqa: PLC0415
        from google.oauth2.credentials import Credentials  # noqa: PLC0415
        from googleapiclient.discovery import build  # noqa: PLC0415

        token = self._oauth.get_access_token(require_write=require_write)
        if self._cached_service is not None and token == self._cached_token:
            return self._cached_service

        creds = Credentials(token=token)
        timeout = self._timeout_seconds
        if timeout is None:
            from moneybin.config import get_settings  # noqa: PLC0415

            timeout = get_settings().gsheet.api_timeout_seconds
        # Wrap httplib2.Http with the timeout, then bind credentials via
        # google_auth_httplib2.AuthorizedHttp. Passing http= to build()
        # disables build()'s default Credentials-derived transport so
        # OUR timeout-bound transport is used end-to-end.
        http_with_timeout = google_auth_httplib2.AuthorizedHttp(
            credentials=creds,
            http=httplib2.Http(timeout=timeout),
        )
        service = build("sheets", "v4", http=http_with_timeout, cache_discovery=False)
        self._cached_service = service
        self._cached_token = token
        return service


def _rectangular_width(values: tuple[tuple[object, ...], ...]) -> int:
    """Return a rectangular grid width, rejecting ragged or empty input."""
    if not values:
        return 0
    width = len(values[0])
    if any(len(row) != width for row in values):
        raise ValueError("sheet value writes must be rectangular")
    return width


def _column_name(index: int) -> str:
    """Convert a one-based column index to its A1 column label."""
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def _quote_a1_sheet_name(sheet_name: str) -> str:
    """Always single-quote a sheet/tab name for A1 range use.

    Earlier versions skipped quoting for identifier-shaped names as an
    optimization. The optimization was unsafe: tab names matching the A1
    cell pattern (``A1``, ``B2``, ``Z100``) bypass quoting and the
    Sheets API then parses them as cell coordinates instead of tab
    names — silently returning wrong data. Always-quote eliminates the
    ambiguity; embedded single quotes still double up per A1 syntax.
    """
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
