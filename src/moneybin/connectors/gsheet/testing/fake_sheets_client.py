"""In-process stub for SheetsClient. Drives unit + integration + E2E tests."""

from __future__ import annotations

from dataclasses import dataclass, field

from moneybin.connectors.gsheet.errors import GSheetUnreachableError
from moneybin.connectors.gsheet.sheets_api import SheetInfo, WorkbookMetadata


@dataclass
class FakeSheetTab:
    """A registered tab within a `FakeWorkbook`."""

    name: str
    gid: int
    headers: list[str]
    rows: list[list[str]]


@dataclass
class FakeWorkbook:
    """An in-memory workbook used by `TestSheetsClient`."""

    title: str
    tabs: list[FakeSheetTab] = field(default_factory=list)


class TestSheetsClient:
    """Implements SheetsAPI Protocol with canned data and error injection."""

    # Tell pytest this is a test stub, not a test class.
    __test__ = False

    def __init__(self) -> None:
        """Initialize an empty fake-client with no registered workbooks."""
        self._workbooks: dict[str, FakeWorkbook] = {}
        self._error_on_next_call: Exception | None = None

    # Test helpers ---------------------------------------------------------
    def register_workbook(self, spreadsheet_id: str, workbook: FakeWorkbook) -> None:
        """Register a workbook so subsequent calls can resolve it."""
        self._workbooks[spreadsheet_id] = workbook

    def inject_error(self, exc: Exception) -> None:
        """Next call raises this exception, then resumes normal behavior."""
        self._error_on_next_call = exc

    def mutate_tab(
        self,
        spreadsheet_id: str,
        gid: int,
        *,
        headers: list[str] | None = None,
        rows: list[list[str]] | None = None,
    ) -> None:
        """Simulate a user editing the sheet between pulls."""
        wb = self._workbooks[spreadsheet_id]
        for tab in wb.tabs:
            if tab.gid == gid:
                if headers is not None:
                    tab.headers = headers
                if rows is not None:
                    tab.rows = rows
                return
        raise KeyError(f"No tab with gid={gid} in workbook {spreadsheet_id}")

    # SheetsAPI Protocol --------------------------------------------------
    def get_workbook_metadata(self, spreadsheet_id: str) -> WorkbookMetadata:
        """Return registered workbook metadata or raise unreachable."""
        self._maybe_raise()
        wb = self._workbooks.get(spreadsheet_id)
        if wb is None:
            raise GSheetUnreachableError(f"Unknown spreadsheet_id: {spreadsheet_id}")
        return WorkbookMetadata(
            title=wb.title,
            sheets=tuple(
                SheetInfo(
                    name=t.name,
                    gid=t.gid,
                    row_count=len(t.rows) + 1,
                    col_count=len(t.headers),
                )
                for t in wb.tabs
            ),
        )

    def read_sheet_values(
        self, spreadsheet_id: str, sheet_name: str
    ) -> list[list[str]]:
        """Return rows for the named tab, headers first."""
        self._maybe_raise()
        wb = self._workbooks.get(spreadsheet_id)
        if wb is None:
            raise GSheetUnreachableError(f"Unknown spreadsheet_id: {spreadsheet_id}")
        for tab in wb.tabs:
            if tab.name == sheet_name:
                return [tab.headers, *tab.rows]
        raise GSheetUnreachableError(f"Unknown sheet_name: {sheet_name}")

    def _maybe_raise(self) -> None:
        """If an error is queued, raise and clear it."""
        if self._error_on_next_call is not None:
            exc, self._error_on_next_call = self._error_on_next_call, None
            raise exc
