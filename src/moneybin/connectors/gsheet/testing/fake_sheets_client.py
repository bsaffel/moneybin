"""In-process stub for SheetsClient. Drives unit + integration + E2E tests."""

from __future__ import annotations

from dataclasses import dataclass, field

from moneybin.connectors.gsheet.errors import GSheetUnreachableError
from moneybin.connectors.gsheet.sheets_api import (
    SheetCreate,
    SheetIdentity,
    SheetInfo,
    SheetRename,
    SheetValueWrite,
    WorkbookMetadata,
)


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
        self._errors_by_operation: dict[str, Exception] = {}
        self.requests: list[tuple[str, tuple[object, ...]]] = []

    # Test helpers ---------------------------------------------------------
    def register_workbook(self, spreadsheet_id: str, workbook: FakeWorkbook) -> None:
        """Register a workbook so subsequent calls can resolve it."""
        self._workbooks[spreadsheet_id] = workbook

    def inject_error(self, exc: Exception) -> None:
        """Next call raises this exception, then resumes normal behavior."""
        self._error_on_next_call = exc

    def inject_error_for(self, operation: str, exc: Exception) -> None:
        """Raise once when the named API operation is next invoked."""
        self._errors_by_operation[operation] = exc

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
        self._maybe_raise("metadata")
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
        self._maybe_raise("read")
        wb = self._workbooks.get(spreadsheet_id)
        if wb is None:
            raise GSheetUnreachableError(f"Unknown spreadsheet_id: {spreadsheet_id}")
        for tab in wb.tabs:
            if tab.name == sheet_name:
                return [tab.headers, *tab.rows]
        raise GSheetUnreachableError(f"Unknown sheet_name: {sheet_name}")

    def create_sheets(
        self, spreadsheet_id: str, sheets: tuple[SheetCreate, ...]
    ) -> tuple[SheetIdentity, ...]:
        """Create tabs atomically with deterministic fake IDs."""
        self._maybe_raise("create")
        workbook = self._require_workbook(spreadsheet_id)
        existing_names = {tab.name for tab in workbook.tabs}
        requested_names = [sheet.name for sheet in sheets]
        if len(set(requested_names)) != len(
            requested_names
        ) or existing_names.intersection(requested_names):
            raise GSheetUnreachableError("Duplicate sheet name")
        next_gid = max((tab.gid for tab in workbook.tabs), default=-1) + 1
        identities: list[SheetIdentity] = []
        for offset, sheet in enumerate(sheets):
            gid = next_gid + offset
            workbook.tabs.append(FakeSheetTab(sheet.name, gid, [], []))
            identities.append(SheetIdentity(sheet.name, gid))
        self.requests.append(("create", tuple(requested_names)))
        return tuple(identities)

    def write_sheet_values(
        self, spreadsheet_id: str, writes: tuple[SheetValueWrite, ...]
    ) -> None:
        """Replace values on exact identities after validating all writes."""
        self._maybe_raise("write")
        workbook = self._require_workbook(spreadsheet_id)
        resolved = [self._require_tab(workbook, write.sheet) for write in writes]
        for write in writes:
            widths = {len(row) for row in write.values}
            if len(widths) > 1:
                raise ValueError("sheet value writes must be rectangular")
        for tab, write in zip(resolved, writes, strict=True):
            rows = [
                [str(cell) if cell is not None else "" for cell in row]
                for row in write.values
            ]
            tab.headers = rows[0] if rows else []
            tab.rows = rows[1:]
        self.requests.append(("write", tuple(write.sheet.gid for write in writes)))

    def promote_sheets(
        self,
        spreadsheet_id: str,
        *,
        managed_prefix: str,
        renames: tuple[SheetRename, ...],
        deletes: tuple[SheetIdentity, ...],
    ) -> None:
        """Apply exact deletes and renames atomically after prevalidation."""
        self._maybe_raise("promote")
        namespace = f"{managed_prefix} "
        if not managed_prefix or any(
            not sheet.name.startswith(namespace)
            for sheet in (*deletes, *(rename.sheet for rename in renames))
        ):
            raise ValueError("promotion identities must be in the managed namespace")
        if any(not rename.new_name.startswith(namespace) for rename in renames):
            raise ValueError("promoted titles must be in the managed namespace")
        workbook = self._require_workbook(spreadsheet_id)
        delete_tabs = [self._require_tab(workbook, sheet) for sheet in deletes]
        rename_tabs = [self._require_tab(workbook, rename.sheet) for rename in renames]
        delete_gids = {tab.gid for tab in delete_tabs}
        remaining = [tab for tab in workbook.tabs if tab.gid not in delete_gids]
        renamed_gids = {tab.gid for tab in rename_tabs}
        final_names = [tab.name for tab in remaining if tab.gid not in renamed_gids]
        final_names.extend(rename.new_name for rename in renames)
        if len(final_names) != len(set(final_names)):
            raise GSheetUnreachableError("Duplicate promoted sheet name")
        workbook.tabs = [tab for tab in workbook.tabs if tab.gid not in delete_gids]
        for tab, rename in zip(rename_tabs, renames, strict=True):
            tab.name = rename.new_name
        self.requests.append((
            "promote",
            tuple(sheet.gid for sheet in deletes)
            + tuple(rename.sheet.gid for rename in renames),
        ))

    def _require_workbook(self, spreadsheet_id: str) -> FakeWorkbook:
        workbook = self._workbooks.get(spreadsheet_id)
        if workbook is None:
            raise GSheetUnreachableError("Unknown spreadsheet")
        return workbook

    @staticmethod
    def _require_tab(workbook: FakeWorkbook, identity: SheetIdentity) -> FakeSheetTab:
        for tab in workbook.tabs:
            if tab.gid == identity.gid and tab.name == identity.name:
                return tab
        raise GSheetUnreachableError("Unknown sheet identity")

    def _maybe_raise(self, operation: str) -> None:
        """If an error is queued, raise and clear it."""
        if self._error_on_next_call is not None:
            exc, self._error_on_next_call = self._error_on_next_call, None
            raise exc
        exc = self._errors_by_operation.pop(operation, None)
        if exc is not None:
            raise exc
