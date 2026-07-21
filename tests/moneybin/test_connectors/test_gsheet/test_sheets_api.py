"""Tests for SheetsClient error mapping and TestSheetsClient stub behavior."""

from unittest.mock import MagicMock

import pytest
from googleapiclient.errors import HttpError

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetAuthError,
    GSheetRateLimitError,
    GSheetUnreachableError,
)
from moneybin.connectors.gsheet.sheets_api import (
    MANAGED_SHEET_METADATA_KEY,
    SheetCreate,
    SheetIdentity,
    SheetRename,
    SheetsAPI,
    SheetsClient,
    SheetValueWrite,
    _map_error,  # pyright: ignore[reportPrivateUsage]
    _quote_a1_sheet_name,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.connectors.gsheet.testing.fake_sheets_client import (
    FakeSheetTab,
    FakeWorkbook,
    TestSheetsClient,
)


def _http_error(status: int) -> HttpError:
    """Construct an HttpError with the given HTTP status."""
    resp = MagicMock()
    resp.status = status
    resp.reason = "test"
    return HttpError(resp, b'{"error": "test"}')


# -- TestSheetsClient stub ----------------------------------------------------


def test_fake_returns_registered_metadata() -> None:
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="Tiller Foundation",
            tabs=[
                FakeSheetTab(
                    name="Transactions",
                    gid=0,
                    headers=["Date", "Amount"],
                    rows=[["2026-01-01", "10.00"]],
                )
            ],
        ),
    )
    meta = client.get_workbook_metadata("ss1")
    assert meta.title == "Tiller Foundation"
    assert meta.sheets[0].name == "Transactions"
    assert meta.sheets[0].gid == 0


def test_fake_returns_values() -> None:
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="WB",
            tabs=[
                FakeSheetTab(
                    name="S",
                    gid=0,
                    headers=["A", "B"],
                    rows=[["1", "2"], ["3", "4"]],
                )
            ],
        ),
    )
    values = client.read_sheet_values("ss1", "S")
    assert values == [["A", "B"], ["1", "2"], ["3", "4"]]


def test_fake_unknown_spreadsheet_raises_unreachable() -> None:
    client = TestSheetsClient()
    with pytest.raises(GSheetUnreachableError):
        client.get_workbook_metadata("missing")


def test_fake_injected_error_raises_once() -> None:
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="x",
            tabs=[FakeSheetTab(name="S", gid=0, headers=["A"], rows=[])],
        ),
    )
    client.inject_error(GSheetAuthError("token revoked"))
    with pytest.raises(GSheetAuthError):
        client.get_workbook_metadata("ss1")
    # Second call resumes normal behavior.
    meta = client.get_workbook_metadata("ss1")
    assert meta.title == "x"


def test_inject_error_clears_after_one_raise() -> None:
    """Explicit: a queued error fires exactly once and then clears."""
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="x",
            tabs=[FakeSheetTab(name="S", gid=0, headers=["A"], rows=[])],
        ),
    )
    client.inject_error(GSheetRateLimitError("429"))
    with pytest.raises(GSheetRateLimitError):
        client.read_sheet_values("ss1", "S")
    # Subsequent calls succeed without re-raising.
    assert client.read_sheet_values("ss1", "S") == [["A"]]
    meta = client.get_workbook_metadata("ss1")
    assert meta.title == "x"


def test_mutate_tab_changes_headers() -> None:
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="x",
            tabs=[
                FakeSheetTab(name="S", gid=0, headers=["Date", "Amount"], rows=[]),
            ],
        ),
    )
    client.mutate_tab("ss1", 0, headers=["Date", "Amount (USD)"])
    values = client.read_sheet_values("ss1", "S")
    assert values[0] == ["Date", "Amount (USD)"]


def test_unknown_sheet_name_raises_unreachable() -> None:
    """Reading a tab that doesn't exist in a registered workbook fails clean."""
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="x",
            tabs=[FakeSheetTab(name="Sheet1", gid=0, headers=["A"], rows=[])],
        ),
    )
    with pytest.raises(GSheetUnreachableError):
        client.read_sheet_values("ss1", "OtherTab")


def test_fake_sheets_client_implements_sheets_api_protocol() -> None:
    """TestSheetsClient must structurally satisfy SheetsAPI."""
    client: SheetsAPI = TestSheetsClient()
    # Touch both methods to keep the structural assignment load-bearing.
    assert callable(client.get_workbook_metadata)
    assert callable(client.read_sheet_values)
    assert callable(client.create_sheets)
    assert callable(client.write_sheet_values)
    assert callable(client.promote_sheets)


def test_fake_create_write_and_promote_are_captured_and_use_stable_ids() -> None:
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="WB",
            tabs=[FakeSheetTab(name="User Notes", gid=7, headers=["Note"], rows=[])],
        ),
    )

    created = client.create_sheets(
        "ss1",
        (
            SheetCreate(
                name="MB Staging run data",
                row_count=2,
                col_count=2,
                gid=42,
                managed_prefix="MB",
            ),
        ),
    )
    client.write_sheet_values(
        "ss1",
        (SheetValueWrite(sheet=created[0], values=(("A", "B"), ("1", "2"))),),
    )
    client.promote_sheets(
        "ss1",
        managed_prefix="MB",
        renames=(SheetRename(sheet=created[0], new_name="MB Bundle run data"),),
        deletes=(),
    )

    assert created[0].gid != 7
    assert client.read_sheet_values("ss1", "MB Bundle run data") == [
        ["A", "B"],
        ["1", "2"],
    ]
    assert client.requests == [
        ("create", ("MB Staging run data",)),
        ("write", (created[0].gid,)),
        ("promote", (created[0].gid,)),
    ]
    assert client.read_sheet_values("ss1", "User Notes") == [["Note"]]


def test_fake_promotion_deletes_only_explicit_sheet_identity() -> None:
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="WB",
            tabs=[
                FakeSheetTab(name="User Notes", gid=1, headers=["Note"], rows=[]),
                FakeSheetTab(
                    name="MB Bundle old data",
                    gid=2,
                    headers=["A"],
                    rows=[],
                    managed_prefix="MB",
                ),
                FakeSheetTab(
                    name="MB Report old report", gid=3, headers=["A"], rows=[]
                ),
            ],
        ),
    )
    staged = client.create_sheets(
        "ss1",
        (
            SheetCreate(
                name="MB Staging run data",
                row_count=1,
                col_count=1,
                gid=42,
                managed_prefix="MB",
            ),
        ),
    )[0]

    client.promote_sheets(
        "ss1",
        managed_prefix="MB",
        renames=(SheetRename(sheet=staged, new_name="MB Bundle run data"),),
        deletes=(SheetIdentity(name="MB Bundle old data", gid=2, managed_prefix="MB"),),
    )

    names = [sheet.name for sheet in client.get_workbook_metadata("ss1").sheets]
    assert names == ["User Notes", "MB Report old report", "MB Bundle run data"]


def test_fake_promotion_rejects_identity_outside_managed_prefix() -> None:
    client = TestSheetsClient()
    client.register_workbook(
        "ss1",
        FakeWorkbook(
            title="WB",
            tabs=[FakeSheetTab(name="User Notes", gid=1, headers=["Note"], rows=[])],
        ),
    )

    with pytest.raises(ValueError, match="managed namespace"):
        client.promote_sheets(
            "ss1",
            managed_prefix="MB",
            renames=(),
            deletes=(SheetIdentity(name="User Notes", gid=1),),
        )

    assert client.read_sheet_values("ss1", "User Notes") == [["Note"]]


@pytest.mark.parametrize("operation", ["create", "write", "promote"])
def test_fake_can_inject_failure_for_each_write_phase(operation: str) -> None:
    client = TestSheetsClient()
    client.register_workbook("ss1", FakeWorkbook(title="WB"))
    client.inject_error_for(operation, GSheetAPIError("injected"))

    with pytest.raises(GSheetAPIError, match="injected"):
        if operation == "create":
            client.create_sheets(
                "ss1", (SheetCreate(name="MB Staging run", row_count=1, col_count=1),)
            )
        elif operation == "write":
            client.write_sheet_values(
                "ss1",
                (
                    SheetValueWrite(
                        sheet=SheetIdentity("MB Staging run", 9), values=(("A",),)
                    ),
                ),
            )
        else:
            client.promote_sheets(
                "ss1",
                managed_prefix="MB",
                renames=(
                    SheetRename(
                        sheet=SheetIdentity("MB Staging run", 9),
                        new_name="MB Bundle run",
                    ),
                ),
                deletes=(),
            )


def test_sheets_client_create_uses_documented_batch_update_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oauth = MagicMock()
    service = MagicMock()
    service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {
        "replies": [
            {
                "addSheet": {
                    "properties": {
                        "title": "MB Staging run data",
                        "sheetId": 42,
                        "gridProperties": {"rowCount": 3, "columnCount": 2},
                    }
                }
            }
        ]
    }
    client = SheetsClient(oauth=oauth)
    build = MagicMock(return_value=service)
    monkeypatch.setattr(client, "_build_service", build)

    result = client.create_sheets(
        "ss1", (SheetCreate(name="MB Staging run data", row_count=3, col_count=2),)
    )

    build.assert_called_once_with(require_write=True)
    service.spreadsheets.return_value.batchUpdate.assert_called_once_with(
        spreadsheetId="ss1",
        body={
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": "MB Staging run data",
                            "gridProperties": {"rowCount": 3, "columnCount": 2},
                        }
                    }
                }
            ]
        },
    )
    assert result == (SheetIdentity(name="MB Staging run data", gid=42),)


def test_sheets_client_create_atomically_marks_managed_sheet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = MagicMock()
    service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {
        "replies": [
            {
                "addSheet": {
                    "properties": {
                        "title": "MB Staging run data",
                        "sheetId": 42,
                    }
                }
            },
            {"createDeveloperMetadata": {"developerMetadata": {"metadataId": 9}}},
        ]
    }
    client = SheetsClient(oauth=MagicMock())
    monkeypatch.setattr(client, "_build_service", MagicMock(return_value=service))

    result = client.create_sheets(
        "ss1",
        (
            SheetCreate(
                name="MB Staging run data",
                row_count=3,
                col_count=2,
                gid=42,
                managed_prefix="MB",
            ),
        ),
    )

    body = service.spreadsheets.return_value.batchUpdate.call_args.kwargs["body"]
    assert body["requests"][0]["addSheet"]["properties"]["sheetId"] == 42
    assert body["requests"][1] == {
        "createDeveloperMetadata": {
            "developerMetadata": {
                "metadataKey": MANAGED_SHEET_METADATA_KEY,
                "metadataValue": "MB",
                "location": {"sheetId": 42},
                "visibility": "DOCUMENT",
            }
        }
    }
    assert result == (SheetIdentity("MB Staging run data", 42, "MB"),)


def test_sheets_client_maps_malformed_successful_create_response_to_typed_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = MagicMock()
    service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {
        "replies": None
    }
    client = SheetsClient(oauth=MagicMock())
    monkeypatch.setattr(client, "_build_service", MagicMock(return_value=service))

    with pytest.raises(GSheetAPIError, match="invalid create response"):
        client.create_sheets("ss1", (SheetCreate("MB Staging run", 1, 1, 42, "MB"),))


def test_sheets_client_reads_only_exact_document_visible_ownership_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = MagicMock()
    service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "properties": {"title": "Output"},
        "sheets": [
            {
                "properties": {
                    "title": "MB Bundle run data",
                    "sheetId": 42,
                    "gridProperties": {"rowCount": 3, "columnCount": 2},
                },
                "developerMetadata": [
                    {
                        "metadataKey": MANAGED_SHEET_METADATA_KEY,
                        "metadataValue": "MB",
                        "visibility": "DOCUMENT",
                    }
                ],
            }
        ],
    }
    client = SheetsClient(oauth=MagicMock())
    monkeypatch.setattr(client, "_build_service", MagicMock(return_value=service))

    metadata = client.get_workbook_metadata("ss1")

    assert metadata.sheets[0].managed_prefix == "MB"


def test_sheets_client_values_batch_update_uses_rectangular_value_ranges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oauth = MagicMock()
    service = MagicMock()
    client = SheetsClient(oauth=oauth)
    build = MagicMock(return_value=service)
    monkeypatch.setattr(client, "_build_service", build)

    client.write_sheet_values(
        "ss1",
        (
            SheetValueWrite(
                sheet=SheetIdentity("MB Staging run data", 42),
                values=(("A", "B"), ("1", "2")),
            ),
        ),
    )

    build.assert_called_once_with(require_write=True)
    service.spreadsheets.return_value.values.return_value.batchUpdate.assert_called_once_with(
        spreadsheetId="ss1",
        body={
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": "'MB Staging run data'!A1:B2",
                    "majorDimension": "ROWS",
                    "values": [["A", "B"], ["1", "2"]],
                }
            ],
        },
    )


def test_sheets_client_promote_uses_atomic_delete_then_rename_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = MagicMock()
    client = SheetsClient(oauth=MagicMock())
    build = MagicMock(return_value=service)
    monkeypatch.setattr(client, "_build_service", build)

    client.promote_sheets(
        "ss1",
        managed_prefix="MB",
        deletes=(SheetIdentity("MB Bundle old data", 4, "MB"),),
        renames=(
            SheetRename(
                sheet=SheetIdentity("MB Staging run data", 42, "MB"),
                new_name="MB Bundle run data",
            ),
        ),
    )

    service.spreadsheets.return_value.batchUpdate.assert_called_once_with(
        spreadsheetId="ss1",
        body={
            "requests": [
                {"deleteSheet": {"sheetId": 4}},
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": 42,
                            "title": "MB Bundle run data",
                        },
                        "fields": "title",
                    }
                },
            ]
        },
    )


# -- _map_error ---------------------------------------------------------------


def test_map_error_401_to_auth_error() -> None:
    mapped = _map_error(_http_error(401))
    assert isinstance(mapped, GSheetAuthError)


def test_map_error_429_to_rate_limit() -> None:
    mapped = _map_error(_http_error(429))
    assert isinstance(mapped, GSheetRateLimitError)


def test_map_error_403_to_unreachable() -> None:
    mapped = _map_error(_http_error(403))
    assert isinstance(mapped, GSheetUnreachableError)


def test_map_error_404_to_unreachable() -> None:
    mapped = _map_error(_http_error(404))
    assert isinstance(mapped, GSheetUnreachableError)


def test_map_error_500_to_api_error() -> None:
    mapped = _map_error(_http_error(500))
    assert isinstance(mapped, GSheetAPIError)
    # Should NOT be one of the more specific subclasses.
    assert not isinstance(
        mapped, GSheetAuthError | GSheetRateLimitError | GSheetUnreachableError
    )


def test_map_error_oserror_to_unreachable() -> None:
    """httplib2 raises OSError subclasses for transport-level failures."""
    mapped = _map_error(ConnectionRefusedError("connection refused"))
    assert isinstance(mapped, GSheetUnreachableError)


def test_map_error_timeout_to_unreachable() -> None:
    mapped = _map_error(TimeoutError("timed out"))
    assert isinstance(mapped, GSheetUnreachableError)


def test_map_error_generic_to_api_error() -> None:
    mapped = _map_error(RuntimeError("unexpected"))
    assert isinstance(mapped, GSheetAPIError)


def _make_http_error_with_leaky_text(status: int) -> Exception:
    """Build an HttpError whose str() contains a URL + API key fragment.

    Real google-api-python-client errors look like:
        <HttpError 403 when requesting https://sheets.googleapis.com/...?key=AIza... returned "...">
    """
    from googleapiclient.errors import HttpError

    resp = MagicMock()
    resp.status = status
    resp.reason = "Forbidden"
    content = (
        b'{"error": {"code": 403, "message": '
        b'"https://sheets.googleapis.com/v4/spreadsheets/sensitive_ID?key=AIzaSyLEAK"}}'
    )
    return HttpError(resp, content, uri="https://sheets.googleapis.com/sensitive_ID")


def test_map_error_strips_http_error_text_from_typed_exception() -> None:
    """str(GSheet*) must not carry the raw HttpError text (URL, API key, body).

    The raw text reaches logs via any caller doing logger.warning(str(e)) —
    including the MCP decorator's catch-all envelope wrapping.
    """
    mapped = _map_error(_make_http_error_with_leaky_text(403))
    assert isinstance(mapped, GSheetUnreachableError)
    message = str(mapped)
    assert "sensitive_ID" not in message
    assert "AIzaSyLEAK" not in message
    assert "googleapis.com" not in message
    # Status code is fine — that's the whole point of typed errors.
    assert "403" in message


def test_map_error_strips_oserror_text() -> None:
    """OSError str() can carry filesystem-style paths or local hostnames."""
    mapped = _map_error(
        ConnectionRefusedError("connection to 192.168.0.42:443 refused")
    )
    message = str(mapped)
    assert "192.168.0.42" not in message
    assert "443" not in message


def test_sheets_client_wires_api_timeout_into_httplib2() -> None:
    """SheetsSettings.api_timeout_seconds must reach the httplib2.Http transport.

    Without this wiring, calls hang on a frozen Google API at the OS TCP
    default — defeating the per-tool timeout cap entirely.
    """
    import httplib2

    from moneybin.connectors.gsheet.sheets_api import SheetsClient

    fake_oauth = MagicMock()
    fake_oauth.get_access_token.return_value = "fake_token"
    client = SheetsClient(oauth=fake_oauth, timeout_seconds=7.5)

    service = client._build_service()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    # google_auth_httplib2.AuthorizedHttp wraps an httplib2.Http on .http
    http = service._http.http  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]  # noqa: SLF001
    assert isinstance(http, httplib2.Http)
    assert http.timeout == 7.5


def test_quote_a1_sheet_name_always_quotes() -> None:
    """Every tab name is single-quoted — no bare-name optimization.

    Cell-coordinate-shaped names (A1, B2, Z100) MUST be quoted or the
    Sheets API parses them as cell ranges instead of tab names.
    """
    assert _quote_a1_sheet_name("Sheet1") == "'Sheet1'"
    assert _quote_a1_sheet_name("transactions_2026") == "'transactions_2026'"
    # Cell-coordinate-shaped tab names — the bug the always-quote fix closes.
    assert _quote_a1_sheet_name("A1") == "'A1'"
    assert _quote_a1_sheet_name("B2") == "'B2'"
    assert _quote_a1_sheet_name("Z100") == "'Z100'"


def test_quote_a1_sheet_name_quotes_names_with_spaces() -> None:
    """Names with spaces, dashes, dots, etc. must be single-quoted."""
    assert _quote_a1_sheet_name("Sheet One") == "'Sheet One'"
    assert _quote_a1_sheet_name("Q1-2026") == "'Q1-2026'"
    assert _quote_a1_sheet_name("2026 budget") == "'2026 budget'"


def test_quote_a1_sheet_name_escapes_embedded_quotes() -> None:
    """Embedded single quotes double up per A1 quoting rules."""
    assert _quote_a1_sheet_name("Sheet's tab") == "'Sheet''s tab'"


def test_quote_a1_sheet_name_quotes_names_starting_with_digit() -> None:
    """Names that start with a digit aren't bare identifiers — quote them."""
    assert _quote_a1_sheet_name("2026") == "'2026'"
