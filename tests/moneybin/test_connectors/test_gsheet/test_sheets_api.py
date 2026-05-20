"""Tests for SheetsClient error mapping and TestSheetsClient stub behavior."""

from unittest.mock import MagicMock

import httpx
import pytest
from googleapiclient.errors import HttpError

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetAuthError,
    GSheetRateLimitError,
    GSheetUnreachableError,
)
from moneybin.connectors.gsheet.sheets_api import (
    SheetsAPI,
    _map_error,  # pyright: ignore[reportPrivateUsage]
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


def test_map_error_network_to_unreachable() -> None:
    mapped = _map_error(httpx.ConnectError("connection refused"))
    assert isinstance(mapped, GSheetUnreachableError)


def test_map_error_timeout_to_unreachable() -> None:
    mapped = _map_error(httpx.ConnectTimeout("timed out"))
    assert isinstance(mapped, GSheetUnreachableError)


def test_map_error_generic_to_api_error() -> None:
    mapped = _map_error(RuntimeError("unexpected"))
    assert isinstance(mapped, GSheetAPIError)
