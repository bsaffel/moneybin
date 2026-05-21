"""Tests for GSheetConnectionService."""

from __future__ import annotations

import pytest

from moneybin.connectors.gsheet.connection_service import (
    AmbiguousDetectionError,
    ConnectionRequest,
    GSheetConnectionService,
    LowConfidenceError,
    rows_to_df,
)
from moneybin.connectors.gsheet.errors import GSheetError
from moneybin.connectors.gsheet.testing.fake_oauth_client import TestOAuthClient
from moneybin.connectors.gsheet.testing.fake_sheets_client import (
    FakeSheetTab,
    FakeWorkbook,
    TestSheetsClient,
)
from moneybin.database import Database


def _make_service(
    db: Database,
) -> tuple[GSheetConnectionService, TestSheetsClient, TestOAuthClient]:
    oauth = TestOAuthClient(authorized=True)
    sheets = TestSheetsClient()
    svc = GSheetConnectionService(db=db, sheets_client=sheets, oauth_client=oauth)
    return svc, sheets, oauth


def _tiller_workbook() -> FakeWorkbook:
    return FakeWorkbook(
        title="Tiller Foundation",
        tabs=[
            FakeSheetTab(
                name="Transactions",
                gid=0,
                headers=[
                    "Date",
                    "Description",
                    "Category",
                    "Amount",
                    "Account",
                ],
                rows=[
                    [
                        "2026-01-15",
                        "Whole Foods",
                        "Groceries",
                        "-87.42",
                        "Chase Checking",
                    ]
                ],
            )
        ],
    )


def _seed_workbook(gid: int = 99) -> FakeWorkbook:
    return FakeWorkbook(
        title="Personal Finance",
        tabs=[
            FakeSheetTab(
                name="Subscriptions",
                gid=gid,
                headers=["Name", "Amount", "Next Charge"],
                rows=[["Netflix", "15.49", "2026-02-15"]],
            )
        ],
    )


def test_connect_transactions_high_confidence(in_memory_db: Database) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss1", _tiller_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss1/edit#gid=0",
        adapter=None,
        account_name="Chase Checking",
        account_id="acct_chase",
        yes=True,
    )
    result = svc.connect(req)
    assert result.connection.adapter == "transactions"
    assert result.initial_pull is not None
    assert result.initial_pull.rows_inserted == 1


def test_connect_seed_explicit(in_memory_db: Database) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss2", _seed_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss2/edit#gid=99",
        adapter="seed",
        alias="subscriptions",
        yes=True,
    )
    result = svc.connect(req)
    assert result.connection.adapter == "seed"
    assert result.connection.alias == "subscriptions"


def test_connect_seed_requires_alias(in_memory_db: Database) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss2", _seed_workbook(gid=0))
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss2/edit#gid=0",
        adapter="seed",
        alias=None,
        yes=True,
    )
    with pytest.raises(GSheetError, match="alias"):
        svc.connect(req)


def test_connect_seed_alias_collision_refused(in_memory_db: Database) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ssA", _seed_workbook(gid=0))
    sheets.register_workbook("ssB", _seed_workbook(gid=0))
    svc.connect(
        ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ssA/edit#gid=0",
            adapter="seed",
            alias="subs",
            yes=True,
            no_initial_pull=True,
        )
    )
    # DuckDB raises a generic ConstraintException (subclass of Exception)
    # when the UNIQUE constraint on alias fires.
    with pytest.raises(Exception, match="(?i)unique|constraint"):  # noqa: B017, BLE001  # DuckDB raises generic Exception subclass
        svc.connect(
            ConnectionRequest(
                url="https://docs.google.com/spreadsheets/d/ssB/edit#gid=0",
                adapter="seed",
                alias="subs",
                yes=True,
                no_initial_pull=True,
            )
        )


def test_connect_low_confidence_refused_for_transactions(
    in_memory_db: Database,
) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook(
        "ss3",
        FakeWorkbook(
            title="Random",
            tabs=[
                FakeSheetTab(
                    name="random",
                    gid=0,
                    headers=["foo", "bar"],
                    rows=[["1", "2"]],
                )
            ],
        ),
    )
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss3/edit#gid=0",
        adapter="transactions",
        yes=True,
    )
    with pytest.raises(LowConfidenceError):
        svc.connect(req)


def test_connect_falls_through_to_seed_offer_on_low_confidence(
    in_memory_db: Database,
) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook(
        "ss3",
        FakeWorkbook(
            title="Random",
            tabs=[
                FakeSheetTab(
                    name="random",
                    gid=0,
                    headers=["foo", "bar"],
                    rows=[["1", "2"]],
                )
            ],
        ),
    )
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss3/edit#gid=0",
        adapter=None,
        accept_seed_fallback=True,
        alias="custom",
        yes=True,
    )
    result = svc.connect(req)
    assert result.connection.adapter == "seed"
    assert result.connection.alias == "custom"


def test_connect_transactions_requires_account_id(in_memory_db: Database) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss1", _tiller_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss1/edit#gid=0",
        adapter="transactions",
        account_name="Chase Checking",
        yes=True,
    )
    with pytest.raises(GSheetError, match="account-id"):
        svc.connect(req)


def _medium_confidence_workbook() -> FakeWorkbook:
    """Header set that map_columns scores at medium confidence."""
    return FakeWorkbook(
        title="Ambiguous Sheet",
        tabs=[
            FakeSheetTab(
                name="Transactions",
                gid=0,
                headers=["When", "Memo", "Bucket", "Amt"],
                rows=[["2026-01-01", "Coffee", "Food", "-4.50"]],
            )
        ],
    )


def test_connect_transactions_medium_confidence_requires_yes(
    in_memory_db: Database,
) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ssM", _medium_confidence_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ssM/edit#gid=0",
        adapter="transactions",
        account_name="Chase Checking",
        account_id="acct_chase",
        yes=False,
    )
    # Either rejected as ambiguous (medium) or low-confidence — both are
    # acceptable for this header set; the guarantee is that it does NOT
    # silently persist without explicit acceptance.
    with pytest.raises((AmbiguousDetectionError, LowConfidenceError)):
        svc.connect(req)


def test_rows_to_df_rejects_duplicate_headers() -> None:
    rows = [
        ["Date", "Amount", "Amount", "Description"],
        ["2026-01-01", "10", "20", "Coffee"],
    ]
    with pytest.raises(GSheetError, match="Duplicate header"):
        rows_to_df(rows)


def test_connect_no_initial_pull_skips_pull(in_memory_db: Database) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss1", _tiller_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss1/edit#gid=0",
        adapter="transactions",
        account_name="Chase Checking",
        account_id="acct_chase",
        yes=True,
        no_initial_pull=True,
    )
    result = svc.connect(req)
    assert result.initial_pull is None
    assert result.connection.status == "healthy"


def test_disconnect_soft_marks_disconnected(in_memory_db: Database) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss2", _seed_workbook())
    result = svc.connect(
        ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ss2/edit#gid=99",
            adapter="seed",
            alias="subscriptions",
            yes=True,
            no_initial_pull=True,
        )
    )
    cid = result.connection.connection_id

    svc.disconnect(cid)

    after = svc.get(cid)
    assert after is not None  # row retained
    assert after.status == "disconnected"


def test_disconnect_purge_drops_view_and_deletes_rows(
    in_memory_db: Database,
) -> None:
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss2", _seed_workbook())
    result = svc.connect(
        ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ss2/edit#gid=99",
            adapter="seed",
            alias="subscriptions",
            yes=True,
        )
    )
    cid = result.connection.connection_id

    # Pre-conditions: rows + view exist.
    pre_rows = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.gsheet_seeds WHERE connection_id = ?",
        [cid],
    ).fetchone()
    assert pre_rows is not None
    assert pre_rows[0] > 0

    svc.disconnect(cid, purge=True)

    # Connection row gone.
    assert svc.get(cid) is None

    # raw.gsheet_seeds wiped for this connection.
    post_rows = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.gsheet_seeds WHERE connection_id = ?",
        [cid],
    ).fetchone()
    assert post_rows is not None
    assert post_rows[0] == 0

    # View dropped.
    view_check = in_memory_db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name = 'gsheet_subscriptions'"
    ).fetchone()
    assert view_check is not None
    assert view_check[0] == 0
