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


def test_connect_transactions_requires_account_id_or_name(
    in_memory_db: Database,
) -> None:
    """Neither account_id nor account_name supplied → connect refuses."""
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss1", _tiller_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss1/edit#gid=0",
        adapter="transactions",
        yes=True,
    )
    with pytest.raises(GSheetError, match="account-id or --account-name"):
        svc.connect(req)


def test_connect_transactions_resolves_account_name_to_id(
    in_memory_db: Database,
) -> None:
    """account_name resolves via AccountService.resolve_strict at connect time.

    The dim_accounts view is SQLMesh-built and isn't materialized in the
    in_memory_db fixture; mocking resolve_strict tests the *wiring* (that
    connect calls the resolver and stores its result as account_id),
    which is what the surface contract guarantees. AccountService has
    its own tests for the SQL lookup logic.
    """
    from unittest.mock import patch

    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss1", _tiller_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss1/edit#gid=0",
        adapter="transactions",
        account_name="Chase Checking",
        yes=True,
        no_initial_pull=True,
    )
    with patch(
        "moneybin.services.account_service.AccountService.resolve_strict",
        return_value="acct_chase_check",
    ) as resolver:
        result = svc.connect(req)
    resolver.assert_called_once_with("Chase Checking")
    assert result.connection.account_id == "acct_chase_check"
    assert result.connection.account_name == "Chase Checking"


def test_connect_transactions_account_name_not_found_surfaces_error(
    in_memory_db: Database,
) -> None:
    """Unknown account_name → AccountNotFoundError surfaces (UserError subclass)."""
    from unittest.mock import patch

    from moneybin.services.account_service import AccountNotFoundError

    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ss1", _tiller_workbook())
    req = ConnectionRequest(
        url="https://docs.google.com/spreadsheets/d/ss1/edit#gid=0",
        adapter="transactions",
        account_name="Nonexistent Account",
        yes=True,
    )
    with patch(
        "moneybin.services.account_service.AccountService.resolve_strict",
        side_effect=AccountNotFoundError(query="Nonexistent Account", candidates=[]),
    ):
        with pytest.raises(AccountNotFoundError):
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


def test_reconnect_medium_confidence_requires_yes(in_memory_db: Database) -> None:
    """Symmetric guard to connect: reconnect must not silently re-pin a medium mapping."""
    svc, sheets, _ = _make_service(in_memory_db)
    sheets.register_workbook("ssR", _tiller_workbook())
    # Establish a healthy connection first.
    result = svc.connect(
        ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ssR/edit#gid=0",
            adapter="transactions",
            account_name="Chase Checking",
            account_id="acct_chase",
            yes=True,
            no_initial_pull=True,
        )
    )
    cid = result.connection.connection_id

    # Swap the sheet for an ambiguous one — same workbook id, new shape.
    sheets.register_workbook("ssR", _medium_confidence_workbook())

    # Reconnect WITHOUT yes — must refuse a medium-confidence remap.
    with pytest.raises((AmbiguousDetectionError, LowConfidenceError)):
        svc.reconnect(cid, yes=False)


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


class TestGSheetPartialMergeColumnMapping:
    """Verify --column-mapping is partial-merge, not whole-map replacement."""

    def test_partial_merge_keeps_detected_for_unspecified_fields(
        self, in_memory_db: Database
    ) -> None:
        """Override for one dest field; others fall back to detection."""
        svc, sheets, _ = _make_service(in_memory_db)
        # Tiller sheet: detection proposes Date→transaction_date, Amount→amount,
        # Description→description. User supplies description override only.
        sheets.register_workbook("ssPM", _tiller_workbook())
        req = ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ssPM/edit#gid=0",
            adapter="transactions",
            account_name="Chase Checking",
            account_id="acct_chase",
            # source→dest override: remap "Description" source col to "description" dest
            # (same result as detection, but exercises the partial-merge path)
            column_mapping={"Description": "description"},
            yes=True,
            no_initial_pull=True,
        )
        result = svc.connect(req)
        cm = result.connection.column_mapping
        # Detected fields must survive; the override field is present too.
        assert cm.get("Date") == "transaction_date"
        assert cm.get("Amount") == "amount"
        assert cm.get("Description") == "description"

    def test_partial_merge_validates_missing_required(
        self, in_memory_db: Database
    ) -> None:
        """Override that leaves a required dest unmapped raises an error."""
        svc, sheets, _ = _make_service(in_memory_db)
        # Sheet where column content can't be detected as a date (no date-like data).
        sheets.register_workbook(
            "ssPM2",
            FakeWorkbook(
                title="Sheet",
                tabs=[
                    FakeSheetTab(
                        name="Data",
                        gid=0,
                        # Gibberish content — no date-like values, so the tabular
                        # column_mapper cannot detect transaction_date by content.
                        headers=["ColA", "ColB", "ColC"],
                        rows=[["xyz", "50", "note"], ["abc", "100", "note2"]],
                    )
                ],
            ),
        )
        # User supplies an override for amount only; detection can't find
        # transaction_date and the override doesn't supply it either.
        req = ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ssPM2/edit#gid=0",
            adapter="transactions",
            account_name="Test",
            account_id="acct_test",
            column_mapping={"ColB": "amount"},  # only amount; no transaction_date
            yes=True,
            no_initial_pull=True,
        )
        with pytest.raises(GSheetError, match="transaction_date"):
            svc.connect(req)

    def test_partial_merge_rejects_unknown_source_column(
        self, in_memory_db: Database
    ) -> None:
        """Override naming a header not in the sheet raises an error."""
        svc, sheets, _ = _make_service(in_memory_db)
        sheets.register_workbook("ssPM3", _tiller_workbook())
        req = ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ssPM3/edit#gid=0",
            adapter="transactions",
            account_name="Chase Checking",
            account_id="acct_chase",
            # "NoSuchCol" is not in the sheet headers.
            column_mapping={"NoSuchCol": "description"},
            yes=True,
            no_initial_pull=True,
        )
        with pytest.raises(GSheetError, match="NoSuchCol"):
            svc.connect(req)


class TestGSheetSharedConfidenceBands:
    """Verify gsheet uses ImportSettings.confidence bands for tier derivation."""

    def test_tier_derived_from_score_with_shared_bands(
        self, in_memory_db: Database
    ) -> None:
        """Detection score drives tier via the shared band thresholds."""
        # The Tiller workbook produces a high-confidence detection (score >= 0.90).
        # Verify that connect() succeeds without --yes for a high-tier result.
        svc, sheets, _ = _make_service(in_memory_db)
        sheets.register_workbook("ssBands", _tiller_workbook())
        req = ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ssBands/edit#gid=0",
            adapter="transactions",
            account_name="Chase Checking",
            account_id="acct_chase",
            yes=False,  # no explicit acceptance — relies on high confidence
            no_initial_pull=True,
        )
        # High confidence should succeed without --yes.
        result = svc.connect(req)
        assert result.connection.adapter == "transactions"

    def test_tier_responds_to_custom_bands(
        self, in_memory_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Lowering T_high to the detection score promotes medium → high tier.

        A sheet with score=0.75 is 'medium' under default bands (t_high=0.90).
        When T_high is lowered to 0.70, the same score clears t_high and
        becomes 'high' — the connect succeeds without --yes.
        """
        import moneybin.config as config_module

        # Lower T_high so that a score of 0.75 clears it → "high".
        monkeypatch.setenv("MONEYBIN_IMPORT___CONFIDENCE__T_HIGH", "0.70")
        # Invalidate the global settings cache so the env var takes effect.
        monkeypatch.setattr(config_module, "_current_settings", None)

        svc, sheets, _ = _make_service(in_memory_db)
        # This workbook produces score≈0.75 (medium under defaults): Date/Description/Amount
        # with a non-parseable date value so the date flag marks it below "high".
        sheets.register_workbook(
            "ssBands2",
            FakeWorkbook(
                title="Score75",
                tabs=[
                    FakeSheetTab(
                        name="Transactions",
                        gid=0,
                        headers=["Date", "Description", "Amount"],
                        rows=[["not-a-date", "Coffee", "-4.50"]],
                    )
                ],
            ),
        )
        req = ConnectionRequest(
            url="https://docs.google.com/spreadsheets/d/ssBands2/edit#gid=0",
            adapter="transactions",
            account_name="Chase Checking",
            account_id="acct_chase",
            yes=False,  # no explicit acceptance — high tier succeeds without --yes
            no_initial_pull=True,
        )
        # With T_high=0.70, score=0.75 ≥ T_high → "high" → no --yes needed.
        result = svc.connect(req)
        assert result.connection.adapter == "transactions"
