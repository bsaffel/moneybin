# tests/moneybin/test_services/test_account_service.py
"""Tests for AccountService, soft-validation classifier, and canonical lists."""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.account_service import (
    CLEAR,
    PLAID_CANONICAL_HOLDER_CATEGORIES,
    PLAID_CANONICAL_SUBTYPES,
    AccountListResult,
    AccountService,
    AccountSettings,
    AccountSettingsRepository,
    is_canonical_holder_category,
    is_canonical_subtype,
    suggest_holder_category,
    suggest_subtype,
)
from tests.moneybin.db_helpers import create_core_tables, create_core_tables_raw


def _make_settings(**overrides: object) -> AccountSettings:
    """Construct an AccountSettings with sensible defaults; override fields as needed."""
    defaults: dict[str, object] = {
        "account_id": "acct_abc",
        "display_name": "Checking",
        "last_four": "1234",
        "iso_currency_code": "USD",
        "archived": False,
        "include_in_net_worth": True,
    }
    return AccountSettings(**{**defaults, **overrides})  # type: ignore[arg-type]


class TestSubtypeClassifier:
    """Tests for Plaid subtype canonical list and soft-validation helpers."""

    @pytest.mark.unit
    def test_canonical_subtypes_present(self) -> None:
        assert "checking" in PLAID_CANONICAL_SUBTYPES
        assert "savings" in PLAID_CANONICAL_SUBTYPES
        assert "credit card" in PLAID_CANONICAL_SUBTYPES
        assert "mortgage" in PLAID_CANONICAL_SUBTYPES

    @pytest.mark.unit
    def test_is_canonical_true_for_known(self) -> None:
        assert is_canonical_subtype("checking") is True

    @pytest.mark.unit
    def test_is_canonical_false_for_unknown(self) -> None:
        assert is_canonical_subtype("chequing") is False

    @pytest.mark.unit
    def test_is_canonical_case_insensitive(self) -> None:
        assert is_canonical_subtype("CHECKING") is True

    @pytest.mark.unit
    def test_suggest_near_miss(self) -> None:
        assert suggest_subtype("chequing") == "checking"

    @pytest.mark.unit
    def test_suggest_returns_none_for_far_miss(self) -> None:
        assert suggest_subtype("xyz_garbage") is None


class TestHolderCategoryClassifier:
    """Tests for holder-category canonical set and soft-validation helpers."""

    @pytest.mark.unit
    def test_canonical_set(self) -> None:
        assert PLAID_CANONICAL_HOLDER_CATEGORIES == frozenset({
            "personal",
            "business",
            "joint",
        })

    @pytest.mark.unit
    def test_is_canonical(self) -> None:
        assert is_canonical_holder_category("personal") is True
        assert is_canonical_holder_category("corporate") is False

    @pytest.mark.unit
    def test_suggest_near_miss(self) -> None:
        assert suggest_holder_category("persoanl") == "personal"


@pytest.fixture()
def account_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with core + raw tables and test data seeded."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    conn = database.conn
    create_core_tables_raw(conn)

    # Insert test accounts — use named columns so DDL-added defaults apply to
    # Phase-2 columns (display_name, archived, etc.) without breaking this fixture.
    conn.execute("""
        INSERT INTO core.dim_accounts
            (account_id, routing_number, account_type, institution_name,
             institution_fid, source_type, source_file, extracted_at,
             loaded_at, updated_at)
        VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)  # noqa: S608  # test input, not executing SQL

    # Insert test balances
    conn.execute("""
        INSERT INTO raw.ofx_balances
            (account_id, statement_start_date, statement_end_date, ledger_balance,
             ledger_balance_date, available_balance, source_file,
             extracted_at, loaded_at, import_id, source_type)
        VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00, '2025-06-30',
         4800.00, 'test.qfx', '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx'),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00, '2025-06-30',
         15000.00, 'other.qfx', '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx')
    """)  # noqa: S608  # test input, not executing SQL

    db_module._database_instance = database  # type: ignore[attr-defined]
    yield database
    db_module._database_instance = None  # type: ignore[attr-defined]
    database.close()


class TestListAccounts:
    """Tests for AccountService.list_accounts()."""

    @pytest.mark.unit
    def test_returns_account_list_result(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        assert isinstance(result, AccountListResult)
        assert len(result.accounts) == 2

    @pytest.mark.unit
    def test_accounts_ordered_by_institution(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        # accounts are now dicts after the AccountListResult shape change
        names = [a["institution_name"] for a in result.accounts]
        assert names == sorted(names)

    @pytest.mark.unit
    def test_account_fields(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        acct = result.accounts[0]
        assert acct["account_id"] in ("ACC001", "ACC002")
        assert acct["account_type"] in ("CHECKING", "SAVINGS")

    @pytest.mark.unit
    def test_to_envelope_sensitivity_medium(self, account_db: Database) -> None:
        # Default (non-redacted) list returns medium sensitivity since it
        # includes institution names and account metadata.
        service = AccountService(account_db)
        result = service.list_accounts()
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"
        data: list[dict[str, Any]] = d["data"]
        assert len(data) == 2
        actions: list[str] = d["actions"]
        assert len(actions) > 0


class TestAccountSettingsModel:
    """Tests for AccountSettings dataclass construction and validation."""

    @pytest.mark.unit
    def test_full_construction(self) -> None:
        s = AccountSettings(
            account_id="acct_abc",
            display_name="Checking",
            official_name="PLATINUM CHECKING ACCOUNT",
            last_four="1234",
            account_subtype="checking",
            holder_category="personal",
            iso_currency_code="USD",
            credit_limit=Decimal("5000.00"),
            archived=False,
            include_in_net_worth=True,
        )
        assert s.display_name == "Checking"
        assert s.credit_limit == Decimal("5000.00")
        assert s.account_subtype == "checking"

    @pytest.mark.unit
    def test_display_name_too_long(self) -> None:
        with pytest.raises(ValueError, match="display_name"):
            AccountSettings(account_id="a", display_name="x" * 81)

    @pytest.mark.unit
    def test_last_four_format(self) -> None:
        with pytest.raises(ValueError, match="last_four"):
            AccountSettings(account_id="a", last_four="abcd")
        with pytest.raises(ValueError, match="last_four"):
            AccountSettings(account_id="a", last_four="123")

    @pytest.mark.unit
    def test_iso_currency_code_format(self) -> None:
        with pytest.raises(ValueError, match="iso_currency_code"):
            AccountSettings(account_id="a", iso_currency_code="usd")  # lowercase
        with pytest.raises(ValueError, match="iso_currency_code"):
            AccountSettings(account_id="a", iso_currency_code="USDD")

    @pytest.mark.unit
    def test_credit_limit_non_negative(self) -> None:
        with pytest.raises(ValueError, match="credit_limit"):
            AccountSettings(account_id="a", credit_limit=Decimal("-1.00"))

    @pytest.mark.unit
    def test_official_name_too_long(self) -> None:
        with pytest.raises(ValueError, match="official_name"):
            AccountSettings(account_id="a", official_name="x" * 201)

    @pytest.mark.unit
    def test_subtype_too_long(self) -> None:
        with pytest.raises(ValueError, match="account_subtype"):
            AccountSettings(account_id="a", account_subtype="x" * 33)


@pytest.fixture()
def test_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """Test database with all schemas + a seeded dim_accounts row for mutator tests.

    Seeds account_id='acct_a' so mutator tests (rename, archive, etc.) can call
    _assert_account_exists without failing on a missing row.
    """
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    create_core_tables(database)
    database.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, account_type, institution_name, source_type)
        VALUES ('acct_a', 'CHECKING', 'Test Bank', 'ofx')
        """
    )
    try:
        yield database
    finally:
        database.close()


class TestAccountSettingsRepository:
    """Tests for AccountSettingsRepository SQL operations."""

    @pytest.mark.unit
    def test_load_returns_none_when_absent(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        assert repo.load("acct_missing") is None

    @pytest.mark.unit
    def test_upsert_then_load(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        s = _make_settings(account_id="acct_a", display_name="Checking")
        repo.upsert(s)
        loaded = repo.load("acct_a")
        assert loaded is not None
        assert loaded.display_name == "Checking"

    @pytest.mark.unit
    def test_upsert_is_idempotent(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        s = _make_settings(account_id="acct_a", display_name="Checking")
        repo.upsert(s)
        repo.upsert(s)  # second write
        rows = test_db.execute(
            "SELECT COUNT(*) FROM app.account_settings WHERE account_id = ?",
            ["acct_a"],
        ).fetchone()
        assert rows[0] == 1  # type: ignore[index]

    @pytest.mark.unit
    def test_upsert_updates_changed_fields(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        repo.upsert(_make_settings(account_id="acct_a", display_name="A"))
        repo.upsert(_make_settings(account_id="acct_a", display_name="B"))
        loaded = repo.load("acct_a")
        assert loaded is not None
        assert loaded.display_name == "B"

    @pytest.mark.unit
    def test_delete(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        repo.upsert(_make_settings(account_id="acct_a", display_name="A"))
        repo.delete("acct_a")
        assert repo.load("acct_a") is None


class TestEmptyResults:
    """Tests for service behavior with no data in tables."""

    @pytest.fixture()
    def empty_db(self, tmp_path: Path) -> Generator[Database, None, None]:
        mock_store = MagicMock()
        mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
        database = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_store,
            no_auto_upgrade=True,
        )
        create_core_tables_raw(database.conn)
        db_module._database_instance = database  # type: ignore[attr-defined]
        yield database
        db_module._database_instance = None  # type: ignore[attr-defined]
        database.close()

    @pytest.mark.unit
    def test_list_accounts_empty_db(self, empty_db: Database) -> None:
        service = AccountService(empty_db)
        result = service.list_accounts()
        assert isinstance(result, AccountListResult)
        assert result.accounts == []


class TestAccountServiceMutators:
    """Tests for AccountService mutator methods."""

    @pytest.mark.unit
    def test_rename_inserts(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result = svc.rename("acct_a", "Checking")
        assert result.display_name == "Checking"

    @pytest.mark.unit
    def test_rename_clears_with_empty_string(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.rename("acct_a", "Checking")
        result = svc.rename("acct_a", "")
        assert result.display_name is None

    @pytest.mark.unit
    def test_include_idempotent(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.set_include_in_net_worth("acct_a", True)
        svc.set_include_in_net_worth("acct_a", True)
        loaded = AccountSettingsRepository(test_db).load("acct_a")
        assert loaded is not None
        assert loaded.include_in_net_worth is True

    @pytest.mark.unit
    def test_archive_cascades_to_include(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result = svc.archive("acct_a")
        assert result.archived is True
        assert result.include_in_net_worth is False

    @pytest.mark.unit
    def test_unarchive_does_not_restore_include(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.archive("acct_a")
        result = svc.unarchive("acct_a")
        assert result.archived is False
        assert result.include_in_net_worth is False  # NOT restored

    @pytest.mark.unit
    def test_settings_update_partial(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update(
            "acct_a", account_subtype="checking", credit_limit=Decimal("5000.00")
        )
        assert updated.account_subtype == "checking"
        assert updated.credit_limit == Decimal("5000.00")
        assert warnings == []  # canonical subtype, no warning
        # Verify persisted, not just returned
        loaded = AccountSettingsRepository(test_db).load("acct_a")
        assert loaded is not None
        assert loaded.account_subtype == "checking"
        assert loaded.credit_limit == Decimal("5000.00")

    @pytest.mark.unit
    def test_settings_update_clears_with_clear_sentinel(
        self, test_db: Database
    ) -> None:
        svc = AccountService(test_db)
        svc.settings_update("acct_a", credit_limit=Decimal("5000.00"))
        updated, _ = svc.settings_update("acct_a", credit_limit=CLEAR)
        assert updated.credit_limit is None

    @pytest.mark.unit
    def test_settings_update_soft_validation_warning(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update("acct_a", account_subtype="chequing")
        assert updated.account_subtype == "chequing"  # write succeeded
        assert len(warnings) == 1
        assert warnings[0]["field"] == "account_subtype"
        assert "chequing" in warnings[0]["message"]
        assert warnings[0]["suggestion"] == "checking"

    @pytest.mark.unit
    def test_settings_update_holder_category_warning(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update("acct_a", holder_category="corporate")
        assert updated.holder_category == "corporate"
        assert len(warnings) == 1
        assert warnings[0]["field"] == "holder_category"


# ---------------------------------------------------------------------------
# Helpers for new extended-read tests
# ---------------------------------------------------------------------------


def _insert_dim_account(
    db: Database,
    account_id: str,
    account_type: str = "CHECKING",
    institution_name: str = "Test Bank",
    source_type: str = "ofx",
    display_name: str | None = None,
    last_four: str | None = None,
    account_subtype: str | None = None,
    holder_category: str | None = None,
    iso_currency_code: str = "USD",
    credit_limit: Decimal | None = None,
    archived: bool = False,
    include_in_net_worth: bool = True,
    routing_number: str | None = None,
    official_name: str | None = None,
) -> None:
    """Insert a row directly into core.dim_accounts for unit testing.

    Bypasses SQLMesh (which is not run in unit tests) and inserts a fully
    resolved row with both source-derived and settings-derived columns.
    Setting archived=TRUE directly here is intentional — it lets tests that
    need to verify archive filtering do so without running the LEFT JOIN
    through app.account_settings.
    """
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            institution_fid, source_type, source_file, extracted_at,
            loaded_at, updated_at,
            display_name, official_name, last_four, account_subtype,
            holder_category, iso_currency_code, credit_limit,
            archived, include_in_net_worth
        ) VALUES (?, ?, ?, ?, NULL, ?, 'test.qfx', '2025-01-01',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            account_id,
            routing_number,
            account_type,
            institution_name,
            source_type,
            display_name,
            official_name,
            last_four,
            account_subtype,
            holder_category,
            iso_currency_code,
            credit_limit,
            archived,
            include_in_net_worth,
        ],
    )


@pytest.fixture()
def extended_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """Database with full Phase-2 dim_accounts DDL for extended-read tests."""
    database = Database(
        tmp_path / "extended_test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    create_core_tables(database)
    try:
        yield database
    finally:
        database.close()


class TestAccountServiceListExtended:
    """Extended list_accounts tests: new columns, archiving, redaction, type filter."""

    @pytest.mark.unit
    def test_list_includes_new_columns(self, extended_db: Database) -> None:
        _insert_dim_account(
            extended_db,
            "acct_ext1",
            display_name="My Checking",
            account_subtype="checking",
            holder_category="personal",
            last_four="9999",
            credit_limit=None,
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts()
        assert len(result.accounts) == 1
        acct = result.accounts[0]
        assert acct["account_id"] == "acct_ext1"
        assert acct["display_name"] == "My Checking"
        assert acct["account_subtype"] == "checking"
        assert acct["holder_category"] == "personal"
        assert acct["archived"] is False
        assert acct["include_in_net_worth"] is True

    @pytest.mark.unit
    def test_list_hides_archived_by_default(self, extended_db: Database) -> None:
        _insert_dim_account(extended_db, "acct_active", institution_name="Alpha Bank")
        _insert_dim_account(
            extended_db, "acct_archived", institution_name="Beta Bank", archived=True
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts()  # default: include_archived=False
        ids = [a["account_id"] for a in result.accounts]
        assert "acct_active" in ids
        assert "acct_archived" not in ids

    @pytest.mark.unit
    def test_list_include_archived_returns_all(self, extended_db: Database) -> None:
        _insert_dim_account(extended_db, "acct_active", institution_name="Alpha Bank")
        _insert_dim_account(
            extended_db, "acct_archived", institution_name="Beta Bank", archived=True
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts(include_archived=True)
        ids = [a["account_id"] for a in result.accounts]
        assert "acct_active" in ids
        assert "acct_archived" in ids
        assert len(ids) == 2

    @pytest.mark.unit
    def test_list_redacted_omits_pii_fields(self, extended_db: Database) -> None:
        _insert_dim_account(
            extended_db,
            "acct_pii",
            last_four="1234",
            credit_limit=Decimal("5000.00"),
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts(redacted=True)
        acct = result.accounts[0]
        assert "last_four" not in acct
        assert "credit_limit" not in acct
        # Sensitivity downgrades to low when redacted
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_list_type_filter(self, extended_db: Database) -> None:
        _insert_dim_account(
            extended_db,
            "acct_checking",
            account_type="CHECKING",
            institution_name="Alpha Bank",
        )
        _insert_dim_account(
            extended_db,
            "acct_savings",
            account_type="SAVINGS",
            institution_name="Alpha Bank",
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts(type_filter="CHECKING")
        ids = [a["account_id"] for a in result.accounts]
        assert "acct_checking" in ids
        assert "acct_savings" not in ids
        assert len(ids) == 1

    @pytest.mark.unit
    def test_list_type_filter_case_insensitive(self, extended_db: Database) -> None:
        # Seed an OFX-style account (uppercase account_type) and a user-set subtype
        # (lowercase). Filter with mixed casing should match.
        _insert_dim_account(
            extended_db,
            "acct_a",
            account_type="CHECKING",
            account_subtype="checking",
        )
        svc = AccountService(extended_db)
        # User filter "checking" should match account_type "CHECKING"
        result = svc.list_accounts(type_filter="checking")
        assert len(result.accounts) == 1
        # User filter "CHECKING" should also match
        result_upper = svc.list_accounts(type_filter="CHECKING")
        assert len(result_upper.accounts) == 1


class TestAccountServiceGetAccount:
    """Tests for AccountService.get_account()."""

    @pytest.mark.unit
    def test_get_returns_full_record(self, extended_db: Database) -> None:
        _insert_dim_account(
            extended_db,
            "acct_get1",
            display_name="Premium Checking",
            last_four="4321",
            account_subtype="checking",
            institution_name="First National",
        )
        svc = AccountService(extended_db)
        result = svc.get_account("acct_get1")
        assert result is not None
        assert result["account_id"] == "acct_get1"
        assert result["display_name"] == "Premium Checking"
        assert result["last_four"] == "4321"
        assert result["account_subtype"] == "checking"
        assert result["institution_name"] == "First National"
        assert result["archived"] is False
        assert result["include_in_net_worth"] is True

    @pytest.mark.unit
    def test_get_returns_none_for_missing(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        assert svc.get_account("acct_missing") is None


class TestAccountServiceSummary:
    """Tests for AccountService.summary()."""

    @pytest.mark.unit
    def test_summary_aggregates_by_type_and_subtype(
        self, extended_db: Database
    ) -> None:
        # 2 checking (one archived), 1 savings
        _insert_dim_account(
            extended_db,
            "acct_chk1",
            account_type="CHECKING",
            institution_name="Alpha Bank",
        )
        _insert_dim_account(
            extended_db,
            "acct_chk2",
            account_type="CHECKING",
            institution_name="Beta Bank",
            archived=True,
        )
        _insert_dim_account(
            extended_db,
            "acct_sav1",
            account_type="SAVINGS",
            institution_name="Gamma Bank",
            include_in_net_worth=False,
        )
        svc = AccountService(extended_db)
        result = svc.summary()
        # total_accounts counts all rows including archived
        assert result["total_accounts"] == 3
        # count_by_type excludes archived
        assert result["count_by_type"] == {"CHECKING": 1, "SAVINGS": 1}
        assert result["count_archived"] == 1
        assert result["count_excluded_from_net_worth"] == 1
        # recent activity is 0 (no transactions seeded)
        assert result["count_with_recent_activity"] == 0

    @pytest.mark.unit
    def test_summary_empty(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        result = svc.summary()
        assert result["total_accounts"] == 0
        assert result["count_by_type"] == {}


class TestMutatorAccountValidation:
    """Tests that mutators reject unknown account_ids."""

    @pytest.mark.unit
    def test_rename_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.rename("ACCTO1_typo", "new name")

    @pytest.mark.unit
    def test_set_include_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.set_include_in_net_worth("ACCTO1_typo", False)

    @pytest.mark.unit
    def test_archive_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.archive("ACCTO1_typo")

    @pytest.mark.unit
    def test_unarchive_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.unarchive("ACCTO1_typo")

    @pytest.mark.unit
    def test_settings_update_rejects_unknown_account(
        self, extended_db: Database
    ) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.settings_update("ACCTO1_typo", official_name="New Name")
