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
from moneybin.services.account_service import (
    PLAID_CANONICAL_HOLDER_CATEGORIES,
    PLAID_CANONICAL_SUBTYPES,
    AccountBalance,
    AccountListResult,
    AccountService,
    AccountSettings,
    AccountSettingsRepository,
    BalanceListResult,
    is_canonical_holder_category,
    is_canonical_subtype,
    suggest_holder_category,
    suggest_subtype,
)
from tests.moneybin.db_helpers import create_core_tables_raw


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

    # Insert test accounts
    conn.execute("""
        INSERT INTO core.dim_accounts VALUES
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
        names = [a.institution_name for a in result.accounts]
        assert names == sorted(names)

    @pytest.mark.unit
    def test_account_fields(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        acct = result.accounts[0]
        assert acct.account_id in ("ACC001", "ACC002")
        assert acct.account_type in ("CHECKING", "SAVINGS")
        assert acct.source_type == "ofx"

    @pytest.mark.unit
    def test_to_envelope_sensitivity_low(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
        data: list[dict[str, Any]] = d["data"]
        assert len(data) == 2
        actions: list[str] = d["actions"]
        assert len(actions) > 0


class TestBalances:
    """Tests for AccountService.balances()."""

    @pytest.mark.unit
    def test_returns_balance_list_result(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.balances()
        assert isinstance(result, BalanceListResult)
        assert len(result.balances) == 2

    @pytest.mark.unit
    def test_balance_fields(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.balances()
        bal = next(b for b in result.balances if b.account_id == "ACC001")
        assert isinstance(bal, AccountBalance)
        assert bal.ledger_balance == Decimal("5000.00")
        assert bal.available_balance == Decimal("4800.00")
        assert bal.institution_name == "Test Bank"

    @pytest.mark.unit
    def test_filter_by_account_id(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.balances(account_id="ACC001")
        assert len(result.balances) == 1
        assert result.balances[0].account_id == "ACC001"

    @pytest.mark.unit
    def test_to_envelope_sensitivity_medium(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.balances()
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"
        data: list[dict[str, Any]] = d["data"]
        assert len(data) == 2


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
    """In-memory test database with all schemas initialized; closed on teardown."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
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

    @pytest.mark.unit
    def test_balances_empty_db(self, empty_db: Database) -> None:
        service = AccountService(empty_db)
        result = service.balances()
        assert isinstance(result, BalanceListResult)
        assert result.balances == []
