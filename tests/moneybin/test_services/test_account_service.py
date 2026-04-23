# tests/moneybin/test_services/test_account_service.py
"""Tests for AccountService."""

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
    AccountBalance,
    AccountListResult,
    AccountService,
    BalanceListResult,
)
from tests.moneybin.db_helpers import create_core_tables_raw


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
        INSERT INTO raw.ofx_balances VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00, '2025-06-30',
         4800.00, 'test.qfx', '2025-01-24', CURRENT_TIMESTAMP),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00, '2025-06-30',
         15000.00, 'other.qfx', '2025-01-24', CURRENT_TIMESTAMP)
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
