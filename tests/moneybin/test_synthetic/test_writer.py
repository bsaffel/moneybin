# ruff: noqa: S101
"""Tests for the synthetic data writer."""

from __future__ import annotations

from collections.abc import Generator
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.testing.synthetic.models import (
    GeneratedAccount,
    GeneratedTransaction,
    GenerationResult,
)


def _make_result(
    accounts: list[GeneratedAccount] | None = None,
    transactions: list[GeneratedTransaction] | None = None,
) -> GenerationResult:
    """Factory for a minimal GenerationResult."""
    if accounts is None:
        accounts = [
            GeneratedAccount(
                name="Test Checking",
                account_id="SYN00420001",
                account_type="checking",
                source_type="ofx",
                institution="Test Bank",
                opening_balance=Decimal("1000.00"),
            ),
        ]
    if transactions is None:
        transactions = [
            GeneratedTransaction(
                date=date(2024, 1, 15),
                amount=Decimal("-42.50"),
                description="TEST STORE",
                account_name="Test Checking",
                category="grocery",
                transaction_type="DEBIT",
                transaction_id="SYN0000000001",
            ),
        ]
    return GenerationResult(
        persona="test",
        seed=42,
        accounts=accounts,
        transactions=transactions,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
    )


class TestSyntheticWriter:
    """Test writing generated data to raw tables."""

    @pytest.fixture
    def db(self, tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database]:
        db = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
        )
        yield db
        db.close()

    def test_write_ofx_account(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ofx_accounts"] == 1
        row = db.execute(
            "SELECT account_id, institution_org FROM raw.ofx_accounts"
        ).fetchone()
        assert row is not None
        assert row[0] == "SYN00420001"
        assert row[1] == "Test Bank"

    def test_write_ofx_transactions(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ofx_transactions"] == 1
        row = db.execute("SELECT amount, payee FROM raw.ofx_transactions").fetchone()
        assert row is not None
        assert float(row[0]) == pytest.approx(-42.50)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
        assert row[1] == "TEST STORE"

    def test_write_ofx_balances(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ofx_balances"] == 1
        row = db.execute("SELECT ledger_balance FROM raw.ofx_balances").fetchone()
        assert row is not None
        assert float(row[0]) == pytest.approx(1000.00)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete

    def test_write_csv_account(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        acct = GeneratedAccount(
            name="Test Card",
            account_id="SYN00420002",
            account_type="credit_card",
            source_type="csv",
            institution="Test CC",
            opening_balance=Decimal("0"),
        )
        txn = GeneratedTransaction(
            date=date(2024, 1, 10),
            amount=Decimal("-25.00"),
            description="STORE",
            account_name="Test Card",
            category="shopping",
            transaction_type="DEBIT",
            transaction_id="SYN0000000001",
        )
        result = _make_result(accounts=[acct], transactions=[txn])
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["csv_accounts"] == 1
        row = db.execute(
            "SELECT account_id, institution_name FROM raw.csv_accounts"
        ).fetchone()
        assert row is not None
        assert row[0] == "SYN00420002"
        assert row[1] == "Test CC"

    def test_write_csv_running_balance(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        acct = GeneratedAccount(
            name="Card",
            account_id="SYN00420002",
            account_type="credit_card",
            source_type="csv",
            institution="Test",
            opening_balance=Decimal("0"),
        )
        txns = [
            GeneratedTransaction(
                date=date(2024, 1, 5),
                amount=Decimal("-50.00"),
                description="A",
                account_name="Card",
                category="food",
                transaction_id="SYN0000000001",
            ),
            GeneratedTransaction(
                date=date(2024, 1, 10),
                amount=Decimal("-30.00"),
                description="B",
                account_name="Card",
                category="food",
                transaction_id="SYN0000000002",
            ),
        ]
        result = _make_result(accounts=[acct], transactions=txns)
        writer = SyntheticWriter(db)
        writer.write(result)
        rows = db.execute(
            "SELECT balance FROM raw.csv_transactions ORDER BY transaction_date"
        ).fetchall()
        assert float(rows[0][0]) == pytest.approx(-50.00)  # type: ignore[reportUnknownArgumentType]  # 0 + (-50)
        assert float(rows[1][0]) == pytest.approx(-80.00)  # type: ignore[reportUnknownArgumentType]  # -50 + (-30)

    def test_write_ground_truth(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ground_truth"] == 1
        row = db.execute("SELECT persona, seed FROM synthetic.ground_truth").fetchone()
        assert row is not None
        assert row[0] == "test"
        assert row[1] == 42

    def test_source_file_uses_synthetic_uri(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        writer.write(result)
        row = db.execute("SELECT source_file FROM raw.ofx_transactions").fetchone()
        assert row is not None
        assert row[0].startswith("synthetic://")
