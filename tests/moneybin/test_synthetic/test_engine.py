# ruff: noqa: S101
"""Tests for the GeneratorEngine orchestrator."""

from __future__ import annotations

from collections.abc import Generator
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.testing.synthetic.models import GeneratedTransaction


class TestGeneratorEngine:
    """Test the full generation pipeline (no DB writes)."""

    def test_generate_produces_result(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        engine = GeneratorEngine("basic", seed=42, years=1)
        result = engine.generate()
        assert result.persona == "basic"
        assert result.seed == 42
        assert len(result.accounts) >= 1
        assert len(result.transactions) > 0

    def test_deterministic_output(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        engine1 = GeneratorEngine("basic", seed=42, years=1)
        engine2 = GeneratorEngine("basic", seed=42, years=1)
        r1 = engine1.generate()
        r2 = engine2.generate()
        assert len(r1.transactions) == len(r2.transactions)
        ids1 = [t.transaction_id for t in r1.transactions]
        ids2 = [t.transaction_id for t in r2.transactions]
        assert ids1 == ids2

    def test_different_seeds_diverge(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        r1 = GeneratorEngine("basic", seed=42, years=1).generate()
        r2 = GeneratorEngine("basic", seed=99, years=1).generate()
        ids1 = {t.transaction_id for t in r1.transactions}
        ids2 = {t.transaction_id for t in r2.transactions}
        # Different seeds should produce different transaction counts
        # (or at least different IDs if counts happen to match)
        assert len(r1.transactions) != len(r2.transactions) or ids1 != ids2

    def test_accounts_have_synthetic_ids(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        for acct in result.accounts:
            assert acct.account_id.startswith("SYN")

    def test_transaction_ids_assigned(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        for txn in result.transactions:
            assert txn.transaction_id.startswith("SYN")
            assert len(txn.transaction_id) == 13  # SYN + 10 digits

    def test_transaction_ids_unique(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        ids = [t.transaction_id for t in result.transactions]
        assert len(ids) == len(set(ids))

    def test_sign_convention(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        for txn in result.transactions:
            if txn.transaction_type == "DIRECTDEP" or txn.transaction_type == "DEP":
                assert txn.amount > 0, f"Income should be positive: {txn}"
            elif txn.transaction_type == "DEBIT":
                assert txn.amount < 0, f"Expense should be negative: {txn}"

    def test_transfer_pairs_match(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        transfers = [t for t in result.transactions if t.transfer_pair_id]
        pairs: dict[str, list[GeneratedTransaction]] = {}
        for t in transfers:
            assert t.transfer_pair_id is not None  # narrowed above by filter
            pairs.setdefault(t.transfer_pair_id, []).append(t)
        for pair_id, txns in pairs.items():
            assert len(txns) == 2, f"Pair {pair_id} should have exactly 2 transactions"
            total = sum((t.amount for t in txns), Decimal(0))
            assert total == Decimal(0), f"Pair {pair_id} should net to zero"

    def test_date_range_correct(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=2).generate()
        current_year = date.today().year
        assert result.start_date.year == current_year - 2
        assert result.end_date.year == current_year - 1
        for txn in result.transactions:
            assert result.start_date <= txn.date <= result.end_date


class TestGeneratorEngineWithDB:
    """Test engine → writer → database integration."""

    @pytest.fixture
    def db(self, tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database]:
        db = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
        )
        yield db
        db.close()

    def test_write_to_database(self, db: Database) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ground_truth"] == len(result.transactions)

    def test_ofx_transactions_in_db(self, db: Database) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        writer = SyntheticWriter(db)
        writer.write(result)
        row = db.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()
        assert row is not None
        assert row[0] > 0

    def test_ground_truth_matches_transactions(self, db: Database) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        writer = SyntheticWriter(db)
        writer.write(result)
        txn_row = db.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()
        csv_row = db.execute("SELECT COUNT(*) FROM raw.csv_transactions").fetchone()
        gt_row = db.execute("SELECT COUNT(*) FROM synthetic.ground_truth").fetchone()
        assert txn_row is not None
        assert csv_row is not None
        assert gt_row is not None
        assert gt_row[0] == txn_row[0] + csv_row[0]
