"""Tests for core.fct_transaction_lines view.

The view flattens splits via UNNEST(t.splits) and falls back to the parent
row for unsplit transactions. Reads from core.fct_transactions, not from
app.transaction_splits — preserving the rule that consumers don't touch app.*.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables

pytestmark = pytest.mark.unit


_MODEL_PATH = (
    Path(__file__).resolve().parents[2]
    / "sqlmesh"
    / "models"
    / "core"
    / "fct_transaction_lines.sql"
)


def _insert_unsplit(db: Database, txn_id: str, amount: Decimal) -> None:
    db.execute(
        """
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount,
             category, has_splits)
        VALUES (?, 'acct_a', DATE '2024-06-01', ?, 'Food', FALSE)
        """,
        [txn_id, amount],
    )


def _insert_split(
    db: Database,
    txn_id: str,
    parent_amount: Decimal,
    splits: list[tuple[str, Decimal, str]],
) -> None:
    """Insert a fact row carrying a LIST(STRUCT) of split children."""
    struct_literals = ", ".join(
        f"STRUCT_PACK(split_id := '{sid}', "
        f"amount := CAST({amt} AS DECIMAL(18, 2)), "
        f"category := '{cat}', subcategory := NULL, note := NULL)"
        for sid, amt, cat in splits
    )
    db.execute(
        f"""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            category, splits, split_count, has_splits
        )
        VALUES (?, 'acct_a', DATE '2024-06-01', ?, 'Misc',
                [{struct_literals}], {len(splits)}, TRUE)
        """,  # noqa: S608  # building test input SQL string, not executing user data
        [txn_id, parent_amount],
    )


class TestFctTransactionLinesModel:
    """Behavioral tests for the view + a model-file structural sanity check."""

    def test_model_file_declares_view_kind(self) -> None:
        """Model is kind VIEW (derived state); name uses fct_ prefix."""
        content = _MODEL_PATH.read_text()
        assert "core.fct_transaction_lines" in content
        assert "kind VIEW" in content
        # Must read from the fact, not directly from app.transaction_splits.
        assert "core.fct_transactions" in content
        # The view's FROM clause must point at the fact, never at app.*.
        # (A header comment may legitimately mention app.transaction_splits.)
        assert "FROM app.transaction_splits" not in content
        assert "JOIN app.transaction_splits" not in content

    def test_unsplit_transaction_appears_as_one_whole_line(self, db: Database) -> None:
        create_core_tables(db)
        _insert_unsplit(db, "txn_whole", Decimal("-25.00"))
        rows = db.execute(
            """
            SELECT transaction_id, line_id, line_kind, line_amount, line_category
            FROM core.fct_transaction_lines
            WHERE transaction_id = 'txn_whole'
            """
        ).fetchall()
        assert rows == [("txn_whole", "whole", "whole", Decimal("-25.00"), "Food")]

    def test_split_transaction_appears_as_n_split_lines(self, db: Database) -> None:
        create_core_tables(db)
        _insert_split(
            db,
            "txn_split",
            Decimal("-100.00"),
            [
                ("s1", Decimal("-60.00"), "Food"),
                ("s2", Decimal("-40.00"), "Fun"),
            ],
        )
        rows = db.execute(
            """
            SELECT line_id, line_kind, line_amount, line_category
            FROM core.fct_transaction_lines
            WHERE transaction_id = 'txn_split'
            ORDER BY line_id
            """
        ).fetchall()
        assert rows == [
            ("s1", "split", Decimal("-60.00"), "Food"),
            ("s2", "split", Decimal("-40.00"), "Fun"),
        ]
        # Critical: the parent row must NOT appear when the transaction has splits.
        whole_count = db.execute(
            """
            SELECT COUNT(*) FROM core.fct_transaction_lines
            WHERE transaction_id = 'txn_split' AND line_kind = 'whole'
            """
        ).fetchone()
        assert whole_count == (0,)

    def test_view_reads_from_fct_not_from_app_splits(self, db: Database) -> None:
        """Clearing core.fct_transactions yields zero view rows even if app.* has splits.

        Defends Architectural Pattern 1: consumers read derived shapes off
        core.fct_transactions, never bypassing into app.*.
        """
        create_core_tables(db)
        # Populate the underlying app table directly — these rows must NOT
        # surface in the view because the fact is empty.
        db.execute(
            """
            INSERT INTO app.transaction_splits
                (split_id, transaction_id, amount, category, ord, created_by)
            VALUES ('s_orphan', 'txn_missing', ?, 'Food', 0, 'cli')
            """,
            [Decimal("-10.00")],
        )
        count = db.execute("SELECT COUNT(*) FROM core.fct_transaction_lines").fetchone()
        assert count == (0,)
