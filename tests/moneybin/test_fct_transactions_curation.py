"""Tests for curation columns on core.fct_transactions.

Uses synthetic inserts directly into the test core.fct_transactions table
(via db_helpers) rather than running the full SQLMesh model — same pattern
as other model tests in this directory. The fct_transaction_lines view DDL
in db_helpers.py mirrors the SQLMesh model verbatim.
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
    / "fct_transactions.sql"
)


class TestFctTransactionsCurationColumns:
    """Structural and behavioral coverage for the seven new curation columns."""

    def test_fct_transactions_exposes_notes_tags_splits_columns(self) -> None:
        """SQLMesh model declares the seven new output columns."""
        content = _MODEL_PATH.read_text()
        for col in (
            "notes",
            "note_count",
            "tags",
            "tag_count",
            "splits",
            "split_count",
            "has_splits",
        ):
            assert col in content, f"missing column declaration: {col}"

    def test_fct_transactions_has_aggregation_ctes(self) -> None:
        """The three aggregation CTEs must be present in the model SQL."""
        content = _MODEL_PATH.read_text()
        for cte in ("notes_agg", "tags_agg", "splits_agg"):
            assert cte in content, f"missing CTE: {cte}"

    def test_no_curation_data_yields_null_lists(self, db: Database) -> None:
        """Transactions without curation rows have NULL list/count columns."""
        create_core_tables(db)
        db.execute(
            """
            INSERT INTO core.fct_transactions
                (transaction_id, account_id, transaction_date, amount, has_splits)
            VALUES (?, ?, DATE '2024-06-01', ?, FALSE)
            """,
            ["txn_plain", "acct_a", Decimal("-12.50")],
        )
        row = db.execute(
            """
            SELECT notes, note_count, tags, tag_count, splits, split_count, has_splits
            FROM core.fct_transactions WHERE transaction_id = 'txn_plain'
            """
        ).fetchone()
        assert row is not None
        notes, note_count, tags, tag_count, splits, split_count, has_splits = row
        assert notes is None
        assert note_count is None
        assert tags is None
        assert tag_count is None
        assert splits is None
        assert split_count is None
        assert has_splits is False

    def test_curation_aggregates_appear_in_fact(self, db: Database) -> None:
        """Insert 1 note, 2 tags, 1 split into a synthetic fct row; assert counts.

        This exercises the test-side CREATE TABLE shape for the curation
        columns (LIST/STRUCT). Production aggregation lives in the SQLMesh
        model; we cover the model SQL separately by structural assertions.
        """
        create_core_tables(db)
        db.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                notes, note_count, tags, tag_count, splits, split_count, has_splits
            )
            VALUES (
                'txn_curated', 'acct_a', DATE '2024-06-02', ?,
                [STRUCT_PACK(
                    note_id := 'n1',
                    "text" := 'first note',
                    author := 'cli',
                    created_at := TIMESTAMP '2024-06-02 10:00:00'
                )],
                1,
                ['tag1', 'tax:business'],
                2,
                [STRUCT_PACK(
                    split_id := 's1',
                    amount := CAST(? AS DECIMAL(18, 2)),
                    category := 'Food',
                    subcategory := NULL,
                    note := NULL
                )],
                1,
                TRUE
            )
            """,
            [Decimal("-50.00"), Decimal("-50.00")],
        )
        row = db.execute(
            """
            SELECT note_count, tag_count, split_count, has_splits,
                   tags[1], tags[2]
            FROM core.fct_transactions WHERE transaction_id = 'txn_curated'
            """
        ).fetchone()
        assert row == (1, 2, 1, True, "tag1", "tax:business")
