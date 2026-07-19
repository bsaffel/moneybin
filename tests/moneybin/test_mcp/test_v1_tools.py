# tests/moneybin/test_mcp/test_v1_tools.py
"""Tests for the transactions_categorize_pending MCP tool.

Stubs ``core.uncategorized_queue`` directly (the SQLMesh layer is
exercised in scenario tests) and exercises the MCP tool wrapper against
that stub to confirm parameter validation, SQL composition, and
envelope shape.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.database import get_database
from moneybin.mcp.tools.transactions_categorize import (
    transactions_categorize_pending,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


class TestCategorizePendingGet:
    """Stubs core.uncategorized_queue; exercises filters + limit.

    Covers transactions_categorize_pending (replaced reports_uncategorized).
    """

    @staticmethod
    def _install_view() -> None:
        with get_database(read_only=False) as db:
            db.conn.execute("""
                CREATE OR REPLACE VIEW core.uncategorized_queue AS
                SELECT
                    'T1' AS transaction_id, 'ACC001' AS account_id,
                    'Test Bank Checking' AS account_name,
                    DATE '2026-04-01' AS txn_date,
                    CAST(-25.00 AS DECIMAL(18,2)) AS amount,
                    'COFFEE SHOP' AS description,
                    CAST(NULL AS VARCHAR) AS merchant_id,
                    'Coffee Shop' AS merchant_normalized,
                    CAST(39 AS INTEGER) AS age_days,
                    CAST(975.0 AS DOUBLE) AS priority_score,
                    'ofx' AS source_type,
                    CAST(NULL AS VARCHAR) AS source_id
                UNION ALL SELECT
                    'T2', 'ACC001', 'Test Bank Checking',
                    DATE '2026-04-10', CAST(-500.00 AS DECIMAL(18,2)),
                    'BIG EXPENSE', NULL, 'Big Expense',
                    CAST(30 AS INTEGER), 15000.0, 'ofx', NULL
                UNION ALL SELECT
                    'T3', 'ACC002', 'Other Bank Savings',
                    DATE '2026-04-15', CAST(-5.00 AS DECIMAL(18,2)),
                    'TINY', NULL, 'Tiny',
                    CAST(25 AS INTEGER), 125.0, 'ofx', NULL
            """)  # noqa: S608  # test input, not executing dynamic SQL

    @pytest.mark.unit
    async def test_returns_all_rows_default_sort_date(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await transactions_categorize_pending()).to_dict()
        # CatPendingPayload → PendingTxnRow amount is TXN_AMOUNT → Tier.HIGH
        # (account_id is RECORD_ID per spec D6).
        assert parsed["summary"]["sensitivity"] == "high"
        # Default sort=date → most recent first (T3 > T2 > T1).
        ids = [row["transaction_id"] for row in parsed["data"]["transactions"]]
        assert ids == ["T3", "T2", "T1"]

    @pytest.mark.unit
    async def test_sort_impact_returns_highest_priority_first(
        self, mcp_db: Path
    ) -> None:
        self._install_view()
        parsed = (await transactions_categorize_pending(sort="impact")).to_dict()
        # Impact sort: T2(500*30=15000) > T1(25*39=975) > T3(5*25=125).
        ids = [row["transaction_id"] for row in parsed["data"]["transactions"]]
        assert ids == ["T2", "T1", "T3"]

    @pytest.mark.unit
    async def test_min_amount_filters_low_value(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await transactions_categorize_pending(min_amount=20.0)).to_dict()
        ids = {row["transaction_id"] for row in parsed["data"]["transactions"]}
        assert ids == {"T1", "T2"}

    @pytest.mark.unit
    async def test_account_filter_narrows_results(self, mcp_db: Path) -> None:
        # Filter via canonical account_id; the resolver resolves the
        # reference against core.dim_accounts.
        self._install_view()
        parsed = (await transactions_categorize_pending(account="ACC002")).to_dict()
        ids = [row["transaction_id"] for row in parsed["data"]["transactions"]]
        assert ids == ["T3"]

    @staticmethod
    def _install_resolver_accounts() -> None:
        """Add display_name / collision-pair rows to core.dim_accounts."""
        with get_database(read_only=False) as db:
            db.conn.execute("""
                INSERT INTO core.dim_accounts
                    (account_id, source_type, display_name, archived)
                VALUES
                ('A1', 'ofx', 'Alpha', false),
                ('AJ1', 'ofx', 'Joint', false),
                ('AJ2', 'ofx', 'Joint', false)
            """)

    @pytest.mark.unit
    async def test_account_filter_accepts_display_name(self, mcp_db: Path) -> None:
        # Install a queue row keyed to A1 and an Alpha→A1 dim_accounts row.
        # The resolver must translate "Alpha" → "A1" before binding to SQL.
        self._install_view()
        self._install_resolver_accounts()
        with get_database(read_only=False) as db:
            db.conn.execute("""
                CREATE OR REPLACE VIEW core.uncategorized_queue AS
                SELECT
                    'TA' AS transaction_id, 'A1' AS account_id,
                    'Alpha' AS account_name,
                    DATE '2026-04-20' AS txn_date,
                    CAST(-10.00 AS DECIMAL(18,2)) AS amount,
                    'ALPHA TXN' AS description,
                    CAST(NULL AS VARCHAR) AS merchant_id,
                    'Alpha Txn' AS merchant_normalized,
                    CAST(20 AS INTEGER) AS age_days,
                    CAST(200.0 AS DOUBLE) AS priority_score,
                    'ofx' AS source_type,
                    CAST(NULL AS VARCHAR) AS source_id
            """)  # noqa: S608  # test input, not executing dynamic SQL
        parsed = (await transactions_categorize_pending(account="Alpha")).to_dict()
        ids = [row["transaction_id"] for row in parsed["data"]["transactions"]]
        assert ids == ["TA"]

    @pytest.mark.unit
    async def test_account_filter_unknown_returns_error_envelope(
        self, mcp_db: Path
    ) -> None:
        self._install_view()
        self._install_resolver_accounts()
        result = await transactions_categorize_pending(account="Nonexistent")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == "account_not_found"

    @pytest.mark.unit
    async def test_account_filter_ambiguous_returns_error_envelope(
        self, mcp_db: Path
    ) -> None:
        self._install_view()
        self._install_resolver_accounts()
        result = await transactions_categorize_pending(account="Joint")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == "account_ambiguous"

    @pytest.mark.unit
    async def test_missing_view_raises_schema_out_of_date(self, mcp_db: Path) -> None:
        # fct_transactions exists (from conftest) but the queue view does not.
        # This is the schema-drift case — must surface a structured error
        # pointing at refresh_run, NOT silently collapse to "no data".
        result = await transactions_categorize_pending()
        parsed = result.to_dict()
        assert parsed["error"]["code"] == "schema_out_of_date"
        assert "refresh" in parsed["error"]["message"].lower()

    @pytest.mark.unit
    async def test_pre_import_returns_empty(self, mcp_db: Path) -> None:
        # Pre-first-import case: drop fct_transactions so the resolver can
        # tell "no data yet" apart from "schema drift". Returns empty data
        # with an "import first" action hint, not an error.
        with get_database(read_only=False) as db:
            db.conn.execute("DROP TABLE core.fct_transactions")
        result = await transactions_categorize_pending()
        parsed = result.to_dict()
        assert parsed["data"]["transactions"] == []
        assert any("import" in a.lower() for a in parsed["actions"])

    @pytest.mark.unit
    async def test_limit_caps_rows(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (
            await transactions_categorize_pending(sort="impact", limit=1)
        ).to_dict()
        assert len(parsed["data"]["transactions"]) == 1
        # Highest impact wins.
        assert parsed["data"]["transactions"][0]["transaction_id"] == "T2"
