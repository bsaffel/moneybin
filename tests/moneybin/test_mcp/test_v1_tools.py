# tests/moneybin/test_mcp/test_v1_tools.py
"""Tests for the transactions_categorize_pending MCP tool.

Seeds ``core.fct_transactions`` directly (the SQLMesh layer is exercised in
integration and scenario tests) and exercises the MCP tool wrapper against it
to confirm parameter validation, SQL composition, and envelope shape.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp.tools.transactions_categorize import (
    transactions_categorize_pending,
)
from tests.moneybin.db_helpers import (
    insert_three_pending_transactions,
    simulate_pre_refresh_fct_transactions,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


class TestCategorizePendingGet:
    """Seeds three pending transactions; exercises filters + limit.

    Covers transactions_categorize_pending (replaced reports_uncategorized).
    """

    @staticmethod
    def _install_view() -> None:
        with get_database(read_only=False) as db:
            insert_three_pending_transactions(db)

    @pytest.mark.unit
    async def test_returns_all_rows_default_sort_date(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (transactions_categorize_pending()).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"
        # Default sort=date → most recent first (T3 > T2 > T1).
        ids = [row["transaction_id"] for row in parsed["data"]["transactions"]]
        assert ids == ["T3", "T2", "T1"]

    @pytest.mark.unit
    async def test_sort_impact_returns_highest_priority_first(
        self, mcp_db: Path
    ) -> None:
        self._install_view()
        parsed = (transactions_categorize_pending(sort="impact")).to_dict()
        # Impact sort: T2(500*30=15000) > T1(25*39=975) > T3(5*25=125).
        ids = [row["transaction_id"] for row in parsed["data"]["transactions"]]
        assert ids == ["T2", "T1", "T3"]

    @pytest.mark.unit
    async def test_min_amount_filters_low_value(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (
            transactions_categorize_pending(min_amount=Decimal("20.00"))
        ).to_dict()
        ids = {row["transaction_id"] for row in parsed["data"]["transactions"]}
        assert ids == {"T1", "T2"}

    @pytest.mark.unit
    async def test_account_filter_narrows_results(self, mcp_db: Path) -> None:
        # Filter via canonical account_id; the resolver resolves the
        # reference against core.dim_accounts.
        self._install_view()
        parsed = (transactions_categorize_pending(account="ACC002")).to_dict()
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
        # Seed a pending row on A1 and an Alpha→A1 dim_accounts row.
        # The resolver must translate "Alpha" → "A1" before binding to SQL.
        self._install_view()
        self._install_resolver_accounts()
        with get_database(read_only=False) as db:
            db.conn.execute("""
                INSERT INTO core.fct_transactions
                    (transaction_id, account_id, transaction_date, amount,
                     currency_code, description, merchant_name, source_type,
                     is_transfer)
                VALUES ('TA', 'A1', DATE '2026-04-20', -10.00, 'USD',
                        'ALPHA TXN', 'Alpha Txn', 'ofx', false)
            """)  # test input, not executing dynamic SQL
        parsed = (transactions_categorize_pending(account="Alpha")).to_dict()
        ids = [row["transaction_id"] for row in parsed["data"]["transactions"]]
        assert ids == ["TA"]

    @pytest.mark.unit
    async def test_account_filter_unknown_returns_error_envelope(
        self, mcp_db: Path
    ) -> None:
        self._install_view()
        self._install_resolver_accounts()
        with pytest.raises(UserError) as exc_info:
            transactions_categorize_pending(account="Nonexistent")
        assert exc_info.value.code == "account_not_found"

    @pytest.mark.unit
    async def test_account_filter_ambiguous_returns_error_envelope(
        self, mcp_db: Path
    ) -> None:
        self._install_view()
        self._install_resolver_accounts()
        with pytest.raises(UserError) as exc_info:
            transactions_categorize_pending(account="Joint")
        assert exc_info.value.code == "account_ambiguous"

    @pytest.mark.unit
    async def test_pre_refresh_fact_raises_schema_out_of_date(
        self, mcp_db: Path
    ) -> None:
        # fct_transactions exists but predates is_categorizable: schema drift,
        # which must surface a refresh_run remediation, not "no data".
        with get_database(read_only=False) as db:
            simulate_pre_refresh_fct_transactions(db)
        with pytest.raises(UserError) as exc_info:
            transactions_categorize_pending()
        assert exc_info.value.code == "infra_schema_drift"
        assert "refresh" in str(exc_info.value).lower()

    @pytest.mark.unit
    async def test_pre_import_returns_empty(self, mcp_db: Path) -> None:
        # Pre-first-import case: drop fct_transactions so the resolver can
        # tell "no data yet" apart from "schema drift". Returns empty data
        # with an "import first" action hint, not an error.
        with get_database(read_only=False) as db:
            db.conn.execute("DROP TABLE core.fct_transactions")
        result = transactions_categorize_pending()
        parsed = result.to_dict()
        assert parsed["data"]["transactions"] == []
        assert any("import" in a.lower() for a in parsed["actions"])

    @pytest.mark.unit
    async def test_limit_caps_rows(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (transactions_categorize_pending(sort="impact", limit=1)).to_dict()
        assert len(parsed["data"]["transactions"]) == 1
        # Highest impact wins.
        assert parsed["data"]["transactions"][0]["transaction_id"] == "T2"
