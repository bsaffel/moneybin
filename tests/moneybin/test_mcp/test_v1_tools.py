# tests/moneybin/test_mcp/test_v1_tools.py
"""Tests for view-backed reports.* MCP tools.

These tests stub the ``reports.*`` views directly (the SQLMesh layer is
exercised in scenario tests) and exercise the MCP tool wrappers against
those stubs to confirm parameter validation, SQL composition, and
envelope shape.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.database import Database, get_database
from moneybin.mcp.tools.reports import (
    reports_balance_drift,
    reports_recurring,
    reports_uncategorized,
)
from moneybin.protocol.envelope import ResponseEnvelope

pytestmark = pytest.mark.usefixtures("mcp_db")


def _create_reports_schema(db: Database) -> None:
    db.conn.execute("CREATE SCHEMA IF NOT EXISTS reports")


class TestReportsRecurringGet:
    """Stubs reports.recurring_subscriptions and exercises filters."""

    @staticmethod
    def _install_view() -> None:
        with get_database() as db:
            _create_reports_schema(db)
            db.conn.execute("""
                CREATE OR REPLACE VIEW reports.recurring_subscriptions AS
                SELECT
                    'Netflix' AS merchant_normalized,
                    'monthly' AS cadence,
                    CAST(15.99 AS DOUBLE) AS avg_amount,
                    CAST(12 AS BIGINT) AS occurrence_count,
                    DATE '2025-01-15' AS first_seen,
                    DATE '2026-04-15' AS last_seen,
                    'active' AS status,
                    CAST(191.88 AS DOUBLE) AS annualized_cost,
                    CAST(0.95 AS DOUBLE) AS confidence
                UNION ALL SELECT
                    'OldGym', 'monthly', 50.0, 6, DATE '2024-01-01',
                    DATE '2024-06-01', 'inactive', 600.0, 0.8
                UNION ALL SELECT
                    'WeakSignal', 'irregular', 7.0, 3, DATE '2025-12-01',
                    DATE '2026-04-01', 'active', 21.0, 0.3
            """)  # noqa: S608  # test input, not executing dynamic SQL

    @pytest.mark.unit
    async def test_default_filters_active_high_confidence(self, mcp_db: Path) -> None:
        self._install_view()
        result = await reports_recurring()
        assert isinstance(result, ResponseEnvelope)
        parsed = result.to_dict()
        # Aggregate per-merchant rollup — sensitivity is "low" per mcp-server.md.
        assert parsed["summary"]["sensitivity"] == "low"
        merchants = {row["merchant_normalized"] for row in parsed["data"]}
        # Default min_confidence=0.5, status='active' → drops OldGym (inactive)
        # and WeakSignal (confidence=0.3).
        assert merchants == {"Netflix"}

    @pytest.mark.unit
    async def test_status_all_keeps_inactive(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_recurring(status="all")).to_dict()
        merchants = {row["merchant_normalized"] for row in parsed["data"]}
        # min_confidence=0.5 still drops WeakSignal but keeps OldGym (inactive).
        assert merchants == {"Netflix", "OldGym"}

    @pytest.mark.unit
    async def test_unknown_status_returns_error_envelope(self, mcp_db: object) -> None:
        # ValueError raised inside the tool is classified by the @mcp_tool
        # decorator as a UserError(invalid_input) and surfaced as an error
        # envelope rather than re-raised.
        result = await reports_recurring(status="bogus")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == "invalid_input"
        assert "Unknown status" in parsed["error"]["message"]

    @pytest.mark.unit
    async def test_unknown_cadence_returns_error_envelope(self, mcp_db: object) -> None:
        result = await reports_recurring(cadence="hourly")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == "invalid_input"
        assert "Unknown cadence" in parsed["error"]["message"]


class TestReportsUncategorizedGet:
    """Stubs reports.uncategorized_queue and exercises filters + limit."""

    @staticmethod
    def _install_view() -> None:
        with get_database() as db:
            _create_reports_schema(db)
            db.conn.execute("""
                CREATE OR REPLACE VIEW reports.uncategorized_queue AS
                SELECT
                    'T1' AS transaction_id, 'ACC001' AS account_id,
                    'Test Bank Checking' AS account_name,
                    DATE '2026-04-01' AS txn_date,
                    CAST(-25.00 AS DECIMAL(18,2)) AS amount,
                    'COFFEE SHOP' AS description,
                    'Coffee Shop' AS merchant_normalized,
                    39 AS age_days,
                    CAST(975.0 AS DOUBLE) AS priority_score,
                    'ofx' AS source_type,
                    CAST(NULL AS VARCHAR) AS source_id
                UNION ALL SELECT
                    'T2', 'ACC001', 'Test Bank Checking',
                    DATE '2026-04-10', CAST(-500.00 AS DECIMAL(18,2)),
                    'BIG EXPENSE', 'Big Expense', 30, 15000.0, 'ofx', NULL
                UNION ALL SELECT
                    'T3', 'ACC002', 'Other Bank Savings',
                    DATE '2026-04-15', CAST(-5.00 AS DECIMAL(18,2)),
                    'TINY', 'Tiny', 25, 125.0, 'ofx', NULL
            """)  # noqa: S608  # test input, not executing dynamic SQL

    @pytest.mark.unit
    async def test_returns_all_rows_by_default(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_uncategorized()).to_dict()
        assert parsed["summary"]["sensitivity"] == "medium"
        ids = [row["transaction_id"] for row in parsed["data"]]
        # Sorted by priority_score DESC.
        assert ids == ["T2", "T1", "T3"]

    @pytest.mark.unit
    async def test_min_amount_filters_low_value(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_uncategorized(min_amount=20.0)).to_dict()
        ids = {row["transaction_id"] for row in parsed["data"]}
        assert ids == {"T1", "T2"}

    @pytest.mark.unit
    async def test_account_filter_narrows_results(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_uncategorized(account="Other Bank Savings")).to_dict()
        ids = [row["transaction_id"] for row in parsed["data"]]
        assert ids == ["T3"]

    @pytest.mark.unit
    async def test_limit_caps_rows(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_uncategorized(limit=1)).to_dict()
        assert len(parsed["data"]) == 1
        # Highest priority wins.
        assert parsed["data"][0]["transaction_id"] == "T2"


class TestReportsBalanceDriftGet:
    """Stubs reports.balance_drift and exercises filters."""

    @staticmethod
    def _install_view() -> None:
        with get_database() as db:
            _create_reports_schema(db)
            db.conn.execute("""
                CREATE OR REPLACE VIEW reports.balance_drift AS
                SELECT
                    'ACC001' AS account_id,
                    'Test Bank Checking' AS account_name,
                    DATE '2026-04-01' AS assertion_date,
                    CAST(1000.00 AS DECIMAL(18,2)) AS asserted_balance,
                    CAST(990.00 AS DECIMAL(18,2)) AS computed_balance,
                    CAST(10.00 AS DECIMAL(18,2)) AS drift,
                    CAST(10.00 AS DECIMAL(18,2)) AS drift_abs,
                    CAST(0.01 AS DOUBLE) AS drift_pct,
                    10 AS days_since_assertion,
                    'drift' AS status
                UNION ALL SELECT
                    'ACC002', 'Other Bank Savings',
                    DATE '2026-04-15', 5000.00, 4999.50,
                    0.50, 0.50, 0.0001, 5,
                    'clean'
                UNION ALL SELECT
                    'ACC001', 'Test Bank Checking',
                    DATE '2025-12-01', 800.00, NULL,
                    NULL, 0.00, NULL, 130,
                    'no-data'
            """)  # noqa: S608  # test input, not executing dynamic SQL

    @pytest.mark.unit
    async def test_default_returns_all_statuses(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_balance_drift()).to_dict()
        assert parsed["summary"]["sensitivity"] == "medium"
        # Sorted by drift_abs DESC.
        statuses = [row["status"] for row in parsed["data"]]
        assert statuses == ["drift", "clean", "no-data"]

    @pytest.mark.unit
    async def test_status_filter_drift_only(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_balance_drift(status="drift")).to_dict()
        statuses = [row["status"] for row in parsed["data"]]
        assert statuses == ["drift"]

    @pytest.mark.unit
    async def test_since_filter(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_balance_drift(since="2026-01-01")).to_dict()
        # The 2025-12-01 row should be excluded.
        assert all(
            row["assertion_date"].isoformat() >= "2026-01-01" for row in parsed["data"]
        )
        assert len(parsed["data"]) == 2

    @pytest.mark.unit
    async def test_account_filter(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await reports_balance_drift(account="Other Bank Savings")).to_dict()
        account_ids = [row["account_id"] for row in parsed["data"]]
        assert account_ids == ["ACC002"]

    @pytest.mark.unit
    async def test_unknown_status_returns_error_envelope(self, mcp_db: object) -> None:
        result = await reports_balance_drift(status="bogus")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == "invalid_input"
        assert "Unknown status" in parsed["error"]["message"]
