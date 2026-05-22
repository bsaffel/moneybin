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

from moneybin import error_codes
from moneybin.database import Database, get_database
from moneybin.mcp.tools.reports import (
    reports_balance_drift,
    reports_recurring,
)
from moneybin.mcp.tools.transactions_categorize import (
    transactions_categorize_pending,
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
                    CAST(NULL AS VARCHAR) AS merchant_id,
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
                    NULL, 'OldGym', 'monthly', 50.0, 6, DATE '2024-01-01',
                    DATE '2024-06-01', 'inactive', 600.0, 0.8
                UNION ALL SELECT
                    NULL, 'WeakSignal', 'irregular', 7.0, 3, DATE '2025-12-01',
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
        # decorator as a UserError(infra_invalid_input) and surfaced as an error
        # envelope rather than re-raised. Reports are read paths, so the
        # prefix-neutral infra_ code is correct (not mutation_).
        result = await reports_recurring(status="bogus")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == error_codes.INFRA_INVALID_INPUT
        assert "Unknown status" in parsed["error"]["message"]

    @pytest.mark.unit
    async def test_unknown_cadence_returns_error_envelope(self, mcp_db: object) -> None:
        result = await reports_recurring(cadence="hourly")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == error_codes.INFRA_INVALID_INPUT
        assert "Unknown cadence" in parsed["error"]["message"]


class TestCategorizePendingGet:
    """Stubs reports.uncategorized_queue; exercises filters + limit.

    Covers transactions_categorize_pending (replaced reports_uncategorized).
    """

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
        assert parsed["summary"]["sensitivity"] == "medium"
        # Default sort=date → most recent first (T3 > T2 > T1).
        ids = [row["transaction_id"] for row in parsed["data"]]
        assert ids == ["T3", "T2", "T1"]

    @pytest.mark.unit
    async def test_sort_impact_returns_highest_priority_first(
        self, mcp_db: Path
    ) -> None:
        self._install_view()
        parsed = (await transactions_categorize_pending(sort="impact")).to_dict()
        # Impact sort: T2(500*30=15000) > T1(25*39=975) > T3(5*25=125).
        ids = [row["transaction_id"] for row in parsed["data"]]
        assert ids == ["T2", "T1", "T3"]

    @pytest.mark.unit
    async def test_min_amount_filters_low_value(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (await transactions_categorize_pending(min_amount=20.0)).to_dict()
        ids = {row["transaction_id"] for row in parsed["data"]}
        assert ids == {"T1", "T2"}

    @pytest.mark.unit
    async def test_account_filter_narrows_results(self, mcp_db: Path) -> None:
        # Filter via canonical account_id; the resolver resolves the
        # reference against core.dim_accounts.
        self._install_view()
        parsed = (await transactions_categorize_pending(account="ACC002")).to_dict()
        ids = [row["transaction_id"] for row in parsed["data"]]
        assert ids == ["T3"]

    @staticmethod
    def _install_resolver_accounts() -> None:
        """Add display_name / collision-pair rows to core.dim_accounts."""
        with get_database() as db:
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
        with get_database() as db:
            db.conn.execute("""
                CREATE OR REPLACE VIEW reports.uncategorized_queue AS
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
        ids = [row["transaction_id"] for row in parsed["data"]]
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
        with get_database() as db:
            db.conn.execute("DROP TABLE core.fct_transactions")
        result = await transactions_categorize_pending()
        parsed = result.to_dict()
        assert parsed["data"] == []
        assert any("import" in a.lower() for a in parsed["actions"])

    @pytest.mark.unit
    async def test_limit_caps_rows(self, mcp_db: Path) -> None:
        self._install_view()
        parsed = (
            await transactions_categorize_pending(sort="impact", limit=1)
        ).to_dict()
        assert len(parsed["data"]) == 1
        # Highest impact wins.
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
        # Filter via canonical account_id; the resolver binds the
        # resolved id to the SQL WHERE clause (not the stub view's
        # free-text account_name).
        self._install_view()
        parsed = (await reports_balance_drift(account="ACC002")).to_dict()
        account_ids = [row["account_id"] for row in parsed["data"]]
        assert account_ids == ["ACC002"]

    @pytest.mark.unit
    async def test_unknown_status_returns_error_envelope(self, mcp_db: object) -> None:
        # Read path → ValueError classified to INFRA_INVALID_INPUT, not MUTATION_*.
        result = await reports_balance_drift(status="bogus")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == error_codes.INFRA_INVALID_INPUT
        assert "Unknown status" in parsed["error"]["message"]
