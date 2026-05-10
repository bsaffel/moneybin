"""Unit tests for NetworthService."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.services.networth_service import NetworthService


def _seed_reports_net_worth(db: Database, rows: list[dict[str, object]]) -> None:
    """Manually CREATE TABLE + INSERT rows into reports.net_worth.

    Bypasses SQLMesh for unit-test speed. The SQLMesh model is actually a VIEW
    over fct_balances_daily JOIN dim_accounts; we substitute a TABLE with the
    same shape. Schema must match `sqlmesh/models/reports/net_worth.sql`'s
    SELECT projection.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS reports.net_worth (
            balance_date DATE,
            net_worth DECIMAL(18, 2),
            account_count INTEGER,
            total_assets DECIMAL(18, 2),
            total_liabilities DECIMAL(18, 2)
        )
        """
    )
    for r in rows:
        db.execute(
            """
            INSERT INTO reports.net_worth
            (balance_date, net_worth, account_count, total_assets, total_liabilities)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                r["balance_date"],
                r["net_worth"],
                r["account_count"],
                r["total_assets"],
                r["total_liabilities"],
            ],
        )


def _seed_dim_accounts(db: Database, rows: list[dict[str, object]]) -> None:
    """Manually create dim_accounts with the columns NetworthService.current uses."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS core.dim_accounts (
            account_id VARCHAR,
            display_name VARCHAR,
            include_in_net_worth BOOLEAN,
            archived BOOLEAN
        )
        """
    )
    for r in rows:
        db.execute(
            """
            INSERT INTO core.dim_accounts (account_id, display_name, include_in_net_worth, archived)
            VALUES (?, ?, ?, ?)
            """,
            [
                r["account_id"],
                r["display_name"],
                r["include_in_net_worth"],
                r["archived"],
            ],
        )


def _seed_fct_balances_daily(db: Database, rows: list[dict[str, object]]) -> None:
    """For per-account breakdown queries (NetworthService.current calls fct_balances_daily)."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS core.fct_balances_daily (
            account_id VARCHAR,
            balance_date DATE,
            balance DECIMAL(18, 2),
            is_observed BOOLEAN,
            observation_source VARCHAR,
            reconciliation_delta DECIMAL(18, 2)
        )
        """
    )
    for r in rows:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily
            (account_id, balance_date, balance, is_observed, observation_source, reconciliation_delta)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                r["account_id"],
                r["balance_date"],
                r["balance"],
                r["is_observed"],
                r["observation_source"] if "observation_source" in r else None,
                r["reconciliation_delta"] if "reconciliation_delta" in r else None,
            ],
        )


class TestCurrent:
    """Tests for NetworthService.current()."""

    @pytest.mark.unit
    def test_current_returns_latest_snapshot(self, db: Database) -> None:
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 1),
                    "net_worth": Decimal("1000.00"),
                    "account_count": 2,
                    "total_assets": Decimal("1500.00"),
                    "total_liabilities": Decimal("-500.00"),
                },
                {
                    "balance_date": date(2026, 1, 31),
                    "net_worth": Decimal("1200.00"),
                    "account_count": 2,
                    "total_assets": Decimal("1700.00"),
                    "total_liabilities": Decimal("-500.00"),
                },
            ],
        )
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        svc = NetworthService(db)
        result = svc.current()
        assert result.balance_date == date(2026, 1, 31)
        assert result.net_worth == Decimal("1200.00")
        assert result.account_count == 2

    @pytest.mark.unit
    def test_current_as_of_date(self, db: Database) -> None:
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 1),
                    "net_worth": Decimal("1000.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1000.00"),
                    "total_liabilities": Decimal("0.00"),
                },
                {
                    "balance_date": date(2026, 2, 1),
                    "net_worth": Decimal("1500.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1500.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        svc = NetworthService(db)
        result = svc.current(as_of_date=date(2026, 1, 15))
        assert result.balance_date == date(2026, 1, 1)
        assert result.net_worth == Decimal("1000.00")

    @pytest.mark.unit
    def test_current_empty_returns_zero(self, db: Database) -> None:
        _seed_reports_net_worth(db, [])
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        svc = NetworthService(db)
        result = svc.current()
        assert result.net_worth == Decimal("0")
        assert result.account_count == 0
        assert result.per_account == []

    @pytest.mark.unit
    def test_current_per_account_breakdown(self, db: Database) -> None:
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "net_worth": Decimal("1500.00"),
                    "account_count": 2,
                    "total_assets": Decimal("1500.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        _seed_dim_accounts(
            db,
            [
                {
                    "account_id": "acct_a",
                    "display_name": "Checking",
                    "include_in_net_worth": True,
                    "archived": False,
                },
                {
                    "account_id": "acct_b",
                    "display_name": "Savings",
                    "include_in_net_worth": True,
                    "archived": False,
                },
                {
                    "account_id": "acct_c",
                    "display_name": "Excluded",
                    "include_in_net_worth": False,
                    "archived": False,
                },
            ],
        )
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("500.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_b",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("1000.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_c",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("999.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
            ],
        )
        svc = NetworthService(db)
        result = svc.current()
        assert len(result.per_account) == 2  # excluded account omitted
        ids = [pa["account_id"] for pa in result.per_account]
        assert "acct_c" not in ids


class TestHistory:
    """Tests for NetworthService.history()."""

    @pytest.mark.unit
    def test_history_monthly(self, db: Database) -> None:
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "net_worth": Decimal("1000.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1000.00"),
                    "total_liabilities": Decimal("0.00"),
                },
                {
                    "balance_date": date(2026, 2, 28),
                    "net_worth": Decimal("1200.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1200.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        svc = NetworthService(db)
        result = svc.history(date(2026, 1, 1), date(2026, 3, 1), interval="monthly")
        assert len(result) == 2
        # Period-over-period change
        feb = next(r for r in result if r["period"].startswith("2026-02"))
        assert feb["change_abs"] == Decimal("200.00")
        # change_pct = 200/1000 = 0.2
        assert feb["change_pct"] is not None
        assert abs(feb["change_pct"] - 0.2) < 0.001

    @pytest.mark.unit
    def test_history_invalid_interval_raises(self, db: Database) -> None:
        svc = NetworthService(db)
        with pytest.raises(ValueError, match="interval"):
            svc.history(date(2026, 1, 1), date(2026, 12, 31), interval="hourly")

    @pytest.mark.unit
    def test_history_first_period_change_is_none(self, db: Database) -> None:
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "net_worth": Decimal("1000.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1000.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        svc = NetworthService(db)
        result = svc.history(date(2026, 1, 1), date(2026, 2, 1), interval="monthly")
        assert len(result) == 1
        assert result[0]["change_abs"] is None
        assert result[0]["change_pct"] is None
