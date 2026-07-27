"""Unit tests for NetworthService."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.privacy.payloads.networth import (
    NetWorthHistoryPayload,
    NetWorthSnapshotPayload,
)
from moneybin.services.networth_service import NetworthService


def _seed_reports_net_worth(db: Database, rows: list[dict[str, object]]) -> None:
    """Manually CREATE TABLE + INSERT rows into reports.net_worth.

    Bypasses SQLMesh for unit-test speed. The SQLMesh model is actually a VIEW
    over fct_balances_daily JOIN dim_accounts; we substitute a TABLE with the
    same shape. Schema must match `src/moneybin/sqlmesh/models/reports/net_worth.sql`'s
    SELECT projection.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS reports.net_worth (
            balance_date DATE,
            currency_code VARCHAR,
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
            (balance_date, currency_code, net_worth, account_count,
             total_assets, total_liabilities)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                r["balance_date"],
                r.get("currency_code", "USD"),
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
            reconciliation_delta DECIMAL(18, 2),
            currency_code VARCHAR
        )
        """
    )
    for r in rows:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily
            (account_id, balance_date, balance, is_observed, observation_source,
             reconciliation_delta, currency_code)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                r["account_id"],
                r["balance_date"],
                r["balance"],
                r["is_observed"],
                r["observation_source"] if "observation_source" in r else None,
                r["reconciliation_delta"] if "reconciliation_delta" in r else None,
                r.get("currency_code", "USD"),
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
        assert isinstance(result, NetWorthSnapshotPayload)
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
    def test_current_empty_returns_explicit_no_data(self, db: Database) -> None:
        _seed_reports_net_worth(db, [])
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        svc = NetworthService(db)
        result = svc.current()
        assert result.balance_date is None
        assert result.net_worth is None
        assert result.total_assets is None
        assert result.total_liabilities is None
        assert result.account_count == 0
        assert result.per_account == []

    @pytest.mark.unit
    def test_current_before_first_row_returns_explicit_no_data(
        self, db: Database
    ) -> None:
        _seed_reports_net_worth(
            db,
            [
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

        result = NetworthService(db).current(as_of_date=date(2026, 1, 1))

        assert result.balance_date is None
        assert result.net_worth is None
        assert result.total_assets is None
        assert result.total_liabilities is None
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
        ids = [pa.account_id for pa in result.per_account]
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
        assert isinstance(result, NetWorthHistoryPayload)
        assert len(result.points) == 2
        # Period-over-period change
        feb = next(
            p for p in result.points if p.period and p.period.startswith("2026-02")
        )
        assert feb.change_abs == Decimal("200.00")
        # change_pct = 200/1000 = 0.2
        assert feb.change_pct is not None
        assert abs(float(feb.change_pct) - 0.2) < 0.001

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
        assert len(result.points) == 1
        assert result.points[0].change_abs is None
        assert result.points[0].change_pct is None


class TestMultiCurrency:
    """multi-currency.md Requirements 5 and 7 — segment, never silently blend.

    ``reports.net_worth`` emits one row per (balance_date, currency_code), so a
    service that keeps its single-row assumption would return one currency's
    total labelled as the whole position.
    """

    @pytest.mark.unit
    def test_current_names_the_currency_of_a_single_currency_position(
        self, db: Database
    ) -> None:
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "currency_code": "EUR",
                    "net_worth": Decimal("1000.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1000.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        result = NetworthService(db).current()
        assert result.currency_code == "EUR"
        assert result.net_worth == Decimal("1000.00")
        assert [s.currency_code for s in result.per_currency] == ["EUR"]

    @pytest.mark.unit
    def test_current_withholds_the_headline_total_when_currencies_are_mixed(
        self, db: Database
    ) -> None:
        """A blended headline would read 300; one segment's would read 100."""
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "currency_code": "USD",
                    "net_worth": Decimal("100.00"),
                    "account_count": 1,
                    "total_assets": Decimal("100.00"),
                    "total_liabilities": Decimal("0.00"),
                },
                {
                    "balance_date": date(2026, 1, 31),
                    "currency_code": "EUR",
                    "net_worth": Decimal("200.00"),
                    "account_count": 1,
                    "total_assets": Decimal("200.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        result = NetworthService(db).current()
        assert result.balance_date == date(2026, 1, 31)
        assert result.currency_code is None
        assert result.net_worth is None
        assert result.total_assets is None
        assert result.total_liabilities is None
        assert [(s.currency_code, s.net_worth) for s in result.per_currency] == [
            ("EUR", Decimal("200.00")),
            ("USD", Decimal("100.00")),
        ]

    @pytest.mark.unit
    def test_current_segments_a_third_currency_rather_than_pairing_two(
        self, db: Database
    ) -> None:
        """Three currencies, so "mixed" cannot be read as "the other one".

        Every other case here has exactly two segments, which an implementation
        that pairs — takes the first and "the rest", or compares head to tail —
        satisfies without ever segmenting. The third currency is what separates
        segmenting from pairing, and it must reach `per_currency` intact rather
        than being folded into a neighbour or dropped past a two-way branch.
        """
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "currency_code": code,
                    "net_worth": amount,
                    "account_count": 1,
                    "total_assets": amount,
                    "total_liabilities": Decimal("0.00"),
                }
                for code, amount in (
                    ("USD", Decimal("100.00")),
                    ("EUR", Decimal("200.00")),
                    ("GBP", Decimal("300.00")),
                )
            ],
        )
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        result = NetworthService(db).current()

        assert result.currency_code is None
        assert result.net_worth is None
        # 600.00 is the blend; its absence is the assertion that matters.
        assert [(s.currency_code, s.net_worth) for s in result.per_currency] == [
            ("EUR", Decimal("200.00")),
            ("GBP", Decimal("300.00")),
            ("USD", Decimal("100.00")),
        ]

    @pytest.mark.unit
    def test_current_resolves_the_latest_date_across_every_currency(
        self, db: Database
    ) -> None:
        """A currency that stopped reporting must not pin the snapshot date."""
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "currency_code": "EUR",
                    "net_worth": Decimal("200.00"),
                    "account_count": 1,
                    "total_assets": Decimal("200.00"),
                    "total_liabilities": Decimal("0.00"),
                },
                {
                    "balance_date": date(2026, 2, 28),
                    "currency_code": "USD",
                    "net_worth": Decimal("100.00"),
                    "account_count": 1,
                    "total_assets": Decimal("100.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        _seed_dim_accounts(db, [])
        _seed_fct_balances_daily(db, [])
        result = NetworthService(db).current()
        assert result.balance_date == date(2026, 2, 28)
        assert [s.currency_code for s in result.per_currency] == ["USD"]
        assert result.currency_code == "USD"
        assert result.net_worth == Decimal("100.00")

    @pytest.mark.unit
    def test_history_tracks_each_currency_as_its_own_series(self, db: Database) -> None:
        """Period change is computed within a currency, never across two."""
        _seed_reports_net_worth(
            db,
            [
                {
                    "balance_date": date(2026, 1, 31),
                    "currency_code": "USD",
                    "net_worth": Decimal("1000.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1000.00"),
                    "total_liabilities": Decimal("0.00"),
                },
                {
                    "balance_date": date(2026, 1, 31),
                    "currency_code": "EUR",
                    "net_worth": Decimal("500.00"),
                    "account_count": 1,
                    "total_assets": Decimal("500.00"),
                    "total_liabilities": Decimal("0.00"),
                },
                {
                    "balance_date": date(2026, 2, 28),
                    "currency_code": "USD",
                    "net_worth": Decimal("1200.00"),
                    "account_count": 1,
                    "total_assets": Decimal("1200.00"),
                    "total_liabilities": Decimal("0.00"),
                },
                {
                    "balance_date": date(2026, 2, 28),
                    "currency_code": "EUR",
                    "net_worth": Decimal("400.00"),
                    "account_count": 1,
                    "total_assets": Decimal("400.00"),
                    "total_liabilities": Decimal("0.00"),
                },
            ],
        )
        points = (
            NetworthService(db)
            .history(date(2026, 1, 1), date(2026, 3, 1), interval="monthly")
            .points
        )
        assert len(points) == 4
        february = {
            (p.currency_code, p.change_abs)
            for p in points
            if p.period and p.period.startswith("2026-02")
        }
        assert february == {
            ("USD", Decimal("200.00")),
            ("EUR", Decimal("-100.00")),
        }

    def test_history_keeps_a_late_arriving_currency_inside_the_cap(
        self, db: Database
    ) -> None:
        """A currency that opens later must survive a truncated history.

        `core:networth_history` is a registered report, so `reports(..., limit=N)`
        keeps `records[:N]`. Ordering on `period` walked all of the older
        currency's months before reaching the newer one's single month, so a
        capped response dropped that currency entirely rather than ending its
        series early. Ordering on each currency's own period index interleaves
        them, so a prefix holds every currency that has data.

        The uneven coverage is the point: with both currencies present in every
        period, `ORDER BY period, currency_code` already alternates and would
        pass against the bug.
        """
        rows: list[dict[str, object]] = [
            {
                "balance_date": date(2026, month, 28),
                "currency_code": "EUR",
                "net_worth": Decimal("500.00"),
                "account_count": 1,
                "total_assets": Decimal("500.00"),
                "total_liabilities": Decimal("0.00"),
            }
            for month in (1, 2, 3, 4)
        ]
        rows.append({
            "balance_date": date(2026, 4, 28),
            "currency_code": "USD",
            "net_worth": Decimal("900.00"),
            "account_count": 1,
            "total_assets": Decimal("900.00"),
            "total_liabilities": Decimal("0.00"),
        })
        _seed_reports_net_worth(db, rows)

        points = (
            NetworthService(db)
            .history(date(2026, 1, 1), date(2026, 5, 1), interval="monthly")
            .points
        )
        leading = [p.currency_code for p in points[:2]]

        assert set(leading) == {"EUR", "USD"}, (
            f"first two points are {leading}; a cap here would hide USD entirely"
        )
