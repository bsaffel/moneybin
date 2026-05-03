"""Unit tests for BalanceService."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.services.balance_service import (
    BalanceAssertionListResult,
    BalanceObservationListResult,
    BalanceService,
)


def _seed_fct_balances_daily(
    db: Database,
    rows: list[dict[str, Any]],
) -> None:
    """Manually CREATE TABLE + INSERT rows into core.fct_balances_daily.

    Bypasses SQLMesh for unit-test speed. The schema below MUST match the
    `columns=` dict in @model() at `sqlmesh/models/core/fct_balances_daily.py`
    (lines 43-50). When that model adds a column, this CREATE must follow.
    """
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
                r.get("observation_source"),
                r.get("reconciliation_delta"),
            ],
        )


class TestAssertionsCRUD:
    """Tests for assert_balance, delete_assertion, and list_assertions."""

    @pytest.mark.unit
    def test_assert_inserts(self, db: Database) -> None:
        svc = BalanceService(db)
        result = svc.assert_balance(
            "acct_a", date(2026, 1, 31), Decimal("1234.56"), notes="from statement"
        )
        assert result.balance == Decimal("1234.56")
        assert result.notes == "from statement"

    @pytest.mark.unit
    def test_assert_upserts_same_date(self, db: Database) -> None:
        svc = BalanceService(db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"))
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("200.00"))
        listed = svc.list_assertions("acct_a")
        assert len(listed) == 1
        assert listed[0].balance == Decimal("200.00")

    @pytest.mark.unit
    def test_delete_removes(self, db: Database) -> None:
        svc = BalanceService(db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"))
        svc.delete_assertion("acct_a", date(2026, 1, 31))
        assert svc.list_assertions("acct_a") == []

    @pytest.mark.unit
    def test_delete_silent_on_missing(self, db: Database) -> None:
        svc = BalanceService(db)
        svc.delete_assertion("acct_a", date(2099, 1, 1))  # no error

    @pytest.mark.unit
    def test_list_filters_by_account(self, db: Database) -> None:
        svc = BalanceService(db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"))
        svc.assert_balance("acct_b", date(2026, 1, 31), Decimal("200.00"))
        a_only = svc.list_assertions("acct_a")
        assert len(a_only) == 1
        assert a_only[0].account_id == "acct_a"

    @pytest.mark.unit
    def test_list_all_assertions(self, db: Database) -> None:
        svc = BalanceService(db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"))
        svc.assert_balance("acct_b", date(2026, 1, 31), Decimal("200.00"))
        all_rows = svc.list_assertions(None)
        assert len(all_rows) == 2


class TestCurrentBalances:
    """Tests for current_balances — most-recent-per-account reads."""

    @pytest.mark.unit
    def test_current_returns_latest_per_account(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 1),
                    "balance": Decimal("100.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("250.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_b",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("500.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
            ],
        )
        svc = BalanceService(db)
        result = svc.current_balances()
        assert len(result) == 2
        bal_a = next(o for o in result if o.account_id == "acct_a")
        assert bal_a.balance == Decimal("250.00")
        assert bal_a.balance_date == date(2026, 1, 31)

    @pytest.mark.unit
    def test_current_filters_by_account(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("100.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_b",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("200.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
            ],
        )
        svc = BalanceService(db)
        result = svc.current_balances(account_ids=["acct_a"])
        assert len(result) == 1
        assert result[0].account_id == "acct_a"

    @pytest.mark.unit
    def test_current_as_of_date(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 1),
                    "balance": Decimal("100.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 2, 1),
                    "balance": Decimal("250.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
            ],
        )
        svc = BalanceService(db)
        result = svc.current_balances(as_of_date=date(2026, 1, 15))
        assert len(result) == 1
        assert result[0].balance == Decimal("100.00")  # the Jan 1 obs


class TestHistory:
    """Tests for history — per-account time-series reads."""

    @pytest.mark.unit
    def test_history_returns_full_series(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 1),
                    "balance": Decimal("100.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 2),
                    "balance": Decimal("100.00"),
                    "is_observed": False,
                },
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 3),
                    "balance": Decimal("150.00"),
                    "is_observed": True,
                    "observation_source": "assertion",
                },
            ],
        )
        svc = BalanceService(db)
        result = svc.history("acct_a")
        assert len(result) == 3
        assert result[0].is_observed is True
        assert result[1].is_observed is False
        assert result[2].observation_source == "assertion"

    @pytest.mark.unit
    def test_history_date_range(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 1),
                    "balance": Decimal("100.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 2, 1),
                    "balance": Decimal("250.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                },
            ],
        )
        svc = BalanceService(db)
        result = svc.history(
            "acct_a", from_date=date(2026, 1, 15), to_date=date(2026, 2, 28)
        )
        assert len(result) == 1
        assert result[0].balance == Decimal("250.00")


class TestReconcile:
    """Tests for reconcile — surfacing rows where delta exceeds threshold."""

    @pytest.mark.unit
    def test_reconcile_returns_nonzero_deltas(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 1),
                    "balance": Decimal("100.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                    "reconciliation_delta": None,
                },
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("250.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                    "reconciliation_delta": Decimal("5.00"),
                },
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 2, 28),
                    "balance": Decimal("300.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                    "reconciliation_delta": Decimal("0.005"),
                },  # below threshold
            ],
        )
        svc = BalanceService(db)
        result = svc.reconcile()
        assert len(result) == 1
        assert result[0].balance_date == date(2026, 1, 31)

    @pytest.mark.unit
    def test_reconcile_threshold(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("250.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                    "reconciliation_delta": Decimal("3.00"),
                },
            ],
        )
        svc = BalanceService(db)
        # Default threshold 0.01 → row included
        assert len(svc.reconcile()) == 1
        # Threshold 5.00 → row excluded
        assert len(svc.reconcile(threshold=Decimal("5.00"))) == 0

    @pytest.mark.unit
    def test_reconcile_filter_by_account(self, db: Database) -> None:
        _seed_fct_balances_daily(
            db,
            [
                {
                    "account_id": "acct_a",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("100.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                    "reconciliation_delta": Decimal("5.00"),
                },
                {
                    "account_id": "acct_b",
                    "balance_date": date(2026, 1, 31),
                    "balance": Decimal("200.00"),
                    "is_observed": True,
                    "observation_source": "ofx",
                    "reconciliation_delta": Decimal("10.00"),
                },
            ],
        )
        svc = BalanceService(db)
        result = svc.reconcile(account_ids=["acct_a"])
        assert len(result) == 1
        assert result[0].account_id == "acct_a"


class TestEnvelopes:
    """Tests for result container to_envelope() methods."""

    @pytest.mark.unit
    def test_observation_list_envelope_default_medium(self) -> None:
        result = BalanceObservationListResult(observations=[])
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"

    @pytest.mark.unit
    def test_observation_list_envelope_low(self) -> None:
        result = BalanceObservationListResult(observations=[], sensitivity="low")
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_assertion_list_envelope_default_medium(self) -> None:
        result = BalanceAssertionListResult(assertions=[])
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"
