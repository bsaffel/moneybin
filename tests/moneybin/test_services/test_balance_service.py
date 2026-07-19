"""Unit tests for BalanceService."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.payloads.balances import (
    BalanceAssertionListPayload,
    BalanceAssertionPayload,
    BalanceAssertionRow,
    BalanceObservationListPayload,
)
from moneybin.services.balance_service import BalanceService
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture()
def assertion_db(db: Database) -> Database:
    """Database with core tables + seeded dim_accounts rows for assertion CRUD tests."""
    create_core_tables(db)
    db.execute(
        """
        INSERT INTO core.dim_accounts (account_id, account_type, institution_name, source_type)
        VALUES ('acct_a', 'CHECKING', 'Test Bank', 'ofx'),
               ('acct_b', 'SAVINGS', 'Other Bank', 'ofx')
        """
    )
    return db


def _seed_fct_balances_daily(
    db: Database,
    rows: list[dict[str, Any]],
) -> None:
    """Manually CREATE TABLE + INSERT rows into core.fct_balances_daily.

    Bypasses SQLMesh for unit-test speed. The schema below MUST match the
    `columns=` dict in @model() at `src/moneybin/sqlmesh/models/core/fct_balances_daily.py`
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
    def test_assert_inserts(self, assertion_db: Database) -> None:
        svc = BalanceService(assertion_db)
        result = svc.assert_balance(
            "acct_a",
            date(2026, 1, 31),
            Decimal("1234.56"),
            notes="from statement",
            actor="cli",
        )
        assert isinstance(result, BalanceAssertionPayload)
        assert result.assertion.balance == Decimal("1234.56")
        assert result.assertion.notes == "from statement"

    @pytest.mark.unit
    def test_assert_upserts_same_date(self, assertion_db: Database) -> None:
        svc = BalanceService(assertion_db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"), actor="cli")
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("200.00"), actor="cli")
        listed = svc.list_assertions("acct_a")
        assert isinstance(listed, BalanceAssertionListPayload)
        assert len(listed.assertions) == 1
        assert listed.assertions[0].balance == Decimal("200.00")

    @pytest.mark.unit
    def test_delete_removes(self, assertion_db: Database) -> None:
        svc = BalanceService(assertion_db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"), actor="cli")
        svc.delete_assertion("acct_a", date(2026, 1, 31), actor="cli")
        assert svc.list_assertions("acct_a").assertions == []

    @pytest.mark.unit
    def test_delete_silent_on_missing(self, assertion_db: Database) -> None:
        svc = BalanceService(assertion_db)
        svc.delete_assertion("acct_a", date(2099, 1, 1), actor="cli")  # no error

    @pytest.mark.unit
    def test_delete_verifies_live_assertion_inside_atomic_write(
        self, assertion_db: Database
    ) -> None:
        svc = BalanceService(assertion_db)
        assertion_date = date(2026, 1, 31)
        svc.assert_balance(
            "acct_a",
            assertion_date,
            Decimal("100.00"),
            actor="cli",
        )
        seen: list[Decimal] = []

        def refuse(assertion: BalanceAssertionRow) -> None:
            seen.append(assertion.balance)
            raise UserError(
                "Confirmation no longer matches the assertion.",
                code="mutation_confirmation_mismatch",
            )

        with pytest.raises(UserError, match="no longer matches"):
            svc.delete_assertion(
                "acct_a",
                assertion_date,
                actor="cli",
                verify=refuse,
            )

        assert seen == [Decimal("100.00")]
        assert svc.list_assertions("acct_a").assertions[0].balance == Decimal("100.00")
        delete_audits = assertion_db.execute(
            """
            SELECT 1
            FROM app.audit_log
            WHERE target_id = ? AND action = 'balance_assertion.delete'
            """,
            ["acct_a|2026-01-31"],
        ).fetchall()
        assert delete_audits == []

    @pytest.mark.unit
    def test_list_filters_by_account(self, assertion_db: Database) -> None:
        svc = BalanceService(assertion_db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"), actor="cli")
        svc.assert_balance("acct_b", date(2026, 1, 31), Decimal("200.00"), actor="cli")
        a_only = svc.list_assertions("acct_a")
        assert isinstance(a_only, BalanceAssertionListPayload)
        assert len(a_only.assertions) == 1
        assert a_only.assertions[0].account_id == "acct_a"

    @pytest.mark.unit
    def test_list_all_assertions(self, assertion_db: Database) -> None:
        svc = BalanceService(assertion_db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"), actor="cli")
        svc.assert_balance("acct_b", date(2026, 1, 31), Decimal("200.00"), actor="cli")
        all_rows = svc.list_assertions(None)
        assert isinstance(all_rows, BalanceAssertionListPayload)
        assert len(all_rows.assertions) == 2


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
        assert isinstance(result, BalanceObservationListPayload)
        assert len(result.observations) == 2
        bal_a = next(o for o in result.observations if o.account_id == "acct_a")
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
        assert isinstance(result, BalanceObservationListPayload)
        assert len(result.observations) == 1
        assert result.observations[0].account_id == "acct_a"

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
        assert isinstance(result, BalanceObservationListPayload)
        assert len(result.observations) == 1
        assert result.observations[0].balance == Decimal("100.00")  # the Jan 1 obs


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
        assert isinstance(result, BalanceObservationListPayload)
        assert len(result.observations) == 3
        assert result.observations[0].is_observed is True
        assert result.observations[1].is_observed is False
        assert result.observations[2].observation_source == "assertion"

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
        assert isinstance(result, BalanceObservationListPayload)
        assert len(result.observations) == 1
        assert result.observations[0].balance == Decimal("250.00")


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
        assert isinstance(result, BalanceObservationListPayload)
        assert len(result.observations) == 1
        assert result.observations[0].balance_date == date(2026, 1, 31)

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
        assert len(svc.reconcile().observations) == 1
        # Threshold 5.00 → row excluded
        assert len(svc.reconcile(threshold=Decimal("5.00")).observations) == 0

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
        assert isinstance(result, BalanceObservationListPayload)
        assert len(result.observations) == 1
        assert result.observations[0].account_id == "acct_a"


class TestTypedPayloads:
    """Verify that typed balance payloads carry DataClass annotations."""

    @pytest.mark.unit
    def test_observation_list_payload_has_annotations(self) -> None:
        """BalanceObservationListPayload resolves to HIGH tier via introspection."""
        from moneybin.privacy.introspection import derive_tier
        from moneybin.privacy.taxonomy import Tier

        tier = derive_tier(BalanceObservationListPayload)
        assert tier == Tier.HIGH  # balance → BALANCE → HIGH (account_id is RECORD_ID)

    @pytest.mark.unit
    def test_assertion_list_payload_has_annotations(self) -> None:
        """BalanceAssertionListPayload resolves to HIGH tier via introspection."""
        from moneybin.privacy.introspection import derive_tier
        from moneybin.privacy.taxonomy import Tier

        tier = derive_tier(BalanceAssertionListPayload)
        assert tier == Tier.HIGH  # balance → BALANCE → HIGH (account_id is RECORD_ID)


class TestAccountValidation:
    """Tests for assert_balance account_id existence check."""

    @pytest.mark.unit
    def test_assert_balance_rejects_unknown_account(self, db: Database) -> None:
        """assert_balance must raise UserError for account_id not in dim_accounts."""
        create_core_tables(db)
        svc = BalanceService(db)
        with pytest.raises(UserError, match="Account not found"):
            svc.assert_balance(
                "ACCTO1_typo", date(2026, 1, 31), Decimal("1234.56"), actor="cli"
            )

    @pytest.mark.unit
    def test_assert_balance_accepts_known_account(self, db: Database) -> None:
        """assert_balance must succeed when account_id is in dim_accounts."""
        create_core_tables(db)
        db.execute(
            """
            INSERT INTO core.dim_accounts
                (account_id, account_type, institution_name, source_type)
            VALUES ('REAL_ACCT', 'CHECKING', 'Test Bank', 'ofx')
            """
        )
        svc = BalanceService(db)
        result = svc.assert_balance(
            "REAL_ACCT", date(2026, 1, 31), Decimal("500.00"), actor="cli"
        )
        assert isinstance(result, BalanceAssertionPayload)
        assert result.assertion.balance == Decimal("500.00")

    @pytest.mark.unit
    def test_delete_assertion_unknown_account_is_noop(self, db: Database) -> None:
        """delete_assertion is forgiving: an unknown account_id no-ops, not raises.

        Asymmetric with assert_balance by design — you can't create an anchor for
        an account that doesn't exist, but removing one is idempotent best-effort.
        """
        create_core_tables(db)
        svc = BalanceService(db)
        # Must not raise (contract also locked by the e2e delete-noop test).
        svc.delete_assertion("ACCTO1_typo", date(2026, 1, 31), actor="cli")
