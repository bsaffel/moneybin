"""Scenario YAML's bridge to the canonical audit SQL."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables
from tests.validation.assertions.audits import assert_transform_audit

pytestmark = pytest.mark.integration

_TXN_COLUMNS = (
    "transaction_id, account_id, transaction_date, amount, amount_absolute, "
    "transaction_direction, description, transaction_type, is_pending, "
    "currency_code, source_type, source_extracted_at, loaded_at, "
    "transaction_year, transaction_month, transaction_day, "
    "transaction_day_of_week, transaction_year_month, transaction_year_quarter"
)


def _seed_pair(db: Database, credit_amount: str) -> None:
    create_core_tables(db)
    db.execute(
        f"INSERT INTO core.fct_transactions ({_TXN_COLUMNS}) VALUES "  # noqa: S608 — test input, not user data
        "('T_DEBIT', 'ACC1', '2026-01-03', -100.00, 100.00, 'expense', 'Out', "
        "'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "
        "2026, 1, 3, 5, '2026-01', '2026-Q1'), "
        f"('T_CREDIT', 'ACC1', '2026-01-03', {credit_amount}, "
        f"{credit_amount.lstrip('-')}, 'income', 'In', "
        "'CREDIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "
        "2026, 1, 3, 5, '2026-01', '2026-Q1')"
    )
    db.execute(
        """
        INSERT INTO core.bridge_transfers
            (transfer_id, debit_transaction_id, credit_transaction_id,
             date_offset_days, amount)
        VALUES ('XFER1', 'T_DEBIT', 'T_CREDIT', 0, 100.00)
        """  # noqa: S608 — test input, not user data
    )


def test_passes_when_the_named_audit_finds_nothing(db: Database) -> None:
    _seed_pair(db, "100.00")

    result = assert_transform_audit(db, audit="bridge_transfers_balanced")

    assert result.passed
    assert result.name == "bridge_transfers_balanced"


def test_fails_and_names_the_violating_ids(db: Database) -> None:
    _seed_pair(db, "99.99")

    result = assert_transform_audit(db, audit="bridge_transfers_balanced")

    assert not result.passed
    assert result.details["violations"] == 1
    assert result.details["violation_ids"] == ["T_DEBIT"]


def test_raises_on_an_audit_name_the_project_does_not_declare(db: Database) -> None:
    create_core_tables(db)

    with pytest.raises(KeyError, match="typo_audit"):
        assert_transform_audit(db, audit="typo_audit")
