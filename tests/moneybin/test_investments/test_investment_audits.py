"""The investment-ledger sign-convention audit (runtime defense against a double flip).

The Plaid amount-sign flip lives in exactly one place
(``prep.stg_plaid__investment_transactions``). A second flip anywhere downstream
turns every buy into income and every sale into an expense — silently, and only
visible as wrong money. This audit is the standing assertion that no such flip
exists; ``DoctorService`` discovers and runs it as a SQLMesh standalone audit.

The tests execute the REAL audit file's SQL (header stripped, query untouched)
against the same core table shape production builds, so the audit itself — not a
copy of it — is what goes RED on a regression.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from moneybin.database import Database, sqlmesh_context
from tests.moneybin.db_helpers import CORE_FCT_INVESTMENT_TRANSACTIONS_DDL

_AUDIT_PATH = (
    Path(__file__).resolve().parents[3]
    / "sqlmesh"
    / "audits"
    / "fct_investment_transactions_sign_convention.sql"
)


def _audit_query() -> str:
    """The audit file's query body, with only the ``AUDIT (...)`` header removed."""
    _header, _semicolon, body = _AUDIT_PATH.read_text().partition(";")
    return body


def _seed(db: Database, txn_id: str, type_: str, amount: str | None) -> None:
    db.execute(
        """
        INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, type, amount)
        VALUES (?, 'acc_1', 'sec_1', ?, ?::DECIMAL(18, 2))
        """,
        [txn_id, type_, amount],
    )


def test_audit_passes_on_a_correctly_signed_ledger(db: Database) -> None:
    """Ledger convention: buys/reinvests are cash OUT (−), sales are cash IN (+)."""
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _seed(db, "buy_ok", "buy", "-2145.50")
    _seed(db, "reinvest_ok", "reinvest", "-25.00")
    _seed(db, "sell_ok", "sell", "6000.00")
    # A bootstrap lot of unknown basis carries a NULL amount, not a zero — and a
    # zero-amount buy is degenerate but not INVERTED. Neither is a sign violation.
    _seed(db, "transfer_unknown_basis", "transfer_in", None)
    _seed(db, "buy_zero", "buy", "0.00")
    assert db.execute(_audit_query()).fetchall() == []


def test_audit_catches_a_second_sign_flip(db: Database) -> None:
    """The regression this exists for: core flips Plaid's amount a second time."""
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _seed(db, "buy_inverted", "buy", "2145.50")  # a buy that reads as income
    _seed(db, "reinvest_inverted", "reinvest", "25.00")
    _seed(db, "sell_inverted", "sell", "-6000.00")  # a sale that reads as spend
    _seed(db, "buy_ok", "buy", "-100.00")  # untouched control
    violations = db.execute(_audit_query()).fetchall()
    assert violations == [
        ("buy_inverted",),
        ("reinvest_inverted",),
        ("sell_inverted",),
    ]


def test_audit_amount_is_the_only_signal_it_reads(db: Database) -> None:
    """Quantity is never flipped at any layer, so the audit must not police it."""
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    db.execute(
        """
        INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, type, quantity, amount)
        VALUES ('buy_ok', 'acc_1', 'sec_1', 'buy', ?::DECIMAL(28, 10),
                ?::DECIMAL(18, 2))
        """,
        [Decimal("10"), Decimal("-2145.50")],
    )
    assert db.execute(_audit_query()).fetchall() == []


@pytest.mark.integration
def test_audit_is_discovered_as_a_sqlmesh_standalone_audit(db: Database) -> None:
    """DoctorService only runs what SQLMesh registers in ``standalone_audits``.

    Without this wiring the audit is dead weight at runtime and the sign
    invariant would be defended in CI only — never on a user's real ledger.
    """
    with sqlmesh_context(db) as ctx:
        audits = ctx.standalone_audits
    assert "fct_investment_transactions_sign_convention" in audits
    # Non-blocking on purpose: a violation surfaces via `moneybin doctor`
    # instead of taking the whole transform offline (SQLMesh default).
    assert audits["fct_investment_transactions_sign_convention"].blocking is False
