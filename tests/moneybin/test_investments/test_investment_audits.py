"""The investment-ledger standalone audits (runtime defenses on core.*).

**Sign convention.** The Plaid amount-sign flip lives in exactly one place
(``prep.stg_plaid__investment_transactions``). A second flip anywhere downstream
turns every buy into income and every sale into an expense — silently, and only
visible as wrong money. That audit is the standing assertion that no such flip
exists; ``DoctorService`` discovers and runs it as a SQLMesh standalone audit.

**FK integrity.** Every ``fct_investment_transactions.account_id`` must resolve
to a ``core.dim_accounts`` row — the same contract
``fct_transactions_fk_integrity`` holds for the banking ledger. Staging's
``COALESCE(al.account_id, r.account_id)`` falls back to the raw Plaid id when no
``app.account_links`` row exists, so an account Plaid delivered a transaction
for but never delivered in the accounts array reaches core as an orphan id.

The tests execute the REAL audit files' SQL (header stripped, query untouched)
against the same core table shape production builds, so the audits themselves —
not copies of them — are what go RED on a regression.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from moneybin.database import SQLMESH_ROOT, Database, sqlmesh_context
from tests.moneybin.db_helpers import (
    CORE_FCT_INVESTMENT_TRANSACTIONS_DDL,
    create_core_tables,
)

_AUDITS_DIR = SQLMESH_ROOT / "audits"
_AUDIT_PATH = _AUDITS_DIR / "fct_investment_transactions_sign_convention.sql"
_FK_AUDIT_PATH = _AUDITS_DIR / "fct_investment_transactions_fk_integrity.sql"
_UNIQUENESS_AUDIT_PATH = _AUDITS_DIR / "fct_investment_transactions_uniqueness.sql"


def _body(path: Path) -> str:
    """An audit file's query body, with only the ``AUDIT (...)`` header removed."""
    _header, _semicolon, body = path.read_text().partition(";")
    return body


def _audit_query() -> str:
    """The sign-convention audit's query body."""
    return _body(_AUDIT_PATH)


def _fk_audit_query() -> str:
    """The FK-integrity audit's query body."""
    return _body(_FK_AUDIT_PATH)


def _uniqueness_audit_query() -> str:
    """The grain/uniqueness audit's query body."""
    return _body(_UNIQUENESS_AUDIT_PATH)


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


def _seed_account(db: Database, account_id: str) -> None:
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, account_type, institution_name, source_type, source_file,
            extracted_at, loaded_at, updated_at, display_name, currency_code,
            archived, include_in_net_worth
        ) VALUES (?, 'INVESTMENT', 'Alpha Brokerage', 'plaid', 'sync_job-1',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  'Alpha Brokerage INVESTMENT', 'USD', FALSE, TRUE)
        """,
        [account_id],
    )


def test_fk_audit_passes_when_every_account_id_resolves(db: Database) -> None:
    create_core_tables(db)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _seed_account(db, "acc_1")
    _seed(db, "buy_ok", "buy", "-2145.50")
    _seed(db, "sell_ok", "sell", "6000.00")
    assert db.execute(_fk_audit_query()).fetchall() == []


def test_fk_audit_catches_an_orphan_account_id(db: Database) -> None:
    """The regression: Plaid delivers a transaction for an account it never listed.

    No ``app.account_links`` row is written for that account, so staging's
    ``COALESCE(al.account_id, r.account_id)`` carries the raw Plaid id into
    core — an account_id with no ``core.dim_accounts`` row, invisible to every
    surface that joins through the dim.
    """
    create_core_tables(db)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _seed_account(db, "acc_1")
    _seed(db, "buy_ok", "buy", "-100.00")  # untouched control on the known account
    db.execute(
        """
        INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, type, amount)
        VALUES ('buy_orphan', 'plaid_acc_X', 'sec_1', 'buy', -500.00)
        """  # noqa: S608 — test input, not user data
    )
    assert db.execute(_fk_audit_query()).fetchall() == [("buy_orphan",)]


def test_uniqueness_audit_passes_on_distinct_ids(db: Database) -> None:
    create_core_tables(db)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _seed(db, "buy_1", "buy", "-2145.50")
    _seed(db, "sell_1", "sell", "6000.00")
    assert db.execute(_uniqueness_audit_query()).fetchall() == []


def test_uniqueness_audit_catches_a_duplicated_id(db: Database) -> None:
    """The grain contract, held mechanically rather than by construction.

    core.fct_investment_transactions is three UNION ALL branches with no dedup,
    and DuckDB enforces no PK — so a duplicated investment_transaction_id would
    silently double that lot's quantity and cost basis in the cost-basis engine.
    Without this fail case the audit's only coverage is the generic
    "all audits pass on clean data" sweep, which never seeds a duplicate: an
    inverted predicate (COUNT(*) > 1 flipped to < 1) would ship green and the
    audit would report `pass` on every ledger while catching nothing.
    """
    create_core_tables(db)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _seed(db, "buy_unique", "buy", "-100.00")  # untouched control
    _seed(db, "buy_dup", "buy", "-2145.50")
    _seed(db, "buy_dup", "buy", "-2145.50")  # same id from a second branch
    assert db.execute(_uniqueness_audit_query()).fetchall() == [("buy_dup",)]


@pytest.mark.integration
def test_fk_audit_is_discovered_as_a_sqlmesh_standalone_audit(db: Database) -> None:
    """An audit file without ``standalone TRUE`` never reaches DoctorService."""
    with sqlmesh_context(db) as ctx:
        audits = ctx.standalone_audits
    assert "fct_investment_transactions_fk_integrity" in audits
    assert audits["fct_investment_transactions_fk_integrity"].blocking is False
