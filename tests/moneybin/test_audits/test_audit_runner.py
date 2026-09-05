"""The shared standalone-audit runner and the semantics of each audit's SQL.

``moneybin.audits.runner`` is the one place a SQLMesh standalone audit is
rendered and executed. ``DoctorService`` and the scenario runner both go
through it, so an audit's SQL is the single definition of its check — the
reconciliation MB-54 asks for.

These tests drive the real ``sqlmesh_context`` discovery path (not a mock):
an audit that loses ``standalone TRUE`` vanishes from the runner's output
entirely rather than reporting a failure, and only a real render catches
that.
"""

from __future__ import annotations

import re

import pytest

from moneybin.audits.runner import run_standalone_audits
from moneybin.database import SQLMESH_ROOT, Database
from tests.moneybin.db_helpers import (
    CORE_FCT_INVESTMENT_TRANSACTIONS_DDL,
    create_core_tables,
)

pytestmark = pytest.mark.integration

_AUDITS_DIR = SQLMESH_ROOT / "audits"

_TXN_COLUMNS = (
    "transaction_id, account_id, transaction_date, amount, amount_absolute, "
    "transaction_direction, description, category, is_transfer, "
    "transfer_pair_id, transaction_type, is_pending, currency_code, "
    "source_type, source_extracted_at, loaded_at, transaction_year, "
    "transaction_month, transaction_day, transaction_day_of_week, "
    "transaction_year_month, transaction_year_quarter"
)


def _declared_audit_names() -> set[str]:
    """Audit names parsed from ``src/moneybin/sqlmesh/audits/*.sql``.

    Ground truth independent of what the runner reports, so a new audit file
    is guarded the moment it lands.
    """
    names: set[str] = set()
    for path in sorted(_AUDITS_DIR.glob("*.sql")):
        header = path.read_text().partition(";")[0]
        match = re.search(r"name\s+(\w+)", header)
        assert match, f"{path}: could not parse an AUDIT name from the header"
        names.add(match.group(1))
    return names


def _txn_values(
    transaction_id: str,
    *,
    amount: str,
    direction: str,
    currency: str = "USD",
    category: str = "NULL",
    is_transfer: str = "FALSE",
    transfer_pair_id: str = "NULL",
    day: int = 1,
) -> str:
    absolute = amount.lstrip("-")
    category_sql = category if category == "NULL" else f"'{category}'"
    pair_sql = (
        transfer_pair_id if transfer_pair_id == "NULL" else f"'{transfer_pair_id}'"
    )
    return (
        f"('{transaction_id}', 'ACC1', '2026-01-{day:02d}', {amount}, {absolute}, "
        f"'{direction}', 'Row {transaction_id}', {category_sql}, {is_transfer}, "
        f"{pair_sql}, 'DEBIT', false, '{currency}', 'ofx', CURRENT_TIMESTAMP, "
        f"CURRENT_TIMESTAMP, 2026, 1, {day}, 3, '2026-01', '2026-Q1')"
    )


def _seed_account(db: Database) -> None:
    create_core_tables(db)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            institution_fid, source_type, source_file, extracted_at, loaded_at,
            updated_at, display_name, currency_code,
            archived, include_in_net_worth
        ) VALUES ('ACC1', '111', 'CHECKING', 'Bank', 'fid', 'ofx',
                  'a.qfx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  CURRENT_TIMESTAMP, 'Bank CHECKING', 'USD', FALSE, TRUE)
        """  # noqa: S608 — test input, not user data
    )


def _insert_transactions(db: Database, *rows: str) -> None:
    db.execute(
        f"INSERT INTO core.fct_transactions ({_TXN_COLUMNS}) VALUES {', '.join(rows)}"  # noqa: S608 — test input, not user data
    )


def _insert_transfer(
    db: Database, transfer_id: str, debit_id: str, credit_id: str, amount: str
) -> None:
    db.execute(
        "INSERT INTO core.bridge_transfers (transfer_id, debit_transaction_id, "  # noqa: S608 — test input, not user data
        "credit_transaction_id, date_offset_days, amount) VALUES "
        f"('{transfer_id}', '{debit_id}', '{credit_id}', 0, {amount})"
    )


def _violations(db: Database, audit: str) -> list[str]:
    outcomes = run_standalone_audits(db, names=[audit])
    assert len(outcomes) == 1
    assert outcomes[0].error is None, outcomes[0].error
    return outcomes[0].violation_ids


def test_runner_reports_an_outcome_for_every_declared_audit(db: Database) -> None:
    _seed_account(db)

    outcomes = run_standalone_audits(db)

    assert {o.name for o in outcomes} == _declared_audit_names()


def test_runner_raises_on_an_unknown_audit_name(db: Database) -> None:
    _seed_account(db)

    with pytest.raises(KeyError, match="no_such_audit"):
        run_standalone_audits(db, names=["no_such_audit"])


def test_balanced_transfers_flags_a_one_cent_gap(db: Database) -> None:
    """A cent is money, not rounding — ``amount`` is DECIMAL(18,2) throughout."""
    _seed_account(db)
    _insert_transactions(
        db,
        _txn_values("T_DEBIT", amount="-100.00", direction="expense"),
        _txn_values("T_CREDIT", amount="99.99", direction="income"),
    )
    _insert_transfer(db, "XFER1", "T_DEBIT", "T_CREDIT", "100.00")

    assert _violations(db, "bridge_transfers_balanced") == ["T_DEBIT"]


def test_balanced_transfers_flags_a_pair_whose_credit_leg_left_the_ledger(
    db: Database,
) -> None:
    """A dangling leg is unbalanced; an inner join would report it as clean."""
    _seed_account(db)
    _insert_transactions(
        db, _txn_values("T_DEBIT", amount="-100.00", direction="expense")
    )
    _insert_transfer(db, "XFER1", "T_DEBIT", "T_MISSING", "100.00")

    assert _violations(db, "bridge_transfers_balanced") == ["T_DEBIT"]


def test_balanced_transfers_flags_a_pair_whose_legs_carry_no_amount(
    db: Database,
) -> None:
    """NULL + NULL is not zero — a comparison on the sum alone would pass it."""
    _seed_account(db)
    db.execute(
        f"INSERT INTO core.fct_transactions ({_TXN_COLUMNS}) VALUES "  # noqa: S608 — test input, not user data
        "('T_DEBIT', 'ACC1', '2026-01-03', NULL, NULL, NULL, 'Out', NULL, "
        "FALSE, NULL, 'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, "
        "CURRENT_TIMESTAMP, 2026, 1, 3, 5, '2026-01', '2026-Q1'), "
        "('T_CREDIT', 'ACC1', '2026-01-03', NULL, NULL, NULL, 'In', NULL, "
        "FALSE, NULL, 'CREDIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, "
        "CURRENT_TIMESTAMP, 2026, 1, 3, 5, '2026-01', '2026-Q1')"
    )
    _insert_transfer(db, "XFER1", "T_DEBIT", "T_CREDIT", "100.00")

    assert _violations(db, "bridge_transfers_balanced") == ["T_DEBIT"]


def test_balanced_transfers_accepts_a_pair_that_nets_to_zero(db: Database) -> None:
    _seed_account(db)
    _insert_transactions(
        db,
        _txn_values("T_DEBIT", amount="-100.00", direction="expense"),
        _txn_values("T_CREDIT", amount="100.00", direction="income"),
    )
    _insert_transfer(db, "XFER1", "T_DEBIT", "T_CREDIT", "100.00")

    assert _violations(db, "bridge_transfers_balanced") == []


def test_balanced_transfers_accepts_a_known_cross_currency_pair(
    db: Database,
) -> None:
    _seed_account(db)
    _insert_transactions(
        db,
        _txn_values("T_DEBIT", amount="-100.00", direction="expense", currency="USD"),
        _txn_values("T_CREDIT", amount="90.00", direction="income", currency="EUR"),
    )
    _insert_transfer(db, "XFER1", "T_DEBIT", "T_CREDIT", "100.00")

    assert _violations(db, "bridge_transfers_balanced") == []


def test_sign_convention_flags_a_direction_that_contradicts_its_amount(
    db: Database,
) -> None:
    """``transaction_direction`` is the amount's sign made explicit."""
    _seed_account(db)
    _insert_transactions(
        db, _txn_values("T_WRONG", amount="-50.00", direction="income")
    )

    assert _violations(db, "fct_transactions_sign_convention") == ["T_WRONG"]


def test_sign_convention_flags_an_absolute_that_contradicts_its_amount(
    db: Database,
) -> None:
    _seed_account(db)
    db.execute(
        f"INSERT INTO core.fct_transactions ({_TXN_COLUMNS}) VALUES "  # noqa: S608 — test input, not user data
        "('T_ABS', 'ACC1', '2026-01-01', -50.00, 49.00, 'expense', 'Row', NULL, "
        "FALSE, NULL, 'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, "
        "CURRENT_TIMESTAMP, 2026, 1, 1, 3, '2026-01', '2026-Q1')"
    )

    assert _violations(db, "fct_transactions_sign_convention") == ["T_ABS"]


def test_sign_convention_flags_a_null_amount(db: Database) -> None:
    _seed_account(db)
    db.execute(
        f"INSERT INTO core.fct_transactions ({_TXN_COLUMNS}) VALUES "  # noqa: S608 — test input, not user data
        "('T_NULL', 'ACC1', '2026-01-01', NULL, NULL, NULL, 'Row', NULL, "
        "FALSE, NULL, 'DEBIT', false, 'USD', 'ofx', CURRENT_TIMESTAMP, "
        "CURRENT_TIMESTAMP, 2026, 1, 1, 3, '2026-01', '2026-Q1')"
    )

    assert _violations(db, "fct_transactions_sign_convention") == ["T_NULL"]


def test_sign_convention_accepts_a_zero_amount(db: Database) -> None:
    """$0.00 is a modeled third direction, not an unclassifiable row."""
    _seed_account(db)
    _insert_transactions(db, _txn_values("T_ZERO", amount="0.00", direction="zero"))

    assert _violations(db, "fct_transactions_sign_convention") == []


def test_sign_convention_accepts_a_positive_amount_on_an_expense_category(
    db: Database,
) -> None:
    """A refund is real data, so category-vs-sign is deliberately not policed.

    Pins the MB-54 reconciliation: the audit checks what the ledger can prove
    (the amount's own sign against its derived columns), never what a category
    label implies. A refund, a statement credit, and an unmatched card payment
    all carry a positive amount under a non-Income category.
    """
    _seed_account(db)
    _insert_transactions(
        db,
        _txn_values(
            "T_REFUND", amount="25.00", direction="income", category="Shopping"
        ),
    )

    assert _violations(db, "fct_transactions_sign_convention") == []
