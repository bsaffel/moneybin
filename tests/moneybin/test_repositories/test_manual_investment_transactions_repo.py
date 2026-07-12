"""Tests for ``ManualInvestmentTransactionsRepo``.

The merge cascade's reference to the manual ledger. The repoint must pair an
``app.audit_log`` row with the write and be reversible through the generic
``BaseRepo.undo_event`` — a merge that repointed the ledger but could not
un-repoint it would make ``accept_merge`` only partly undoable.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.repositories.manual_investment_transactions_repo import (
    ManualInvestmentTransactionsRepo,
)


def _insert_event(
    db: Database,
    *,
    source_transaction_id: str = "manual_buy",
    security_id: str | None = "sec_old",
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees,
            created_by, investment_transaction_id
        ) VALUES (?, 'imp_1', 'acc_1', ?, 'VTI', 'buy', DATE '2024-05-01',
                  10.5, 100.25, -1052.63, 2.00, 'cli', 'itx_gold')
        """,
        [source_transaction_id, security_id],
    )


def _row(db: Database, source_transaction_id: str = "manual_buy") -> tuple[Any, ...]:
    row = db.execute(
        "SELECT security_id, security_ref, quantity, amount, "
        "investment_transaction_id FROM raw.manual_investment_transactions "
        "WHERE source_transaction_id = ?",
        [source_transaction_id],
    ).fetchone()
    assert row is not None
    return row


def _audit_rows(db: Database) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE action LIKE 'manual_investment.%'
         ORDER BY rowid
        """
    ).fetchall()


def test_repoint_moves_security_and_audits(db: Database) -> None:
    _insert_event(db)

    event = ManualInvestmentTransactionsRepo(db).repoint_security(
        source_transaction_id="manual_buy",
        new_security_id="sec_new",
        actor="cli",
    )

    assert event.target_id == "manual_buy"
    assert _row(db)[0] == "sec_new"

    audit = _audit_rows(db)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "manual_investment.repoint_security"
    assert (schema, table, target_id) == (
        "raw",
        "manual_investment_transactions",
        "manual_buy",
    )
    assert json.loads(before)["security_id"] == "sec_old"
    assert json.loads(after)["security_id"] == "sec_new"
    assert actor == "cli"


def test_repoint_leaves_every_other_column_untouched(db: Database) -> None:
    """The gold key hashes source_transaction_id + account_id, not security_id."""
    _insert_event(db)

    ManualInvestmentTransactionsRepo(db).repoint_security(
        source_transaction_id="manual_buy",
        new_security_id="sec_new",
        actor="cli",
    )

    _security_id, security_ref, quantity, amount, gold_key = _row(db)
    assert security_ref == "VTI"  # the user's original typed ref is the audit trail
    assert (str(quantity), str(amount)) == ("10.5000000000", "-1052.63")
    assert gold_key == "itx_gold"


def test_repoint_records_parent_audit_id(db: Database) -> None:
    _insert_event(db)

    event = ManualInvestmentTransactionsRepo(db).repoint_security(
        source_transaction_id="manual_buy",
        new_security_id="sec_new",
        actor="cli",
        parent_audit_id="parent_1",
    )

    assert event.parent_audit_id == "parent_1"


def test_repoint_raises_for_missing_row(db: Database) -> None:
    with pytest.raises(ValueError, match="not found"):
        ManualInvestmentTransactionsRepo(db).repoint_security(
            source_transaction_id="nope",
            new_security_id="sec_new",
            actor="cli",
        )


def test_repoint_raises_when_already_on_the_target(db: Database) -> None:
    """A no-op repoint would emit a before == after audit row the undo engine skips."""
    _insert_event(db, security_id="sec_new")

    with pytest.raises(ValueError, match="already carries"):
        ManualInvestmentTransactionsRepo(db).repoint_security(
            source_transaction_id="manual_buy",
            new_security_id="sec_new",
            actor="cli",
        )


def test_undo_event_restores_the_prior_security(db: Database) -> None:
    _insert_event(db)
    repo = ManualInvestmentTransactionsRepo(db)
    event = repo.repoint_security(
        source_transaction_id="manual_buy",
        new_security_id="sec_new",
        actor="cli",
    )

    repo.undo_event(event, actor="cli")

    security_id, _ref, quantity, amount, gold_key = _row(db)
    assert security_id == "sec_old"
    # Decimals and dates round-trip through the JSON audit capture unchanged.
    assert (str(quantity), str(amount)) == ("10.5000000000", "-1052.63")
    assert gold_key == "itx_gold"


def test_list_ids_for_security_scopes_to_that_security(db: Database) -> None:
    _insert_event(db, source_transaction_id="manual_b", security_id="sec_old")
    _insert_event(db, source_transaction_id="manual_a", security_id="sec_old")
    _insert_event(db, source_transaction_id="manual_c", security_id="sec_other")

    ids = ManualInvestmentTransactionsRepo(db).list_ids_for_security("sec_old")

    assert ids == ["manual_a", "manual_b"]
