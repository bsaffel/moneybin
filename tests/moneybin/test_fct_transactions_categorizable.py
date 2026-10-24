"""Integration tests for core.fct_transactions.is_categorizable.

categorizable-transactions.md R1/R2: TRUE exactly when a row is not a confirmed
transfer leg and its account resolved in core.dim_accounts and is not
archived; never NULL. Seeds raw.*/app.* directly, as
test_fct_transactions_currency.py does, so each row fails at most one
condition.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

pytestmark = pytest.mark.integration


def _insert_account(db: Database, account_id: str) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, account_type, source_file, extracted_at,
             source_type, source_origin)
        VALUES (?, 'CHECKING', 'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank')
        """,  # test fixture, not executing user SQL
        [account_id],
    )


def _insert_transaction(
    db: Database, *, source_id: str, account_id: str, amount: Decimal
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, transaction_type, date_posted,
             date_posted_source_date,
             amount, payee, source_file, extracted_at, source_type,
             source_origin, currency_code)
        VALUES (?, ?, 'DEBIT', '2026-07-01'::TIMESTAMP, DATE '2026-07-01', ?, 'Test Payee',
                'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank', 'USD')
        """,  # test fixture, not executing user SQL
        [source_id, account_id, amount],
    )


def seed_one_row_per_exclusion(db: Database) -> None:
    """Five transactions keyed by distinct amounts, each failing at most one condition.

    -12.00 is categorizable. -500.00 and 500.00 are the two legs of an accepted
    transfer between two open accounts. -30.00 sits on an archived account.
    -8.00 sits on an account with no raw account row, so it never resolves.
    """
    for account_id in ("acct_open", "acct_other", "acct_archived"):
        _insert_account(db, account_id)
    db.execute(
        "INSERT INTO app.account_settings (account_id, archived) VALUES (?, TRUE)",
        ["acct_archived"],
    )
    _insert_transaction(
        db, source_id="t_pending", account_id="acct_open", amount=Decimal("-12.00")
    )
    _insert_transaction(
        db, source_id="t_out", account_id="acct_open", amount=Decimal("-500.00")
    )
    _insert_transaction(
        db, source_id="t_in", account_id="acct_other", amount=Decimal("500.00")
    )
    _insert_transaction(
        db, source_id="t_archived", account_id="acct_archived", amount=Decimal("-30.00")
    )
    _insert_transaction(
        db, source_id="t_ghost", account_id="acct_ghost", amount=Decimal("-8.00")
    )
    # A confirmed transfer is an input here, not the mechanism under test.
    MatchDecisionsRepo(db).insert(
        match_id="m_transfer01",
        source_transaction_id_a="t_out",
        source_type_a="ofx",
        source_origin_a="test_bank",
        source_transaction_id_b="t_in",
        source_type_b="ofx",
        source_origin_b="test_bank",
        account_id="acct_open",
        account_id_b="acct_other",
        confidence_score=1.0,
        match_signals={},
        match_type="transfer",
        match_status="accepted",
        decided_by="system",
        actor="test",
    )


@pytest.mark.slow
def test_is_categorizable_is_true_only_for_the_row_failing_no_condition(
    db: Database,
) -> None:
    seed_one_row_per_exclusion(db)
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    # Each row reaches exactly the predicate it is meant to fail.
    inputs = {
        amount: (is_transfer, archived)
        for amount, is_transfer, archived in db.execute(
            """
            SELECT t.amount, t.is_transfer, a.archived
            FROM core.fct_transactions AS t
            LEFT JOIN core.dim_accounts AS a USING (account_id)
            """
        ).fetchall()
    }
    assert inputs == {
        Decimal("-12.00"): (False, False),
        Decimal("-500.00"): (True, False),
        Decimal("500.00"): (True, False),
        Decimal("-30.00"): (False, True),
        Decimal("-8.00"): (False, None),
    }

    # Dict equality also proves no row is NULL: None is not False.
    assert dict(
        db.execute(
            "SELECT amount, is_categorizable FROM core.fct_transactions"
        ).fetchall()
    ) == {
        Decimal("-12.00"): True,
        Decimal("-500.00"): False,
        Decimal("500.00"): False,
        Decimal("-30.00"): False,
        Decimal("-8.00"): False,
    }


@pytest.mark.slow
def test_an_archive_change_advances_updated_at_on_every_row_of_the_account(
    db: Database,
) -> None:
    """Archiving moves is_categorizable, so updated_at must move with it.

    Both transactions carry their own currency, so the inherited-currency fold
    never applies. Only acct_moved records an archive change; acct_renamed has
    a newer settings write but no archive change, so its row must not advance.
    """
    for account_id in ("acct_moved", "acct_renamed"):
        _insert_account(db, account_id)
    db.execute(
        """
        INSERT INTO app.account_settings
            (account_id, archived, updated_at, archived_changed_at)
        VALUES
            ('acct_moved', TRUE, '2000-01-01'::TIMESTAMP, '2099-01-01'::TIMESTAMP),
            ('acct_renamed', FALSE, '2099-01-01'::TIMESTAMP, NULL)
        """  # test fixture, not executing user SQL
    )
    _insert_transaction(
        db, source_id="t_moved", account_id="acct_moved", amount=Decimal("-30.00")
    )
    _insert_transaction(
        db, source_id="t_renamed", account_id="acct_renamed", amount=Decimal("-9.00")
    )
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    stamp = datetime(2099, 1, 1, tzinfo=UTC)
    rows = {
        account_id: (currency_code, updated_at)
        for account_id, currency_code, updated_at in db.execute(
            "SELECT account_id, currency_code, updated_at FROM core.fct_transactions"
        ).fetchall()
    }
    assert rows["acct_moved"] == ("USD", stamp)
    assert rows["acct_renamed"][0] == "USD"
    assert rows["acct_renamed"][1] < stamp
    assert db.execute(
        "SELECT updated_at FROM core.dim_accounts WHERE account_id = 'acct_moved'"
    ).fetchone() == (stamp,)
