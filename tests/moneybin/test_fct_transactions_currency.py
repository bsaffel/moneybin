"""Integration tests for currency_code inheritance in core.fct_transactions.

multi-currency.md Requirement 3: a transaction whose own currency is unknown
inherits its account's currency_code, never a blind default. Seeds raw.*/app.*
directly (mirrors test_fct_balances_plaid.py) to isolate the SQL from the
extractor/resolver path.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _insert_dim_account_inputs(db: Database, *, account_id: str) -> None:
    """Minimal raw.ofx_accounts row so core.dim_accounts has this account_id."""
    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, account_type, source_file, extracted_at,
             source_type, source_origin)
        VALUES (?, 'CHECKING', 'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [account_id],
    )


def _set_account_currency(db: Database, *, account_id: str, currency_code: str) -> None:
    db.execute(
        "INSERT INTO app.account_settings (account_id, currency_code) VALUES (?, ?)",
        [account_id, currency_code],
    )


def _insert_ofx_transaction(
    db: Database, *, txn_id: str, account_id: str, currency_code: str | None
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, transaction_type, date_posted,
             amount, payee, source_file, extracted_at, source_type,
             source_origin, currency_code)
        VALUES (?, ?, 'DEBIT', '2026-07-01'::TIMESTAMP, -10.00, 'Test Payee',
                'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, account_id, currency_code],
    )


@pytest.mark.slow
def test_transaction_currency_inherits_from_account_when_unknown(db: Database) -> None:
    """A transaction with no captured currency inherits the account's currency_code."""
    _insert_dim_account_inputs(db, account_id="a_eur")
    _set_account_currency(db, account_id="a_eur", currency_code="EUR")
    _insert_ofx_transaction(db, txn_id="t1", account_id="a_eur", currency_code=None)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    # core.fct_transactions carries no source_transaction_id (only the gold-key
    # transaction_id hash); each test uses a distinct account_id, so filtering
    # by account_id is the simplest correct way to find this test's one row.
    row = db.execute(
        "SELECT currency_code FROM core.fct_transactions WHERE account_id = 'a_eur'"
    ).fetchone()
    assert row is not None
    assert row[0] == "EUR"


@pytest.mark.slow
def test_transaction_own_currency_wins_over_account_currency(db: Database) -> None:
    """A transaction's own captured currency is never overridden by account inheritance."""
    _insert_dim_account_inputs(db, account_id="a_usd")
    _set_account_currency(db, account_id="a_usd", currency_code="USD")
    _insert_ofx_transaction(db, txn_id="t2", account_id="a_usd", currency_code="GBP")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM core.fct_transactions WHERE account_id = 'a_usd'"
    ).fetchone()
    assert row is not None
    assert row[0] == "GBP"


@pytest.mark.slow
def test_transaction_currency_falls_back_to_account_default_when_neither_known(
    db: Database,
) -> None:
    """No captured, source, or account currency: the row stays unknown.

    Neither a captured transaction currency nor an explicit account currency is
    set here, and the OFX fixture carries no balance row to supply one, so
    nothing in the chain knows what this amount is denominated in.

    This assertion used to read ``"USD"``, and its docstring asked that a future
    change to dim_accounts' default arrive as a deliberate, visible diff here
    rather than silently. This is that diff: M1K.1 Part B removed the blind
    default, so an unknown currency is now representable end-to-end
    (multi-currency.md Requirement 8) instead of being guessed. `system doctor`
    reports these rows for the user to resolve with `accounts set --currency`.
    """
    _insert_dim_account_inputs(db, account_id="a_unknown")
    # No app.account_settings row at all for this account.
    _insert_ofx_transaction(db, txn_id="t3", account_id="a_unknown", currency_code=None)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM core.fct_transactions WHERE account_id = 'a_unknown'"
    ).fetchone()
    assert row is not None
    assert row[0] is None


@pytest.mark.slow
def test_cleared_account_currency_advances_transaction_freshness(
    db: Database,
) -> None:
    """A cleared inherited currency remains visible to incremental consumers."""
    _insert_dim_account_inputs(db, account_id="a_cleared")
    db.execute(
        """
        INSERT INTO app.account_settings (account_id, currency_code, updated_at)
        VALUES ('a_cleared', NULL, '2099-01-01 09:00:00'::TIMESTAMP)
        """
    )
    _insert_ofx_transaction(
        db, txn_id="t-cleared", account_id="a_cleared", currency_code=None
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT t.currency_code, t.updated_at, a.updated_at
        FROM core.fct_transactions AS t
        JOIN core.dim_accounts AS a USING (account_id)
        WHERE t.account_id = 'a_cleared'
        """
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] == row[2]


@pytest.mark.slow
def test_two_banks_sharing_an_account_key_keep_their_own_currencies(
    db: Database,
) -> None:
    """A source-native account id is unique per institution, not globally.

    Two OFX providers can both call an account "1001"; app.account_links maps
    each to its own canonical id, and every sibling join in staging scopes by
    (source_account_key, source_origin) precisely so they stay distinct. A
    currency lookup grouping on the account key alone hands both canonical
    accounts whichever bank reported most recently, so the euro account's
    currency-less facts inherit USD and land in the dollar subtotal — the
    silent blend this milestone exists to prevent.

    Both banks report a currency here, and they differ. That is what isolates
    this guard: with one bank, or with matching currencies, the unscoped lookup
    returns the right answer by accident.
    """
    for canonical, origin, currency, when in (
        ("acct_eur00000", "bank_eur", "EUR", "2026-07-01"),
        ("acct_usd00000", "bank_usd", "USD", "2026-07-02"),
    ):
        db.execute(
            """
            INSERT INTO app.account_links
                (link_id, account_id, ref_kind, ref_value, source_type,
                 source_origin, status, decided_by, decided_at)
            VALUES (?, ?, 'source_native', '1001', 'ofx', ?, 'accepted',
                    'system', CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture, not executing user SQL
            [f"lnk_{origin}", canonical, origin],
        )
        db.execute(
            """
            INSERT INTO raw.ofx_accounts
                (account_id, account_type, source_file, extracted_at,
                 source_type, source_origin)
            VALUES ('1001', 'CHECKING', ?, ?::TIMESTAMP, 'ofx', ?)
            """,  # noqa: S608  # test fixture, not executing user SQL
            [f"ofx_{origin}", when, origin],
        )
        db.execute(
            """
            INSERT INTO raw.ofx_balances
                (account_id, statement_start_date, statement_end_date,
                 ledger_balance, ledger_balance_date, source_file, extracted_at,
                 source_type, source_origin, currency_code)
            VALUES ('1001', ?::TIMESTAMP, ?::TIMESTAMP, 100.00, ?::TIMESTAMP,
                    ?, ?::TIMESTAMP, 'ofx', ?, ?)
            """,  # noqa: S608  # test fixture, not executing user SQL
            [when, when, when, f"ofx_{origin}", when, origin, currency],
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT account_id, currency_code FROM core.dim_accounts "
        "WHERE account_id IN ('acct_eur00000', 'acct_usd00000')"
    ).fetchall()

    assert dict(rows) == {"acct_eur00000": "EUR", "acct_usd00000": "USD"}
