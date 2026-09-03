"""A blank category counts as absent everywhere the pipeline reads one.

``stg_plaid__accounts.sql`` already states the rule for its own free-text
columns: ``''`` passes a NULL check while rendering as a malformed label.
``stg_tabular__transactions`` applies it to ``currency`` and not to
``category``, and ``stg_manual__transactions`` applies it to nothing, so a cell
holding only spaces arrives at ``core.fct_transactions.category`` non-NULL.
``core.uncategorized_queue`` selects ``WHERE category IS NULL``, so that
transaction is missing from the queue nobody can curate it out of.

Seeds ``raw.*`` directly (mirrors ``test_int_unioned_currency.py``) to isolate
the staging SQL from the importer path.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _insert_tabular_transaction(
    db: Database, *, txn_id: str, category: str | None
) -> None:
    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, category)
        VALUES (?, 'acct_blank_cat', '2026-07-01'::DATE, -10.00, 'Test Payee',
                '/tmp/blank_cat.csv', 'csv', 'test_bank',
                '00000000-0000-0000-0000-0000000000b1', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, category],
    )


def _insert_manual_transaction(
    db: Database, *, txn_id: str, category: str | None
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_transactions
            (source_transaction_id, import_id, account_id, transaction_date,
             amount, description, created_by, category)
        VALUES (?, 'manual_blank_cat', 'acct_blank_cat', '2026-07-01'::DATE,
                -10.00, 'Test Manual Entry', 'cli', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, category],
    )


def _fact_category(db: Database, description: str) -> tuple[str | None, ...] | None:
    return db.execute(
        "SELECT category FROM core.fct_transactions WHERE description = ?",
        [description],
    ).fetchone()


@pytest.mark.slow
def test_a_whitespace_category_from_a_csv_is_absent_in_the_fact(
    db: Database,
) -> None:
    """A CSV cell holding only spaces is not a category a person wrote.

    Left raw, it reaches the fact as ``'   '`` — non-NULL, so
    ``core.uncategorized_queue`` skips the row and every table that renders the
    column shows a blank cell instead of the placeholder.
    """
    _insert_tabular_transaction(db, txn_id="csv_blank", category="   ")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = _fact_category(db, "Test Payee")
    assert row is not None
    assert row[0] is None


@pytest.mark.slow
def test_a_whitespace_category_from_a_manual_entry_is_absent_in_the_fact(
    db: Database,
) -> None:
    """The manual arm trims nothing at all, so it has the same gap."""
    _insert_manual_transaction(db, txn_id="manual_blank", category="   ")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = _fact_category(db, "Test Manual Entry")
    assert row is not None
    assert row[0] is None


@pytest.mark.slow
def test_a_category_a_person_wrote_survives_the_trim(db: Database) -> None:
    """The restraint half: only blank becomes absent, and padding is stripped.

    Without this the fix could pass by nulling the column outright — the defect
    #515 removed from the Plaid arm, reintroduced one source over.
    """
    _insert_tabular_transaction(db, txn_id="csv_real", category="  Groceries  ")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = _fact_category(db, "Test Payee")
    assert row is not None
    assert row[0] == "Groceries"
