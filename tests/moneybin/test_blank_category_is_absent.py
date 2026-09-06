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
    db: Database,
    *,
    txn_id: str,
    category: str | None,
    subcategory: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, category,
             subcategory)
        VALUES (?, 'acct_blank_cat', '2026-07-01'::DATE, -10.00, 'Test Payee',
                '/tmp/blank_cat.csv', 'csv', 'test_bank',
                '00000000-0000-0000-0000-0000000000b1', ?, ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, category, subcategory],
    )


def _insert_manual_transaction(
    db: Database,
    *,
    txn_id: str,
    category: str | None,
    subcategory: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_transactions
            (source_transaction_id, import_id, account_id, transaction_date,
             amount, description, created_by, category, subcategory)
        VALUES (?, 'manual_blank_cat', 'acct_blank_cat', '2026-07-01'::DATE,
                -10.00, 'Test Manual Entry', 'cli', ?, ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, category, subcategory],
    )


def _fact_category(db: Database, description: str) -> tuple[str | None, ...] | None:
    return db.execute(
        "SELECT category FROM core.fct_transactions WHERE description = ?",
        [description],
    ).fetchone()


def _fact_pair(db: Database, description: str) -> tuple[str | None, ...] | None:
    return db.execute(
        "SELECT category, subcategory FROM core.fct_transactions WHERE description = ?",
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
def test_a_non_breaking_space_category_is_absent_in_the_fact(
    db: Database,
) -> None:
    """Blank means what the service means by it, not what ASCII space means.

    A non-breaking space is what copying a cell out of a spreadsheet produces,
    and an ideographic space is ordinary CJK input. Both render as an empty
    cell and both are refused on the write path, so a character-list trim that
    happened to omit them would leave the import path disagreeing with the
    validator on the likeliest real input.
    """
    _insert_tabular_transaction(db, txn_id="csv_nbsp", category="\xa0　")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = _fact_category(db, "Test Payee")
    assert row is not None
    assert row[0] is None


@pytest.mark.slow
def test_a_blanked_category_takes_its_subcategory_with_it(db: Database) -> None:
    """Nulling the two columns independently manufactures an orphan.

    A subcategory is a child of a category here: ``resolve_category_id`` short-
    circuits to NULL the moment ``category`` is NULL, so no lone subcategory can
    ever resolve to a ``category_id``. ``core.fct_transaction_lines`` then
    coalesces the two columns independently, rendering this row's subcategory
    beside the *parent's* category. ``SplitTarget`` refuses the shape on MCP and
    the service now refuses it on every write path, so the import path must not
    be the one place that still creates it.
    """
    _insert_tabular_transaction(
        db, txn_id="csv_orphan", category="   ", subcategory="Coffee"
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = _fact_pair(db, "Test Payee")
    assert row is not None
    assert row == (None, None)


@pytest.mark.slow
def test_a_blanked_category_takes_its_subcategory_with_it_on_the_manual_arm(
    db: Database,
) -> None:
    """The manual arm carries the same pair, so it carries the same rule."""
    _insert_manual_transaction(
        db, txn_id="manual_orphan", category="   ", subcategory="Coffee"
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = _fact_pair(db, "Test Manual Entry")
    assert row is not None
    assert row == (None, None)


@pytest.mark.slow
def test_a_blank_subcategory_leaves_its_category_standing(db: Database) -> None:
    """The cascade runs one way only.

    A category with a blank subcategory is a real, resolvable state — 17 of the
    seeded categories are top-level with a NULL subcategory. Nulling the parent
    too would discard a categorization the user made.
    """
    _insert_tabular_transaction(
        db, txn_id="csv_blank_sub", category="Groceries", subcategory="   "
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = _fact_pair(db, "Test Payee")
    assert row is not None
    assert row == ("Groceries", None)


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
