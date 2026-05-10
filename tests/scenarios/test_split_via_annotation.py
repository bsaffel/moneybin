"""Scenario: split-via-annotation surfaces correctly in core.

A curator imports a known-amount transaction and adds 3 splits across
categories. The post-state must show:

1. Three rows in ``core.fct_transaction_lines`` for the parent
   transaction with ``line_kind = 'split'`` (one row per split,
   none for the parent itself — the UNNEST view replaces the parent
   row when ``has_splits`` is true).
2. The parent row in ``core.fct_transactions`` keeps its full original
   amount and carries ``has_splits = TRUE``.
3. Split amounts sum to the parent amount (balanced fixture — the
   sum-of-children == parent.amount invariant is warn-not-block in the
   service layer per spec, so this scenario uses a balanced setup;
   imbalance behavior is covered by ``splits_balance`` unit tests).

Negative coverage: the test also asserts the unsplit-row count is zero
for this transaction in ``fct_transaction_lines``. A regression where
the UNNEST view's ``WHERE NOT t.has_splits OR NOT s.split_id IS NULL``
clause accidentally emitted both the parent and the split children
would show up here.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.services.transaction_service import TransactionService
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_split_via_annotation() -> None:
    scenario = load_shipped_scenario("split-via-annotation")
    assert scenario is not None

    csv_fixture = FixtureSpec(
        path="curation-split/grocery_run.csv",
        account="curation-checking",
        source_type="csv",
    )

    with scenario_env(scenario) as (db, _tmp, env):
        load_fixture_into_db(db, csv_fixture)
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        row = db.execute(
            """
            SELECT transaction_id, amount FROM core.fct_transactions
             WHERE source_type = 'csv'
               AND transaction_date = DATE '2024-08-20'
            """
        ).fetchone()
        assert row is not None, "imported CSV row missing from fct_transactions"
        transaction_id = str(row[0])
        parent_amount = Decimal(str(row[1]))
        # Hand-derived from the fixture: the fixture lists -100.00.
        assert parent_amount == Decimal("-100.00"), (
            f"parent amount {parent_amount} != fixture -100.00 — "
            f"fixture loader or merge regressed"
        )

        # Add 3 splits whose amounts sum to the parent amount. Hand
        # decomposition: 60 + 25 + 15 = 100; signs negative because
        # parent is an expense.
        svc = TransactionService(db)
        svc.add_split(
            transaction_id,
            Decimal("-60.00"),
            category="Groceries",
            actor="cli",
        )
        svc.add_split(
            transaction_id,
            Decimal("-25.00"),
            category="Household",
            actor="cli",
        )
        svc.add_split(
            transaction_id,
            Decimal("-15.00"),
            category="Personal Care",
            actor="cli",
        )

        # core.fct_transactions / fct_transaction_lines are SQLMesh-managed
        # views derived from prep + app — re-transform so the splits land.
        run_step("transform", scenario.setup, db, env=env)

        # Assertion 1: parent row keeps full amount and has_splits=TRUE.
        parent = db.execute(
            """
            SELECT amount, has_splits, split_count FROM core.fct_transactions
             WHERE transaction_id = ?
            """,
            [transaction_id],
        ).fetchone()
        assert parent is not None
        assert Decimal(str(parent[0])) == Decimal("-100.00"), (
            f"parent amount mutated by split creation: {parent[0]}"
        )
        assert bool(parent[1]) is True, "has_splits not set on parent row"
        assert int(parent[2]) == 3, f"split_count={parent[2]} on parent, expected 3"

        # Assertion 2: fct_transaction_lines has exactly 3 split rows
        # for this transaction; zero rows of line_kind='whole'.
        line_rows = db.execute(
            """
            SELECT line_kind, line_amount, line_category
              FROM core.fct_transaction_lines
             WHERE transaction_id = ?
             ORDER BY line_kind, line_amount
            """,
            [transaction_id],
        ).fetchall()
        line_kinds = [r[0] for r in line_rows]
        assert line_kinds == ["split", "split", "split"], (
            f"expected 3 split rows + 0 whole rows, got line_kinds={line_kinds}"
        )

        # Assertion 3: split amounts sum to parent amount. Hand-derived:
        # -60 + -25 + -15 = -100.
        line_amounts = [Decimal(str(r[1])) for r in line_rows]
        assert sum(line_amounts) == Decimal("-100.00"), (
            f"split sum {sum(line_amounts)} != parent -100.00"
        )

        # Assertion 4: each split's category landed in line_category
        # (not the parent's NULL category) — a regression in the
        # COALESCE(s.category, t.category) on the view would surface
        # here as None or a parent-derived value.
        line_cats = sorted(str(r[2]) for r in line_rows)
        assert line_cats == ["Groceries", "Household", "Personal Care"], (
            f"split categories did not propagate to line_category: {line_cats}"
        )
