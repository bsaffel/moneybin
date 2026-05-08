"""Scenario: audit log captures every event; idempotent re-applies are marked.

Per transaction-curation spec Req 30, the audit log records every
mutation attempt — including idempotent re-applications — and tags those
re-applies with ``context_json.noop=true`` so reviewers can distinguish
"nothing changed" from "no event was emitted." Replaying a fixed
sequence of curation operations whose semantics are idempotent must:

1. Leave ``app.transaction_notes`` / ``transaction_tags`` /
   ``transaction_splits`` row counts and contents identical to the
   first-run end-state.
2. Exactly DOUBLE the ``app.audit_log`` row count (every operation in
   the second run still emits an event).
3. Mark the second-run re-applications with ``context_json.noop=true``
   on the audit row, where the operation is genuinely a noop on state
   (the ``add_tags`` no-op branch in ``TransactionService.add_tags``).

The sequence below is deliberately constrained to operations that are
either idempotent at the API level (``add_tags`` re-add, ``set_splits``
declarative clear-and-replace) or whose emission shape is invariant on
replay (``rename_tag`` with no matching rows still emits a parent
event). Notes are the one non-idempotent surface (``add_note`` always
inserts a fresh ``note_id``); the test creates the note once in setup
and exercises only ``edit_note`` inside the replayed sequence.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.services.transaction_service import TransactionService
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


def _curation_sequence(
    svc: TransactionService, *, transaction_id: str, note_id: str
) -> None:
    """A fixed sequence of curation ops whose final-state is reproducible.

    All ops in this sequence are idempotent at the row level:
    ``edit_note`` writes the same text; ``add_tags`` re-add hits the
    no-op branch on replay (and emits ``context_json.noop=true``);
    ``set_splits`` clear-and-replaces, recreating the same final shape.

    ``rename_tag`` is intentionally excluded: rename is a one-shot
    operation by design (there's no idempotent re-rename — the source
    tag no longer exists after run 1), and including it in a replay
    sequence either raises a duplicate-key conflict or requires
    contortions that obscure the actual idempotency property under
    test. The plan's example sequence listed rename for narrative
    completeness; in practice rename gets its own dedicated audit-chain
    test (see ``test_e2e_transaction_curation`` in Task 14).
    """
    svc.edit_note(note_id, "checked statement, this is the fence repair", actor="cli")
    svc.add_tags(transaction_id, ["tax:business-expense"], actor="cli")
    svc.add_tags(transaction_id, ["review-later"], actor="cli")
    svc.set_splits(
        transaction_id,
        [
            {"amount": Decimal("-3.00"), "category": "Coffee"},
            {"amount": Decimal("-1.50"), "category": "Tip"},
        ],
        actor="cli",
    )


def _state_snapshot(db: Database, *, transaction_id: str) -> dict[str, object]:
    """Capture the user-state row counts + contents for equality checks."""
    notes = db.execute(
        "SELECT text, author FROM app.transaction_notes "
        "WHERE transaction_id = ? ORDER BY created_at, note_id",
        [transaction_id],
    ).fetchall()
    tags = db.execute(
        "SELECT tag FROM app.transaction_tags WHERE transaction_id = ? ORDER BY tag",
        [transaction_id],
    ).fetchall()
    splits = db.execute(
        "SELECT amount, category, ord FROM app.transaction_splits "
        "WHERE transaction_id = ? ORDER BY ord",
        [transaction_id],
    ).fetchall()
    return {"notes": notes, "tags": tags, "splits": splits}


@pytest.mark.scenarios
@pytest.mark.slow
def test_audit_log_idempotency() -> None:
    scenario = load_shipped_scenario("audit-log-idempotency")
    assert scenario is not None

    csv_fixture = FixtureSpec(
        path="curation-manual-dedup/imported.csv",
        account="curation-checking",
        source_type="csv",
    )

    with scenario_env(scenario) as (db, _tmp, env):
        # Seed: one CSV row in core.fct_transactions so we have a real
        # transaction_id to attach curation state to.
        load_fixture_into_db(db, csv_fixture)
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        row = db.execute(
            """
            SELECT transaction_id FROM core.fct_transactions
             WHERE source_type = 'csv'
             ORDER BY transaction_id LIMIT 1
            """
        ).fetchone()
        assert row is not None
        transaction_id = str(row[0])

        svc = TransactionService(db)
        # Pre-create the note AND the initial splits so the replayed
        # sequence's edit_note has a stable target and set_splits emits
        # the same event count on every replay (one clear + N adds,
        # versus zero-clear + N adds on a fresh row). Both setup
        # operations live OUTSIDE the replayed sequence so the
        # audit-doubling expectation is exact.
        note = svc.add_note(transaction_id, "initial note", actor="cli")
        svc.set_splits(
            transaction_id,
            [
                {"amount": Decimal("-3.00"), "category": "Coffee"},
                {"amount": Decimal("-1.50"), "category": "Tip"},
            ],
            actor="cli",
        )

        # Run 1
        audit_before_first = _audit_count(db)
        _curation_sequence(svc, transaction_id=transaction_id, note_id=note.note_id)
        audit_after_first = _audit_count(db)
        first_state = _state_snapshot(db, transaction_id=transaction_id)
        first_run_emitted = audit_after_first - audit_before_first

        # Run 2 — same sequence, same parameters.
        _curation_sequence(svc, transaction_id=transaction_id, note_id=note.note_id)
        audit_after_second = _audit_count(db)
        second_state = _state_snapshot(db, transaction_id=transaction_id)
        second_run_emitted = audit_after_second - audit_after_first

        # Assertion 1: final state row counts + contents identical.
        assert first_state == second_state, (
            f"replay produced different state\nfirst:  {first_state}\n"
            f"second: {second_state}"
        )

        # Assertion 2: audit row count exactly doubled across the
        # second run — every operation in run 2 still emitted an event.
        assert second_run_emitted == first_run_emitted, (
            f"second-run emitted {second_run_emitted} audit events vs "
            f"{first_run_emitted} on first run — audit log did not double"
        )
        assert audit_after_second - audit_before_first == 2 * first_run_emitted

        # Assertion 3: the second run's noop tag re-adds carry
        # ``context_json.noop=true``. The first ``add_tags`` call in the
        # sequence re-adds 'tax:business-expense' which is already
        # present (added in run 1) → noop branch.
        noop_count = db.execute(
            """
            SELECT COUNT(*) FROM app.audit_log
             WHERE action = 'tag.add'
               AND target_id = ?
               AND json_extract_string(context_json, '$.noop') = 'true'
            """,
            [transaction_id],
        ).fetchone()
        assert noop_count is not None
        # Two add_tags calls in the sequence; on replay both should be
        # noops since both tags already exist (one direct, one via
        # rename in run 1). Hand-derived: 2 noop emissions in run 2,
        # 0 in run 1 → total 2.
        assert int(noop_count[0]) == 2, (
            f"expected 2 noop tag.add events on replay, got {noop_count[0]} — "
            f"idempotent re-applies are not being marked"
        )


def _audit_count(db: Database) -> int:
    row = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    return int(row[0]) if row else 0
