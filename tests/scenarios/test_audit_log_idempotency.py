"""Scenario: replaying idempotent curation is state-stable and audit-clean.

The audit log records only real mutations: an idempotent re-apply (re-adding
a tag that already exists) performs no row change and emits NO audit row
(REC-PR3 DN2 — repos audit real mutations only). Replaying a fixed sequence
of curation operations must:

1. Leave ``app.transaction_notes`` / ``transaction_tags`` /
   ``transaction_splits`` row counts and contents identical to the
   first-run end-state (true idempotency of state).
2. Emit FEWER events on the second run, by exactly the count of ops that
   became no-ops on replay — here the two ``add_tags`` re-adds (both tags
   already exist from run 1), so run 2 emits ``first_run - 2`` events.
3. Carry no ``context_json.noop`` rows anywhere, and exactly one
   ``tag.add`` per distinct tag (the re-add emitted nothing).

The sequence below is deliberately constrained: ``edit_note`` rewrites the
same text (a real UPDATE, emitted both runs); ``add_tags`` re-adds are the
idempotent no-ops; ``set_splits`` declaratively clears-and-replaces, emitting
the same per-row event count both runs. Notes are non-idempotent at creation
(``add_note`` mints a fresh ``note_id``), so the note is created once in setup
and only ``edit_note`` runs inside the replayed sequence.
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
    no-op branch on replay (no row change, no audit row — DN2);
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

        # Assertion 2: run 2 emits exactly 2 fewer events than run 1 (DN2).
        # The sequence's only ops that become no-ops on replay are the two
        # add_tags re-adds (both tags already exist from run 1); every other
        # op (edit_note, set_splits clear+add) is a real mutation both runs.
        # Hand-derived: 2 idempotent re-adds emit nothing on replay.
        assert second_run_emitted == first_run_emitted - 2, (
            f"second-run emitted {second_run_emitted} audit events vs "
            f"{first_run_emitted} on first run — expected exactly 2 fewer "
            f"(the two idempotent tag re-adds emit no audit row under DN2)"
        )

        # Assertion 3: no noop-marked rows anywhere, and exactly one tag.add
        # per distinct tag — the re-adds emitted nothing rather than a noop row.
        no_noops = db.execute(
            "SELECT COUNT(*) FROM app.audit_log "
            "WHERE json_extract_string(context_json, '$.noop') = 'true'"
        ).fetchone()
        assert no_noops is not None and int(no_noops[0]) == 0, (
            "no-op audit rows must not be emitted (DN2); "
            f"found {no_noops[0] if no_noops else '?'}"
        )
        # Row-grain target_id is the composite "transaction_id:tag", so match the
        # delimiter-anchored prefix rather than the bare transaction_id.
        tag_adds = db.execute(
            "SELECT COUNT(*) FROM app.audit_log "
            "WHERE action = 'tag.add' AND target_id LIKE ?",
            [f"{transaction_id}:%"],
        ).fetchone()
        # Two distinct tags, each added once in run 1; run 2 re-adds emit nothing.
        assert tag_adds is not None and int(tag_adds[0]) == 2, (
            f"expected exactly 2 tag.add events (one per tag), got "
            f"{tag_adds[0] if tag_adds else '?'}"
        )


def _audit_count(db: Database) -> int:
    row = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    return int(row[0]) if row else 0
