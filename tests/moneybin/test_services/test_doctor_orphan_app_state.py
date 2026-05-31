"""Integration tests for ``DoctorService._run_orphan_app_state``.

Seeds real ``app.transaction_notes`` / ``app.transaction_tags`` rows against
real ``core.fct_transactions`` stubs to verify orphan detection and the
end-to-end ``run_all`` wiring that fills ``recovery_actions``.
"""

# pyright: reportPrivateUsage=false
from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.database import Database
from moneybin.services.doctor_service import DoctorService
from tests.moneybin.db_helpers import create_core_tables


def _seed_core_txn(db: Database, transaction_id: str) -> None:
    """Insert a minimal core.fct_transactions row so non-orphans pass."""
    db.execute(
        "INSERT INTO core.fct_transactions "  # noqa: S608  # test input, not user SQL
        "(transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [transaction_id, "acct1", date(2024, 1, 1), Decimal("10.00"), "x", "csv"],
    )


def _insert_note(db: Database, *, note_id: str, transaction_id: str) -> None:
    db.execute(
        "INSERT INTO app.transaction_notes "  # noqa: S608  # test input, not user SQL
        "(note_id, transaction_id, text, author) VALUES (?, ?, ?, ?)",
        [note_id, transaction_id, "n", "mcp"],
    )


def _insert_tag(db: Database, *, transaction_id: str, tag: str) -> None:
    db.execute(
        "INSERT INTO app.transaction_tags "  # noqa: S608  # test input, not user SQL
        "(transaction_id, tag, applied_by) VALUES (?, ?, ?)",
        [transaction_id, tag, "mcp"],
    )


def _insert_pending_manual(
    db: Database,
    *,
    source_transaction_id: str,
    predicted_transaction_id: str,
    account_id: str = "acct1",
) -> None:
    """Insert a manual row in raw with a predicted transaction_id but NO core row.

    Mirrors the state between ``transactions_create`` and the first
    ``refresh_run`` — the audit must NOT flag notes/tags written against
    ``predicted_transaction_id`` as orphans during this window.
    """
    db.execute(
        "INSERT INTO raw.manual_transactions "  # noqa: S608  # test input, not user SQL
        "(source_transaction_id, import_id, account_id, transaction_date, "
        " amount, description, created_by, transaction_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            source_transaction_id,
            "imp1",
            account_id,
            date(2024, 1, 1),
            Decimal("10.00"),
            "manual entry",
            "mcp",
            predicted_transaction_id,
        ],
    )


def test_orphan_app_state_skipped_before_core_exists(db: Database) -> None:
    # Without core.fct_transactions, the audit returns 'skipped' rather than
    # spuriously failing — matches the pattern used by every other FK audit.
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "skipped"


def test_orphan_app_state_passes_when_all_resolve(db: Database) -> None:
    create_core_tables(db)
    _seed_core_txn(db, "txn1")
    _insert_note(db, note_id="n1", transaction_id="txn1")
    _insert_tag(db, transaction_id="txn1", tag="foo")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "pass"
    assert result.affected_ids == []


def test_orphan_note_is_flagged(db: Database) -> None:
    create_core_tables(db)
    _insert_note(db, note_id="orphan_n1", transaction_id="missing_txn")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "fail"
    assert "note:orphan_n1" in result.affected_ids


def test_orphan_tag_is_flagged_once_per_transaction(db: Database) -> None:
    create_core_tables(db)
    # Two tag rows for the same orphan transaction — recipe will clear them
    # all with one transactions_tags_set call, so affected_ids carries one
    # entry per orphan transaction_id, not per (transaction, tag) pair.
    _insert_tag(db, transaction_id="missing_txn", tag="a")
    _insert_tag(db, transaction_id="missing_txn", tag="b")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "fail"
    assert result.affected_ids.count("tag:missing_txn") == 1


def test_orphan_mix_of_notes_and_tags(db: Database) -> None:
    create_core_tables(db)
    _seed_core_txn(db, "kept")
    _insert_note(db, note_id="keep_n", transaction_id="kept")
    _insert_note(db, note_id="orphan_n1", transaction_id="gone1")
    _insert_tag(db, transaction_id="gone2", tag="x")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "fail"
    assert set(result.affected_ids) == {"note:orphan_n1", "tag:gone2"}


def test_pending_manual_note_is_not_flagged(db: Database) -> None:
    """Notes written against a manual transaction before refresh must pass.

    Reproduces the data-loss scenario flagged by PR #231's reviewers:
    ``transactions_create`` returns a predicted ``transaction_id`` whose row
    sits in ``raw.manual_transactions`` until the next ``refresh_run``
    materializes it into ``core.fct_transactions``. A note added in that
    window must NOT be flagged as an orphan — destroying it would discard
    legitimate user curation.
    """
    create_core_tables(db)
    _insert_pending_manual(
        db,
        source_transaction_id="manual_abc",
        predicted_transaction_id="pending_txn_hash",
    )
    _insert_note(db, note_id="pending_n", transaction_id="pending_txn_hash")
    _insert_tag(db, transaction_id="pending_txn_hash", tag="pending")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "pass"
    assert result.affected_ids == []


def test_truly_orphaned_note_still_flagged_alongside_pending_manual(
    db: Database,
) -> None:
    """Pending-manual suppression is *narrow*: real orphans still fire.

    Guards against an over-broad suppression — the third NOT EXISTS arm
    must only exclude rows whose transaction_id is present in raw.manual,
    not blanket-exempt every note.
    """
    create_core_tables(db)
    # One pending manual (legitimate) — should be suppressed.
    _insert_pending_manual(
        db,
        source_transaction_id="manual_pending",
        predicted_transaction_id="pending_txn",
    )
    _insert_note(db, note_id="pending_n", transaction_id="pending_txn")
    # One truly orphaned note (no row in raw OR core) — should still fire.
    _insert_note(db, note_id="orphan_n", transaction_id="totally_gone")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "fail"
    assert result.affected_ids == ["note:orphan_n"]


def _stub_prep_int_transactions_matched(db: Database) -> None:
    """Create a minimal prep.int_transactions__matched table for the deduped test.

    Production builds this as a SQLMesh view; for the deduped-manual case
    we just need a column shape we can INSERT a marker row into. The audit
    only references ``source_transaction_id`` on this table.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__matched "
        "(source_transaction_id VARCHAR, match_group_id VARCHAR)"
    )


def test_deduped_manual_note_is_flagged_after_matching(db: Database) -> None:
    """Once the matcher processes a manual, the suppression lifts.

    Reproduces the dedup edge case PR #231 reviewers raised: a manual
    transaction gets created with predicted id T123, the user adds a note
    against T123, then refresh runs matching → the manual joins a dedup
    group with a different canonical id, so T123 is NOT in core. The
    note is now genuinely orphaned (it points at an id that will never
    exist in core), and the audit must report it rather than silently
    suppressing forever.

    Signal that distinguishes pending vs deduped:
    ``prep.int_transactions__matched`` carries a row referencing the
    manual's ``source_transaction_id`` once the matcher has run.
    Pre-matcher: no row → suppress (still pending). Post-matcher: row
    exists → don't suppress → report the orphan.
    """
    create_core_tables(db)
    _stub_prep_int_transactions_matched(db)
    # Manual was inserted, matched (prep row exists), and absorbed into a
    # dedup group with a different canonical id (so T123 is NOT in core).
    _insert_pending_manual(
        db,
        source_transaction_id="manual_deduped",
        predicted_transaction_id="T123_deduped_away",
    )
    db.execute(
        "INSERT INTO prep.int_transactions__matched "  # noqa: S608  # test input
        "(source_transaction_id, match_group_id) VALUES (?, ?)",
        ["manual_deduped", "group_canonical_id"],
    )
    _insert_note(db, note_id="lost_note", transaction_id="T123_deduped_away")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "fail"
    assert "note:lost_note" in result.affected_ids


def test_pending_manual_still_suppressed_when_prep_view_empty_of_it(
    db: Database,
) -> None:
    """Pending suppression still applies when matcher hasn't seen the manual.

    The complement to ``test_deduped_manual_note_is_flagged_after_matching``:
    if prep.int_transactions__matched exists but has no row for this
    manual's source_transaction_id, the matcher hasn't processed it yet —
    notes against the predicted id are pending, not orphaned.
    """
    create_core_tables(db)
    _stub_prep_int_transactions_matched(db)
    _insert_pending_manual(
        db,
        source_transaction_id="manual_truly_pending",
        predicted_transaction_id="T456_pending",
    )
    # Prep view has rows for OTHER source ids but not for our manual.
    db.execute(
        "INSERT INTO prep.int_transactions__matched "  # noqa: S608  # test input
        "(source_transaction_id, match_group_id) VALUES (?, ?)",
        ["some_other_source", "group_y"],
    )
    _insert_note(db, note_id="pending_n", transaction_id="T456_pending")
    result = DoctorService(db)._run_orphan_app_state()
    assert result.status == "pass"


def test_run_all_populates_recovery_actions_for_orphan_app_state(
    db: Database,
) -> None:
    """End-to-end wiring check for the orphan_app_state recipe.

    SQLMesh isn't booted in this fixture (no ``monkeypatch`` of
    ``sqlmesh_context``), so ``_run_sqlmesh_audits`` degrades to a single
    ``sqlmesh_audits_unavailable`` skipped result instead of producing the
    real 3 SQLMesh-audit results. That's fine for this test — we filter by
    name to find ``orphan_app_state`` and exercise ``_apply_recipe`` for it
    plus every non-SQLMesh ``_run_*`` method. SQLMesh-recipe wiring is
    covered separately by the round-trip-executable test.
    """
    create_core_tables(db)
    _insert_note(db, note_id="orphan_n1", transaction_id="missing_txn_a")
    _insert_tag(db, transaction_id="missing_txn_b", tag="z")

    report = DoctorService(db).run_all()

    orphan_results = [r for r in report.invariants if r.name == "orphan_app_state"]
    assert len(orphan_results) == 1
    orphan = orphan_results[0]
    assert orphan.status == "fail"
    assert orphan.recovery_actions is not None
    tools = sorted(a.tool for a in orphan.recovery_actions)
    assert tools == ["transactions_notes_delete", "transactions_tags_set"]
    # Round-trip-executable spot-check: a notes-delete action carries
    # exactly the note_id we seeded as an orphan. Confidence is "suggested"
    # (not "certain") for notes because the single-id delete is not
    # idempotent across a batch — see orphan_app_state recipe docstring.
    notes_delete = next(
        a for a in orphan.recovery_actions if a.tool == "transactions_notes_delete"
    )
    assert notes_delete.arguments == {"note_id": "orphan_n1"}
    assert notes_delete.confidence == "suggested"
    # The tag-clear action stays "certain" — setting tags to [] is idempotent.
    tags_set = next(
        a for a in orphan.recovery_actions if a.tool == "transactions_tags_set"
    )
    assert tags_set.confidence == "certain"
