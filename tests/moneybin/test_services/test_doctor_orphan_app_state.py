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


def test_run_all_populates_recovery_actions_for_orphan_app_state(
    db: Database,
) -> None:
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
