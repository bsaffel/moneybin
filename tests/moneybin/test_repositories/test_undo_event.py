"""Generic inverse synthesis: ``BaseRepo.undo_event`` (REC-PR3 Phase 4).

The undo consumer reverses any audited ``app.*`` mutation from its full-row
``before``/``after`` capture alone (Req 4/5): INSERT→delete, DELETE→reinsert,
UPDATE→restore-before. One generic implementation on ``BaseRepo`` serves every
repo — including ``MatchDecisionsRepo``, whose ``match_signals`` JSON column the
reverser binds natively (no per-repo override). The undo is itself audited
(``is_undo``/``undoes_operation_id``) so it is undoable in turn.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services.audit_service import AuditEvent, AuditService


def _seed_match(repo: MatchDecisionsRepo, match_id: str = "m1") -> AuditEvent:
    return repo.insert(
        match_id=match_id,
        source_transaction_id_a="txn_a",
        source_type_a="csv",
        source_origin_a="o_a",
        source_transaction_id_b="txn_b",
        source_type_b="csv",
        source_origin_b="o_b",
        account_id="acct_1",
        confidence_score=0.87,
        match_signals={"date_distance": 0, "description_similarity": 0.87},
        match_status="pending",
        decided_by="auto",
        actor="cli",
    )


class TestUndoOfInsert:
    """Undoing an INSERT deletes the row and audits the reversal."""

    def test_deletes_inserted_row(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        add = repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        repo.undo_event(add, actor="cli")
        row = db.execute(
            "SELECT 1 FROM app.transaction_tags WHERE transaction_id = ? AND tag = ?",
            ["txn_1", "trip"],
        ).fetchone()
        assert row is None

    def test_undo_audit_shape(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        add = repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        undo = repo.undo_event(add, actor="cli")
        assert undo is not None
        assert undo.is_undo is True
        assert undo.undoes_operation_id == add.operation_id
        assert undo.action == "tag.add.undo"
        # The undo's before/after mirror the original (before=after, after=before).
        assert undo.before_value is not None
        assert undo.before_value["tag"] == "trip"
        assert undo.after_value is None
        assert undo.target_id == add.target_id


class TestUndoOfDelete:
    """Undoing a DELETE reinserts the removed row with its original fields."""

    def test_reinserts_removed_row(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        delete = repo.delete(note_id="n1", actor="cli")
        repo.undo_event(delete, actor="cli")
        row = db.execute(
            "SELECT transaction_id, text, author FROM app.transaction_notes "
            "WHERE note_id = ?",
            ["n1"],
        ).fetchone()
        assert row == ("txn_1", "hi", "cli")

    def test_undo_audit_shape(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        delete = repo.delete(note_id="n1", actor="cli")
        undo = repo.undo_event(delete, actor="cli")
        assert undo is not None
        assert undo.is_undo is True
        assert undo.undoes_operation_id == delete.operation_id
        assert undo.action == "note.delete.undo"
        assert undo.after_value is not None and undo.after_value["text"] == "hi"
        assert undo.before_value is None


class TestUndoOfUpdate:
    """Undoing an UPDATE restores every column to its before-image."""

    def test_restores_prior_text(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        edit = repo.edit(note_id="n1", text="bye", actor="cli")
        repo.undo_event(edit, actor="cli")
        row = db.execute(
            "SELECT text FROM app.transaction_notes WHERE note_id = ?", ["n1"]
        ).fetchone()
        assert row == ("hi",)

    def test_undo_audit_shape(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        edit = repo.edit(note_id="n1", text="bye", actor="cli")
        undo = repo.undo_event(edit, actor="cli")
        assert undo is not None
        assert undo.action == "note.edit.undo"
        # before mirrors the original after (text=bye); after mirrors before (hi).
        assert undo.before_value is not None and undo.before_value["text"] == "bye"
        assert undo.after_value is not None and undo.after_value["text"] == "hi"


class TestUndoOfPkChangingUpdate:
    """A pk-changing UPDATE (tag rename) is located by the after-image's pk."""

    def test_restores_old_tag(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        repo.add(transaction_id="txn_1", tag="old", actor="cli")
        rename = repo.rename_row(
            transaction_id="txn_1", old_tag="old", new_tag="new", actor="cli"
        )
        repo.undo_event(rename, actor="cli")
        tags = [
            r[0]
            for r in db.execute(
                "SELECT tag FROM app.transaction_tags WHERE transaction_id = ?",
                ["txn_1"],
            ).fetchall()
        ]
        assert tags == ["old"]


class TestUndoOfNoop:
    """A no-op event (before == after, or both None) writes nothing."""

    def test_returns_none_no_audit(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        before_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        assert before_count is not None
        same = {"note_id": "n1", "transaction_id": "t", "text": "x"}
        noop = AuditEvent(
            audit_id="a1",
            occurred_at="",
            actor="cli",
            action="note.edit",
            target_schema="app",
            target_table="transaction_notes",
            target_id="t",
            before_value=dict(same),
            after_value=dict(same),
            parent_audit_id=None,
            operation_id="op_x",
        )
        result = repo.undo_event(noop, actor="cli")
        assert result is None
        after_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        assert after_count == before_count  # no audit row written


class TestUndoOfMatchDecisionGeneric:
    """Generic reverser handles ``match_decisions`` (JSON column) with no override."""

    def test_undo_insert_deletes(self, db: Database) -> None:
        repo = MatchDecisionsRepo(db)
        insert = _seed_match(repo)
        repo.undo_event(insert, actor="cli")
        row = db.execute(
            "SELECT 1 FROM app.match_decisions WHERE match_id = ?", ["m1"]
        ).fetchone()
        assert row is None

    def test_undo_reverse_restores_status_and_signals(self, db: Database) -> None:
        repo = MatchDecisionsRepo(db)
        _seed_match(repo)
        # reverse() only accepts accepted/rejected rows; accept before reversing.
        repo.update_status("m1", status="accepted", decided_by="user", actor="cli")
        reverse = repo.reverse("m1", reversed_by="user", actor="cli")
        repo.undo_event(reverse, actor="cli")
        row = db.execute(
            "SELECT match_status, reversed_at, match_signals "
            "FROM app.match_decisions WHERE match_id = ?",
            ["m1"],
        ).fetchone()
        assert row is not None
        assert row[0] == "accepted"  # restored from 'reversed'
        assert row[1] is None  # reversed_at cleared back to before-image
        assert json.loads(row[2])["description_similarity"] == 0.87


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def _legacy_note_event(db: Database, value: str) -> AuditEvent:
    event = AuditService(db).record_audit_event(
        action="note.delete",
        target=("app", "transaction_notes", "n_legacy"),
        before={
            "note_id": "n_legacy",
            "transaction_id": "txn_legacy",
            "text": "historical",
            "author": "cli",
            "created_at": value,
        },
        after=None,
        actor="cli",
    )
    db.execute(
        "UPDATE app.audit_log SET occurred_at = ? WHERE audit_id = ?",
        [datetime(2025, 1, 1, tzinfo=UTC), event.audit_id],
    )
    return AuditService(db).events_for_operation(event.operation_id)[0]


def legacy_note_policy(db: Database, *, count: int = 1) -> None:
    """Seed accepted synthetic provenance shared by normal and alias replay tests."""
    rule = {
        "schema": "app",
        "table": "transaction_notes",
        "column": "created_at",
        "writer_era": "local-naive",
        "zone": "America/New_York",
        "fold": None,
        "recorded_from": "2020-01-01T00:00:00.000000Z",
        "recorded_until": "2026-01-01T00:00:00.000000Z",
    }
    updated = db.execute(
        "UPDATE app.utc_upgrade_provenance SET rules = ? WHERE contract_version = 2",
        [json.dumps([rule] * count)],
    ).fetchone()
    assert updated == (1,)


def test_legacy_audit_replay_preserves_instants_and_original_json(db: Database) -> None:
    event = _legacy_note_event(db, "2024-07-01T12:00:00")
    legacy_note_policy(db)
    evidence = db.execute(
        "SELECT before_value FROM app.audit_log WHERE audit_id = ?", [event.audit_id]
    ).fetchone()
    original_image = dict(event.before_value or {})
    repo = TransactionNotesRepo(db)
    undo = repo.undo_event(event, actor="cli")
    assert db.execute("SELECT created_at FROM app.transaction_notes").fetchone() == (
        datetime(2024, 7, 1, 16, tzinfo=UTC),
    )
    assert undo is not None and undo.after_value is not None
    assert undo.after_value["created_at"] == "2024-07-01T16:00:00.000000Z"
    assert event.before_value == original_image
    assert (
        db.execute(
            "SELECT before_value FROM app.audit_log WHERE audit_id = ?",
            [event.audit_id],
        ).fetchone()
        == evidence
    )
    redo = repo.undo_event(undo, actor="cli")
    assert redo is not None
    assert db.execute("SELECT COUNT(*) FROM app.transaction_notes").fetchone() == (0,)
    repo.undo_event(redo, actor="cli")
    assert db.execute("SELECT created_at FROM app.transaction_notes").fetchone() == (
        datetime(2024, 7, 1, 16, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    "value,rule_count",
    [
        ("2024-07-01T12:00:00", 0),
        ("2024-07-01T12:00:00", 2),
        ("2024-11-03T01:30:00", 1),
        ("2024-03-10T02:30:00", 1),
    ],
)
def test_legacy_audit_refuses_uncertain_time_before_mutation(
    db: Database,
    value: str,
    rule_count: int,
) -> None:
    event = _legacy_note_event(db, value)
    if rule_count:
        legacy_note_policy(db, count=rule_count)
    count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError) as exc:
        TransactionNotesRepo(db).undo_event(event, actor="cli")
    assert exc.value.code == error_codes.RECOVERY_NO_PATH
    assert db.execute("SELECT COUNT(*) FROM app.transaction_notes").fetchone() == (0,)
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == count


def test_audit_temporal_decoder_preserves_nulls_omissions_and_business_dates(
    db: Database,
) -> None:
    from moneybin.temporal_contract import AuditTemporalPolicy, decode_audit_row

    event = replace(
        _legacy_note_event(db, "2024-07-01T12:00:00"), target_table="account_settings"
    )
    image = {
        "archived_changed_at": None,
        "archived_at": "2024-07-01",
        "display_name": "2024-07-01T12:00:00",
        "metadata": {"nested": [1]},
    }
    decoded = decode_audit_row(event, image, AuditTemporalPolicy(rules=()))
    assert decoded == image
    assert "updated_at" not in decoded
    assert decoded is not image
    assert decoded["metadata"] is not image["metadata"]


def test_new_audit_images_use_canonical_utc_and_business_dates(db: Database) -> None:
    from moneybin.repositories.base import BaseRepo
    from moneybin.repositories.exchange_rate_repo import ExchangeRateOverridesRepo

    repo = TransactionNotesRepo(db)
    repo.add(transaction_id="t", note_id="n", text="x", actor="cli")
    db.execute(
        "UPDATE app.transaction_notes SET created_at = ? WHERE note_id = 'n'",
        [datetime(2024, 7, 1, 12, tzinfo=timezone(timedelta(hours=-4)))],
    )
    edited = repo.edit(note_id="n", text="y", actor="cli")
    assert edited.before_value is not None
    assert edited.before_value["created_at"] == "2024-07-01T16:00:00.000000Z"
    rate = ExchangeRateOverridesRepo(db).set(
        "USD",
        "EUR",
        date(2024, 7, 1),
        rate=Decimal("0.9"),
        note=None,
        actor="cli",
    )
    assert rate.after_value is not None
    assert rate.after_value["rate_date"] == "2024-07-01"
    # A database round trip coerces naive inputs before capture; exercise the
    # serialization boundary directly to prove it never guesses an offset.
    with pytest.raises(ValueError):
        BaseRepo._serialize_for_audit({"instant": datetime(2024, 7, 1, 12)})  # pyright: ignore[reportPrivateUsage]  # strict serialization boundary
    events = AuditService(db).list_events(target_table="transaction_notes")
    assert events
    assert all(
        event.occurred_at.endswith("Z") and "T" in event.occurred_at for event in events
    )


@pytest.mark.parametrize("fold,hour", [(0, 5), (1, 6)])
def test_audit_policy_resolves_only_explicit_fold_and_recorded_era(
    db: Database,
    fold: Literal[0, 1],
    hour: int,
) -> None:
    from moneybin.temporal_contract import (
        AuditTemporalPolicy,
        LegacyTimeRule,
        decode_audit_row,
    )

    event = _legacy_note_event(db, "2024-11-03T01:30:00")
    rule = LegacyTimeRule(
        schema="app",
        table="transaction_notes",
        column="created_at",
        writer_era="local-naive",
        zone="America/New_York",
        fold=fold,
        recorded_from=datetime(2025, 1, 1, tzinfo=UTC),
        recorded_until=datetime(2026, 1, 1, tzinfo=UTC),
    )
    image = {"created_at": "2024-11-03T01:30:00"}
    policy = AuditTemporalPolicy(rules=(rule,))
    assert decode_audit_row(event, image, policy)["created_at"] == datetime(
        2024,
        11,
        3,
        hour,
        30,
        tzinfo=UTC,
    )
    # The recorded era selects the event, not the older captured instant.
    with pytest.raises(ValueError):
        decode_audit_row(
            replace(event, occurred_at="2026-01-01T00:00:00.000000Z"),
            image,
            policy,
        )
    assert decode_audit_row(
        event,
        {"created_at": "2024-11-03T01:30:00-04:00"},
        AuditTemporalPolicy(rules=()),
    )["created_at"] == datetime(
        2024,
        11,
        3,
        5,
        30,
        tzinfo=UTC,
    )


@pytest.mark.parametrize(
    "change",
    [
        {"column": "text"},
        {"recorded_from": datetime(2025, 1, 1)},
        {"recorded_until": datetime(2024, 1, 1, tzinfo=UTC)},
        {"writer_era": "unknown"},
        {"writer_era": "utc-naive", "zone": "America/New_York"},
    ],
)
def test_audit_policy_rejects_unsupported_provenance(change: dict[str, object]) -> None:
    from pydantic import TypeAdapter

    from moneybin.temporal_contract import LegacyTimeRule

    values: dict[str, object] = {
        "schema": "app",
        "table": "transaction_notes",
        "column": "created_at",
        "writer_era": "local-naive",
        "zone": "America/New_York",
        "recorded_from": datetime(2025, 1, 1, tzinfo=UTC),
        "recorded_until": datetime(2026, 1, 1, tzinfo=UTC),
    }
    values.update(change)
    with pytest.raises(ValueError):
        TypeAdapter(LegacyTimeRule).validate_json(json.dumps(values, default=str))
