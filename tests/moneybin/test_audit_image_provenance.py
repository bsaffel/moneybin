"""Legacy clocks are attributed to the exact event and captured image."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Literal

import pytest
from pydantic import TypeAdapter

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.import_previews_repo import ImportPreviewsRepo
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.services.undo_service import UndoService
from moneybin.temporal_contract import (
    AuditTemporalPolicy,
    LegacyTimeRule,
    decode_audit_row,
    parse_audit_temporal_policy,
)


def _event(audit_id: str = "consume") -> AuditEvent:
    return AuditEvent(
        audit_id=audit_id,
        occurred_at="2026-10-01T12:03:00Z",
        actor="cli",
        action=f"import_preview.{audit_id}",
        target_schema="app",
        target_table="import_previews",
        target_id="synthetic-preview",
        before_value=None,
        after_value=None,
        parent_audit_id=None,
        operation_id="synthetic-operation",
    )


def _rule(
    audit_id: str | None = None,
    image_side: Literal["before", "after"] | None = None,
    *,
    local: bool = False,
) -> LegacyTimeRule:
    return LegacyTimeRule(
        schema="app",
        table="import_previews",
        column="updated_at",
        writer_era="local-naive" if local else "utc-naive",
        zone="America/New_York" if local else "UTC",
        recorded_from=datetime(2026, 10, 1, tzinfo=UTC),
        recorded_until=datetime(2026, 10, 2, tzinfo=UTC),
        audit_id=audit_id,
        image_side=image_side,
    )


@pytest.mark.parametrize(
    "event_id,side,wall_time,expected",
    [
        ("consume", "before", "2026-10-01T08:00:00", "2026-10-01T12:00:00+00:00"),
        ("consume", "after", "2026-10-01T12:01:00", "2026-10-01T12:01:00+00:00"),
        ("complete", "before", "2026-10-01T12:01:00", "2026-10-01T12:01:00+00:00"),
        ("complete", "after", "2026-10-01T08:02:00", "2026-10-01T12:02:00+00:00"),
    ],
)
def test_event_and_image_provenance_separates_same_occurrence_clocks(
    event_id: str,
    side: Literal["before", "after"],
    wall_time: str,
    expected: str,
) -> None:
    policy = AuditTemporalPolicy(
        rules=(
            _rule("consume", "before", local=True),
            _rule("consume", "after"),
            _rule("complete", "before"),
            _rule("complete", "after", local=True),
        )
    )
    image = {"updated_at": wall_time}
    decoded = decode_audit_row(_event(event_id), image, policy, image_side=side)
    assert decoded["updated_at"] == datetime.fromisoformat(expected)
    assert image == {"updated_at": wall_time}


@pytest.mark.parametrize("side", ["before", "after"])
def test_unscoped_legacy_rule_still_applies_to_each_image(
    side: Literal["before", "after"],
) -> None:
    # Old persisted rules omit both selectors, rather than explicitly setting null.
    values = json.loads(TypeAdapter(LegacyTimeRule).dump_json(_rule(local=True)))
    values.pop("audit_id")
    values.pop("image_side")
    policy = parse_audit_temporal_policy([(2, json.dumps([values]))])
    assert decode_audit_row(
        _event(), {"updated_at": "2026-10-01T08:00:00"}, policy, image_side=side
    )["updated_at"] == datetime(2026, 10, 1, 12, tzinfo=UTC)


@pytest.mark.parametrize("mismatch", ["event", "side", "field", "occurrence"])
def test_scoped_rule_refuses_wrong_attribution(mismatch: str) -> None:
    event = _event("another-event" if mismatch == "event" else "consume")
    if mismatch == "occurrence":
        event = replace(event, occurred_at="2026-10-02T00:00:00Z")
    side = "after" if mismatch == "side" else "before"
    column = "issued_at" if mismatch == "field" else "updated_at"
    with pytest.raises(ValueError, match="exactly one provenance rule"):
        decode_audit_row(
            event,
            {column: "2026-10-01T08:00:00"},
            AuditTemporalPolicy(rules=(_rule("consume", "before", local=True),)),
            image_side=side,
        )


@pytest.mark.parametrize("overlap", ["scoped", "unscoped"])
def test_scoped_rules_never_override_overlapping_provenance(overlap: str) -> None:
    rule = _rule("consume", "before", local=True)
    competing = rule if overlap == "scoped" else _rule()
    with pytest.raises(ValueError, match="exactly one provenance rule"):
        decode_audit_row(
            _event(),
            {"updated_at": "2026-10-01T08:00:00"},
            AuditTemporalPolicy(rules=(rule, competing)),
            image_side="before",
        )


@pytest.mark.parametrize(
    "selectors",
    [
        {"audit_id": "consume"},
        {"image_side": "before"},
        {"audit_id": "", "image_side": "before"},
        {"audit_id": "   ", "image_side": "before"},
        {"audit_id": 123, "image_side": "before"},
        {"audit_id": "consume", "image_side": "other"},
        {"audit_id": "consume", "image_side": "before", "unknown": True},
    ],
)
def test_persisted_rule_refuses_invalid_image_selectors(
    selectors: dict[str, object],
) -> None:
    values = json.loads(TypeAdapter(LegacyTimeRule).dump_json(_rule()))
    values.update(selectors)
    with pytest.raises(ValueError):
        parse_audit_temporal_policy([(2, json.dumps([values]))])


def test_scoped_policy_json_round_trip_preserves_attribution() -> None:
    rules = (_rule("consume", "before", local=True), _rule("consume", "after"))
    encoded = TypeAdapter(tuple[LegacyTimeRule, ...]).dump_json(rules).decode()
    assert parse_audit_temporal_policy([(2, encoded)]).rules == rules


def test_mixed_preview_undo_redo_preserves_original_audit_json(db: Database) -> None:
    repo = ImportPreviewsRepo(db)
    preview_id = repo.issue(
        file_path="synthetic.csv",
        file_sha256="a" * 64,
        file_size_bytes=0,
        channel="tabular",
        source_bytes=b"",
        snapshot={},
        issued_at=datetime(2026, 10, 1, 12, tzinfo=UTC),
        expires_at=datetime(2026, 10, 2, tzinfo=UTC),
        actor="cli",
    )
    audit = AuditService(db)
    issued = audit.list_events(target_table="import_previews")[0].after_value
    assert issued is not None
    before_consume = issued | {"updated_at": "2026-10-01T08:00:00"}
    after_consume = issued | {
        "updated_at": "2026-10-01T12:01:00",
        "consumed_at": "2026-10-01T12:01:00Z",
    }
    after_complete = after_consume | {
        "updated_at": "2026-10-01T08:02:00",
        "import_id": "synthetic-import",
    }
    events = [
        audit.record_audit_event(
            action=action,
            target=("app", "import_previews", preview_id),
            before=before,
            after=after,
            actor="cli",
        )
        for action, before, after in [
            ("import_preview.consume", before_consume, after_consume),
            ("import_preview.complete", after_consume, after_complete),
        ]
    ]
    for event in events:
        db.execute(
            "UPDATE app.audit_log SET occurred_at = ? WHERE audit_id = ?",
            [datetime(2026, 10, 1, 12, 3, tzinfo=UTC), event.audit_id],
        )
    events = [replace(event, occurred_at="2026-10-01T12:03:00Z") for event in events]
    rules = (
        _rule(events[0].audit_id, "before", local=True),
        _rule(events[0].audit_id, "after"),
        _rule(events[1].audit_id, "before"),
        _rule(events[1].audit_id, "after", local=True),
    )
    db.execute(
        "UPDATE app.utc_upgrade_provenance SET rules = ?",
        [
            TypeAdapter(tuple[LegacyTimeRule, ...]).dump_json(rules).decode(),
        ],
    )
    db.execute(
        "UPDATE app.import_previews SET consumed_at = ?, updated_at = ?, import_id = ? "
        "WHERE preview_id = ?",
        [
            datetime(2026, 10, 1, 12, 1, tzinfo=UTC),
            datetime(2026, 10, 1, 12, 2, tzinfo=UTC),
            "synthetic-import",
            preview_id,
        ],
    )
    evidence_sql = (
        "SELECT audit_id, before_value::VARCHAR, after_value::VARCHAR "
        "FROM app.audit_log WHERE audit_id IN (?, ?) ORDER BY audit_id"
    )
    event_ids = [event.audit_id for event in events]
    original = db.execute(evidence_sql, event_ids).fetchall()
    undo_complete = repo.undo_event(events[1], actor="cli")
    undo_consume = repo.undo_event(events[0], actor="cli")
    assert undo_complete is not None and undo_consume is not None
    assert db.execute(
        "SELECT updated_at, consumed_at, import_id FROM app.import_previews"
    ).fetchone() == (datetime(2026, 10, 1, 12, tzinfo=UTC), None, None)
    repo.undo_event(undo_consume, actor="cli")
    repo.undo_event(undo_complete, actor="cli")
    assert db.execute(
        "SELECT updated_at, consumed_at, import_id FROM app.import_previews"
    ).fetchone() == (
        datetime(2026, 10, 1, 12, 2, tzinfo=UTC),
        datetime(2026, 10, 1, 12, 1, tzinfo=UTC),
        "synthetic-import",
    )
    assert db.execute(evidence_sql, event_ids).fetchall() == original


def _note_clock_event(db: Database, *, same_instant: bool = False) -> AuditEvent:
    before = {
        "note_id": "synthetic-note",
        "transaction_id": "synthetic-transaction",
        "text": "synthetic note",
        "author": "cli",
        "created_at": "2026-10-01T08:00:00",
    }
    after = before | {
        "created_at": "2026-10-01T04:00:00" if same_instant else "2026-10-01T08:00:00",
    }
    event = AuditService(db).record_audit_event(
        action="note.edit",
        target=("app", "transaction_notes", "synthetic-note"),
        before=before,
        after=after,
        actor="cli",
    )
    db.execute(
        "UPDATE app.audit_log SET occurred_at = ? WHERE audit_id = ?",
        [datetime(2026, 10, 1, 12, 3, tzinfo=UTC), event.audit_id],
    )
    db.execute(
        "INSERT INTO app.transaction_notes "
        "(note_id, transaction_id, text, author, created_at) VALUES (?, ?, ?, ?, ?)",
        [
            "synthetic-note",
            "synthetic-transaction",
            "synthetic note",
            "cli",
            datetime(2026, 10, 1, 8 if same_instant else 12, tzinfo=UTC),
        ],
    )
    rules = tuple(
        replace(rule, table="transaction_notes", column="created_at")
        for rule in (
            _rule(event.audit_id, "before"),
            _rule(event.audit_id, "after", local=True),
        )
    )
    db.execute(
        "UPDATE app.utc_upgrade_provenance SET rules = ?",
        [
            TypeAdapter(tuple[LegacyTimeRule, ...]).dump_json(rules).decode(),
        ],
    )
    return AuditService(db).events_for_operation(event.operation_id)[0]


@pytest.mark.parametrize("through_service", [False, True])
def test_equal_lexical_images_reverse_different_proven_instants(
    db: Database,
    through_service: bool,
) -> None:
    event = _note_clock_event(db)
    evidence = db.execute(
        "SELECT before_value, after_value FROM app.audit_log WHERE audit_id = ?",
        [event.audit_id],
    ).fetchone()
    if through_service:
        service = UndoService(db)
        assert service.get(event.operation_id).reversible_source_event_count == 1
        assert service.get(event.operation_id).can_undo
        assert service.history()[0].can_undo
        undo = service.undo(event.operation_id, actor="cli")
        assert undo.reversed_row_count == 1
    else:
        assert TransactionNotesRepo(db).undo_event(event, actor="cli") is not None
    assert db.execute("SELECT created_at FROM app.transaction_notes").fetchone() == (
        datetime(2026, 10, 1, 8, tzinfo=UTC),
    )
    assert (
        db.execute(
            "SELECT before_value, after_value FROM app.audit_log WHERE audit_id = ?",
            [event.audit_id],
        ).fetchone()
        == evidence
    )


def test_different_lexical_images_with_same_proven_instant_are_noop(
    db: Database,
) -> None:
    event = _note_clock_event(db, same_instant=True)
    audit_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    service = UndoService(db)
    assert not service.get(event.operation_id).can_undo
    assert service.get(event.operation_id).reversible_source_event_count == 0
    assert not service.history()[0].can_undo
    with pytest.raises(UserError):
        service.undo(event.operation_id, actor="cli")
    assert TransactionNotesRepo(db).undo_event(event, actor="cli") is None
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit_count


def test_uncertain_equal_images_refuse_replay_and_never_appear_undoable(
    db: Database,
) -> None:
    event = _note_clock_event(db)
    db.execute("UPDATE app.utc_upgrade_provenance SET rules = '[]'")
    service = UndoService(db)
    assert not service.get(event.operation_id).can_undo
    assert not service.history()[0].can_undo
    with pytest.raises(UserError):
        service.undo(event.operation_id, actor="cli")
    with pytest.raises(UserError):
        TransactionNotesRepo(db).undo_event(event, actor="cli")
    assert db.execute("SELECT created_at FROM app.transaction_notes").fetchone() == (
        datetime(2026, 10, 1, 12, tzinfo=UTC),
    )
