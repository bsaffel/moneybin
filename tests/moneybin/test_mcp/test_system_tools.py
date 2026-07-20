"""Tests for system_* MCP tools."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastmcp import FastMCP

from moneybin.errors import UserError
from moneybin.mcp.tools.system import (
    register_system_coarse_reads,
    register_system_tools,
    system_audit_coarse,
    system_status,
    system_status_coarse,
)
from tests.moneybin.test_mcp.schema_assertions import (
    assert_recovery_actions_executable,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.parametrize("section", ["overview", "doctor", "categorization"])
async def test_system_status_coarse_dispatches_each_section(section: str) -> None:
    response = await system_status_coarse(sections=[section])  # pyright: ignore[reportArgumentType]

    assert response.data.sections[0].kind == section


async def test_system_status_coarse_defaults_to_fixed_section_order() -> None:
    response = await system_status_coarse()

    assert [section.kind for section in response.data.sections] == [
        "overview",
        "doctor",
        "categorization",
    ]


async def test_system_status_coarse_rejects_explicit_empty_sections() -> None:
    response = await system_status_coarse(sections=[])

    assert response.error is not None
    assert response.error.code == "infra_invalid_input"


async def test_system_status_coarse_rejects_duplicate_sections() -> None:
    response = await system_status_coarse(sections=["overview", "overview"])

    assert response.error is not None
    assert response.error.code == "infra_invalid_input"


async def test_system_status_coarse_full_includes_auto_categorization() -> None:
    response = await system_status_coarse(
        sections=["categorization"],
        detail="full",
    )

    section = response.data.sections[0]
    assert section.kind == "categorization"
    assert hasattr(section.statistics, "auto")


async def test_system_status_auto_action_describes_persisted_rules() -> None:
    response = await system_status_coarse(
        sections=["categorization"],
        detail="full",
    )

    text = " ".join(response.actions).lower()
    assert "transactions_categorize_rules(view='history')" in text
    assert "proposal" not in text
    assert "inactive" not in text


async def test_system_audit_coarse_detail_requires_exactly_one_identifier() -> None:
    missing = await system_audit_coarse(view="detail")
    duplicate = await system_audit_coarse(
        view="detail",
        operation_id="op_demo",
        audit_id="audit_demo",
    )

    assert missing.error is not None
    assert missing.error.code == "AUDIT_IDENTIFIER_REQUIRED"
    assert duplicate.error is not None
    assert duplicate.error.code == "AUDIT_IDENTIFIER_REQUIRED"


@pytest.mark.parametrize("view", ["events", "history"])
async def test_system_audit_coarse_rejects_detail_identifier_for_other_views(
    view: str,
) -> None:
    response = await system_audit_coarse(
        view=view,  # pyright: ignore[reportArgumentType]
        operation_id="op_demo",
    )

    assert response.error is not None
    assert response.error.code == "AUDIT_IDENTIFIER_NOT_ALLOWED"


async def test_system_audit_coarse_dispatches_events_and_history() -> None:
    events = await system_audit_coarse(view="events")
    history = await system_audit_coarse(view="history")

    assert events.data.kind == "events"
    assert history.data.kind == "history"


async def test_system_audit_coarse_operation_detail(mcp_db: object) -> None:
    operation_id = _make_tag_op()

    response = await system_audit_coarse(
        view="detail",
        operation_id=operation_id,
    )

    assert response.data.kind == "detail"
    assert response.data.operation_id == operation_id
    assert response.data.audit_id is None
    assert response.data.events[0].operation_id == operation_id


async def test_system_audit_coarse_audit_detail(mcp_db: object) -> None:
    operation_id = _make_tag_op()
    events = await system_audit_coarse(view="events")
    audit_id = next(
        event.audit_id
        for event in events.data.events
        if event.operation_id == operation_id
    )

    response = await system_audit_coarse(
        view="detail",
        audit_id=audit_id,
    )

    assert response.data.kind == "detail"
    assert response.data.operation_id is None
    assert response.data.audit_id == audit_id
    assert response.data.events[0].audit_id == audit_id


async def test_system_audit_coarse_paginates_events(mcp_db: object) -> None:
    _make_tag_op("first")
    _make_tag_op("second")

    first = await system_audit_coarse(view="events", limit=1)
    second = await system_audit_coarse(
        view="events",
        limit=1,
        cursor=first.next_cursor,
    )

    assert first.summary.returned_count == 1
    assert first.summary.has_more is True
    assert first.next_cursor is not None
    assert second.data.events[0].audit_id != first.data.events[0].audit_id


async def test_system_audit_event_continuation_ignores_newer_insert() -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for audit_id, occurred_at in (
            ("audit-original-third", "2099-07-18 10:00:00"),
            ("audit-original-second", "2099-07-18 11:00:00"),
            ("audit-original-first", "2099-07-18 12:00:00"),
        ):
            db.execute(
                """
                INSERT INTO app.audit_log (
                    audit_id, occurred_at, actor, action, target_schema,
                    target_table, target_id, operation_id
                ) VALUES (?, ?, 'cli', 'tag.add', 'app',
                          'transaction_tags', ?, ?)
                """,
                [audit_id, occurred_at, audit_id, f"op-{audit_id}"],
            )

    first = await system_audit_coarse(view="events", limit=1)
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.audit_log (
                audit_id, occurred_at, actor, action, target_schema,
                target_table, target_id, operation_id
            ) VALUES ('audit-new-head', '2100-01-01 00:00:00', 'cli',
                      'tag.add', 'app', 'transaction_tags', 'new', 'op-new')
            """
        )
    second = await system_audit_coarse(
        view="events",
        limit=1,
        cursor=first.next_cursor,
    )

    assert [event.audit_id for event in first.data.events] == ["audit-original-first"]
    assert [event.audit_id for event in second.data.events] == ["audit-original-second"]
    assert first.summary.total_count == 3
    assert second.summary.total_count == 3


async def test_system_audit_coarse_paginates_tied_events_without_gaps(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    timestamp = "2099-07-18 12:00:00"
    expected = ["audit-c", "audit-b", "audit-a"]
    with get_database(read_only=False) as db:
        for audit_id in ("audit-a", "audit-c", "audit-b"):
            db.execute(
                """
                INSERT INTO app.audit_log (
                    audit_id, occurred_at, actor, action, target_schema,
                    target_table, target_id, operation_id
                ) VALUES (?, ?, 'cli', 'tag.add', 'app', 'transaction_tags', ?, ?)
                """,
                [audit_id, timestamp, audit_id, f"op-{audit_id}"],
            )

    observed: list[str] = []
    cursor: str | None = None
    for _ in expected:
        response = await system_audit_coarse(
            view="events",
            limit=1,
            cursor=cursor,
        )
        observed.append(response.data.events[0].audit_id)
        cursor = response.next_cursor

    assert observed == expected
    assert cursor is None


async def test_system_audit_coarse_paginates_history(mcp_db: object) -> None:
    expected = {_make_tag_op(tag) for tag in ("history-a", "history-b", "history-c")}
    observed: set[str] = set()
    cursor: str | None = None

    while True:
        response = await system_audit_coarse(
            view="history",
            limit=1,
            cursor=cursor,
        )
        observed.add(response.data.operations[0].operation_id)
        cursor = response.next_cursor
        if cursor is None:
            break

    assert observed == expected


async def test_system_audit_history_continuation_ignores_newer_operation() -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for operation_id, occurred_at in (
            ("op-original-third", "2099-07-18 10:00:00"),
            ("op-original-second", "2099-07-18 11:00:00"),
            ("op-original-first", "2099-07-18 12:00:00"),
        ):
            db.execute(
                """
                INSERT INTO app.audit_log (
                    audit_id, occurred_at, actor, action, target_schema,
                    target_table, target_id, operation_id
                ) VALUES (?, ?, 'cli', 'tag.add', 'app',
                          'transaction_tags', ?, ?)
                """,
                [
                    f"audit-{operation_id}",
                    occurred_at,
                    operation_id,
                    operation_id,
                ],
            )

    first = await system_audit_coarse(view="history", limit=1)
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.audit_log (
                audit_id, occurred_at, actor, action, target_schema,
                target_table, target_id, operation_id
            ) VALUES ('audit-new-operation', '2100-01-01 00:00:00', 'cli',
                      'tag.add', 'app', 'transaction_tags', 'new', 'op-new')
            """
        )
    second = await system_audit_coarse(
        view="history",
        limit=1,
        cursor=first.next_cursor,
    )

    assert [operation.operation_id for operation in first.data.operations] == [
        "op-original-first"
    ]
    assert [operation.operation_id for operation in second.data.operations] == [
        "op-original-second"
    ]
    assert first.summary.total_count == 3
    assert second.summary.total_count == 3


async def test_system_audit_coarse_rejects_malformed_and_cross_view_cursors(
    mcp_db: object,
) -> None:
    _make_tag_op("cursor-a")
    _make_tag_op("cursor-b")
    first = await system_audit_coarse(view="events", limit=1)

    malformed = await system_audit_coarse(view="events", cursor="not-base64")
    from moneybin.mcp.pagination import encode_keyset_cursor

    invalid_timestamp = await system_audit_coarse(
        view="events",
        cursor=encode_keyset_cursor(
            namespace="system_audit",
            scope={"view": "events"},
            snapshot=("not-a-timestamp", "audit-a"),
            after=("still-not-a-timestamp", "audit-b"),
            total=2,
        ),
    )
    cross_view = await system_audit_coarse(
        view="history",
        cursor=first.next_cursor,
    )

    assert malformed.error is not None
    assert malformed.error.code == "infra_invalid_input"
    assert invalid_timestamp.error is not None
    assert invalid_timestamp.error.code == "infra_invalid_input"
    assert cross_view.error is not None
    assert cross_view.error.code == "infra_invalid_input"


async def test_system_audit_coarse_returns_only_replacement_actions(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database
    from moneybin.services.audit_service import AuditService

    operation_id = _make_tag_op("actions-a")
    _make_tag_op("actions-b")
    with get_database(read_only=True) as db:
        audit_id = AuditService(db).events_for_operation(operation_id)[0].audit_id

    events = await system_audit_coarse(view="events", limit=1)
    history = await system_audit_coarse(view="history", limit=1)
    operation_detail = await system_audit_coarse(
        view="detail",
        operation_id=operation_id,
    )
    audit_detail = await system_audit_coarse(
        view="detail",
        audit_id=audit_id,
    )

    assert events.actions == [
        "Inspect an operation with system_audit(view='detail', operation_id=...)",
        "Continue with "
        f"system_audit(view='events', limit=1, cursor='{events.next_cursor}')",
    ]
    assert history.actions == [
        "Inspect an operation with system_audit(view='detail', operation_id=...)",
        "Reverse an operation with system_audit_undo(operation_id=...)",
        "Continue with "
        f"system_audit(view='history', limit=1, cursor='{history.next_cursor}')",
    ]
    assert operation_detail.actions == [
        f"Reverse with system_audit_undo(operation_id='{operation_id}')",
    ]
    assert audit_detail.actions == [
        "Use the event operation_id with system_audit(view='detail', "
        "operation_id=...) to inspect undoability.",
    ]


async def test_register_system_coarse_reads_registers_only_replacements() -> None:
    server = FastMCP("system-coarse")

    register_system_coarse_reads(server)
    names = {
        tool.name
        for tool in await server._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    }

    assert names == {"system_status", "system_audit"}


@pytest.mark.unit
async def test_system_status_returns_response_envelope(mcp_db: object) -> None:
    """system_status returns a valid ResponseEnvelope."""
    result = system_status()
    parsed = result.to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    # SystemStatusPayload is all AGGREGATE / TXN_TYPE / TIMESTAMP_OBSERVABILITY → Tier.LOW
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_system_status_data_keys(mcp_db: object) -> None:
    """system_status data dict has all required domain keys."""
    result = system_status()
    parsed = result.to_dict()
    data = parsed["data"]
    assert "accounts" in data
    assert "transactions" in data
    assert "matches" in data
    assert "account_links" in data
    assert "merchant_links" in data
    assert "security_links" in data
    assert "categorization" in data


@pytest.mark.unit
async def test_system_status_accounts_count(mcp_db: object) -> None:
    """Accounts count reflects the mcp_db fixture's 2 accounts."""
    result = system_status()
    parsed = result.to_dict()
    assert parsed["data"]["accounts"]["count"] == 2


@pytest.mark.unit
async def test_system_status_transactions_empty(mcp_db: object) -> None:
    """Transactions count is 0 when no transactions are inserted."""
    result = system_status()
    parsed = result.to_dict()
    txn = parsed["data"]["transactions"]
    assert txn["count"] == 0
    assert txn["date_range"] == [None, None]
    assert txn["last_import_at"] is None


@pytest.mark.unit
async def test_system_status_queue_counts_are_integers(mcp_db: object) -> None:
    """matches.pending_review and categorization.uncategorized are integers."""
    result = system_status()
    parsed = result.to_dict()
    assert isinstance(parsed["data"]["matches"]["pending_review"], int)
    assert isinstance(parsed["data"]["categorization"]["uncategorized"], int)


@pytest.mark.unit
async def test_system_status_actions_non_empty(mcp_db: object) -> None:
    """system_status provides at least one action hint."""
    result = system_status()
    parsed = result.to_dict()
    assert len(parsed["actions"]) >= 1


@pytest.mark.unit
async def test_register_system_tools() -> None:
    """register_system_tools registers system_status with a FastMCP server."""
    srv = FastMCP("test")
    register_system_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "system_status" in names


# ── system_doctor ─────────────────────────────────────────────────────────────


@pytest.mark.unit
async def test_system_doctor_returns_envelope(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_doctor

    result = system_doctor()
    parsed = result.to_dict()
    assert "summary" in parsed
    assert "data" in parsed


@pytest.mark.unit
async def test_system_doctor_data_has_required_keys(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_doctor

    result = system_doctor()
    data = result.to_dict()["data"]
    assert "passing" in data
    assert "failing" in data
    assert "warning" in data
    assert "transaction_count" in data
    assert "invariants" in data


@pytest.mark.unit
async def test_system_doctor_transaction_count_is_int(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_doctor

    result = system_doctor()
    data = result.to_dict()["data"]
    assert isinstance(data["transaction_count"], int)


@pytest.mark.unit
async def test_system_doctor_sensitivity_is_low(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_doctor

    result = system_doctor()
    assert result.to_dict()["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_system_doctor_invariants_carry_recovery_actions_field(
    mcp_db: object,
) -> None:
    """Every invariant payload carries a ``recovery_actions`` list (possibly empty).

    PR4: DoctorService populates this from the recipe registry. A passing
    invariant has an empty list; a failing one with a registered recipe gets
    pre-built actions an agent can execute.
    """
    from moneybin.mcp.tools.system import system_doctor

    result = system_doctor()
    invariants = result.to_dict()["data"]["invariants"]
    assert invariants, "expected at least one invariant in the payload"
    for inv in invariants:
        assert "recovery_actions" in inv
        assert isinstance(inv["recovery_actions"], list)


@pytest.mark.unit
async def test_system_doctor_orphan_state_emits_executable_actions(
    mcp_db: object,
) -> None:
    """Orphan app rows do not advertise retired or invalid cleanup tools."""
    from moneybin.database import get_database
    from moneybin.mcp.tools.system import system_doctor

    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO app.transaction_notes "  # noqa: S608  # test input
            "(note_id, transaction_id, text, author) "
            "VALUES ('orphn1', 'missing_txn_a', 'x', 'mcp')"
        )
        db.execute(
            "INSERT INTO app.transaction_tags "  # noqa: S608  # test input
            "(transaction_id, tag, applied_by) "
            "VALUES ('missing_txn_b', 'z', 'mcp')"
        )

    result = system_doctor()
    invariants = result.to_dict()["data"]["invariants"]
    orphan = next(i for i in invariants if i["name"] == "orphan_app_state")
    assert orphan["status"] == "fail"
    assert [action["tool"] for action in orphan["recovery_actions"]] == [
        "transactions_annotate",
        "transactions_annotate",
    ]


# ── system_audit_undo / _history / _get ─────────────────────────────────────


def _make_tag_op(tag: str = "trip") -> str:
    """Create one audited operation adding ``tag`` to txn_1; return its op id."""
    from moneybin.database import get_database
    from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
    from moneybin.services.mutation_context import operation

    with get_database(read_only=False) as db, operation() as op:
        TransactionTagsRepo(db).add(transaction_id="txn_1", tag=tag, actor="cli")
    return op


def _make_note_op() -> str:
    """Add note n1 to txn_1; return its op id (paired with _make_note_edit_op)."""
    from moneybin.database import get_database
    from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
    from moneybin.services.mutation_context import operation

    with get_database(read_only=False) as db, operation() as op:
        TransactionNotesRepo(db).add(
            transaction_id="txn_1", note_id="n1", text="hi", actor="cli"
        )
    return op


def _make_note_edit_op() -> str:
    """Edit note n1 — a second op on the SAME row, so it blocks the add's undo.

    Cascade is row-grain: target_id is the entity PK.
    """
    from moneybin.database import get_database
    from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
    from moneybin.services.mutation_context import operation

    with get_database(read_only=False) as db, operation() as op:
        TransactionNotesRepo(db).edit(note_id="n1", text="edited", actor="cli")
    return op


@pytest.mark.unit
async def test_audit_undo_reverses_operation(mcp_db: object) -> None:
    from moneybin.database import get_database
    from moneybin.mcp.tools.system import system_audit_undo

    op = _make_tag_op()
    result = await system_audit_undo(op)
    parsed = result.to_dict()
    assert parsed["status"] == "ok"
    assert parsed["data"]["undone_operation_id"] == op
    assert parsed["data"]["reversed_row_count"] == 1
    assert parsed["data"]["tables"] == ["transaction_tags"]
    with get_database(read_only=True) as db:
        remaining = db.execute("SELECT COUNT(*) FROM app.transaction_tags").fetchone()
    assert remaining == (0,)


@pytest.mark.unit
async def test_audit_undo_not_found_returns_error_envelope(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_undo

    result = await system_audit_undo("op_missing")
    parsed = result.to_dict()
    assert parsed["status"] == "error"
    assert parsed["error"]["code"] == "undo_operation_not_found"
    assert result.recovery_actions
    await assert_recovery_actions_executable(result.recovery_actions)
    assert [(action.tool, action.arguments) for action in result.recovery_actions] == [
        ("system_audit", {"view": "history"})
    ]


@pytest.mark.unit
async def test_audit_undo_cascade_blocked_lists_blocker(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_undo

    op1 = _make_note_op()  # add note n1
    op2 = _make_note_edit_op()  # edit the same row n1, later → blocks op1
    parsed = (await system_audit_undo(op1)).to_dict()
    assert parsed["status"] == "error"
    assert parsed["error"]["code"] == "undo_cascade_blocked"
    blockers = [a["arguments"]["operation_id"] for a in parsed["recovery_actions"]]
    assert blockers == [op2]


@pytest.mark.unit
async def test_audit_undo_sensitivity_low(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_undo

    op = _make_tag_op()
    parsed = (await system_audit_undo(op)).to_dict()
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_audit_history_lists_operations_newest_first(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_history

    op1 = _make_tag_op("a")
    op2 = _make_tag_op("b")
    parsed = (system_audit_history()).to_dict()
    ids = [o["operation_id"] for o in parsed["data"]["operations"]]
    assert ids[:2] == [op2, op1]
    assert parsed["data"]["operations"][0]["can_undo"] is True


@pytest.mark.unit
async def test_audit_get_returns_before_after(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_get

    op = _make_tag_op()
    parsed = (system_audit_get(op)).to_dict()
    assert parsed["data"]["operation_id"] == op
    assert len(parsed["data"]["events"]) == 1
    event = parsed["data"]["events"][0]
    assert event["action"] == "tag.add"
    assert event["after_value"]["tag"] == "trip"
    assert parsed["data"]["can_undo"] is True
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_audit_get_not_found_returns_error_envelope(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_get

    with pytest.raises(UserError, match="No operation found"):
        system_audit_get("op_missing")


@pytest.mark.unit
async def test_audit_get_event_carries_undo_fields(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_get, system_audit_undo

    op = _make_tag_op()
    undo = (await system_audit_undo(op)).to_dict()
    undo_op = undo["data"]["undo_operation_id"]
    parsed = (system_audit_get(undo_op)).to_dict()
    event = parsed["data"]["events"][0]
    # Symmetric with the history entry: structural undo signal, not a string-match
    # on the .undo action suffix.
    assert event["is_undo"] is True
    assert event["undoes_operation_id"] == op


@pytest.mark.unit
async def test_audit_history_entry_carries_recovery_actions(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_history

    op1 = _make_note_op()  # add note n1
    op2 = _make_note_edit_op()  # edit the same row n1 → blocks op1
    parsed = (system_audit_history()).to_dict()
    entry = next(o for o in parsed["data"]["operations"] if o["operation_id"] == op1)
    # Blocked op1's pre-built recovery_actions point the agent straight at undoing
    # op2 — the structured action the service computed, not just the raw id.
    actions = entry["recovery_actions"]
    assert actions
    assert actions[0]["tool"] == "system_audit_undo"
    assert actions[0]["arguments"]["operation_id"] == op2


@pytest.mark.unit
async def test_audit_get_hint_distinguishes_unresolvable(mcp_db: object) -> None:
    from moneybin.database import get_database
    from moneybin.mcp.tools.system import system_audit_get
    from moneybin.services.audit_service import AuditService
    from moneybin.services.mutation_context import operation

    with get_database(read_only=False) as db, operation() as op:
        AuditService(db).record_audit_event(
            action="manual.create",
            target=("raw", "manual_transactions", "imp_1"),
            before=None,
            after={"row_count": 2},
            actor="cli",
        )
    parsed = (system_audit_get(op)).to_dict()
    assert parsed["data"]["can_undo"] is False
    assert parsed["data"]["undo_blocked_by"] is None
    hint = " ".join(parsed["actions"]).lower()
    # The dead-end "see undo_blocked_by" (which is null here) must be gone.
    assert "undo_blocked_by" not in hint
    assert "outside" in hint or "re-apply" in hint


@pytest.mark.unit
async def test_audit_get_hint_for_marker_only(mcp_db: object) -> None:
    from moneybin.database import get_database
    from moneybin.mcp.tools.system import system_audit_get
    from moneybin.services.audit_service import AuditService
    from moneybin.services.mutation_context import operation

    with get_database(read_only=False) as db, operation() as op:
        AuditService(db).record_audit_event(
            action="tag.rename",
            target=("app", "transaction_tags", None),
            before={"old_tag": "ghost"},
            after={"new_tag": "x", "row_count": 0},
            actor="cli",
        )
    parsed = (system_audit_get(op)).to_dict()
    assert parsed["data"]["can_undo"] is False
    hint = " ".join(parsed["actions"]).lower()
    # Marker-only: not the raw-import "re-apply" message; says nothing to reverse.
    assert "re-apply" not in hint
    assert "reversible" in hint or "already reversed" in hint


@pytest.mark.unit
async def test_register_undo_tools() -> None:
    srv = FastMCP("test")
    register_system_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert names == {"system_status", "system_audit", "system_audit_undo"}


@pytest.mark.unit
async def test_system_status_degraded_when_db_locked(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A writer holding the lock yields a degraded snapshot, not an error.

    The DatabaseLockError recovery action points the agent at system_status to
    identify the lock holder, but system_status opens the DB read-only — which
    fails under contention. The connection view is collected before the DB open
    now, and a lock failure returns a degraded envelope still carrying
    database_connections instead of looping to another lock error.
    """
    import moneybin.database as db_module
    from moneybin.database import DatabaseLockError

    def locked_get_database(**_kwargs: object) -> object:
        raise DatabaseLockError("held by another process")

    def no_blockers(_db_path: Path) -> list[dict[str, Any]]:
        return []

    monkeypatch.setattr(db_module, "get_database", locked_get_database)
    monkeypatch.setattr(
        "moneybin.mcp.tools.system.find_blocking_processes", no_blockers
    )

    result = system_status()
    parsed = result.to_dict()
    assert parsed["summary"]["degraded"] is True
    assert "lock" in parsed["summary"]["degraded_reason"].lower()
    # database_connections is still present (read from the lock file + lsof, no
    # DB needed); the inventory fields are zero-filled under the degraded flag.
    assert "database_connections" in parsed["data"]
    assert parsed["data"]["accounts"]["count"] == 0
    assert parsed["data"]["transactions"]["count"] == 0


@pytest.mark.unit
async def test_system_status_recomputes_connections_after_lock_error(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The degraded envelope re-snapshots connections after the read fails.

    A writer can acquire the lock between the preflight connection snapshot and
    the read-only open failing under contention. system_status recomputes the
    connection block in the DatabaseLockError path, so the degraded envelope
    names the writer that actually caused the lock rather than the (empty)
    preflight view — which is exactly what the recovery action sends the agent
    to system_status to find.
    """
    import moneybin.database as db_module
    from moneybin.database import DatabaseLockError

    def locked_get_database(**_kwargs: object) -> object:
        raise DatabaseLockError("held by another process")

    # First block call (preflight) sees no writer; the second (except-path
    # recompute) sees the writer that appeared in between.
    blocks: list[dict[str, Any]] = [
        {"writers": [], "readers": []},
        {
            "writers": [
                {
                    "pid": 4242,
                    "command": "moneybin transform apply",
                    "started_at": "2026-06-10T00:00:00+00:00",
                    "operation_type": "transform_apply",
                }
            ],
            "readers": [],
        },
    ]
    block_results = iter(blocks)

    def next_block(_db_path: Path) -> dict[str, Any]:
        return next(block_results)

    monkeypatch.setattr(db_module, "get_database", locked_get_database)
    monkeypatch.setattr(
        "moneybin.mcp.tools.system._database_connections_block", next_block
    )

    result = system_status()
    parsed = result.to_dict()
    assert parsed["summary"]["degraded"] is True
    writers = parsed["data"]["database_connections"]["writers"]
    assert len(writers) == 1
    assert writers[0]["pid"] == 4242
    assert writers[0]["operation_type"] == "transform_apply"


@pytest.mark.unit
async def test_system_status_opens_with_short_max_wait(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """system_status (the lock-recovery tool) opens read-only with a short wait.

    Canary: it must degrade fast under contention rather than burn a third of
    the 30 s MCP dispatch budget retrying the read — the diagnostic data is
    collected from the lock file, not the read. Pins max_wait so a refactor
    can't regress it back to the 10 s default.
    """
    import moneybin.database as db_module
    from moneybin.database import DatabaseLockError

    captured: dict[str, Any] = {}

    def capturing_get_database(*, read_only: bool, **kwargs: object) -> object:
        captured["max_wait"] = kwargs.get("max_wait")
        raise DatabaseLockError("held by another process")

    def no_blockers(_db_path: Path) -> list[dict[str, Any]]:
        return []

    monkeypatch.setattr(db_module, "get_database", capturing_get_database)
    monkeypatch.setattr(
        "moneybin.mcp.tools.system.find_blocking_processes", no_blockers
    )

    system_status()
    assert captured["max_wait"] == 2.0
