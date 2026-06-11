"""Tests for system_* MCP tools."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.system import register_system_tools, system_status

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.unit
async def test_system_status_returns_response_envelope(mcp_db: object) -> None:
    """system_status returns a valid ResponseEnvelope."""
    result = await system_status()
    parsed = result.to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    # SystemStatusPayload is all AGGREGATE / TXN_TYPE / TIMESTAMP_OBSERVABILITY → Tier.LOW
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_system_status_data_keys(mcp_db: object) -> None:
    """system_status data dict has all required domain keys."""
    result = await system_status()
    parsed = result.to_dict()
    data = parsed["data"]
    assert "accounts" in data
    assert "transactions" in data
    assert "matches" in data
    assert "categorization" in data


@pytest.mark.unit
async def test_system_status_accounts_count(mcp_db: object) -> None:
    """Accounts count reflects the mcp_db fixture's 2 accounts."""
    result = await system_status()
    parsed = result.to_dict()
    assert parsed["data"]["accounts"]["count"] == 2


@pytest.mark.unit
async def test_system_status_transactions_empty(mcp_db: object) -> None:
    """Transactions count is 0 when no transactions are inserted."""
    result = await system_status()
    parsed = result.to_dict()
    txn = parsed["data"]["transactions"]
    assert txn["count"] == 0
    assert txn["date_range"] == [None, None]
    assert txn["last_import_at"] is None


@pytest.mark.unit
async def test_system_status_queue_counts_are_integers(mcp_db: object) -> None:
    """matches.pending_review and categorization.uncategorized are integers."""
    result = await system_status()
    parsed = result.to_dict()
    assert isinstance(parsed["data"]["matches"]["pending_review"], int)
    assert isinstance(parsed["data"]["categorization"]["uncategorized"], int)


@pytest.mark.unit
async def test_system_status_actions_non_empty(mcp_db: object) -> None:
    """system_status provides at least one action hint."""
    result = await system_status()
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

    result = await system_doctor()
    parsed = result.to_dict()
    assert "summary" in parsed
    assert "data" in parsed


@pytest.mark.unit
async def test_system_doctor_data_has_required_keys(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_doctor

    result = await system_doctor()
    data = result.to_dict()["data"]
    assert "passing" in data
    assert "failing" in data
    assert "warning" in data
    assert "transaction_count" in data
    assert "invariants" in data


@pytest.mark.unit
async def test_system_doctor_transaction_count_is_int(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_doctor

    result = await system_doctor()
    data = result.to_dict()["data"]
    assert isinstance(data["transaction_count"], int)


@pytest.mark.unit
async def test_system_doctor_sensitivity_is_low(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_doctor

    result = await system_doctor()
    # SystemDoctorPayload has DESCRIPTION fields → Tier.MEDIUM derived sensitivity
    assert result.to_dict()["summary"]["sensitivity"] == "medium"


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

    result = await system_doctor()
    invariants = result.to_dict()["data"]["invariants"]
    assert invariants, "expected at least one invariant in the payload"
    for inv in invariants:
        assert "recovery_actions" in inv
        assert isinstance(inv["recovery_actions"], list)


@pytest.mark.unit
async def test_system_doctor_orphan_state_emits_executable_actions(
    mcp_db: object,
) -> None:
    """Seed orphan note + tag → system_doctor emits round-trip-executable actions.

    The returned invariant carries certain recovery actions whose tool names
    and arguments validate against the registered MCP tool schemas.
    """
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

    result = await system_doctor()
    invariants = result.to_dict()["data"]["invariants"]
    orphan = next(i for i in invariants if i["name"] == "orphan_app_state")
    assert orphan["status"] == "fail"
    tools = sorted(a["tool"] for a in orphan["recovery_actions"])
    assert tools == ["transactions_notes_delete", "transactions_tags_set"]
    # Spot-check confidence + idempotent flags ride through the envelope.
    # Confidence varies by tool: tags-clear is certain (idempotent), single-id
    # notes-delete is suggested (non-idempotent across a batch) — see
    # orphan_app_state recipe docstring.
    expected_confidence = {
        "transactions_notes_delete": "suggested",
        "transactions_tags_set": "certain",
    }
    for action in orphan["recovery_actions"]:
        assert action["confidence"] == expected_confidence[action["tool"]]
        assert "idempotent" in action


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

    parsed = (await system_audit_undo("op_missing")).to_dict()
    assert parsed["status"] == "error"
    assert parsed["error"]["code"] == "undo_operation_not_found"
    assert parsed["recovery_actions"]  # history hint present


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
    parsed = (await system_audit_history()).to_dict()
    ids = [o["operation_id"] for o in parsed["data"]["operations"]]
    assert ids[:2] == [op2, op1]
    assert parsed["data"]["operations"][0]["can_undo"] is True


@pytest.mark.unit
async def test_audit_get_returns_before_after(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_get

    op = _make_tag_op()
    parsed = (await system_audit_get(op)).to_dict()
    assert parsed["data"]["operation_id"] == op
    assert len(parsed["data"]["events"]) == 1
    event = parsed["data"]["events"][0]
    assert event["action"] == "tag.add"
    assert event["after_value"]["tag"] == "trip"
    assert parsed["data"]["can_undo"] is True
    # before/after carry TXN_AMOUNT → high sensitivity (matches system_audit)
    assert parsed["summary"]["sensitivity"] == "high"


@pytest.mark.unit
async def test_audit_get_not_found_returns_error_envelope(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_get

    parsed = (await system_audit_get("op_missing")).to_dict()
    assert parsed["status"] == "error"
    assert parsed["error"]["code"] == "undo_operation_not_found"


@pytest.mark.unit
async def test_audit_get_event_carries_undo_fields(mcp_db: object) -> None:
    from moneybin.mcp.tools.system import system_audit_get, system_audit_undo

    op = _make_tag_op()
    undo = (await system_audit_undo(op)).to_dict()
    undo_op = undo["data"]["undo_operation_id"]
    parsed = (await system_audit_get(undo_op)).to_dict()
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
    parsed = (await system_audit_history()).to_dict()
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
    parsed = (await system_audit_get(op)).to_dict()
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
    parsed = (await system_audit_get(op)).to_dict()
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
    assert {"system_audit_undo", "system_audit_history", "system_audit_get"} <= names


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

    result = await system_status()
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

    result = await system_status()
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

    await system_status()
    assert captured["max_wait"] == 2.0
