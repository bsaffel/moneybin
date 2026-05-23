"""Tests for system_* MCP tools."""

from __future__ import annotations

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


# ── system_audit_undo / _history / _get ─────────────────────────────────────


def _make_tag_op(tag: str = "trip") -> str:
    """Create one audited operation adding ``tag`` to txn_1; return its op id."""
    from moneybin.database import get_database
    from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
    from moneybin.services.mutation_context import operation

    with get_database() as db, operation() as op:
        TransactionTagsRepo(db).add(transaction_id="txn_1", tag=tag, actor="cli")
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

    op1 = _make_tag_op("a")
    op2 = _make_tag_op("b")  # same target, later → blocks op1
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
async def test_register_undo_tools() -> None:
    srv = FastMCP("test")
    register_system_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert {"system_audit_undo", "system_audit_history", "system_audit_get"} <= names
