"""Tests for transactions_* MCP tools."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.transactions import (
    register_transaction_coarse_writes,
    register_transactions_tools,
    transactions_annotate_coarse,
    transactions_matches_pending,
    transactions_matches_run,
    transactions_review,
)
from moneybin.mcp.write_contracts import (
    NoteSet,
    SplitsSet,
    SplitTarget,
    TagRename,
    TagsSet,
)
from moneybin.services.transaction_service import TransactionService

pytestmark = pytest.mark.usefixtures("mcp_db")


def _seed_annotation_transactions() -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description,
                transaction_type, is_pending, currency_code, source_type,
                source_extracted_at, loaded_at,
                transaction_year, transaction_month, transaction_day,
                transaction_day_of_week, transaction_year_month,
                transaction_year_quarter
            ) VALUES
            ('TX_1', 'ACC001', '2026-07-01', -20.00, 20.00, 'expense',
             'Restaurant', 'DEBIT', false, 'USD', 'ofx',
             '2026-07-01', CURRENT_TIMESTAMP,
             2026, 7, 1, 3, '2026-07', '2026-Q3'),
            ('TX_2', 'ACC001', '2026-07-02', -30.00, 30.00, 'expense',
             'Grocer', 'DEBIT', false, 'USD', 'ofx',
             '2026-07-02', CURRENT_TIMESTAMP,
             2026, 7, 2, 4, '2026-07', '2026-Q3')
            """  # noqa: S608  # test input, not executing SQL
        )
        db.execute(
            """
            INSERT INTO app.transaction_tags (transaction_id, tag, applied_by)
            VALUES ('TX_RENAME', 'food', 'test')
            """  # noqa: S608  # test input, not executing SQL
        )


@pytest.mark.unit
async def test_annotation_batch_applies_all_variants(mcp_db: object) -> None:
    _seed_annotation_transactions()

    requests = [
        NoteSet(kind="note_set", transaction_id="TX_1", note="trip"),
        TagsSet(kind="tags_set", transaction_id="TX_1", tags=["travel"]),
        SplitsSet(
            kind="splits_set",
            transaction_id="TX_2",
            splits=[
                SplitTarget(amount=Decimal("-20"), category=None),
                SplitTarget(amount=Decimal("-10"), category=None),
            ],
        ),
        TagRename(kind="tag_rename", old_name="food", new_name="dining"),
    ]
    required = await transactions_annotate_coarse(requests=requests)
    assert required.error is not None
    assert required.error.code == "mutation_confirmation_required"
    token = required.error.details["confirmation_token"]

    response = await transactions_annotate_coarse(
        requests=requests,
        confirmation_token=str(token),
    )

    assert response.data.applied_count == 4
    assert response.data.operation_id
    assert [outcome.kind for outcome in response.data.outcomes] == [
        "note_set",
        "tags_set",
        "splits_set",
        "tag_rename",
    ]
    assert all(outcome.changed for outcome in response.data.outcomes)
    assert all(
        outcome.operation_id == response.data.operation_id
        for outcome in response.data.outcomes
    )

    with get_database(read_only=True) as db:
        service = TransactionService(db)
        assert [note.text for note in service.list_notes("TX_1")] == ["trip"]
        assert service.list_tags("TX_1") == ["travel"]
        assert [split.amount for split in service.list_splits("TX_2")] == [
            Decimal("-20"),
            Decimal("-10"),
        ]
        assert service.list_tags("TX_RENAME") == ["dining"]
        events = service._audit.events_for_operation(  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # audit verification
            response.data.operation_id
        )
        rename_parent = next(event for event in events if event.action == "tag.rename")
        rename_children = [
            event for event in events if event.action == "tag.rename_row"
        ]
        assert rename_children
        assert all(
            event.parent_audit_id == rename_parent.audit_id for event in rename_children
        )


@pytest.mark.unit
async def test_annotation_batch_non_destructive_change_needs_no_confirmation(
    mcp_db: object,
) -> None:
    _seed_annotation_transactions()

    response = await transactions_annotate_coarse(
        requests=[TagsSet(kind="tags_set", transaction_id="TX_1", tags=["travel"])]
    )

    assert response.error is None
    assert response.data.applied_count == 1


@pytest.mark.unit
async def test_annotation_batch_retry_is_nothing_to_do(mcp_db: object) -> None:
    _seed_annotation_transactions()
    request = [TagRename(kind="tag_rename", old_name="food", new_name="dining")]
    required = await transactions_annotate_coarse(requests=request)
    token = required.error.details["confirmation_token"]
    applied = await transactions_annotate_coarse(
        requests=request,
        confirmation_token=str(token),
    )
    assert applied.error is None

    retry = await transactions_annotate_coarse(requests=request)

    assert retry.error is not None
    assert retry.error.code == "mutation_nothing_to_do"


@pytest.mark.unit
async def test_annotation_confirmation_binds_payload_and_resolved_targets(
    mcp_db: object,
) -> None:
    _seed_annotation_transactions()
    request = [TagRename(kind="tag_rename", old_name="food", new_name="dining")]
    required = await transactions_annotate_coarse(requests=request)
    token = str(required.error.details["confirmation_token"])

    mismatched_payload = await transactions_annotate_coarse(
        requests=[TagRename(kind="tag_rename", old_name="food", new_name="travel")],
        confirmation_token=token,
    )

    assert mismatched_payload.error is not None
    assert mismatched_payload.error.code == "mutation_confirmation_mismatch"
    with get_database(read_only=True) as db:
        assert TransactionService(db).list_tags("TX_RENAME") == ["food"]


@pytest.mark.unit
async def test_annotation_confirmation_rechecks_live_entity_resolution(
    mcp_db: object,
) -> None:
    _seed_annotation_transactions()
    request = [TagRename(kind="tag_rename", old_name="food", new_name="dining")]
    required = await transactions_annotate_coarse(requests=request)
    token = str(required.error.details["confirmation_token"])
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.transaction_tags (transaction_id, tag, applied_by)
            VALUES ('TX_NEW', 'food', 'test')
            """
        )

    mismatched_targets = await transactions_annotate_coarse(
        requests=request,
        confirmation_token=token,
    )

    assert mismatched_targets.error is not None
    assert mismatched_targets.error.code == "mutation_confirmation_mismatch"
    with get_database(read_only=True) as db:
        service = TransactionService(db)
        assert service.list_tags("TX_RENAME") == ["food"]
        assert service.list_tags("TX_NEW") == ["food"]


@pytest.mark.unit
async def test_annotation_batch_rolls_back_when_last_request_is_invalid(
    mcp_db: object,
) -> None:
    _seed_annotation_transactions()

    response = await transactions_annotate_coarse(
        requests=[
            NoteSet(kind="note_set", transaction_id="TX_1", note="trip"),
            NoteSet(kind="note_set", transaction_id="UNKNOWN", note="bad"),
        ]
    )

    assert response.error is not None
    with get_database(read_only=True) as db:
        assert TransactionService(db).list_notes("TX_1") == []


@pytest.mark.unit
async def test_annotation_coarse_registrar_exposes_only_batch_tool() -> None:
    server = FastMCP("test")
    register_transaction_coarse_writes(server)

    names = {
        tool.name
        for tool in await server._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # test server inventory
    }
    assert names == {"transactions_annotate"}


@pytest.mark.unit
async def test_review_status_returns_envelope(mcp_db: object) -> None:
    """transactions_review returns a valid ResponseEnvelope."""
    parsed = (await transactions_review()).to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_review_status_data_shape(mcp_db: object) -> None:
    """Data dict carries the five queue counts and a total equal to their sum."""
    data = (await transactions_review()).to_dict()["data"]
    assert "matches_pending" in data
    assert "categorize_pending" in data
    assert "account_links_pending" in data
    assert "merchant_links_pending" in data
    assert "security_links_pending" in data
    assert "total" in data
    assert isinstance(data["matches_pending"], int)
    assert isinstance(data["categorize_pending"], int)
    assert isinstance(data["account_links_pending"], int)
    assert isinstance(data["merchant_links_pending"], int)
    assert isinstance(data["security_links_pending"], int)
    assert data["total"] == (
        data["matches_pending"]
        + data["categorize_pending"]
        + data["account_links_pending"]
        + data["merchant_links_pending"]
        + data["security_links_pending"]
    )


@pytest.mark.unit
async def test_review_status_actions_non_empty(mcp_db: object) -> None:
    """Tool provides next-step action hints."""
    parsed = (await transactions_review()).to_dict()
    assert len(parsed["actions"]) >= 1


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_threads_mcp_actor(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """transactions_matches_run audits its writes as actor="mcp", not "system"."""
    from moneybin.matching.engine import MatchResult

    mock_run.return_value = MatchResult(auto_merged=2, pending_review=1)

    await transactions_matches_run()

    mock_run.assert_called_once_with(actor="mcp")


@pytest.mark.unit
async def test_register_includes_review_status() -> None:
    """register_transactions_tools registers transactions_review."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "transactions_review" in names
    assert "transactions_recurring_list" not in names


@pytest.mark.unit
async def test_matches_pending_component_key_present(mcp_db: object) -> None:
    """Each pending dedup row carries a component_key field."""
    # Seed two edges forming a 3-copy cluster and one unrelated edge
    import json
    from datetime import UTC, datetime

    with get_database(read_only=False) as db:
        for match_id, stid_a, stype_a, stid_b, stype_b, acct in [
            ("mc_ab", "t1", "csv", "t2", "ofx", "ACC001"),
            ("mc_bc", "t2", "ofx", "t3", "tiller", "ACC001"),
            ("mc_zz", "x1", "csv", "x2", "ofx", "ACC002"),
        ]:
            db.execute(
                """
                INSERT INTO app.match_decisions (
                    match_id, source_transaction_id_a, source_type_a,
                    source_origin_a, source_transaction_id_b, source_type_b,
                    source_origin_b, account_id, confidence_score, match_signals,
                    match_type, match_tier, account_id_b, match_status,
                    match_reason, decided_by, decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,  # noqa: S608  # test input, not executing SQL
                [
                    match_id,
                    stid_a,
                    stype_a,
                    "origin_a",
                    stid_b,
                    stype_b,
                    "origin_b",
                    acct,
                    0.9,
                    json.dumps({}),
                    "dedup",
                    "3",
                    None,
                    "pending",
                    None,
                    "matcher",
                    datetime.now(tz=UTC).isoformat(),
                ],
            )

    result = (await transactions_matches_pending(match_type="dedup")).to_dict()
    matches = result["data"]["matches"]
    keys = {m["match_id"]: m["component_key"] for m in matches}

    # All rows carry component_key
    assert all("component_key" in m for m in matches)
    # Same cluster shares one key
    assert keys["mc_ab"] == keys["mc_bc"]
    # Different account is its own cluster
    assert keys["mc_zz"] != keys["mc_ab"]


@pytest.mark.unit
async def test_matches_pending_reports_dedup_group_count(mcp_db: object) -> None:
    """The payload carries the distinct-dedup-component count (not an action string)."""
    result = (await transactions_matches_pending()).to_dict()
    # Empty queue → zero groups; the field is structured payload data.
    assert result["data"]["n_dedup_groups"] == 0


@pytest.mark.unit
async def test_matches_pending_dedup_group_count_zero_for_transfer_scope(
    mcp_db: object,
) -> None:
    """n_dedup_groups must honour the match_type filter, not the full queue.

    A transfer-scoped call returns transfer rows; reporting the whole dedup
    queue's group count alongside them would be a self-contradictory payload.
    """
    import json
    from datetime import UTC, datetime

    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.match_decisions (
                match_id, source_transaction_id_a, source_type_a,
                source_origin_a, source_transaction_id_b, source_type_b,
                source_origin_b, account_id, confidence_score, match_signals,
                match_type, match_tier, account_id_b, match_status,
                match_reason, decided_by, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,  # noqa: S608  # test input, not executing SQL
            [
                "td_ab",
                "t1",
                "csv",
                "origin_a",
                "t2",
                "ofx",
                "origin_b",
                "ACC001",
                0.9,
                json.dumps({}),
                "dedup",
                "3",
                None,
                "pending",
                None,
                "matcher",
                datetime.now(tz=UTC).isoformat(),
            ],
        )

    # Dedup scope sees the one pending component...
    dedup = (await transactions_matches_pending(match_type="dedup")).to_dict()
    assert dedup["data"]["n_dedup_groups"] == 1
    # ...transfer scope sees none (no dedup rows in scope).
    transfer = (await transactions_matches_pending(match_type="transfer")).to_dict()
    assert transfer["data"]["n_dedup_groups"] == 0
