"""Tests for transactions_* MCP tools."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.database import Database, get_database
from moneybin.mcp.tools.transactions import (
    register_transaction_coarse_writes,
    register_transactions_tools,
    transactions_annotate_coarse,
    transactions_matches_pending,
    transactions_matches_run,
    transactions_review,
)
from moneybin.protocol.write_contracts import (
    NoteAdd,
    NoteDelete,
    NoteEdit,
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
        NoteAdd(kind="note_add", transaction_id="TX_1", text="trip"),
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
        "note_add",
        "tags_set",
        "splits_set",
        "tag_rename",
    ]
    assert all(outcome.changed for outcome in response.data.outcomes)
    assert all(
        outcome.operation_id == response.data.operation_id
        for outcome in response.data.outcomes
    )
    created_note_id = response.data.outcomes[0].target_ids[0]

    with get_database(read_only=True) as db:
        service = TransactionService(db)
        assert [note.text for note in service.list_notes("TX_1")] == ["trip"]
        assert service.list_notes("TX_1")[0].note_id == created_note_id
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
async def test_note_add_and_edit_need_no_confirmation(mcp_db: object) -> None:
    _seed_annotation_transactions()

    added = await transactions_annotate_coarse(
        requests=[NoteAdd(kind="note_add", transaction_id="TX_1", text="first")]
    )
    assert added.error is None
    note_id = added.data.outcomes[0].target_ids[0]

    edited = await transactions_annotate_coarse(
        requests=[NoteEdit(kind="note_edit", note_id=note_id, text="edited")]
    )

    assert edited.error is None
    with get_database(read_only=True) as db:
        assert TransactionService(db).list_notes("TX_1")[0].text == "edited"


@pytest.mark.unit
async def test_note_delete_requires_confirmation(mcp_db: object) -> None:
    _seed_annotation_transactions()
    added = await transactions_annotate_coarse(
        requests=[NoteAdd(kind="note_add", transaction_id="TX_1", text="delete me")]
    )
    note_id = added.data.outcomes[0].target_ids[0]

    required = await transactions_annotate_coarse(
        requests=[NoteDelete(kind="note_delete", note_id=note_id)]
    )

    assert required.error is not None
    assert required.error.code == "mutation_confirmation_required"
    token = str(required.error.details["confirmation_token"])
    deleted = await transactions_annotate_coarse(
        requests=[NoteDelete(kind="note_delete", note_id=note_id)],
        confirmation_token=token,
    )
    assert deleted.error is None
    assert deleted.data.outcomes[0].target_ids == [note_id]


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
async def test_annotation_confirmation_rejects_state_added_before_live_preflight(
    mcp_db: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale token cannot delete state added immediately before live preflight."""
    _seed_annotation_transactions()
    with get_database(read_only=False) as db:
        note = TransactionService(db).add_note("TX_1", "approved-old", actor="test")
    request = [NoteDelete(kind="note_delete", note_id=note.note_id)]
    required = await transactions_annotate_coarse(requests=request)
    token = str(required.error.details["confirmation_token"])
    original_begin = Database.begin

    def begin_after_concurrent_write(db: Database) -> None:
        db.execute(
            """
            UPDATE app.transaction_notes
               SET text = ?
             WHERE note_id = ?
            """,
            ["concurrent edit", note.note_id],
        )
        original_begin(db)

    monkeypatch.setattr(Database, "begin", begin_after_concurrent_write)

    stale = await transactions_annotate_coarse(
        requests=request,
        confirmation_token=token,
    )

    assert stale.error is not None
    assert stale.error.code == "mutation_confirmation_mismatch"
    with get_database(read_only=True) as db:
        assert [note.text for note in TransactionService(db).list_notes("TX_1")] == [
            "concurrent edit"
        ]


@pytest.mark.unit
async def test_annotation_batch_rolls_back_when_last_request_is_invalid(
    mcp_db: object,
) -> None:
    _seed_annotation_transactions()

    response = await transactions_annotate_coarse(
        requests=[
            NoteAdd(kind="note_add", transaction_id="TX_1", text="trip"),
            NoteAdd(kind="note_add", transaction_id="UNKNOWN", text="bad"),
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
    parsed = (transactions_review()).to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_review_status_data_shape(mcp_db: object) -> None:
    """Data dict carries the five queue counts and a total equal to their sum."""
    data = (transactions_review()).to_dict()["data"]
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
    parsed = (transactions_review()).to_dict()
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

    transactions_matches_run()

    mock_run.assert_called_once_with(actor="mcp")


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_discloses_transfers_it_retired(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """The MCP twin of the CLI run gap: the agent gets the count and a way back.

    ``transactions_matches_set`` already reports this; the run tool reaches the
    same reconciliation through ``MatchingService.run`` and owed the same
    disclosure. Without it an agent reads ``auto_merged=0`` as "nothing
    happened" while accepted transfers were reversed underneath it.
    """
    from moneybin.matching.engine import MatchResult

    mock_run.return_value = MatchResult(transfers_retired=2)

    parsed = transactions_matches_run().to_dict()

    assert parsed["data"]["transfers_retired"] == 2
    assert any("system_audit_undo" in a for a in parsed["actions"]), (
        f"no action points at the recovery route: {parsed['actions']}"
    )


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_omits_the_retirement_action_when_none_retired(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """Negative twin: the urgent action appears only when it is true."""
    from moneybin.matching.engine import MatchResult

    mock_run.return_value = MatchResult(auto_merged=2, transfers_retired=0)

    parsed = transactions_matches_run().to_dict()

    assert parsed["data"]["transfers_retired"] == 0
    assert not any("system_audit_undo" in a for a in parsed["actions"])


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_discloses_a_retirement_that_outlived_a_crash(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """A crashed run still owes the agent the reversals it already committed.

    ``MatchRunError`` carries the count precisely because the reconciliation
    commits before the tiers that can fail. ``refresh()`` was its only reader,
    so this tool raised a bare error and the one record of an undone user
    decision died with it — the outcome an agent is least able to notice.
    """
    from moneybin.errors import UserError
    from moneybin.matching.engine import MatchResult, MatchRunError

    mock_run.side_effect = MatchRunError(
        RuntimeError("transfer tier failed"),
        partial=MatchResult(transfers_retired=2),
    )

    with pytest.raises(UserError) as excinfo:
        transactions_matches_run()

    assert "2" in str(excinfo.value), "the failure hid how many transfers it reversed"
    tools = {a.tool for a in excinfo.value.recovery_actions or []}
    assert "system_audit" in tools, f"no recovery action reaches the audit: {tools}"


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_discloses_merges_that_outlived_a_crash(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """A crash before any retirement still leaves committed merges behind.

    Keying the disclosure on ``transfers_retired`` alone re-raises this run bare:
    a tier persisted four merges — which suppress the duplicate side of four
    transactions — and died before the reconciliation could reverse anything. An
    agent that sees only a crash has no way to learn the ledger moved.
    """
    from moneybin.errors import UserError
    from moneybin.matching.engine import MatchResult, MatchRunError

    mock_run.side_effect = MatchRunError(
        RuntimeError("tier 3 failed"),
        partial=MatchResult(auto_merged=4, transfers_retired=0),
    )

    with pytest.raises(UserError) as excinfo:
        transactions_matches_run()

    assert "4" in str(excinfo.value), "the failure hid the merges it had committed"


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_keeps_the_raw_cause_out_of_the_mcp_error(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """The committed-count summary may cross to the model; the cause may not.

    Everything downstream of the tiers is DuckDB and repository work, so the
    wrapped cause carries binder text, file paths, and column or row values
    verbatim. `.claude/rules/security.md` puts those on the wrong side of an MCP
    boundary: log the detail, return a generic message. The counts stay because
    they are counts.
    """
    from moneybin.errors import UserError
    from moneybin.matching.engine import MatchResult, MatchRunError

    mock_run.side_effect = MatchRunError(
        RuntimeError(
            'Binder Error: no column "acct_9876543210" in /Users/someone/db.duckdb'
        ),
        partial=MatchResult(auto_merged=4),
    )

    with pytest.raises(UserError) as excinfo:
        transactions_matches_run()

    message = str(excinfo.value)
    assert "4" in message, "the generic message dropped the committed count"
    assert "acct_9876543210" not in message
    assert "/Users/someone/db.duckdb" not in message
    assert "Binder Error" not in message


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_lets_a_crash_that_committed_nothing_through_bare(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """Negative twin: no committed work, no disclosure to make.

    Translating every wrapped failure into a partial-progress ``UserError`` would
    tell an agent to go looking for decisions that were never written. The
    ordinary crash presentation is the correct one there.
    """
    from moneybin.errors import UserError
    from moneybin.matching.engine import MatchResult, MatchRunError

    mock_run.side_effect = MatchRunError(
        RuntimeError("tier 3 failed"), partial=MatchResult()
    )

    with pytest.raises(MatchRunError):
        transactions_matches_run()
    with pytest.raises(BaseException) as excinfo:  # noqa: B017, PT011  # identity check
        transactions_matches_run()
    assert not isinstance(excinfo.value, UserError)


@pytest.mark.unit
async def test_standard_registrar_has_no_review_aliases() -> None:
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert names == {"transactions", "transactions_annotate"}


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

    result = (transactions_matches_pending(match_type="dedup")).to_dict()
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
    result = (transactions_matches_pending()).to_dict()
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
    dedup = (transactions_matches_pending(match_type="dedup")).to_dict()
    assert dedup["data"]["n_dedup_groups"] == 1
    # ...transfer scope sees none (no dedup rows in scope).
    transfer = (transactions_matches_pending(match_type="transfer")).to_dict()
    assert transfer["data"]["n_dedup_groups"] == 0


def _set_outcome(*, match_status: str, transfers_retired: int) -> object:
    from moneybin.services.matching_service import MatchDecisionOutcome

    return MatchDecisionOutcome(
        match_status=match_status, transfers_retired=transfers_retired
    )


def test_matches_set_reports_the_status_that_committed() -> None:
    """An agent's accept can be refused by the reconciliation it triggers.

    ``set_status`` writes the decision, then reverses whichever accepted transfer
    loses the earliest-decided-first tiebreak — possibly this very row. Echoing
    the requested status back contradicts ``MatchSetPayload``'s own docstring and
    leaves the agent no way to detect that its write did not stand.
    """
    from moneybin.mcp.tools.transactions import transactions_matches_set

    with patch("moneybin.mcp.tools.transactions.MatchingService") as service:
        service.return_value.set_status.return_value = _set_outcome(
            match_status="reversed", transfers_retired=1
        )
        envelope = transactions_matches_set("tx_stale00001", "accepted")

    payload = envelope.to_dict()["data"]
    assert payload["match_status"] == "reversed"
    assert payload["transfers_retired"] == 1


def test_matches_set_points_a_retiring_accept_at_the_operation_undo() -> None:
    """``matches undo`` reverses this row only, never the transfer it retired.

    An agent driving off ``actions[]`` gets one route back; if that route cannot
    restore the *other* transfer this call reversed, the user's transfer stays
    missing.
    """
    from moneybin.mcp.tools.transactions import transactions_matches_set

    with patch("moneybin.mcp.tools.transactions.MatchingService") as service:
        service.return_value.set_status.return_value = _set_outcome(
            match_status="accepted", transfers_retired=2
        )
        envelope = transactions_matches_set("dd_100000001", "accepted")

    actions = envelope.to_dict()["actions"]
    assert any("system_audit_undo" in action for action in actions)


def test_matches_set_points_a_retiring_accept_at_the_rematch_it_owes() -> None:
    """The reversal freed two legs, and this call runs no Tier 4 over them.

    ``set_status`` reconciles inside its own transaction and returns; only a
    matcher pass can propose the transfer those freed legs may now form. Without
    this hint an agent has no way to know the run is unfinished, and the
    replacement waits for an unrelated refresh.
    """
    from moneybin.mcp.tools.transactions import transactions_matches_set

    with patch("moneybin.mcp.tools.transactions.MatchingService") as service:
        service.return_value.set_status.return_value = _set_outcome(
            match_status="accepted", transfers_retired=2
        )
        envelope = transactions_matches_set("dd_100000001", "accepted")

    actions = envelope.to_dict()["actions"]
    # refresh_run, not the granular matcher callback: only registered standard
    # tools may be named in emitted text, and an agent cannot call what the
    # surface does not expose.
    assert any("refresh_run" in action for action in actions), (
        f"no action points at the follow-up matcher pass: {actions}"
    )


def test_matches_set_asks_for_no_rematch_when_it_retired_nothing() -> None:
    """Negative twin: an ordinary accept freed no legs, so it owes no pass.

    Without it, a hint appended unconditionally would send an agent through a
    full matcher run after every routine decision.
    """
    from moneybin.mcp.tools.transactions import transactions_matches_set

    with patch("moneybin.mcp.tools.transactions.MatchingService") as service:
        service.return_value.set_status.return_value = _set_outcome(
            match_status="accepted", transfers_retired=0
        )
        envelope = transactions_matches_set("dd_100000001", "accepted")

    actions = envelope.to_dict()["actions"]
    assert not any("refresh_run" in action for action in actions)


def test_matches_set_stays_quiet_about_undo_when_nothing_was_retired() -> None:
    """Negative twin: an ordinary accept must not advertise a recovery it needs.

    Without it, an unconditional hint would satisfy the test above while telling
    every caller that a decision of the user's had been undone.
    """
    from moneybin.mcp.tools.transactions import transactions_matches_set

    with patch("moneybin.mcp.tools.transactions.MatchingService") as service:
        service.return_value.set_status.return_value = _set_outcome(
            match_status="accepted", transfers_retired=0
        )
        envelope = transactions_matches_set("dd_100000001", "accepted")

    actions = envelope.to_dict()["actions"]
    assert not any("system_audit_undo" in action for action in actions)


def test_matches_set_withholds_undo_when_the_accept_came_back_reversed() -> None:
    """``undo`` is the one command guaranteed to fail on a reversed row.

    ``MatchDecisionsRepo.reverse`` raises unless the status is accepted or
    rejected, so advertising it here sends the agent at a ValueError in exactly
    the outcome the reconciliation just produced. The row is not the agent's to
    undo — it never stood.
    """
    from moneybin.mcp.tools.transactions import transactions_matches_set

    with patch("moneybin.mcp.tools.transactions.MatchingService") as service:
        service.return_value.set_status.return_value = _set_outcome(
            match_status="reversed", transfers_retired=0
        )
        envelope = transactions_matches_set("tx_stale00001", "accepted")

    actions = envelope.to_dict()["actions"]
    assert not any("matches undo" in action for action in actions), (
        "a reversed decision was offered an undo that reverse() refuses"
    )
    assert any("reversed" in action for action in actions), (
        "the reversed outcome was left with no applicable next step"
    )


def test_matches_set_still_offers_undo_when_the_decision_stood() -> None:
    """Negative twin: withholding undo everywhere would also pass the test above.

    An accept that committed as accepted is precisely what ``undo`` exists for,
    and it is the only MCP-reachable route back.
    """
    from moneybin.mcp.tools.transactions import transactions_matches_set

    with patch("moneybin.mcp.tools.transactions.MatchingService") as service:
        service.return_value.set_status.return_value = _set_outcome(
            match_status="accepted", transfers_retired=0
        )
        envelope = transactions_matches_set("dd_100000001", "accepted")

    actions = envelope.to_dict()["actions"]
    assert any("matches undo" in action for action in actions)
