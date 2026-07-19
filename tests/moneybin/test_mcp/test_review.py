"""Tests for legacy and dormant normalized review reads."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.reviews import (
    register_review_coarse_reads,
    register_review_coarse_writes,
    reviews_coarse,
    reviews_decide_coarse,
)
from moneybin.mcp.tools.transactions import (
    register_transactions_tools,
    review,
    transactions_review,
)
from moneybin.mcp.write_contracts import (
    CategorizationDecisionRequest,
    MatchDecisionRequest,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.categorization import CategorizationService

from .schema_assertions import (
    assert_literal_values,
    call_tool_raw,
    isolated_server,
    listed_tool,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.unit
async def test_review_returns_envelope(mcp_db: object) -> None:
    """`review` returns a valid ResponseEnvelope."""
    parsed = (await review()).to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_review_data_shape(mcp_db: object) -> None:
    """Data carries the five queue counts and a total equal to their sum."""
    data = (await review()).to_dict()["data"]
    assert "matches_pending" in data
    assert "categorize_pending" in data
    assert "account_links_pending" in data
    assert "merchant_links_pending" in data
    assert "security_links_pending" in data
    assert "total" in data
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
async def test_review_actions_mention_drill_down_queues(mcp_db: object) -> None:
    """actions[] guides the agent to all five MCP-drillable queues, each with a drill-down tool.

    All five review queues have dedicated drill-down tools:
    - transactions_matches_pending for the matches queue
    - transactions_categorize_pending for the categorize queue
    - accounts_links_pending for the account-links queue
    - merchants_links_pending for the merchant-links queue (added in M1T)
    - investments_securities_links_pending for the security-links queue
      (added M1G.4)
    """
    parsed = (await review()).to_dict()
    actions_text = " ".join(parsed["actions"])
    assert "transactions_matches_pending" in actions_text
    assert "transactions_categorize_pending" in actions_text
    assert "accounts_links_pending" in actions_text
    assert "merchants_links_pending" in actions_text
    assert "investments_securities_links_pending" in actions_text


@pytest.mark.unit
async def test_transactions_review_alias_returns_same_shape(mcp_db: object) -> None:
    """`transactions_review` is a deprecated alias with the same data shape."""
    data = (await transactions_review()).to_dict()["data"]
    assert "matches_pending" in data
    assert "categorize_pending" in data
    assert "account_links_pending" in data
    assert "merchant_links_pending" in data
    assert "security_links_pending" in data
    assert "total" in data


@pytest.mark.unit
async def test_register_includes_review_and_alias() -> None:
    """register_transactions_tools registers both `review` and `transactions_review`."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "review" in names
    assert "transactions_review" in names


@pytest.mark.unit
async def test_transactions_review_description_starts_with_deprecated() -> None:
    """`transactions_review` description must start with 'DEPRECATED:'."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    tools = {t.name: t for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    desc = tools["transactions_review"].description or ""
    assert desc.startswith("DEPRECATED:"), (
        f"transactions_review description must start with 'DEPRECATED:' but got: {desc[:80]!r}"
    )


@pytest.mark.parametrize(
    "kind",
    ["categorization", "matches", "account_links", "merchant_links", "security_links"],
)
async def test_review_queue_uses_one_envelope(kind: str) -> None:
    response = await reviews_coarse(kind=kind, status="pending")  # type: ignore[arg-type]
    assert response.data.kind == kind
    assert response.data.status == "pending"


async def test_review_summary_returns_exact_kind_status_matrix() -> None:
    response = await reviews_coarse()

    observed = {
        (count.kind, count.status): count.count for count in response.data.counts
    }
    expected = {
        (kind, status)
        for kind in (
            "categorization",
            "matches",
            "account_links",
            "merchant_links",
            "security_links",
        )
        for status in ("pending", "history")
    }
    assert set(observed) == expected
    assert response.data.total == sum(observed.values())


@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        ({"kind": "summary", "limit": 1}, "REVIEW_PAGINATION_NOT_ALLOWED"),
        (
            {"kind": "summary", "cursor": "anything"},
            "REVIEW_PAGINATION_NOT_ALLOWED",
        ),
        ({"kind": "summary", "status": "history"}, "REVIEW_STATUS_NOT_ALLOWED"),
    ],
)
async def test_review_summary_rejects_incompatible_arguments(
    kwargs: dict[str, Any],
    code: str,
) -> None:
    response = await reviews_coarse(**kwargs)  # type: ignore[arg-type]

    assert response.error is not None
    assert response.error.code == code


def _insert_account_link_decision(
    *,
    decision_id: str,
    provisional_account_id: str,
    candidate_account_id: str,
    status: str,
    decided_at: str,
) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.account_link_decisions (
                decision_id, provisional_account_id, candidate_account_id,
                confidence_score, match_signals, status, decided_by,
                match_reason, decided_at
            ) VALUES (?, ?, ?, 0.85, ?, ?, 'auto', NULL, ?)
            """,  # noqa: S608  # test input, not executing SQL
            [
                decision_id,
                provisional_account_id,
                candidate_account_id,
                json.dumps({"signal": "name"}),
                status,
                decided_at,
            ],
        )


async def test_review_rows_expose_common_and_typed_fields(mcp_db: object) -> None:
    _insert_account_link_decision(
        decision_id="decision-common",
        provisional_account_id="PROV-COMMON",
        candidate_account_id="ACC001",
        status="pending",
        decided_at="2026-07-18T12:00:00",
    )

    response = await reviews_coarse(kind="account_links", status="pending")

    assert response.summary.total_count == 1
    assert response.summary.returned_count == 1
    row = response.data.rows[0]
    assert row.decision_id == "decision-common"
    assert row.kind == "account_links"
    assert row.status == "pending"
    assert row.created_at == "2026-07-18 12:00:00"
    assert row.summary
    assert row.details.state == "pending"
    assert row.details.candidates[0].decision_id == "decision-common"


async def test_review_history_calls_history_not_pending() -> None:
    with (
        patch.object(AccountLinksService, "history", return_value=[]) as history,
        patch.object(
            AccountLinksService,
            "pending",
            side_effect=AssertionError("pending fallback used"),
        ),
    ):
        response = await reviews_coarse(kind="account_links", status="history")

    history.assert_called_once_with(limit=None)
    assert response.data.status == "history"
    assert response.data.rows == []


def _pending_match(match_id: str) -> dict[str, Any]:
    return {
        "match_id": match_id,
        "match_type": "dedup",
        "match_tier": "exact",
        "confidence_score": 0.9,
        "source_type_a": "csv",
        "source_transaction_id_a": f"{match_id}-a",
        "source_type_b": "ofx",
        "source_transaction_id_b": f"{match_id}-b",
        "match_status": "pending",
        "component_key": match_id,
        "decided_by": "auto",
        "decided_at": "2026-07-18T12:00:00",
    }


async def test_review_pagination_is_stable_filter_bound_and_executable() -> None:
    rows = [_pending_match("match-b"), _pending_match("match-a")]
    with patch(
        "moneybin.mcp.tools.reviews.MatchingService.get_pending",
        return_value=rows,
    ):
        first = await reviews_coarse(kind="matches", status="pending", limit=1)
        assert [row.decision_id for row in first.data.rows] == ["match-a"]
        assert first.next_cursor is not None
        assert first.summary.total_count == 2
        assert first.summary.has_more is True
        assert (
            "reviews(kind='matches', status='pending', limit=1, "
            f"cursor='{first.next_cursor}')"
        ) in " ".join(first.actions)

        second = await reviews_coarse(
            kind="matches",
            status="pending",
            limit=1,
            cursor=first.next_cursor,
        )
        assert [row.decision_id for row in second.data.rows] == ["match-b"]
        assert second.next_cursor is None
        assert second.summary.has_more is False

        incompatible = await reviews_coarse(
            kind="matches",
            status="history",
            limit=1,
            cursor=first.next_cursor,
        )
        assert incompatible.error is not None
        assert incompatible.error.code == "REVIEW_CURSOR_INVALID"


async def test_review_dormant_registrar_renders_closed_contract() -> None:
    mcp = isolated_server(register_review_coarse_reads)

    tools = await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert {tool.name for tool in tools} == {"reviews"}
    tool = await listed_tool(mcp, "reviews")
    assert tool.outputSchema is None
    assert_literal_values(
        tool.inputSchema,
        ("properties", "kind"),
        {
            "summary",
            "categorization",
            "matches",
            "account_links",
            "merchant_links",
            "security_links",
        },
    )
    assert_literal_values(
        tool.inputSchema,
        ("properties", "status"),
        {"pending", "history"},
    )


@pytest.mark.parametrize(
    ("kind", "expected_sensitivity", "expected_classes"),
    [
        ("summary", "low", ["aggregate", "txn_type"]),
        (
            "categorization",
            "high",
            [
                "aggregate",
                "category",
                "description",
                "record_id",
                "timestamp_observability",
                "txn_amount",
                "txn_date",
                "txn_type",
            ],
        ),
        (
            "matches",
            "low",
            ["aggregate", "record_id", "timestamp_observability", "txn_type"],
        ),
        (
            "account_links",
            "medium",
            [
                "aggregate",
                "record_id",
                "timestamp_observability",
                "txn_type",
                "user_note",
            ],
        ),
        (
            "merchant_links",
            "medium",
            [
                "aggregate",
                "merchant_name",
                "record_id",
                "timestamp_observability",
                "txn_type",
            ],
        ),
        (
            "security_links",
            "medium",
            [
                "aggregate",
                "record_id",
                "timestamp_observability",
                "txn_type",
                "user_note",
            ],
        ),
    ],
)
async def test_review_raw_transport_is_canonical_and_uses_public_actor(
    kind: str,
    expected_sensitivity: str,
    expected_classes: list[str],
) -> None:
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(register_review_coarse_reads)

    with patch("moneybin.mcp.decorator.write_privacy_event", captured.append):
        response = await call_tool_raw(
            mcp,
            "reviews",
            {"kind": kind, "status": "pending"},
        )

    text = response.content[0]
    assert hasattr(text, "text")
    assert response.structuredContent is not None
    assert json.loads(text.text) == response.structuredContent  # type: ignore[union-attr]
    assert response.structuredContent["data"]["kind"] == kind
    assert len(captured) == 1
    assert captured[0]["actor"] == "mcp.reviews"
    assert captured[0]["sensitivity"] == expected_sensitivity
    assert captured[0]["classes_returned"] == expected_classes


async def test_review_cursor_error_is_canonical_and_sanitized() -> None:
    mcp = isolated_server(register_review_coarse_reads)
    invalid_cursor = "secret-account-1234"

    response = await call_tool_raw(
        mcp,
        "reviews",
        {"kind": "matches", "cursor": invalid_cursor},
    )

    text = response.content[0]
    assert hasattr(text, "text")
    assert response.structuredContent is not None
    assert json.loads(text.text) == response.structuredContent  # type: ignore[union-attr]
    assert response.structuredContent["error"]["code"] == "REVIEW_CURSOR_INVALID"
    assert invalid_cursor not in text.text  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "arguments",
    [
        {"kind": "unknown"},
        {"status": "unknown"},
        {"limit": "50"},
        {"unknown": "value"},
    ],
)
async def test_review_raw_transport_rejects_invalid_arguments(
    arguments: dict[str, Any],
) -> None:
    mcp = isolated_server(register_review_coarse_reads)

    response = await call_tool_raw(mcp, "reviews", arguments)

    assert response.isError is True


def _seed_ordinary_decisions() -> tuple[str, str, str]:
    transaction_id = "TX_REVIEW_DECIDE"
    match_id = "MATCH_REVIEW_DECIDE"
    category = "Task 5 Review"
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description,
                transaction_type, is_pending, currency_code, source_type,
                source_extracted_at, loaded_at, transaction_year,
                transaction_month, transaction_day, transaction_day_of_week,
                transaction_year_month, transaction_year_quarter
            ) VALUES (
                ?, 'ACC001', '2026-07-18', -12.00, 12.00, 'expense',
                'Task 5 review decision', 'DEBIT', false, 'USD', 'ofx',
                '2026-07-18', CURRENT_TIMESTAMP, 2026, 7, 18, 6,
                '2026-07', '2026-Q3'
            )
            """,  # noqa: S608  # test fixture data
            [transaction_id],
        )
        CategorizationService(db).create_category(category, actor="test")
        MatchDecisionsRepo(db).insert(
            match_id=match_id,
            source_transaction_id_a="ordinary-a",
            source_type_a="csv",
            source_origin_a="fixture-a",
            source_transaction_id_b="ordinary-b",
            source_type_b="ofx",
            source_origin_b="fixture-b",
            account_id="ACC001",
            confidence_score=0.9,
            match_signals={"reason": "fixture"},
            match_status="pending",
            decided_by="auto",
            actor="test",
        )
    return transaction_id, match_id, category


async def test_ordinary_decisions_route_by_kind_and_share_operation() -> None:
    transaction_id, match_id, category = _seed_ordinary_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=transaction_id,
                decision="accept",
                category=category,
            ),
            MatchDecisionRequest(
                kind="match",
                decision_id=match_id,
                decision="reject",
            ),
        ]
    )

    assert [item.kind for item in response.data.results] == [
        "categorization",
        "match",
    ]
    assert response.data.applied_count == 2
    assert response.data.operation_id
    assert all(
        item.operation_id == response.data.operation_id
        for item in response.data.results
    )


async def test_ordinary_batch_preflights_before_first_write() -> None:
    transaction_id, _match_id, category = _seed_ordinary_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=transaction_id,
                decision="accept",
                category=category,
            ),
            MatchDecisionRequest(
                kind="match",
                decision_id="missing-match",
                decision="reject",
            ),
        ]
    )

    assert response.error is not None
    assert response.error.details is not None
    assert response.error.details["errors"] == [
        {
            "index": 1,
            "kind": "match",
            "decision_id": "missing-match",
            "code": "mutation_not_found",
            "reason": "No match decision exists for this id.",
        }
    ]
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?",
            [transaction_id],
        ).fetchone()
    assert row is None


async def test_ordinary_categorization_accept_preserves_commit_merchant_semantics() -> (
    None
):
    transaction_id, _match_id, category = _seed_ordinary_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=transaction_id,
                decision="accept",
                category=category,
                canonical_merchant_name="Task Five Merchant",
            )
        ]
    )

    assert response.error is None
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT canonical_name, category, exemplars "
            "FROM app.user_merchants WHERE canonical_name = ?",
            ["Task Five Merchant"],
        ).fetchone()
    assert row is not None
    assert row[0] == "Task Five Merchant"
    assert row[1] == category
    assert list(row[2]) == ["Task 5 review decision"]


async def test_ordinary_all_already_satisfied_is_nothing_to_do() -> None:
    transaction_id, match_id, category = _seed_ordinary_decisions()
    decisions = [
        CategorizationDecisionRequest(
            kind="categorization",
            decision_id=transaction_id,
            decision="accept",
            category=category,
        ),
        MatchDecisionRequest(
            kind="match",
            decision_id=match_id,
            decision="reject",
        ),
    ]

    first = await reviews_decide_coarse(decisions=decisions)
    assert first.error is None
    second = await reviews_decide_coarse(decisions=decisions)

    assert second.error is not None
    assert second.error.code == "mutation_nothing_to_do"


async def test_review_dormant_write_registrar_is_closed_and_max_risk() -> None:
    mcp = isolated_server(register_review_coarse_writes)

    tools = {
        tool.name: tool
        for tool in await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    }
    assert set(tools) == {"reviews_decide", "identity_links_decide"}
    reviews_tool = await listed_tool(mcp, "reviews_decide")
    identity_tool = await listed_tool(mcp, "identity_links_decide")
    assert reviews_tool.outputSchema is None
    assert identity_tool.outputSchema is None
    assert reviews_tool.annotations is not None
    assert reviews_tool.annotations.destructiveHint is False
    assert identity_tool.annotations is not None
    assert identity_tool.annotations.destructiveHint is True
