"""Tests for normalized review reads and decisions."""

from __future__ import annotations

import json
import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest
from fastmcp.server.elicitation import AcceptedElicitation
from prometheus_client import REGISTRY

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.mcp.tools import reviews as reviews_module
from moneybin.mcp.tools.reviews import (
    _identity_binding,  # pyright: ignore[reportPrivateUsage]
    _preview_identity_decisions,  # pyright: ignore[reportPrivateUsage]  # the untested wiring is the subject
    identity_links_decide_coarse,
    register_review_coarse_reads,
    register_review_coarse_writes,
    reviews_coarse,
    reviews_decide_coarse,
)
from moneybin.metrics.registry import MERCHANT_EXEMPLAR_COUNT
from moneybin.orchestration.refresh import RefreshResult
from moneybin.privacy.payloads.reviews import IdentityLinksDecidePayload
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.protocol.write_contracts import (
    AccountLinkDecisionRequest,
    CategorizationDecisionRequest,
    IdentityDecisionRequest,
    MatchDecisionRequest,
    MerchantLinkDecisionRequest,
    OrdinaryReviewDecisionRequest,
    SecurityLinkDecisionRequest,
)
from moneybin.repositories.categorization_decisions_repo import (
    categorization_decision_id,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.repositories.security_price_repo import SecurityPriceRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.auto_rule_service import (
    AutoConfirmResult,
    AutoReviewResult,
    AutoRuleDecisionResult,
    AutoRuleService,
)
from moneybin.services.categorization import CategorizationService
from moneybin.services.identity_confirmation import IDENTITY_BLAST_RADIUS_CATEGORIES
from moneybin.services.merchant_links_service import MerchantLinksService
from moneybin.services.review_decisions_service import (
    IdentityDecisionPlan,
    IdentityDecisionPlanItem,
    ReviewDecisionsService,
)
from moneybin.services.undo_service import UndoService
from tests.moneybin.db_helpers import (
    CORE_FCT_INVESTMENT_LOTS_DDL,
    CORE_FCT_INVESTMENT_TRANSACTIONS_DDL,
    install_uncategorized_queue_view,
)

from .schema_assertions import (
    assert_literal_values,
    call_tool_raw,
    isolated_server,
    listed_tool,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.fixture(autouse=True)
def _canonical_uncategorized_queue(  # pyright: ignore[reportUnusedFunction]  # autouse pytest fixture
    mcp_db: Path,
) -> None:
    """Give every review test the canonical queue view production always has.

    ``core.uncategorized_queue`` is the single definition of an uncategorized
    transaction, so a review DB without it is a drifted one — a state one test
    below asserts on deliberately by dropping the view again.
    """
    with get_database(read_only=False) as db:
        install_uncategorized_queue_view(db)


_NOW = datetime.now(tz=UTC).isoformat()


def _identity_plan(
    decisions: Sequence[
        AccountLinkDecisionRequest
        | MerchantLinkDecisionRequest
        | SecurityLinkDecisionRequest
    ],
    *,
    state_version: str = "initial",
) -> IdentityDecisionPlan:
    """Build a complete deterministic identity batch plan for boundary tests."""
    items = tuple(
        IdentityDecisionPlanItem(
            request=request,
            changed=True,
            status="accepted" if request.decision == "accept" else "rejected",
            source_id=f"{request.kind}-source",
            target_id=request.target_id or f"{request.kind}-candidate",
            group_key=(request.kind, request.decision_id),
            before_state={"version": state_version, "index": index},
            affected_ids={
                "accounts": (f"account-{index}",)
                if request.kind == "account_link"
                else (),
                "merchants": (f"merchant-{index}",)
                if request.kind == "merchant_link"
                else (),
                "securities": (f"security-{index}",)
                if request.kind == "security_link"
                else (),
                "transactions": (f"transaction-{index}",),
                "lots": (f"lot-{index}",) if request.kind == "security_link" else (),
                "price_marks": (),
            },
        )
        for index, request in enumerate(decisions)
    )
    return IdentityDecisionPlan(items=items)


def _identity_preview(plan: IdentityDecisionPlan) -> Any:
    """Wrap a plan the way the read-only preview hands it to the confirm gate.

    The merge facts are empty here on purpose: these tests pin the confirmation
    protocol — binding, token, replay — and an account merge's rendered prose is
    covered where the renderer lives.
    """
    return reviews_module._IdentityPreview(  # pyright: ignore[reportPrivateUsage]
        plan=plan,
        merges=(),
        kinds=tuple(
            sorted({
                item.request.kind
                for item in plan.items
                if item.changed and item.request.decision == "accept"
            })
        ),
    )


def _identity_decision_status(kind: str, decision_id: str) -> str:
    table = {
        "account_link": "app.account_link_decisions",
        "merchant_link": "app.merchant_link_decisions",
        "security_link": "app.security_link_decisions",
    }[kind]
    with get_database(read_only=True) as db:
        row = db.execute(
            f"SELECT status FROM {table} WHERE decision_id = ?",  # noqa: S608  # table allowlist
            [decision_id],
        ).fetchone()
    assert row is not None
    return str(row[0])


def _seed_identity_account(account_id: str, display_name: str) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.dim_accounts (
                account_id, account_type, institution_name, display_name
            ) VALUES (?, 'CHECKING', 'Test Bank', ?)
            """,
            [account_id, display_name],
        )


def _seed_identity_account_link(account_id: str, link_id: str) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.account_links (
                link_id, account_id, ref_kind, ref_value, source_type,
                source_origin, status, decided_by, decided_at
            ) VALUES (
                ?, ?, 'source_native', ?, 'csv', 'test-bank',
                'accepted', 'auto', ?
            )
            """,
            [link_id, account_id, f"key-{account_id}", _NOW],
        )


def _identity_account_setup(label: str) -> dict[str, str]:
    provisional = f"PROV_{label}"
    candidate = f"ACC_{label}"
    decision_id = f"account-{label}"
    _seed_identity_account(provisional, f"Imported {label}")
    _seed_identity_account(candidate, f"Canonical {label}")
    _seed_identity_account_link(provisional, f"link-{label}")
    _insert_account_link_decision(
        decision_id=decision_id,
        provisional_account_id=provisional,
        candidate_account_id=candidate,
        status="pending",
        decided_at=_NOW,
    )
    return {
        "candidate": candidate,
        "decision_id": decision_id,
        "provisional": provisional,
    }


def _seed_identity_merchant(merchant_id: str) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.user_merchants (
                merchant_id, match_type, canonical_name, created_by
            ) VALUES (?, 'oneOf', ?, 'user')
            """,
            [merchant_id, f"Merchant {merchant_id}"],
        )


def _identity_merchant_setup(label: str) -> dict[str, str]:
    decision_id = f"merchant-{label}"
    merchant_id = f"merchant-target-{label}"
    _seed_identity_merchant(merchant_id)
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.merchant_link_decisions (
                decision_id, ref_kind, ref_value, source_type,
                provider_merchant_name, candidate_merchant_id,
                confidence_score, match_signals, status, decided_by, decided_at
            ) VALUES (
                ?, 'merchant_entity_id', ?, 'plaid', ?, ?, 0.62, '{}',
                'pending', 'auto', ?
            )
            """,
            [
                decision_id,
                f"entity-{label}",
                f"Provider {label}",
                merchant_id,
                _NOW,
            ],
        )
    return {"decision_id": decision_id, "merchant_id": merchant_id}


def _mint_identity_security(
    *,
    name: str,
    created_by: str,
    ticker: str,
) -> str:
    with get_database(read_only=False) as db:
        event = SecuritiesRepo(db).upsert(
            security_id=None,
            name=name,
            security_type="etf",
            ticker=ticker,
            created_by=created_by,
            actor="system" if created_by == "plaid" else "mcp",
        )
    assert event.target_id is not None
    return event.target_id


def _identity_security_setup(label: str) -> dict[str, str]:
    survivor = _mint_identity_security(
        name=f"Canonical security {label}",
        created_by="user",
        ticker=f"C{label[:4].upper()}",
    )
    provisional = _mint_identity_security(
        name=f"Provider security {label}",
        created_by="plaid",
        ticker=f"P{label[:4].upper()}",
    )
    ref_value = f"security-ref-{label}"
    decision_id = f"security-{label}"
    with get_database(read_only=False) as db:
        SecurityLinksRepo(db).insert(
            security_id=provisional,
            ref_kind="plaid_security_id",
            ref_value=ref_value,
            source_type="plaid",
            decided_by="auto",
            actor="system",
        )
        SecurityLinkDecisionsRepo(db).insert(
            decision_id=decision_id,
            ref_kind="plaid_security_id",
            ref_value=ref_value,
            source_type="plaid",
            candidate_security_id=survivor,
            provider_ticker=f"P{label[:4].upper()}",
            provider_name=f"Provider security {label}",
            confidence_score=0.5,
            match_reason="fuzzy_name",
            actor="system",
        )
    return {
        "decision_id": decision_id,
        "provisional": provisional,
        "survivor": survivor,
    }


def _identity_feed_key_setup(label: str) -> dict[str, str]:
    """A pending price-feed decision, deliberately with NO accepted binding.

    The absence is the fixture's whole point: a feed key asks whether a market-data
    symbol names this security, and ``PriceService`` queues it precisely because no
    link exists yet. Seeding one would make this an identity merge wearing a
    feed-key ``ref_kind`` and would pass against the merge-only code it exists to
    catch.
    """
    security = _mint_identity_security(
        name=f"Feed security {label}",
        created_by="user",
        ticker=f"F{label[:3].upper()}",
    )
    ref_value = f"FEED{label[:3].upper()}"
    decision_id = f"feed-{label}"
    with get_database(read_only=False) as db:
        SecurityLinkDecisionsRepo(db).insert(
            decision_id=decision_id,
            ref_kind="tiingo_ticker",
            ref_value=ref_value,
            source_type="tiingo",
            candidate_security_id=security,
            provider_ticker=ref_value,
            provider_name=f"Feed security {label}",
            confidence_score=0.5,
            match_reason="name_divergence",
            actor="system",
        )
    return {
        "decision_id": decision_id,
        "security": security,
        "ref_value": ref_value,
    }


def _accepted_feed_binding(ref_value: str) -> str | None:
    """The security a ``tiingo_ticker`` ref is bound to, or ``None`` if unbound."""
    with get_database(read_only=True) as db:
        row = db.execute(
            """
            SELECT security_id FROM app.security_links
            WHERE ref_kind = 'tiingo_ticker' AND ref_value = ?
              AND source_type = 'tiingo' AND status = 'accepted'
            """,
            [ref_value],
        ).fetchone()
    return str(row[0]) if row is not None else None


def _seed_price_mark(security_id: str, price_date: date) -> None:
    """Author one user price mark on ``security_id`` — a row a merge must move."""
    with get_database(read_only=False) as db:
        SecurityPriceRepo(db).set(
            security_id,
            price_date,
            "USD",
            close=Decimal("101.50"),
            note=None,
            actor="mcp",
        )


def _security_exists(security_id: str) -> bool:
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT 1 FROM app.securities WHERE security_id = ?",
            [security_id],
        ).fetchone()
    return row is not None


def _seed_identity_merchant_transaction(
    transaction_id: str,
    merchant_id: str,
) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description,
                merchant_id, transaction_type, is_pending, currency_code,
                source_type, source_extracted_at, loaded_at, transaction_year,
                transaction_month, transaction_day, transaction_day_of_week,
                transaction_year_month, transaction_year_quarter
            ) VALUES (
                ?, 'ACC001', '2026-07-18', -4.00, 4.00, 'expense',
                'Existing merchant transaction', ?, 'DEBIT', false, 'USD',
                'ofx', '2026-07-18', CURRENT_TIMESTAMP, 2026, 7, 18, 6,
                '2026-07', '2026-Q3'
            )
            """,
            [transaction_id, merchant_id],
        )


@pytest.mark.parametrize(
    "kind",
    [
        "categorization",
        "auto_rules",
        "matches",
        "account_links",
        "merchant_links",
        "security_links",
    ],
)
async def test_review_queue_uses_one_envelope(kind: str) -> None:
    response = await reviews_coarse(kind=kind, status="pending")  # type: ignore[arg-type]
    assert response.data.kind == kind
    assert response.data.status == "pending"


async def test_categorization_queue_reports_a_missing_canonical_view() -> None:
    """An absent core.uncategorized_queue is surfaced, not rendered as "none pending".

    core.uncategorized_queue is the single definition of an uncategorized
    transaction, so when the view is missing the queue's contents are unknown
    — not empty. Returning an empty queue would tell a curator their work is
    done while the refresh that builds the view has never run.
    """
    with get_database(read_only=False) as db:
        db.execute("DROP VIEW core.uncategorized_queue")

    response = await reviews_coarse(kind="categorization", status="pending")

    assert response.error is not None
    assert response.error.code == error_codes.INFRA_SCHEMA_DRIFT


async def test_review_summary_returns_exact_kind_status_matrix() -> None:
    response = await reviews_coarse()

    observed = {
        (count.kind, count.status): count.count for count in response.data.counts
    }
    expected = {
        (kind, status)
        for kind in (
            "categorization",
            "auto_rules",
            "matches",
            "account_links",
            "merchant_links",
            "security_links",
        )
        for status in ("pending", "history")
    }
    assert set(observed) == expected
    assert response.data.total == sum(observed.values())


async def test_review_summary_degrades_when_one_queue_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One broken queue is marked unavailable; the other six still report counts.

    A missing view (core.uncategorized_queue after #340 moved it) previously
    failed the whole aggregate, so the default reviews() call was a dead end
    while six of seven queues were healthy.
    """
    real_count = reviews_module._review_count  # pyright: ignore[reportPrivateUsage]

    def count_or_fail(db: object, *, kind: str, status: str) -> int:
        if kind == "categorization":
            raise duckdb.CatalogException("Table with name uncategorized_queue does")
        return real_count(db, kind=kind, status=status)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(reviews_module, "_review_count", count_or_fail)

    response = await reviews_coarse(kind="summary")

    assert response.error is None
    healthy = {count.kind for count in response.data.counts}
    assert "categorization" not in healthy
    assert "matches" in healthy
    assert "auto_rules" in healthy
    unavailable = {entry.kind for entry in response.data.unavailable}
    assert unavailable == {"categorization"}
    assert response.data.unavailable[0].code
    assert response.summary.degraded is True


async def test_review_summary_counts_auto_rules_without_blast_radius_scan() -> None:
    with (
        patch.object(AutoRuleService, "count_pending_proposals", return_value=2),
        patch.object(
            AutoRuleService,
            "count_proposal_history",
            return_value=1,
        ),
        patch.object(
            AutoRuleService,
            "list_proposal_history",
            side_effect=AssertionError("summary materialized proposal history"),
        ),
        patch.object(
            AutoRuleService,
            "review",
            side_effect=AssertionError("summary ran transaction-wide blast scan"),
        ),
    ):
        response = await reviews_coarse()

    counts = {(item.kind, item.status): item.count for item in response.data.counts}
    assert counts[("auto_rules", "pending")] == 2
    assert counts[("auto_rules", "history")] == 1


@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        ({"kind": "summary", "limit": 1}, "review_pagination_not_allowed"),
        (
            {"kind": "summary", "cursor": "anything"},
            "review_pagination_not_allowed",
        ),
        ({"kind": "summary", "status": "history"}, "review_status_not_allowed"),
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


async def test_auto_rule_review_preserves_proposal_blast_radius() -> None:
    proposal = {
        "proposed_rule_id": "proposal-1",
        "merchant_pattern": "DEMO MARKET",
        "match_type": "contains",
        "category": "Groceries",
        "subcategory": None,
        "trigger_count": 3,
        "sample_txn_ids": ["txn-1", "txn-2"],
        "estimated_match_count": 27,
        "is_broad": True,
    }
    with (
        patch(
            "moneybin.mcp.tools.reviews.AutoRuleService.count_pending_proposals",
            return_value=1,
        ),
        patch(
            "moneybin.mcp.tools.reviews.AutoRuleService.review",
            return_value=AutoReviewResult(proposals=[proposal], total_count=1),
        ) as review,
    ):
        response = await reviews_coarse(kind="auto_rules", status="pending")

    review.assert_called_once_with(limit=1)
    row = response.data.rows[0]
    assert row.decision_id == "proposal-1"
    assert row.kind == "auto_rules"
    assert row.details.proposal.estimated_match_count == 27
    assert row.details.proposal.is_broad is True
    assert "reviews_decide" in " ".join(response.actions)


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
        assert incompatible.error.code == "review_cursor_invalid"


async def test_review_pending_continuation_does_not_skip_after_first_row_removed() -> (
    None
):
    initial = [_pending_match("match-a"), _pending_match("match-b")]
    after_decision = [_pending_match("match-b")]
    with patch(
        "moneybin.mcp.tools.reviews.MatchingService.get_pending",
        side_effect=[initial, after_decision],
    ):
        first = await reviews_coarse(kind="matches", status="pending", limit=1)
        second = await reviews_coarse(
            kind="matches",
            status="pending",
            limit=1,
            cursor=first.next_cursor,
        )

    assert [row.decision_id for row in first.data.rows] == ["match-a"]
    assert [row.decision_id for row in second.data.rows] == ["match-b"]
    assert second.next_cursor is None


async def test_review_cursor_snapshot_excludes_prepends_and_preserves_total() -> None:
    initial = [_pending_match("match-a"), _pending_match("match-b")]
    prepended = _pending_match("match-new")
    prepended["confidence_score"] = 1.0
    with patch(
        "moneybin.mcp.tools.reviews.MatchingService.get_pending",
        side_effect=[initial, [prepended, *initial]],
    ):
        first = await reviews_coarse(kind="matches", status="pending", limit=1)
        second = await reviews_coarse(
            kind="matches",
            status="pending",
            limit=1,
            cursor=first.next_cursor,
        )

    assert [row.decision_id for row in first.data.rows] == ["match-a"]
    assert [row.decision_id for row in second.data.rows] == ["match-b"]
    assert first.summary.total_count == second.summary.total_count == 2


async def test_review_cursor_rejects_continuation_before_its_snapshot() -> None:
    """`after` may never sort ahead of the snapshot it continues.

    The forged key is well-shaped — a float and a string, exactly the matches
    queue contract — so only the ordering guard can reject it. Confidence sorts
    DESC, so 1.0 sorts ahead of the 0.9 snapshot page one froze, the `> after`
    filter stops narrowing anything, and the page re-serves match-a.
    """
    from moneybin.protocol.pagination import encode_keyset_cursor

    rows = [_pending_match("match-a"), _pending_match("match-b")]
    with patch(
        "moneybin.mcp.tools.reviews.MatchingService.get_pending",
        return_value=rows,
    ):
        first = await reviews_coarse(kind="matches", status="pending", limit=1)
        assert [row.decision_id for row in first.data.rows] == ["match-a"]

        response = await reviews_coarse(
            kind="matches",
            status="pending",
            limit=1,
            cursor=encode_keyset_cursor(
                namespace="reviews",
                scope={"kind": "matches", "status": "pending"},
                snapshot=(0.9, "match-a"),
                after=(1.0, "match-z"),
                total=2,
            ),
        )

    assert response.error is not None
    assert response.error.code == "review_cursor_invalid"


async def test_review_cursor_validates_key_shape_when_queue_is_empty() -> None:
    from moneybin.protocol.pagination import encode_keyset_cursor

    cursor = encode_keyset_cursor(
        namespace="reviews",
        scope={"kind": "matches", "status": "pending"},
        snapshot=("not-a-confidence", "match-a"),
        after=("still-not-a-confidence", "match-b"),
        total=2,
    )
    with patch(
        "moneybin.mcp.tools.reviews.MatchingService.get_pending",
        return_value=[],
    ):
        response = await reviews_coarse(
            kind="matches",
            status="pending",
            cursor=cursor,
        )

    assert response.error is not None
    assert response.error.code == "review_cursor_invalid"


async def test_review_standard_registrar_renders_closed_contract() -> None:
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
            "auto_rules",
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
        # `description`/medium even on a summary where every queue reported:
        # `ReviewsSummaryView` statically declares `unavailable:
        # list[QueueUnavailable]`, whose `reason`/`hint` carry exception-derived
        # text, and this path classifies the declared container rather than the
        # variants a given call actually returned (`system_status` does the
        # latter, so it stays low until it degrades). Over-reporting is the safe
        # direction; narrowing it needs an instance-walking classifier.
        ("summary", "medium", ["aggregate", "description", "txn_type"]),
        (
            "auto_rules",
            "medium",
            [
                "aggregate",
                "category",
                "merchant_name",
                "record_id",
                "timestamp_observability",
                "txn_type",
            ],
        ),
        (
            "categorization",
            "high",
            [
                "aggregate",
                "category",
                # M1K.1: PendingTxnRow carries the currency its txn_amount is
                # denominated in, so the queue no longer shows a bare number.
                "currency",
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
    assert response.structuredContent["error"]["code"] == "review_cursor_invalid"
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


def _seed_ordinary_decisions() -> tuple[str, str, str, str]:
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
        db.execute(
            """
            CREATE OR REPLACE VIEW core.uncategorized_queue AS
            SELECT
                transaction_id,
                account_id,
                CAST(NULL AS VARCHAR) AS account_name,
                transaction_date AS txn_date,
                amount,
                currency_code,
                description,
                CAST(NULL AS VARCHAR) AS merchant_id,
                CAST(NULL AS VARCHAR) AS merchant_normalized,
                CAST(1 AS INTEGER) AS age_days,
                CAST(1 AS DOUBLE) AS priority_score,
                source_type,
                CAST(NULL AS VARCHAR) AS source_id
            FROM core.fct_transactions AS tx
            WHERE NOT EXISTS (
                SELECT 1
                FROM app.transaction_categories AS tc
                WHERE tc.transaction_id = tx.transaction_id
            )
            """
        )
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
    return (
        transaction_id,
        categorization_decision_id(transaction_id),
        match_id,
        category,
    )


def _seed_alternating_ordinary_decisions() -> dict[str, str]:
    """Seed two categorization rows and two match rows for ordered mixed batches."""
    first_transaction_id, first_categorization_id, reject_match_id, category = (
        _seed_ordinary_decisions()
    )
    second_transaction_id = "TX_REVIEW_DECIDE_SECOND"
    keep_match_id = "MATCH_REVIEW_KEEP"
    stale_match_id = "MATCH_REVIEW_STALE"
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
                ?, 'ACC001', '2026-07-19', -13.00, 13.00, 'expense',
                'Task 3 second review decision', 'DEBIT', false, 'USD', 'ofx',
                '2026-07-19', CURRENT_TIMESTAMP, 2026, 7, 19, 7,
                '2026-07', '2026-Q3'
            )
            """,  # noqa: S608  # test fixture data
            [second_transaction_id],
        )
        repo = MatchDecisionsRepo(db)
        repo.insert(
            match_id=keep_match_id,
            source_transaction_id_a="ordinary-keep-a",
            source_type_a="ofx",
            source_origin_a="fixture-a",
            source_transaction_id_b="ordinary-keep-b",
            source_type_b="ofx",
            source_origin_b="fixture-b",
            account_id="checking",
            account_id_b="brokerage",
            confidence_score=0.9,
            match_signals={"reason": "standing transfer"},
            match_type="transfer",
            match_status="accepted",
            decided_by="user",
            actor="test",
        )
        repo.insert(
            match_id="MATCH_REVIEW_EDGE",
            source_transaction_id_a="ordinary-keep-a",
            source_type_a="ofx",
            source_origin_a="fixture-a",
            source_transaction_id_b="ordinary-stale-a",
            source_type_b="csv",
            source_origin_b="fixture-c",
            account_id="checking",
            confidence_score=0.9,
            match_signals={"reason": "accepted dedup edge"},
            match_type="dedup",
            match_status="accepted",
            decided_by="user",
            actor="test",
        )
        repo.insert(
            match_id=stale_match_id,
            source_transaction_id_a="ordinary-stale-a",
            source_type_a="csv",
            source_origin_a="fixture-c",
            source_transaction_id_b="ordinary-stale-b",
            source_type_b="ofx",
            source_origin_b="fixture-d",
            account_id="checking",
            account_id_b="savings",
            confidence_score=0.9,
            match_signals={"reason": "stale transfer"},
            match_type="transfer",
            match_status="pending",
            decided_by="auto",
            actor="test",
        )
    return {
        "first_transaction_id": first_transaction_id,
        "first_categorization_id": first_categorization_id,
        "reject_match_id": reject_match_id,
        "second_transaction_id": second_transaction_id,
        "second_categorization_id": categorization_decision_id(second_transaction_id),
        "stale_match_id": stale_match_id,
        "category": category,
    }


def _retirement_metric() -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_transfer_retirements_total",
            {"cause": "dedup_component"},
        )
        or 0.0
    )


def _merchant_exemplar_metrics() -> dict[str, float]:
    return {
        str(sample.labels["merchant_id"]): sample.value
        for family in MERCHANT_EXEMPLAR_COUNT.collect()
        for sample in family.samples
        if sample.name == "moneybin_merchant_exemplar_count"
    }


async def test_ordinary_decisions_preserve_mixed_order_and_final_effects() -> None:
    seeded = _seed_alternating_ordinary_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=seeded["first_categorization_id"],
                decision="accept",
                category=seeded["category"],
            ),
            MatchDecisionRequest(
                kind="match",
                decision_id=seeded["reject_match_id"],
                decision="reject",
            ),
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=seeded["second_categorization_id"],
                decision="accept",
                category=seeded["category"],
            ),
            MatchDecisionRequest(
                kind="match",
                decision_id=seeded["stale_match_id"],
                decision="accept",
            ),
        ]
    )

    assert response.error is None
    assert [
        (item.kind, item.decision_id, item.status, item.changed)
        for item in response.data.results
    ] == [
        ("categorization", seeded["first_categorization_id"], "accepted", True),
        ("match", seeded["reject_match_id"], "rejected", True),
        ("categorization", seeded["second_categorization_id"], "accepted", True),
        ("match", seeded["stale_match_id"], "reversed", True),
    ]
    assert response.data.applied_count == 4
    assert response.data.transfers_retired == 0
    assert response.data.operation_id
    assert all(
        item.operation_id == response.data.operation_id
        for item in response.data.results
    )
    with get_database(read_only=True) as db:
        assert db.execute(
            """
            SELECT decision_id, status
            FROM app.categorization_decisions
            WHERE decision_id IN (?, ?)
            ORDER BY decision_id
            """,
            [
                seeded["first_categorization_id"],
                seeded["second_categorization_id"],
            ],
        ).fetchall() == sorted([
            (seeded["first_categorization_id"], "accepted"),
            (seeded["second_categorization_id"], "accepted"),
        ])
        assert db.execute(
            """
            SELECT match_id, match_status
            FROM app.match_decisions
            WHERE match_id IN (?, ?)
            ORDER BY match_id
            """,
            [seeded["reject_match_id"], seeded["stale_match_id"]],
        ).fetchall() == sorted([
            (seeded["reject_match_id"], "rejected"),
            (seeded["stale_match_id"], "reversed"),
        ])
        audits = db.execute(
            """
            SELECT action, target_id
            FROM app.audit_log
            WHERE actor = 'mcp'
            ORDER BY rowid
            """
        ).fetchall()
    assert audits == [
        ("categorization_decision.insert", seeded["first_categorization_id"]),
        ("categorization_decision.insert", seeded["second_categorization_id"]),
        ("category.set", seeded["first_transaction_id"]),
        (
            "categorization_decision.update_status",
            seeded["first_categorization_id"],
        ),
        ("match_decision.update_status", seeded["reject_match_id"]),
        ("category.set", seeded["second_transaction_id"]),
        (
            "categorization_decision.update_status",
            seeded["second_categorization_id"],
        ),
        ("match_decision.update_status", seeded["stale_match_id"]),
        ("match_decision.reverse", seeded["stale_match_id"]),
    ]


async def test_a_batch_match_accept_reports_that_it_reconciled() -> None:
    """The count is 0 here, and the point is that it is not ``None``.

    Accepting a match runs the transfer reconciliation, so this batch's
    ``transfers_retired`` is a measurement — nothing collided in this fixture.
    Collapsing that to ``None`` would make "no reconciliation ran" and "one
    ran and reversed nothing" the same answer, which is the distinction the
    identity payload's re-match counts already draw.
    """
    _transaction_id, _categorization_id, match_id, _category = (
        _seed_ordinary_decisions()
    )

    response = await reviews_decide_coarse(
        decisions=[
            MatchDecisionRequest(kind="match", decision_id=match_id, decision="accept")
        ]
    )

    assert response.error is None
    assert response.data.results[0].status == "accepted"
    assert response.data.transfers_retired == 0
    assert not any("retired" in action for action in response.actions)


async def test_a_batch_without_a_match_accept_runs_no_reconciliation() -> None:
    """Negative twin: a rejection folds nothing, so no pass runs at all."""
    _transaction_id, _categorization_id, match_id, _category = (
        _seed_ordinary_decisions()
    )

    response = await reviews_decide_coarse(
        decisions=[
            MatchDecisionRequest(kind="match", decision_id=match_id, decision="reject")
        ]
    )

    assert response.error is None
    assert response.data.transfers_retired is None


async def test_auto_rule_decisions_route_through_existing_decision_tool() -> None:
    request_type = reviews_module.AutoRuleDecisionRequest
    decisions = [
        request_type(
            kind="auto_rule",
            decision_id="proposal-accept",
            decision="accept",
            allow_broad=True,
        ),
        request_type(
            kind="auto_rule",
            decision_id="proposal-reject",
            decision="reject",
        ),
    ]
    with (
        patch.object(
            reviews_module.AutoRuleService,
            "decide",
            return_value=AutoRuleDecisionResult(
                statuses={
                    "proposal-accept": "pending",
                    "proposal-reject": "rejected",
                },
                impact=AutoConfirmResult(
                    approved=0,
                    rejected=1,
                    skipped=1,
                    newly_categorized=4,
                    rule_ids=[],
                ),
            ),
        ) as decide,
    ):
        response = await reviews_decide_coarse(
            decisions=decisions,
        )

    decide.assert_called_once_with(
        expected_pending_ids=["proposal-accept", "proposal-reject"],
        accept=["proposal-accept"],
        reject=["proposal-reject"],
        actor="mcp",
        allow_broad_ids={"proposal-accept"},
    )
    assert [row.status for row in response.data.results] == [
        "pending",
        "rejected",
    ]
    assert response.data.applied_count == 1
    assert response.data.auto_rule_impact is not None
    assert response.data.auto_rule_impact.approved == 0
    assert response.data.auto_rule_impact.rejected == 1
    assert response.data.auto_rule_impact.skipped == 1
    assert response.data.auto_rule_impact.newly_categorized == 4
    assert response.data.auto_rule_impact.rule_ids == []


def _seed_real_auto_rule_decisions() -> tuple[str, str, str]:
    category = "Real Auto Rule Review"
    with get_database(read_only=False) as db:
        categorization = CategorizationService(db)
        categorization.create_category(category, actor="test")
        for transaction_id, description in (
            ("auto-trigger-accept", "REAL ACCEPT SHOP"),
            ("auto-trigger-reject", "REAL REJECT SHOP"),
            ("auto-backfill-accept", "REAL ACCEPT SHOP"),
            ("auto-backfill-reject", "REAL REJECT SHOP"),
        ):
            db.execute(
                """
                INSERT INTO core.fct_transactions (
                    transaction_id, account_id, transaction_date, amount,
                    description, source_type
                ) VALUES (?, 'ACC001', DATE '2026-07-18', -7.00, ?, 'ofx')
                """,
                [transaction_id, description],
            )
        for transaction_id in ("auto-trigger-accept", "auto-trigger-reject"):
            categorization.write_categorization(
                transaction_id=transaction_id,
                category=category,
                subcategory=None,
                categorized_by="user",
            )
        auto_rules = AutoRuleService(db)
        accept_id = auto_rules.record_categorization(
            "auto-trigger-accept",
            category,
        )
        reject_id = auto_rules.record_categorization(
            "auto-trigger-reject",
            category,
        )
        assert accept_id is not None
        assert reject_id is not None
    return accept_id, reject_id, category


async def test_real_auto_rule_success_reports_rows_and_aggregate_impact() -> None:
    accept_id, reject_id, category = _seed_real_auto_rule_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            reviews_module.AutoRuleDecisionRequest(
                kind="auto_rule",
                decision_id=accept_id,
                decision="accept",
            ),
            reviews_module.AutoRuleDecisionRequest(
                kind="auto_rule",
                decision_id=reject_id,
                decision="reject",
            ),
        ]
    )

    assert response.error is None
    assert [
        (row.decision_id, row.status, row.changed) for row in response.data.results
    ] == [
        (accept_id, "approved", True),
        (reject_id, "rejected", True),
    ]
    assert response.data.applied_count == 2
    assert response.data.auto_rule_impact is not None
    assert response.data.auto_rule_impact.approved == 1
    assert response.data.auto_rule_impact.rejected == 1
    assert response.data.auto_rule_impact.skipped == 0
    assert response.data.auto_rule_impact.newly_categorized == 1
    assert len(response.data.auto_rule_impact.rule_ids) == 1

    with get_database(read_only=True) as db:
        proposal_rows = db.execute(
            """
            SELECT proposed_rule_id, status, rule_id
            FROM app.proposed_rules
            WHERE proposed_rule_id IN (?, ?)
            """,
            [accept_id, reject_id],
        ).fetchall()
        assert {str(row[0]): (row[1], row[2]) for row in proposal_rows} == {
            accept_id: (
                "approved",
                response.data.auto_rule_impact.rule_ids[0],
            ),
            reject_id: ("rejected", None),
        }
        accepted_backfill = db.execute(
            """
            SELECT category, categorized_by, rule_id
            FROM app.transaction_categories
            WHERE transaction_id = 'auto-backfill-accept'
            """
        ).fetchone()
        rejected_backfill = db.execute(
            """
            SELECT category
            FROM app.transaction_categories
            WHERE transaction_id = 'auto-backfill-reject'
            """
        ).fetchone()
    assert accepted_backfill == (
        category,
        "auto_rule",
        response.data.auto_rule_impact.rule_ids[0],
    )
    assert rejected_backfill is None


def test_auto_rule_reject_forbids_allow_broad() -> None:
    with pytest.raises(ValueError, match="Reject forbids allow_broad"):
        reviews_module.AutoRuleDecisionRequest(
            kind="auto_rule",
            decision_id="proposal-reject",
            decision="reject",
            allow_broad=True,
        )


async def test_ordinary_decisions_have_no_auto_rule_impact() -> None:
    _transaction_id, categorization_id, _match_id, category = _seed_ordinary_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
                decision="accept",
                category=category,
            )
        ]
    )

    assert response.data.auto_rule_impact is None


async def test_ordinary_batch_preflights_before_first_write() -> None:
    transaction_id, categorization_id, _match_id, category = _seed_ordinary_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
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
    _transaction_id, categorization_id, _match_id, category = _seed_ordinary_decisions()

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
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


async def test_ordinary_batch_coalesces_shared_new_merchant_in_input_order() -> None:
    _first_transaction_id, first_id, _match_id, category = _seed_ordinary_decisions()
    second_transaction_id = "TX_REVIEW_DECIDE_SECOND"
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
                ?, 'ACC001', '2026-07-18', -18.00, 18.00, 'expense',
                'Task 5 second review decision', 'DEBIT', false, 'USD', 'ofx',
                '2026-07-18', CURRENT_TIMESTAMP, 2026, 7, 18, 6,
                '2026-07', '2026-Q3'
            )
            """,  # noqa: S608  # test fixture data
            [second_transaction_id],
        )
    second_id = categorization_decision_id(second_transaction_id)

    response = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=first_id,
                decision="accept",
                category=category,
                canonical_merchant_name="Shared Review Merchant",
            ),
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=second_id,
                decision="accept",
                category=category,
                canonical_merchant_name="Shared Review Merchant",
            ),
        ]
    )

    assert response.error is None
    with get_database(read_only=True) as db:
        merchants = db.execute(
            "SELECT merchant_id, exemplars FROM app.user_merchants "
            "WHERE canonical_name = ?",
            ["Shared Review Merchant"],
        ).fetchall()
        decisions = db.execute(
            "SELECT merchant_id, status FROM app.categorization_decisions "
            "WHERE decision_id IN (?, ?) ORDER BY decision_id",
            [first_id, second_id],
        ).fetchall()
    assert len(merchants) == 1
    assert list(merchants[0][1]) == [
        "Task 5 review decision",
        "Task 5 second review decision",
    ]
    assert decisions == [
        (merchants[0][0], "accepted"),
        (merchants[0][0], "accepted"),
    ]


def test_ordinary_outer_commit_failure_rolls_back_state_and_committed_metrics(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    seeded = _seed_alternating_ordinary_decisions()
    requests: list[OrdinaryReviewDecisionRequest] = [
        CategorizationDecisionRequest(
            kind="categorization",
            decision_id=seeded["first_categorization_id"],
            decision="accept",
            category=seeded["category"],
            canonical_merchant_name="Rolled Back Review Merchant",
        ),
        MatchDecisionRequest(
            kind="match",
            decision_id=seeded["stale_match_id"],
            decision="accept",
        ),
    ]
    retirement_before = _retirement_metric()
    merchant_metrics_before = _merchant_exemplar_metrics()
    caplog.clear()

    def _fail_commit() -> None:
        raise RuntimeError("outer commit failed")

    with get_database(read_only=False) as db:
        monkeypatch.setattr(db, "commit", _fail_commit)
        with caplog.at_level(
            logging.INFO,
            logger="moneybin.services.categorization.applier",
        ):
            with pytest.raises(RuntimeError, match="outer commit failed"):
                ReviewDecisionsService(db, actor="mcp").apply_ordinary(requests)

    assert _retirement_metric() == retirement_before
    assert _merchant_exemplar_metrics() == merchant_metrics_before
    assert "user merchant" not in caplog.text
    with get_database(read_only=True) as db:
        assert (
            db.execute(
                "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?",
                [seeded["first_transaction_id"]],
            ).fetchone()
            is None
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.user_merchants WHERE canonical_name = ?",
                ["Rolled Back Review Merchant"],
            ).fetchone()
            is None
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.categorization_decisions WHERE decision_id = ?",
                [seeded["first_categorization_id"]],
            ).fetchone()
            is None
        )
        assert db.execute(
            "SELECT match_status FROM app.match_decisions WHERE match_id = ?",
            [seeded["stale_match_id"]],
        ).fetchone() == ("pending",)
        assert db.execute(
            "SELECT COUNT(*) FROM app.audit_log WHERE actor = 'mcp'"
        ).fetchone() == (0,)


def test_ordinary_matching_metric_failure_preserves_later_committed_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seeded = _seed_alternating_ordinary_decisions()
    merchant_name = "Committed Review Merchant"
    requests: list[OrdinaryReviewDecisionRequest] = [
        CategorizationDecisionRequest(
            kind="categorization",
            decision_id=seeded["first_categorization_id"],
            decision="accept",
            category=seeded["category"],
            canonical_merchant_name=merchant_name,
        ),
        MatchDecisionRequest(
            kind="match",
            decision_id=seeded["stale_match_id"],
            decision="accept",
        ),
    ]
    retirement_before = _retirement_metric()
    merchant_metrics_before = _merchant_exemplar_metrics()

    def _fail_matching_metric(_: int) -> None:
        raise RuntimeError("matching metric unavailable")

    monkeypatch.setattr(
        "moneybin.matching.application.record_dedup_retirements",
        _fail_matching_metric,
    )
    with get_database(read_only=False) as db:
        outcome = ReviewDecisionsService(db, actor="mcp").apply_ordinary(requests)

    assert [item.status for item in outcome.items] == ["accepted", "reversed"]
    assert outcome.transfers_retired == 0
    assert _retirement_metric() == retirement_before
    with get_database(read_only=True) as db:
        merchant = db.execute(
            "SELECT merchant_id, exemplars FROM app.user_merchants "
            "WHERE canonical_name = ?",
            [merchant_name],
        ).fetchone()
        assert merchant is not None
        assert list(merchant[1]) == ["Task 5 review decision"]
        assert db.execute(
            "SELECT status FROM app.categorization_decisions WHERE decision_id = ?",
            [seeded["first_categorization_id"]],
        ).fetchone() == ("accepted",)
        assert db.execute(
            "SELECT match_status FROM app.match_decisions WHERE match_id = ?",
            [seeded["stale_match_id"]],
        ).fetchone() == ("reversed",)
    merchant_metrics_after = _merchant_exemplar_metrics()
    assert str(merchant[0]) not in merchant_metrics_before
    assert merchant_metrics_after[str(merchant[0])] == 1


async def test_ordinary_already_decided_ids_return_structured_errors() -> None:
    _transaction_id, categorization_id, match_id, category = _seed_ordinary_decisions()
    decisions = [
        CategorizationDecisionRequest(
            kind="categorization",
            decision_id=categorization_id,
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
    assert second.error.code == "mutation_invalid_input"
    assert second.error.details is not None
    assert [
        (error["kind"], error["decision_id"], error["code"])
        for error in second.error.details["errors"]
    ] == [
        ("categorization", categorization_id, "mutation_constraint_violation"),
        ("match", match_id, "mutation_constraint_violation"),
    ]


async def test_categorization_reject_persists_and_leaves_pending_queue() -> None:
    transaction_id, categorization_id, _match_id, _category = _seed_ordinary_decisions()
    pending = await reviews_coarse(kind="categorization", status="pending")
    row = next(
        item for item in pending.data.rows if item.decision_id == categorization_id
    )
    assert row.details.transaction.transaction_id == transaction_id

    rejected = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
                decision="reject",
            )
        ]
    )

    assert rejected.error is None
    assert rejected.data.results[0].status == "rejected"
    pending_after = await reviews_coarse(kind="categorization", status="pending")
    assert categorization_id not in {
        item.decision_id for item in pending_after.data.rows
    }
    history = await reviews_coarse(kind="categorization", status="history")
    history_row = next(
        item for item in history.data.rows if item.decision_id == categorization_id
    )
    assert history_row.status == "rejected"


async def test_categorization_history_uses_immutable_attempt_snapshot() -> None:
    transaction_id, categorization_id, _match_id, category = _seed_ordinary_decisions()
    accepted = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
                decision="accept",
                category=category,
            )
        ]
    )
    assert accepted.error is None
    with get_database(read_only=False) as db:
        service = CategorizationService(db)
        service.create_category("Changed Later", actor="test")
        service.set_category(
            transaction_id,
            category="Changed Later",
            actor="test",
        )

    history = await reviews_coarse(kind="categorization", status="history")
    row = next(
        item for item in history.data.rows if item.decision_id == categorization_id
    )

    assert row.details.category == category
    assert row.details.category_id != "Changed Later"


async def test_categorization_clear_projects_next_versioned_attempt() -> None:
    transaction_id, categorization_id, _match_id, category = _seed_ordinary_decisions()
    accepted = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
                decision="accept",
                category=category,
            )
        ]
    )
    assert accepted.error is None
    with get_database(read_only=False) as db:
        CategorizationService(db).clear_category(transaction_id, actor="test")

    pending = await reviews_coarse(kind="categorization", status="pending")

    assert [item.decision_id for item in pending.data.rows] == [
        categorization_decision_id(transaction_id, attempt_number=2)
    ]


async def test_categorization_accept_undo_preserves_history_and_new_attempt() -> None:
    transaction_id, categorization_id, _match_id, category = _seed_ordinary_decisions()
    accepted = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
                decision="accept",
                category=category,
            )
        ]
    )
    assert accepted.error is None
    with get_database(read_only=False) as db:
        UndoService(db).undo(str(accepted.data.operation_id), actor="mcp")

    history = await reviews_coarse(kind="categorization", status="history")
    pending = await reviews_coarse(kind="categorization", status="pending")

    assert categorization_id in {item.decision_id for item in history.data.rows}
    assert [item.decision_id for item in pending.data.rows] == [
        categorization_decision_id(transaction_id, attempt_number=2)
    ]


async def test_categorization_reject_undo_preserves_history_and_new_attempt() -> None:
    transaction_id, categorization_id, _match_id, _category = _seed_ordinary_decisions()
    rejected = await reviews_decide_coarse(
        decisions=[
            CategorizationDecisionRequest(
                kind="categorization",
                decision_id=categorization_id,
                decision="reject",
            )
        ]
    )
    assert rejected.error is None
    with get_database(read_only=False) as db:
        UndoService(db).undo(str(rejected.data.operation_id), actor="mcp")

    history = await reviews_coarse(kind="categorization", status="history")
    pending = await reviews_coarse(kind="categorization", status="pending")

    assert categorization_id in {item.decision_id for item in history.data.rows}
    assert [item.decision_id for item in pending.data.rows] == [
        categorization_decision_id(transaction_id, attempt_number=2)
    ]


async def test_categorization_pending_uses_batch_attempt_projection() -> None:
    _seed_ordinary_decisions()

    with patch.object(
        CategorizationService,
        "review_decision_for_transaction",
        side_effect=AssertionError("per-row decision lookup"),
    ):
        response = await reviews_coarse(kind="categorization", status="pending")

    assert response.error is None
    assert len(response.data.rows) == 1


@contextmanager
def _human_confirms() -> Generator[None]:
    """Answer the confirmation prompt the way a person at the client would.

    An account merge no longer accepts a confirmation token, so a test that
    needs the merge to actually apply has to go through the prompt. Scoped
    rather than applied for the whole test: a batch loop that also exercises
    merchant and security links must leave those on the token path, and a
    patch that outlived its case would silently convert them.

    Tests that are about the token contract itself must NOT use this — they
    would stop exercising the thing they are named for.
    """
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))

    def _supports(_ctx: object) -> bool:
        return True

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
        mp.setattr("moneybin.mcp.confirmation.supports_elicitation", _supports)
        yield


async def test_identity_standard_boundary_accepts_and_rejects_each_domain() -> None:
    account_accept = _identity_account_setup("accept")
    account_reject = _identity_account_setup("reject")
    merchant_accept = _identity_merchant_setup("accept")
    merchant_reject = _identity_merchant_setup("reject")
    security_accept = _identity_security_setup("accept")
    security_reject = _identity_security_setup("reject")
    cases = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=account_accept["decision_id"],
            decision="accept",
            target_id=account_accept["candidate"],
        ),
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=account_reject["decision_id"],
            decision="reject",
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=merchant_accept["decision_id"],
            decision="accept",
            target_id=merchant_accept["merchant_id"],
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=merchant_reject["decision_id"],
            decision="reject",
        ),
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=security_accept["decision_id"],
            decision="accept",
            target_id=security_accept["survivor"],
        ),
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=security_reject["decision_id"],
            decision="reject",
        ),
    ]

    for request in cases:
        if request.decision != "accept":
            response = await identity_links_decide_coarse(decisions=[request])
        elif request.kind == "account_link":
            # A merge has no token path at all — the prompt is the only route.
            with _human_confirms():
                response = await identity_links_decide_coarse(decisions=[request])
        else:
            required = await identity_links_decide_coarse(decisions=[request])
            assert required.error is not None
            assert required.error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
            assert required.error.details is not None
            response = await identity_links_decide_coarse(
                decisions=[request],
                confirmation_token=str(required.error.details["confirmation_token"]),
            )

        assert response.error is None
        assert response.data.results[0].status == (
            "accepted" if request.decision == "accept" else "rejected"
        )
        assert (
            _identity_decision_status(request.kind, request.decision_id)
            == response.data.results[0].status
        )


async def test_identity_account_merge_rechecks_live_state_after_the_prompt() -> None:
    """Agreeing to a prompt approves one exact merge, not merges in general.

    Removing the token path must not also remove the recheck that rides on it:
    the grant is a digest of the batch plus its complete live before-state, and
    it is re-verified inside the write transaction. If the decision shifts
    between the prompt and the commit, the merge the person agreed to is not
    the merge about to run, and it must not proceed.

    The state change here is the same one the token version of this test used
    (``confidence_score``), so what moved is the confirmation vehicle, not the
    condition being detected.
    """
    setup = _identity_account_setup("state-mismatch")
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["candidate"],
        )
    ]

    _real_preview = _preview_identity_decisions

    # Shift the persisted decision out from under the approved digest at the
    # one moment it matters: after the batch is planned — so the binding the
    # person approves is the pre-drift one — and before apply re-plans it
    # inside the write transaction.
    def _drift_then_preview(requests: list[IdentityDecisionRequest]) -> Any:
        preview = _real_preview(requests)
        with get_database(read_only=False) as db:
            db.execute(
                """
                UPDATE app.account_link_decisions
                SET confidence_score = 0.61
                WHERE decision_id = ?
                """,
                [setup["decision_id"]],
            )
        return preview

    with _human_confirms():
        with patch.object(
            reviews_module,
            "_preview_identity_decisions",
            side_effect=_drift_then_preview,
        ):
            mismatched = await identity_links_decide_coarse(decisions=decisions)

    assert mismatched.error is not None
    assert mismatched.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert _identity_decision_status("account_link", setup["decision_id"]) == "pending"


async def test_identity_account_merge_is_refused_without_a_token_to_redeem() -> None:
    """A client that cannot prompt gets a refusal, not a key to its own merge.

    Every other destructive operation degrades to an opaque token here. For an
    account merge that degradation was the whole hole: the token is returned to
    the calling agent, so the documented "confirm before merging" contract was
    satisfiable without a person ever seeing it. The refusal must carry no
    token at all -- withholding it from the message while minting it anyway
    would leave the entry live for the very next call.
    """
    setup = _identity_account_setup("no-token")
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["candidate"],
        )
    ]

    refused = await identity_links_decide_coarse(decisions=decisions)

    assert refused.error is not None
    assert refused.error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert "confirmation_token" not in (refused.error.details or {})
    assert _identity_decision_status("account_link", setup["decision_id"]) == "pending"


async def test_identity_account_merge_refuses_a_supplied_token() -> None:
    """A token minted elsewhere must not buy its way past the merge prompt.

    Without this the refusal above is cosmetic: a caller holding any live token
    -- one issued for a sibling operation, or carried over from before this
    contract -- could still merge without a prompt.
    """
    setup = _identity_account_setup("token-refused")
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["candidate"],
        )
    ]

    refused = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token="any-token-at-all",  # noqa: S106  # opaque grant id, not a credential
    )

    assert refused.error is not None
    assert refused.error.code == error_codes.MUTATION_INVALID_INPUT
    assert _identity_decision_status("account_link", setup["decision_id"]) == "pending"


async def test_identity_mixed_accept_batch_forces_the_whole_batch_to_the_prompt() -> (
    None
):
    """One account accept strips the token path from every decision beside it.

    A merchant accept on its own degrades to a token, so a batch carrying both
    kinds is the boundary where this gate can be drawn wrong: reading "every
    decision is an account link" instead of "any decision is" would hand the
    mixed batch a token, and that batch still re-keys a transaction history.
    The single-kind tests above cannot see that difference.
    """
    account = _identity_account_setup("mixed-batch")
    merchant = _identity_merchant_setup("mixed-batch")
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=account["decision_id"],
            decision="accept",
            target_id=account["candidate"],
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=merchant["decision_id"],
            decision="accept",
            target_id=merchant["merchant_id"],
        ),
    ]

    refused = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token="any-token-at-all",  # noqa: S106  # opaque grant id, not a credential
    )
    degraded = await identity_links_decide_coarse(decisions=decisions)
    with _human_confirms():
        response = await identity_links_decide_coarse(decisions=decisions)

    assert refused.error is not None
    assert refused.error.code == error_codes.MUTATION_INVALID_INPUT
    assert degraded.error is not None
    assert degraded.error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert "confirmation_token" not in (degraded.error.details or {})
    assert response.error is None
    assert _identity_decision_status("account_link", account["decision_id"]) == (
        "accepted"
    )
    assert _identity_decision_status("merchant_link", merchant["decision_id"]) == (
        "accepted"
    )


async def test_identity_mixed_batch_refusal_names_the_decisions_left_to_resubmit() -> (
    None
):
    """A refusal that hides half the work left to do is a dead end.

    The batch is refused whole, so the merchant decision beside the merge is
    dropped too — but the CLI command the refusal names only completes the
    account link. A caller following the hint literally would merge and then
    believe the merchant decision had been applied.
    """
    account = _identity_account_setup("mixed-hint")
    merchant = _identity_merchant_setup("mixed-hint")
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=account["decision_id"],
            decision="accept",
            target_id=account["candidate"],
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=merchant["decision_id"],
            decision="accept",
            target_id=merchant["merchant_id"],
        ),
    ]

    mixed = await identity_links_decide_coarse(decisions=decisions)
    account_only = await identity_links_decide_coarse(decisions=decisions[:1])

    assert mixed.error is not None
    assert mixed.error.hint is not None
    assert "moneybin accounts links set" in mixed.error.hint
    assert "merchant_link" in mixed.error.hint
    # An account-only batch has nothing left over, so it must not grow the
    # sentence -- the note is about the remainder, not decoration.
    assert account_only.error is not None
    assert account_only.error.hint is not None
    assert "merchant_link" not in account_only.error.hint
    assert _identity_decision_status("merchant_link", merchant["decision_id"]) == (
        "pending"
    )


async def test_identity_refusal_counts_a_second_merge_the_cli_will_not_reach() -> None:
    """`accounts links set` decides one link, so a second merge stays pending.

    Both decisions are `account_link`, so a remainder derived from the batch's
    deduplicated kinds collapses them into the one excluded entry and reports
    nothing left over — the refusal then reads as if a single CLI call
    finishes the batch.
    """
    first = _identity_account_setup("two-merges-a")
    second = _identity_account_setup("two-merges-b")
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=first["decision_id"],
            decision="accept",
            target_id=first["candidate"],
        ),
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=second["decision_id"],
            decision="accept",
            target_id=second["candidate"],
        ),
    ]

    refused = await identity_links_decide_coarse(decisions=decisions)

    assert refused.error is not None
    assert refused.error.hint is not None
    assert "1 decision" in refused.error.hint
    assert "account_link" in refused.error.hint
    assert _identity_decision_status("account_link", first["decision_id"]) == "pending"
    assert _identity_decision_status("account_link", second["decision_id"]) == "pending"


async def test_identity_refusal_counts_a_reject_dropped_with_the_batch() -> None:
    """A reject is refused with the batch even though it is not an accept.

    Only accepts reach the batch's kind set, so a reject travelling beside a
    merge left no trace in the remainder — yet nothing was written and it has
    to be sent again like everything else.
    """
    account = _identity_account_setup("merge-plus-reject")
    merchant = _identity_merchant_setup("merge-plus-reject")
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=account["decision_id"],
            decision="accept",
            target_id=account["candidate"],
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=merchant["decision_id"],
            decision="reject",
        ),
    ]

    refused = await identity_links_decide_coarse(decisions=decisions)

    assert refused.error is not None
    assert refused.error.hint is not None
    assert "1 decision" in refused.error.hint
    assert "merchant_link" in refused.error.hint
    assert _identity_decision_status("merchant_link", merchant["decision_id"]) == (
        "pending"
    )


async def test_identity_security_persisted_state_mismatch_consumes_token() -> None:
    setup = _identity_security_setup("state-mismatch")
    decisions = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["survivor"],
        )
    ]
    required = await identity_links_decide_coarse(decisions=decisions)
    assert required.error is not None
    assert required.error.details is not None
    token = str(required.error.details["confirmation_token"])
    with get_database(read_only=False) as db:
        db.execute(
            "UPDATE app.securities SET name = ? WHERE security_id = ?",
            ["Changed after confirmation", setup["survivor"]],
        )

    mismatched = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token=token,
    )
    replayed = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token=token,
    )

    assert mismatched.error is not None
    assert mismatched.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert replayed.error is not None
    assert replayed.error.code == error_codes.MUTATION_CONFIRMATION_REPLAYED
    assert _identity_decision_status("security_link", setup["decision_id"]) == "pending"


async def test_identity_security_confirmation_ignores_unrelated_catalog_state() -> None:
    setup = _identity_security_setup("stable")
    decisions = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["survivor"],
        )
    ]
    required = await identity_links_decide_coarse(decisions=decisions)
    assert required.error is not None
    assert required.error.details is not None
    _mint_identity_security(
        name="Unrelated security",
        created_by="user",
        ticker="OTHER",
    )

    response = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert response.error is None
    assert _identity_decision_status("security_link", setup["decision_id"]) == (
        "accepted"
    )


def test_every_preparer_reports_every_blast_radius_category() -> None:
    """Set equality, because the two failure directions are opposite and silent.

    ``_identity_binding`` indexes ``affected_ids`` by
    ``IDENTITY_BLAST_RADIUS_CATEGORIES``. A preparer missing a key raises KeyError
    inside the confirm path — loud, but only for whichever domain the batch happens
    to touch. A preparer carrying a key the constant omits is the dangerous one: it
    reports no error at all and the confirmation prompt just never mentions those
    rows, which is exactly how a merge came to move every price override behind a
    summary that counted five categories.
    """
    account = _identity_account_setup("radius")
    merchant = _identity_merchant_setup("radius")
    security = _identity_security_setup("radius")
    requests = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=account["decision_id"],
            decision="accept",
            target_id=account["candidate"],
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=merchant["decision_id"],
            decision="accept",
            target_id=merchant["merchant_id"],
        ),
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=security["decision_id"],
            decision="accept",
            target_id=security["survivor"],
        ),
    ]
    with get_database(read_only=True) as db:
        plan = ReviewDecisionsService(db, actor="mcp").plan_identity(list(requests))

    assert len(plan.items) == 3
    for item in plan.items:
        assert set(item.affected_ids) == set(IDENTITY_BLAST_RADIUS_CATEGORIES), (
            f"{item.request.kind} preparer disagrees with the category constant"
        )


async def test_identity_decide_accepts_a_feed_key_decision() -> None:
    """The coarse path must route a feed key to the bind, as the fine one does.

    ``_prepare_security`` called ``accept_impact()`` for every accept. That
    preflight demands an already-accepted binding to move onto the survivor, which
    a feed-key decision never has, so this tool refused every ``tiingo_ticker`` and
    ``coingecko_slug`` row with "nothing to merge away" — while its own description
    advertised those decisions as supported. The fine-grained
    ``investments_securities_links_set`` routed correctly the whole time, so the
    queue was drainable by one surface and permanently stuck on the other.
    """
    setup = _identity_feed_key_setup("acc")
    decisions = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["security"],
        )
    ]

    required = await identity_links_decide_coarse(decisions=decisions)
    assert required.error is not None
    assert required.error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert required.error.details is not None
    response = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert response.error is None
    assert _identity_decision_status("security_link", setup["decision_id"]) == (
        "accepted"
    )
    assert _accepted_feed_binding(setup["ref_value"]) == setup["security"]


async def test_identity_decide_deletes_nothing_when_binding_a_feed_key() -> None:
    """A bind is not a merge, and the merge path ends by DELETEing a catalog row.

    Reaching that step with a feed-key decision would destroy the very security the
    user is trying to price. Asserted separately from the accept because a routing
    fix that bound the link but still ran the merge's cascade would satisfy the
    other test alone.
    """
    setup = _identity_feed_key_setup("keep")
    decisions = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["security"],
        )
    ]

    required = await identity_links_decide_coarse(decisions=decisions)
    assert required.error is not None
    assert required.error.details is not None
    await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert _security_exists(setup["security"])


async def test_identity_confirmation_counts_the_price_marks_a_merge_moves() -> None:
    """A row the merge mutates but the prompt omits is a write nobody agreed to.

    ``accept_merge`` re-points every override onto the survivor, but the coarse
    confirmation counted only accounts, merchants, securities, transactions, and
    lots — so a user with hand-authored valuations approved a batch whose summary
    never mentioned them.
    """
    setup = _identity_security_setup("marks")
    _seed_price_mark(setup["provisional"], date(2026, 7, 1))
    _seed_price_mark(setup["provisional"], date(2026, 7, 2))
    decisions = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["survivor"],
        )
    ]

    required = await identity_links_decide_coarse(decisions=decisions)

    assert required.error is not None
    assert required.error.details is not None
    assert required.error.details["blast_radius"] == {
        "accounts": 0,
        "merchants": 0,
        "securities": 2,
        "transactions": 0,
        "lots": 0,
        "price_marks": 2,
    }


async def test_a_price_mark_authored_after_preview_invalidates_the_token() -> None:
    """Approval binds to the rows the merge will move, marks included.

    A mark authored between preview and submission is a valuation the user never
    saw and never agreed to re-point. Counting marks in the blast radius is what
    makes the recomputed binding differ, so the stale grant is refused instead of
    silently carrying a wider merge than the one that was approved.
    """
    setup = _identity_security_setup("mark-drift")
    decisions = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["survivor"],
        )
    ]
    required = await identity_links_decide_coarse(decisions=decisions)
    assert required.error is not None
    assert required.error.details is not None
    token = str(required.error.details["confirmation_token"])
    _seed_price_mark(setup["provisional"], date(2026, 7, 3))

    mismatched = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token=token,
    )

    assert mismatched.error is not None
    assert mismatched.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert _identity_decision_status("security_link", setup["decision_id"]) == "pending"


async def test_identity_confirmation_uses_exact_merchant_blast_radius() -> None:
    setup = _identity_merchant_setup("blast")
    _seed_identity_merchant_transaction(
        "merchant-blast-existing",
        setup["merchant_id"],
    )
    decisions = [
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["merchant_id"],
        )
    ]

    required = await identity_links_decide_coarse(decisions=decisions)

    assert required.error is not None
    assert required.error.details is not None
    assert required.error.details["blast_radius"] == {
        "accounts": 0,
        "merchants": 1,
        "securities": 0,
        "transactions": 0,
        "lots": 0,
        "price_marks": 0,
    }
    _seed_identity_merchant_transaction(
        "merchant-blast-unrelated",
        setup["merchant_id"],
    )
    response = await identity_links_decide_coarse(
        decisions=decisions,
        confirmation_token=str(required.error.details["confirmation_token"]),
    )
    assert response.error is None


async def test_identity_account_batch_rejects_intermediate_merge_graph() -> None:
    first = _identity_account_setup("graph-a-b")
    _seed_identity_account("GRAPH_C", "Graph account C")
    _seed_identity_account_link("GRAPH_C", "link-graph-c-a")
    _insert_account_link_decision(
        decision_id="account-graph-c-a",
        provisional_account_id="GRAPH_C",
        candidate_account_id=first["provisional"],
        status="pending",
        decided_at=_NOW,
    )

    response = await identity_links_decide_coarse(
        decisions=[
            AccountLinkDecisionRequest(
                kind="account_link",
                decision_id=first["decision_id"],
                decision="accept",
                target_id=first["candidate"],
            ),
            AccountLinkDecisionRequest(
                kind="account_link",
                decision_id="account-graph-c-a",
                decision="accept",
                target_id=first["provisional"],
            ),
        ]
    )

    assert response.error is not None
    assert response.error.code == error_codes.MUTATION_INVALID_INPUT
    assert response.error.details is not None
    assert response.error.details["errors"][0]["index"] == 1
    assert "intermediate" in response.error.details["errors"][0]["reason"]
    assert _identity_decision_status("account_link", first["decision_id"]) == "pending"
    assert _identity_decision_status("account_link", "account-graph-c-a") == "pending"


async def test_identity_mixed_late_failure_rolls_back_then_shares_operation_id() -> (
    None
):
    account = _identity_account_setup("atomic")
    merchant = _identity_merchant_setup("atomic")
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=account["decision_id"],
            decision="reject",
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id=merchant["decision_id"],
            decision="reject",
        ),
    ]

    with patch.object(
        MerchantLinksService,
        "set",
        side_effect=RuntimeError("injected late failure"),
    ):
        failed = await identity_links_decide_coarse(decisions=decisions)
        assert failed.error is not None
        assert failed.error.code == error_codes.INFRA_UNCLASSIFIED_ERROR

    assert _identity_decision_status("account_link", account["decision_id"]) == (
        "pending"
    )
    assert _identity_decision_status("merchant_link", merchant["decision_id"]) == (
        "pending"
    )

    response = await identity_links_decide_coarse(decisions=decisions)

    assert response.error is None
    assert len({item.operation_id for item in response.data.results}) == 1
    assert response.data.results[0].operation_id == response.data.operation_id
    with get_database(read_only=True) as db:
        audit_operation_ids = {
            str(row[0])
            for row in db.execute(
                """
                SELECT DISTINCT operation_id
                FROM app.audit_log
                WHERE target_id IN (?, ?)
                """,
                [account["decision_id"], merchant["decision_id"]],
            ).fetchall()
        }
    assert audit_operation_ids == {response.data.operation_id}


async def test_identity_standard_write_reports_its_prompt_disclosure_tier() -> None:
    """The privacy event records what the tool may show, not what it returns.

    The payload is three low-tier classes, but the merge prompt renders ledger
    dates and user-written account labels, so the tool declares
    ``discloses=Tier.MEDIUM`` and the event records ``medium``.

    This batch rejects a *merchant* link — it renders no ledger facts and elicits
    nothing — and still reports ``medium``. That is the declared maximum working
    as intended rather than a miscount: a static declaration is per-tool, so it
    over-reports on the calls that disclose less. Over-reporting a tier is the
    safe direction; the per-call alternative would have to be trusted to lower
    itself correctly on every path.

    ``classes_returned`` stays payload-derived, which is the other half: the
    declaration raises the tier without inventing data classes the response
    never carried.
    """
    setup = _identity_merchant_setup("sensitivity")
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(register_review_coarse_writes)

    with patch("moneybin.mcp.decorator.write_privacy_event", captured.append):
        response = await call_tool_raw(
            mcp,
            "identity_links_decide",
            {
                "decisions": [
                    {
                        "kind": "merchant_link",
                        "decision_id": setup["decision_id"],
                        "decision": "reject",
                    }
                ]
            },
        )

    assert response.structuredContent is not None
    assert response.structuredContent["status"] == "ok"
    assert len(captured) == 1
    assert captured[0]["sensitivity"] == "medium"
    assert captured[0]["classes_returned"] == [
        "aggregate",
        "record_id",
        "txn_type",
    ]


def test_identity_confirmation_binds_order_state_ids_and_blast_radius() -> None:
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="accept",
            target_id="account-target",
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id="merchant-decision",
            decision="reject",
        ),
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id="security-decision",
            decision="accept",
            target_id="security-target",
        ),
    ]
    plan = _identity_plan(decisions)

    binding = _identity_binding(decisions, plan)

    assert binding.arguments["decisions"] == [
        decision.model_dump(mode="json") for decision in decisions
    ]
    assert binding.arguments["before_state"] == [
        {"version": "initial", "index": 0},
        {"version": "initial", "index": 1},
        {"version": "initial", "index": 2},
    ]
    assert binding.resolved_ids == (
        "account-decision",
        "account_link-source",
        "account-target",
        "merchant-decision",
        "merchant_link-source",
        "merchant_link-candidate",
        "security-decision",
        "security_link-source",
        "security-target",
    )
    assert binding.blast_radius == {
        "accounts": 1,
        "merchants": 1,
        "securities": 1,
        "transactions": 3,
        "lots": 1,
        "price_marks": 0,
    }


def test_identity_plan_ignores_unchanged_accept_for_destructive_gate() -> None:
    """Only an accept that will materially transition state needs confirmation."""
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="already-accepted",
            decision="accept",
            target_id="account-target",
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id="pending-reject",
            decision="reject",
        ),
    ]
    initial = _identity_plan(decisions)
    plan = IdentityDecisionPlan(
        items=(replace(initial.items[0], changed=False), initial.items[1])
    )

    assert plan.changed_count == 1
    assert plan.destructive is False


async def test_identity_confirmation_rechecks_live_state_and_consumes_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the full token protocol: binding recheck, mismatch, single use.

    Uses a merchant link rather than an account one because an account merge
    has no token path left to pin — it takes the prompt or nothing. The
    account path's own live-state recheck is covered by
    ``test_identity_account_merge_rechecks_live_state_after_the_prompt``.
    """
    decisions = [
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id="merchant-decision",
            decision="accept",
            target_id="merchant-target",
        )
    ]
    initial = _identity_plan(decisions)
    changed = _identity_plan(decisions, state_version="changed")
    monkeypatch.setattr(
        "moneybin.mcp.confirmation._active_context",
        lambda: None,
    )

    with patch.object(
        reviews_module,
        "_preview_identity_decisions",
        return_value=_identity_preview(initial),
    ):
        required = await identity_links_decide_coarse(decisions=decisions)

    assert required.error is not None
    assert required.error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert required.error.details is not None
    token = str(required.error.details["confirmation_token"])

    def reject_changed_state(
        requests: list[object],
        *,
        grant: object,
        expected_binding: object,
    ) -> IdentityDecisionPlan:
        del expected_binding
        assert grant is not None
        grant.verify(_identity_binding(decisions, changed))  # type: ignore[attr-defined]
        raise AssertionError("changed binding must be rejected")

    with (
        patch.object(
            reviews_module,
            "_preview_identity_decisions",
            return_value=_identity_preview(initial),
        ),
        patch.object(
            reviews_module,
            "_apply_identity_decisions",
            side_effect=reject_changed_state,
        ),
    ):
        mismatched = await identity_links_decide_coarse(
            decisions=decisions,
            confirmation_token=token,
        )
        replayed = await identity_links_decide_coarse(
            decisions=decisions,
            confirmation_token=token,
        )

    assert mismatched.error is not None
    assert mismatched.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert replayed.error is not None
    assert replayed.error.code == error_codes.MUTATION_CONFIRMATION_REPLAYED


async def test_identity_reject_batch_uses_public_privacy_actor() -> None:
    decisions = [
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id="merchant-decision",
            decision="reject",
        )
    ]
    plan = _identity_plan(decisions)
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(register_review_coarse_writes)

    with (
        patch.object(
            reviews_module,
            "_preview_identity_decisions",
            return_value=_identity_preview(plan),
        ),
        patch.object(reviews_module, "_apply_identity_decisions", return_value=plan),
        patch("moneybin.mcp.decorator.write_privacy_event", captured.append),
    ):
        response = await call_tool_raw(
            mcp,
            "identity_links_decide",
            {
                "decisions": [
                    decision.model_dump(mode="json", exclude_none=True)
                    for decision in decisions
                ]
            },
        )

    assert response.structuredContent is not None
    assert response.structuredContent["data"]["applied_count"] == 1
    assert len(captured) == 1
    assert captured[0]["actor"] == "mcp.identity_links_decide"


def test_identity_batch_rolls_back_before_outcome_metrics_on_late_failure() -> None:
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="reject",
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id="merchant-decision",
            decision="reject",
        ),
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id="security-decision",
            decision="reject",
        ),
    ]
    plan = _identity_plan(decisions)
    db = MagicMock()
    service = ReviewDecisionsService(db, actor="mcp")

    with (
        patch(
            "moneybin.services.review_decisions_service.AccountLinksService"
        ) as account_class,
        patch(
            "moneybin.services.review_decisions_service.MerchantLinksService"
        ) as merchant_class,
        patch(
            "moneybin.services.review_decisions_service.SecurityLinksService"
        ) as security_class,
        patch.object(service, "plan_identity", return_value=plan),
    ):
        account_service = account_class.return_value
        merchant_service = merchant_class.return_value
        security_service = security_class.return_value
        merchant_service.set.side_effect = RuntimeError("late merchant failure")
        with (
            pytest.raises(RuntimeError, match="late merchant failure"),
        ):
            service.apply_identity(decisions, verify=lambda _: None)

    db.begin.assert_called_once_with()
    db.rollback.assert_called_once_with()
    db.commit.assert_not_called()
    account_service.record_committed_outer_decisions.assert_not_called()
    merchant_service.record_committed_outer_outcomes.assert_not_called()
    security_service.record_committed_outer_outcomes.assert_not_called()


def test_identity_batch_reruns_the_matcher_after_an_accepted_merge() -> None:
    """A batched merge is still a merge — it must re-match once the batch commits.

    ``set`` returns early under ``in_outer_txn`` and never reaches its own
    post-commit tail, so the batch path owns this trigger. Without it, driving
    the same accept through ``identity_links_decide`` instead of
    ``accounts_links_set`` silently keeps the duplicate.
    """
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="accept",
            target_id="account-candidate",
        ),
    ]
    plan = _identity_plan(decisions)
    db = MagicMock()
    service = ReviewDecisionsService(db, actor="mcp")

    with (
        patch(
            "moneybin.services.review_decisions_service.AccountLinksService"
        ) as account_class,
        patch.object(service, "plan_identity", return_value=plan),
    ):
        account_service = account_class.return_value
        service.apply_identity(decisions, verify=lambda _: None)

    db.commit.assert_called_once_with()
    account_service.rematch_after_merge.assert_called_once_with()


def test_identity_batch_of_rejects_does_not_rerun_the_matcher() -> None:
    """A reject repoints nothing, so no account gains new dedup candidates."""
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="reject",
        ),
    ]
    plan = _identity_plan(decisions)
    db = MagicMock()
    service = ReviewDecisionsService(db, actor="mcp")

    with (
        patch(
            "moneybin.services.review_decisions_service.AccountLinksService"
        ) as account_class,
        patch.object(service, "plan_identity", return_value=plan),
    ):
        account_service = account_class.return_value
        service.apply_identity(decisions, verify=lambda _: None)

    account_service.rematch_after_merge.assert_not_called()


def test_identity_batch_of_an_unchanged_accept_does_not_rerun_the_matcher() -> None:
    """An accept that changed nothing repointed nothing, so there is nothing to re-match.

    ``account_merged`` requires ``changed`` as well as ``decision == "accept"``,
    because a decision resubmitted at its current status takes
    ``_prepare_account``'s ``current == status`` branch and writes no repoint.
    The reject case cannot prove this: it would pass with the ``changed``
    conjunct deleted. This fixture keeps the accept and removes only the change,
    so dropping that conjunct turns it red.
    """
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="accept",
            target_id="account-candidate",
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id="merchant-decision",
            decision="reject",
        ),
    ]
    plan = _identity_plan(decisions)
    # Only the account accept is unchanged; the merchant reject keeps
    # changed_count above zero so apply_identity does not short-circuit.
    plan = replace(
        plan,
        items=(replace(plan.items[0], changed=False), plan.items[1]),
    )
    db = MagicMock()
    service = ReviewDecisionsService(db, actor="mcp")

    with (
        patch(
            "moneybin.services.review_decisions_service.AccountLinksService"
        ) as account_class,
        patch("moneybin.services.review_decisions_service.MerchantLinksService"),
        patch.object(service, "plan_identity", return_value=plan),
    ):
        account_service = account_class.return_value
        service.apply_identity(decisions, verify=lambda _: None)

    account_service.rematch_after_merge.assert_not_called()


def test_identity_batch_carries_the_rematch_outcome_out() -> None:
    """Firing the pass is not enough — the batch must report what it did.

    ``identity_links_decide`` is the seam an agent drives, and the pass it
    triggers can auto-merge rows without asking. If the ``RefreshResult`` stops
    inside ``apply_identity``, a batched accept returns an apparently clean
    merge while silently collapsing duplicates — the exact failure the direct
    ``accounts_links_set`` path reports and this one would not.
    """
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="accept",
            target_id="account-candidate",
        ),
    ]
    plan = _identity_plan(decisions)
    db = MagicMock()
    service = ReviewDecisionsService(db, actor="mcp")

    with (
        patch(
            "moneybin.services.review_decisions_service.AccountLinksService"
        ) as account_class,
        patch.object(service, "plan_identity", return_value=plan),
    ):
        account_class.return_value.rematch_after_merge.return_value = RefreshResult(
            applied=True,
            duration_seconds=0.0,
            matches_auto_merged=2,
            matches_pending_review=5,
        )
        result = service.apply_identity(decisions, verify=lambda _: None)

    assert result.rematch is not None
    assert result.rematch.matches_auto_merged == 2
    assert result.rematch.matches_pending_review == 5


def test_identity_batch_of_rejects_carries_no_rematch_outcome() -> None:
    """No pass ran, so the batch reports absence rather than a zero."""
    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="reject",
        ),
    ]
    plan = _identity_plan(decisions)
    db = MagicMock()
    service = ReviewDecisionsService(db, actor="mcp")

    with (
        patch("moneybin.services.review_decisions_service.AccountLinksService"),
        patch.object(service, "plan_identity", return_value=plan),
    ):
        result = service.apply_identity(decisions, verify=lambda _: None)

    assert result.rematch is None


async def _decide_with_rematch(
    monkeypatch: pytest.MonkeyPatch, rematch: RefreshResult | None
) -> ResponseEnvelope[IdentityLinksDecidePayload]:
    """Drive identity_links_decide with a stubbed apply carrying ``rematch``.

    Reaches the tool wrapper itself rather than ``apply_identity`` below it,
    because the field population and actions[] ordering are the wiring under
    test — the service-level tests above cannot see either.
    """
    from types import SimpleNamespace

    decisions: list[IdentityDecisionRequest] = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="accept",
            target_id="account-candidate",
        ),
    ]
    plan = _identity_plan(decisions)
    applied = replace(plan, rematch=rematch)

    def _verify(_binding: object) -> None:
        return None

    def _preview(_decisions: object) -> object:
        return SimpleNamespace(plan=plan, merges=(), kinds=("account_link",))

    async def _granted(**_kw: object) -> object:
        return SimpleNamespace(verify=_verify)

    def _apply(_decisions: object, **_kw: object) -> IdentityDecisionPlan:
        return applied

    monkeypatch.setattr(reviews_module, "_preview_identity_decisions", _preview)
    monkeypatch.setattr(reviews_module, "grant_confirmation_or_raise", _granted)
    monkeypatch.setattr(reviews_module, "_apply_identity_decisions", _apply)
    return await reviews_module.identity_links_decide_coarse(decisions=decisions)


async def test_identity_links_decide_reports_the_rematch_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The batch tool's payload carries the same three counts as the direct one."""
    envelope = await _decide_with_rematch(
        monkeypatch,
        RefreshResult(
            applied=True,
            duration_seconds=0.0,
            matches_auto_merged=2,
            matches_pending_review=5,
            matches_pending_transfers=3,
        ),
    )

    assert envelope.data.rematch_auto_merged == 2
    assert envelope.data.rematch_pending_review == 5
    assert envelope.data.rematch_pending_transfers == 3
    assert any("5 new duplicate" in action for action in envelope.actions)
    assert any("3 possible transfer" in action for action in envelope.actions)


async def test_identity_links_decide_flags_a_failed_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A half-failed pass reaches the batch tool's actions, not just the direct one."""
    envelope = await _decide_with_rematch(
        monkeypatch,
        RefreshResult(
            applied=False,
            duration_seconds=1.0,
            error="sqlmesh apply failed",
            matching_error="matcher blew up",
        ),
    )

    assert any("rebuild" in action.lower() for action in envelope.actions)
    assert any("stopped partway" in action for action in envelope.actions)


async def test_identity_links_decide_reports_no_rematch_when_none_ran(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No pass ran, so all three counts are absent rather than zero-as-fact."""
    envelope = await _decide_with_rematch(monkeypatch, None)

    assert envelope.data.rematch_auto_merged is None
    assert envelope.data.rematch_pending_review is None
    assert envelope.data.rematch_pending_transfers is None


def test_identity_batch_emits_each_domain_metric_only_after_commit() -> None:
    decisions = [
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id="account-decision",
            decision="reject",
        ),
        MerchantLinkDecisionRequest(
            kind="merchant_link",
            decision_id="merchant-decision",
            decision="reject",
        ),
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id="security-decision",
            decision="reject",
        ),
    ]
    plan = _identity_plan(decisions)
    db = MagicMock()
    service = ReviewDecisionsService(db, actor="mcp")

    with (
        patch(
            "moneybin.services.review_decisions_service.AccountLinksService"
        ) as account_class,
        patch(
            "moneybin.services.review_decisions_service.MerchantLinksService"
        ) as merchant_class,
        patch(
            "moneybin.services.review_decisions_service.SecurityLinksService"
        ) as security_class,
        patch.object(service, "plan_identity", return_value=plan),
    ):
        result = service.apply_identity(decisions, verify=lambda _: None)

    # Equality, not identity: apply_identity now returns a copy carrying the
    # post-merge rematch outcome. On this all-rejects batch that outcome is
    # None, so the copy equals the plan field for field.
    assert result == plan
    db.commit.assert_called_once_with()
    db.rollback.assert_not_called()
    account_class.return_value.record_committed_outer_decisions.assert_called_once_with()
    merchant_class.return_value.record_committed_outer_outcomes.assert_called_once_with((
        "rejected",
    ))
    security_class.return_value.record_committed_outer_outcomes.assert_called_once_with((
        "rejected",
    ))


async def test_review_standard_write_registrar_is_closed_and_max_risk() -> None:
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


def _seed_investment_history(security_id: str, label: str) -> None:
    """One core ledger event and one open tax lot on ``security_id``.

    A bind moves neither. They exist so the blast radius has something it could
    wrongly claim: against an empty candidate every gating rule reports the same
    empty tuple and the test cannot tell them apart.

    The two fact tables are created here rather than in the shared MCP template
    because ``create_core_tables_raw`` does not build them, and
    ``_query_ids`` swallows the resulting CatalogException and returns ``()``.
    Every blast-radius assertion in this module was therefore reading an empty
    tuple that no gating rule could have changed — which is precisely how a bind
    came to claim every transaction and lot of its candidate unnoticed.
    """
    with get_database(read_only=False) as db:
        db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
        db.execute(CORE_FCT_INVESTMENT_LOTS_DDL)
        db.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity, amount, currency_code)
            VALUES (?, 'ACC001', ?, DATE '2026-01-15', 'buy', 10, -1500.00, 'USD')
            """,  # noqa: S608  # test fixture insert, static SQL
            [f"txn-{label}", security_id],
        )
        db.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 original_quantity, remaining_quantity, currency_code, is_open)
            VALUES (?, 'ACC001', ?, DATE '2026-01-15', 10, 10, 'USD', TRUE)
            """,  # noqa: S608  # test fixture insert, static SQL
            [f"lot-{label}", security_id],
        )


def test_a_feed_key_bind_claims_no_transaction_and_no_lot() -> None:
    """A bind creates one link. It re-points nothing, so it moves nothing.

    Reporting the candidate's whole ledger as "affected" is wrong twice. It
    contradicts the bind's own mutation contract in the prompt a human reads, and
    — because the blast radius is part of the digest their approval is bound to —
    it makes any refresh or ledger write between preview and commit invalidate a
    grant for rows the bind never touches.

    ``securities`` is asserted non-empty in the same breath: a fix that simply
    blanked the radius for feed keys would satisfy the first two assertions and
    understate a write that genuinely does make one security priceable.
    """
    setup = _identity_feed_key_setup("radius")
    _seed_investment_history(setup["security"], "radius")

    with get_database(read_only=True) as db:
        plan = ReviewDecisionsService(db, actor="mcp").plan_identity([
            SecurityLinkDecisionRequest(
                kind="security_link",
                decision_id=setup["decision_id"],
                decision="accept",
                target_id=setup["security"],
            )
        ])

    (item,) = plan.items
    assert item.affected_ids["transactions"] == ()
    assert item.affected_ids["lots"] == ()
    assert item.affected_ids["securities"] == (setup["security"],)


def _snapshot_marks(item: IdentityDecisionPlanItem) -> list[object]:
    """The price-mark rows a plan item bound its approval to."""
    state = cast("dict[str, list[object]]", item.before_state)
    return state["security_price_overrides"]


def test_a_feed_key_bind_snapshots_none_of_the_candidates_marks() -> None:
    """A bind's source_id IS the survivor, so its whole mark history was captured.

    `before_state` was ungated on the theory that it reads only rows this
    decision can change. True for a merge, which re-points every override — but
    a bind changes no mark at all, and its `source_id` is the existing candidate
    rather than a provisional about to disappear. Snapshotting there binds the
    approval to a security's entire valuation history, so a mark the user edits
    between elicitation and submit rejects the whole batch as mismatched on
    state the bind never touched.
    """
    setup = _identity_feed_key_setup("marks-bind")
    _seed_price_mark(setup["security"], date(2026, 7, 1))
    _seed_price_mark(setup["security"], date(2026, 7, 2))

    with get_database(read_only=True) as db:
        plan = ReviewDecisionsService(db, actor="mcp").plan_identity([
            SecurityLinkDecisionRequest(
                kind="security_link",
                decision_id=setup["decision_id"],
                decision="accept",
                target_id=setup["security"],
            )
        ])

    (item,) = plan.items
    assert _snapshot_marks(item) == []


def test_an_identity_merge_still_snapshots_the_marks_it_re_points() -> None:
    """Paired with the bind above: the gate must not blank the merge's snapshot.

    A merge does move these rows, so they are exactly the state its approval has
    to be bound to — dropping them here would let a mark authored after preview
    ride along on a stale grant, which is the failure
    `test_a_price_mark_authored_after_preview_invalidates_the_token` exists for.
    """
    setup = _identity_security_setup("marks-snapshot")
    _seed_price_mark(setup["provisional"], date(2026, 7, 1))

    with get_database(read_only=True) as db:
        plan = ReviewDecisionsService(db, actor="mcp").plan_identity([
            SecurityLinkDecisionRequest(
                kind="security_link",
                decision_id=setup["decision_id"],
                decision="accept",
                target_id=setup["survivor"],
            )
        ])

    (item,) = plan.items
    assert len(_snapshot_marks(item)) == 1


def test_an_identity_merge_still_claims_the_rows_it_re_points() -> None:
    """The merge side must keep counting what it moves.

    Paired with the bind above: gating the transaction and lot queries on the
    wrong flag in either direction passes one test and fails the other, and a
    merge that under-reports its blast radius is the more dangerous of the two.
    """
    setup = _identity_security_setup("radius-merge")
    _seed_investment_history(setup["provisional"], "radius-merge")

    with get_database(read_only=True) as db:
        plan = ReviewDecisionsService(db, actor="mcp").plan_identity([
            SecurityLinkDecisionRequest(
                kind="security_link",
                decision_id=setup["decision_id"],
                decision="accept",
                target_id=setup["survivor"],
            )
        ])

    (item,) = plan.items
    assert item.affected_ids["transactions"] == ("txn-radius-merge",)
    assert item.affected_ids["lots"] == ("lot-radius-merge",)


def _seed_account_ledger(account_id: str, label: str, rows: int) -> None:
    """Give an account ``rows`` transactions so its ledger size identifies it."""
    with get_database(read_only=False) as db:
        for i in range(rows):
            db.execute(
                """
                INSERT INTO core.fct_transactions
                    (transaction_id, account_id, transaction_date, amount)
                VALUES (?, ?, ?, ?)
                """,
                [
                    f"txn-{label}-{i}",
                    account_id,
                    date(2026, 5, 1 + i),
                    Decimal("-9.00"),
                ],
            )


async def test_identity_preview_absorbs_the_source_into_the_target(
    mcp_db: object,
) -> None:
    """The batch preview folds the provisional account into the candidate.

    Every other test in this module patches ``_preview_identity_decisions``
    out, so its body — which maps ``item.source_id`` and ``item.target_id``
    onto absorbed and survivor — has never run under test. A swap there
    produces a confirmation prompt describing the merge backwards while the
    renderer's own tests, which build ``merges`` by hand, stay green.

    The two ledgers are sized differently on purpose: asserting ids alone would
    still pass if the pair were read into the wrong roles, since both ids
    appear either way.
    """
    setup = _identity_account_setup("preview-direction")
    _seed_account_ledger(setup["provisional"], "prov-direction", rows=2)
    _seed_account_ledger(setup["candidate"], "cand-direction", rows=4)

    preview = _preview_identity_decisions([
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=setup["decision_id"],
            decision="accept",
            target_id=setup["candidate"],
        )
    ])

    (merge,) = preview.merges
    assert preview.kinds == ("account_link",)
    assert merge.absorbed.account_id == setup["provisional"]
    assert merge.absorbed.transactions == 2
    assert merge.survivor.account_id == setup["candidate"]
    assert merge.survivor.transactions == 4


async def test_identity_preview_describes_every_account_merge_in_the_batch(
    mcp_db: object,
) -> None:
    """Two account links in one batch produce two merge descriptions, not one.

    The tool takes an ordered list and nothing bounds it to a single account
    merge, but the ``merges`` comprehension has only ever run over a one-accept
    batch. A per-accept map that dropped past the first, or paired the wrong
    source with the wrong target across two accepts, would hand the human a
    prompt describing one merge while committing two.

    Each of the four ledgers is a different size, so a pair read into the wrong
    roles — or a merge built from one decision's source and the other's target —
    fails on the counts rather than passing on ids that appear either way.
    """
    first = _identity_account_setup("preview-batch-one")
    second = _identity_account_setup("preview-batch-two")
    _seed_account_ledger(first["provisional"], "prov-batch-one", rows=2)
    _seed_account_ledger(first["candidate"], "cand-batch-one", rows=4)
    _seed_account_ledger(second["provisional"], "prov-batch-two", rows=3)
    _seed_account_ledger(second["candidate"], "cand-batch-two", rows=5)

    preview = _preview_identity_decisions([
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=first["decision_id"],
            decision="accept",
            target_id=first["candidate"],
        ),
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=second["decision_id"],
            decision="accept",
            target_id=second["candidate"],
        ),
    ])

    assert [merge.absorbed.account_id for merge in preview.merges] == [
        first["provisional"],
        second["provisional"],
    ]
    assert [merge.survivor.account_id for merge in preview.merges] == [
        first["candidate"],
        second["candidate"],
    ]
    assert [merge.absorbed.transactions for merge in preview.merges] == [2, 3]
    assert [merge.survivor.transactions for merge in preview.merges] == [4, 5]


async def test_identity_preview_describes_no_merge_for_a_reject(
    mcp_db: object,
) -> None:
    """A rejected link moves no history, so the prompt must not describe one.

    ``merges`` is filtered on ``decision == "accept"``. Losing that filter
    would render the whole absorbed-into-survivor block — "its transactions
    move onto that account's history" — above a decision that keeps the two
    accounts apart.
    """
    setup = _identity_account_setup("preview-reject")
    _seed_account_ledger(setup["provisional"], "prov-reject", rows=2)

    preview = _preview_identity_decisions([
        AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=setup["decision_id"],
            decision="reject",
        )
    ])

    assert preview.merges == ()
