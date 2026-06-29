"""Tests for merchants_links_* MCP tools (M1T).

Mirrors test_accounts_links.py for the merchant-links review surface. All
tests use the mcp_db fixture (session-template DB + monkeypatch for
get_settings/SecretStore).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.merchants import (
    merchants_links_history,
    merchants_links_pending,
    merchants_links_run,
    merchants_links_set,
    register_merchants_tools,
)
from moneybin.services.merchant_resolver import HarvestResult

pytestmark = pytest.mark.usefixtures("mcp_db")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime.now(tz=UTC).isoformat()


def _insert_decision(
    *,
    decision_id: str,
    ref_kind: str = "merchant_entity_id",
    ref_value: str = "entity_abc123",
    source_type: str = "plaid",
    provider_merchant_name: str | None = "Starbucks",
    candidate_merchant_id: str = "merch000001",
    confidence_score: float = 0.85,
    match_signals: dict[str, object] | None = None,
    status: str = "pending",
    decided_by: str = "auto",
) -> None:
    """Insert one row into app.merchant_link_decisions via a write connection."""
    signals = match_signals if match_signals is not None else {"entity_id": True}
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.merchant_link_decisions (
                decision_id, ref_kind, ref_value, source_type, provider_merchant_name,
                candidate_merchant_id, confidence_score, match_signals,
                status, decided_by, match_reason, decided_at, reversed_at, reversed_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,  # noqa: S608  # test input, not executing SQL
            [
                decision_id,
                ref_kind,
                ref_value,
                source_type,
                provider_merchant_name,
                candidate_merchant_id,
                confidence_score,
                json.dumps(signals),
                status,
                decided_by,
                None,
                _NOW,
                None,
                None,
            ],
        )


def _seed_merchant(merchant_id: str) -> None:
    """Make ``merchant_id`` resolvable in core.dim_merchants (via app.user_merchants).

    The accept path validates the target exists in the merchant catalog; the
    dim is a view over app.user_merchants, so seeding the source row is what
    lets an accept bind instead of raising.
    """
    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO app.user_merchants "
            "(merchant_id, match_type, canonical_name, created_by) "
            "VALUES (?, 'oneOf', ?, 'user')",
            [merchant_id, f"Name {merchant_id}"],
        )


# ---------------------------------------------------------------------------
# merchants_links_pending
# ---------------------------------------------------------------------------


class TestMerchantsLinksPending:
    """Tests for merchants_links_pending."""

    async def test_returns_envelope(self) -> None:
        """merchants_links_pending returns a valid ResponseEnvelope."""
        parsed = (await merchants_links_pending()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed

    async def test_empty_queue(self) -> None:
        """Empty queue returns groups=[], n_pending=0."""
        data = (await merchants_links_pending()).to_dict()["data"]
        assert data["groups"] == []
        assert data["n_pending"] == 0

    async def test_total_count_reflects_pending(self, mcp_db: object) -> None:
        """summary.total_count >= 1 when a pending decision exists."""
        _insert_decision(decision_id="mp001", ref_value="entity_001")
        parsed = (await merchants_links_pending()).to_dict()
        assert parsed["summary"]["total_count"] >= 1

    async def test_pending_grouped_by_ref_value(self, mcp_db: object) -> None:
        """Pending decisions are grouped by ref_value (provider entity id)."""
        _insert_decision(
            decision_id="mp010",
            ref_value="entity_X",
            candidate_merchant_id="merch000001",
        )
        _insert_decision(
            decision_id="mp011",
            ref_value="entity_X",
            candidate_merchant_id="merch000002",
        )
        _insert_decision(
            decision_id="mp012",
            ref_value="entity_Y",
            candidate_merchant_id="merch000001",
        )

        data = (await merchants_links_pending()).to_dict()["data"]
        groups = data["groups"]
        assert len(groups) == 2

        entity_x = next(g for g in groups if g["ref_value"] == "entity_X")
        assert len(entity_x["candidates"]) == 2

        entity_y = next(g for g in groups if g["ref_value"] == "entity_Y")
        assert len(entity_y["candidates"]) == 1

    async def test_candidate_fields_present(self, mcp_db: object) -> None:
        """Each candidate carries decision_id, candidate_merchant_id, confidence."""
        _insert_decision(
            decision_id="mp020",
            ref_value="entity_C",
            candidate_merchant_id="merch000003",
            confidence_score=0.9,
        )

        data = (await merchants_links_pending()).to_dict()["data"]
        groups = data["groups"]
        assert len(groups) == 1
        cand = groups[0]["candidates"][0]
        assert cand["decision_id"] == "mp020"
        assert cand["candidate_merchant_id"] == "merch000003"
        assert "confidence" in cand

    async def test_n_pending_counts_distinct_ref_values(self, mcp_db: object) -> None:
        """n_pending counts distinct provider entity ids, not raw decision rows."""
        _insert_decision(
            decision_id="mp030",
            ref_value="entity_D",
            candidate_merchant_id="merch000001",
        )
        _insert_decision(
            decision_id="mp031",
            ref_value="entity_D",
            candidate_merchant_id="merch000002",
        )

        data = (await merchants_links_pending()).to_dict()["data"]
        # Two rows but one entity id → n_pending = 1
        assert data["n_pending"] == 1

    async def test_actions_point_to_set(self) -> None:
        """actions[] hint points the agent at merchants_links_set."""
        result = (await merchants_links_pending()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "merchants_links_set" in actions_text


# ---------------------------------------------------------------------------
# merchants_links_set
# ---------------------------------------------------------------------------


class TestMerchantsLinksSet:
    """Tests for merchants_links_set."""

    async def test_accept_returns_envelope(self, mcp_db: object) -> None:
        """merchants_links_set (accept) returns a valid ResponseEnvelope."""
        _insert_decision(decision_id="ms001", ref_value="entity_S1")
        _seed_merchant("merch000001")
        parsed = (
            await merchants_links_set(
                decision_id="ms001", target_merchant_id="merch000001"
            )
        ).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_accept_payload_status_accepted(self, mcp_db: object) -> None:
        """Accepting a decision returns status='accepted' in payload."""
        _insert_decision(decision_id="ms010", ref_value="entity_S2")
        _seed_merchant("merch000001")
        data = (
            await merchants_links_set(
                decision_id="ms010", target_merchant_id="merch000001"
            )
        ).to_dict()["data"]
        assert data["decision_id"] == "ms010"
        assert data["status"] == "accepted"

    async def test_reject_null_returns_rejected(self, mcp_db: object) -> None:
        """Passing target_merchant_id=None returns status='rejected'."""
        _insert_decision(decision_id="ms020", ref_value="entity_S3")
        data = (
            await merchants_links_set(decision_id="ms020", target_merchant_id=None)
        ).to_dict()["data"]
        assert data["status"] == "rejected"

    async def test_set_sensitivity_is_low(self, mcp_db: object) -> None:
        """merchants_links_set response carries low sensitivity (ids + status only)."""
        _insert_decision(decision_id="ms030", ref_value="entity_S4")
        parsed = (
            await merchants_links_set(decision_id="ms030", target_merchant_id=None)
        ).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    async def test_set_actions_point_back_to_pending(self, mcp_db: object) -> None:
        """actions[] after set points back at merchants_links_pending."""
        _insert_decision(decision_id="ms040", ref_value="entity_S5")
        result = (
            await merchants_links_set(decision_id="ms040", target_merchant_id=None)
        ).to_dict()
        actions_text = " ".join(result["actions"])
        assert "merchants_links_pending" in actions_text


# ---------------------------------------------------------------------------
# merchants_links_history
# ---------------------------------------------------------------------------


class TestMerchantsLinksHistory:
    """Tests for merchants_links_history."""

    async def test_empty_history(self) -> None:
        """Empty history returns decisions=[]."""
        data = (await merchants_links_history()).to_dict()["data"]
        assert data["decisions"] == []

    async def test_returns_envelope(self) -> None:
        """merchants_links_history returns a valid ResponseEnvelope."""
        parsed = (await merchants_links_history()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_history_row_fields(self, mcp_db: object) -> None:
        """History rows carry decision_id, ref_value, status, decided_by, confidence."""
        _insert_decision(
            decision_id="mh001",
            ref_value="entity_H1",
            status="accepted",
            confidence_score=0.75,
            decided_by="user",
        )

        data = (await merchants_links_history()).to_dict()["data"]
        assert len(data["decisions"]) == 1
        row = data["decisions"][0]
        assert row["decision_id"] == "mh001"
        assert row["ref_value"] == "entity_H1"
        assert row["status"] == "accepted"
        assert row["decided_by"] == "user"
        assert "confidence" in row

    async def test_history_limit(self, mcp_db: object) -> None:
        """Limit parameter is respected."""
        for i in range(5):
            _insert_decision(
                decision_id=f"mhlim{i:03d}",
                ref_value=f"entity_LIM{i}",
                status="accepted",
            )

        data = (await merchants_links_history(limit=2)).to_dict()["data"]
        assert len(data["decisions"]) <= 2

    async def test_history_actions_point_to_pending(self) -> None:
        """History actions[] hint points back at merchants_links_pending."""
        result = (await merchants_links_history()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "merchants_links_pending" in actions_text


# ---------------------------------------------------------------------------
# merchants_links_run
# ---------------------------------------------------------------------------


class TestMerchantsLinksRun:
    """Tests for merchants_links_run."""

    @patch("moneybin.mcp.tools.merchants.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
    async def test_run_returns_envelope(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """merchants_links_run returns a valid ResponseEnvelope."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = HarvestResult(bound=3, conflicts=0)

        parsed = (await merchants_links_run()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed

    @patch("moneybin.mcp.tools.merchants.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
    async def test_run_payload_contains_counts(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """data.bound + data.conflicts reflect the HarvestResult from service.run()."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = HarvestResult(bound=5, conflicts=2)

        data = (await merchants_links_run()).to_dict()["data"]
        assert data["bound"] == 5
        assert data["conflicts"] == 2

    @patch("moneybin.mcp.tools.merchants.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
    async def test_run_sensitivity_is_low(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """merchants_links_run has low sensitivity (counts only)."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = HarvestResult(bound=0, conflicts=0)

        parsed = (await merchants_links_run()).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    @patch("moneybin.mcp.tools.merchants.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
    async def test_run_actions_point_to_pending(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """actions[] after run points at merchants_links_pending."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = HarvestResult(bound=2, conflicts=1)

        result = (await merchants_links_run()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "merchants_links_pending" in actions_text


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestMerchantsLinksRegistration:
    """Verify merchants_links_* tools are registered with the FastMCP server."""

    async def test_tools_registered(self) -> None:
        """register_merchants_tools includes all four merchants_links_* tools."""
        srv = FastMCP("test")
        register_merchants_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert "merchants_links_pending" in names
        assert "merchants_links_set" in names
        assert "merchants_links_history" in names
        assert "merchants_links_run" in names


# ---------------------------------------------------------------------------
# Actor threading
# ---------------------------------------------------------------------------


class TestMerchantsLinksActor:
    """Verify the MCP actor is passed through to the service."""

    @patch("moneybin.mcp.tools.merchants.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.set")
    async def test_set_uses_mcp_actor(
        self, mock_set: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """merchants_links_set calls MerchantLinksService with actor='mcp'."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        await merchants_links_set(decision_id="d_actor", target_merchant_id=None)

        mock_set.assert_called_once_with(
            "d_actor", target_merchant_id=None, decided_by="user"
        )
