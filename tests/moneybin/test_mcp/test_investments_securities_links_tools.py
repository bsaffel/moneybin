"""Tests for investments_securities_links_* MCP tools (M1G.4 review-surface fix wave).

Mirrors test_merchants_links_tools.py for the security-links review surface.
Seeds via the real repos (SecuritiesRepo/SecurityLinksRepo/
SecurityLinkDecisionsRepo) rather than raw SQL — matches
test_security_links_service.py's fixture shape. All tests use the mcp_db
fixture (session-template DB + monkeypatch for get_settings/SecretStore).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.investments import (
    investments_securities_links_history,
    investments_securities_links_pending,
    investments_securities_links_set,
    register_investments_tools,
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo

pytestmark = pytest.mark.usefixtures("mcp_db")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REF_KIND = "plaid_security_id"
_REF_VALUE = "sec_1"


def _mint(*, name: str, created_by: str, ticker: str | None = None) -> str:
    with get_database(read_only=False) as db:
        event = SecuritiesRepo(db).upsert(
            security_id=None,
            name=name,
            security_type="etf",
            ticker=ticker,
            created_by=created_by,
            actor="system" if created_by == "plaid" else "cli",
        )
    assert event.target_id is not None
    return event.target_id


def _bind(
    security_id: str,
    *,
    ref_kind: str = _REF_KIND,
    ref_value: str = _REF_VALUE,
) -> None:
    with get_database(read_only=False) as db:
        SecurityLinksRepo(db).insert(
            security_id=security_id,
            ref_kind=ref_kind,
            ref_value=ref_value,
            source_type="plaid",
            decided_by="auto",
            actor="system",
        )


def _insert_decision(
    *,
    decision_id: str,
    candidate_security_id: str,
    ref_kind: str = _REF_KIND,
    ref_value: str = _REF_VALUE,
    provider_ticker: str | None = None,
    provider_name: str | None = None,
    confidence_score: float | None = 0.5,
    match_reason: str | None = None,
) -> None:
    with get_database(read_only=False) as db:
        SecurityLinkDecisionsRepo(db).insert(
            decision_id=decision_id,
            ref_kind=ref_kind,
            ref_value=ref_value,
            source_type="plaid",
            candidate_security_id=candidate_security_id,
            provider_ticker=provider_ticker,
            provider_name=provider_name,
            confidence_score=confidence_score,
            match_reason=match_reason,
            actor="system",
        )


def _merge_setup() -> dict[str, str]:
    """Provisional (plaid-minted) security bound to sec_1, proposed to merge."""
    survivor = _mint(
        name="Vanguard Total Stock Market ETF", created_by="user", ticker="VTI"
    )
    provisional = _mint(
        name="Vanguard Total Stock Mkt ETF", created_by="plaid", ticker="VTI"
    )
    _bind(provisional)
    decision_id = "dsec00000001"
    _insert_decision(
        decision_id=decision_id,
        candidate_security_id=survivor,
        provider_ticker="VTI",
        provider_name="Vanguard Total Stock Mkt ETF",
        match_reason="fuzzy_name",
    )
    return {
        "survivor": survivor,
        "provisional": provisional,
        "decision_id": decision_id,
    }


# ---------------------------------------------------------------------------
# investments_securities_links_pending
# ---------------------------------------------------------------------------


class TestInvestmentsSecuritiesLinksPending:
    """Tests for investments_securities_links_pending."""

    async def test_returns_envelope(self) -> None:
        """investments_securities_links_pending returns a valid ResponseEnvelope."""
        parsed = (await investments_securities_links_pending()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed

    async def test_empty_queue(self) -> None:
        """Empty queue returns groups=[], n_pending=0."""
        data = (await investments_securities_links_pending()).to_dict()["data"]
        assert data["groups"] == []
        assert data["n_pending"] == 0

    async def test_pending_shows_both_provider_fields_and_reason(
        self, mcp_db: object
    ) -> None:
        """Both provider_ticker/provider_name AND each candidate's match_reason surface.

        Regression guard for the review-surface fix: the text CLI once hid
        provider_name behind provider_ticker and match_reason entirely — the
        JSON/MCP payload must carry both so the CLI's fix has something real
        to render.
        """
        setup = _merge_setup()

        data = (await investments_securities_links_pending()).to_dict()["data"]
        groups = data["groups"]
        assert len(groups) == 1
        group = groups[0]
        assert group["provider_ticker"] == "VTI"
        assert group["provider_name"] == "Vanguard Total Stock Mkt ETF"
        cand = group["candidates"][0]
        assert cand["decision_id"] == setup["decision_id"]
        assert cand["candidate_security_id"] == setup["survivor"]
        assert cand["match_reason"] == "fuzzy_name"

    async def test_actions_point_to_set(self) -> None:
        """actions[] hint points the agent at investments_securities_links_set."""
        result = (await investments_securities_links_pending()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "investments_securities_links_set" in actions_text


# ---------------------------------------------------------------------------
# investments_securities_links_set
# ---------------------------------------------------------------------------


class TestInvestmentsSecuritiesLinksSet:
    """Tests for investments_securities_links_set."""

    async def test_accept_returns_envelope(self, mcp_db: object) -> None:
        """investments_securities_links_set (accept) returns a valid ResponseEnvelope."""
        setup = _merge_setup()
        parsed = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], into=setup["survivor"]
            )
        ).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_accept_payload_status_accepted(self, mcp_db: object) -> None:
        """Accepting a decision returns status='accepted' in payload."""
        setup = _merge_setup()
        data = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], into=setup["survivor"]
            )
        ).to_dict()["data"]
        assert data["decision_id"] == setup["decision_id"]
        assert data["status"] == "accepted"

    async def test_reject_null_returns_rejected(self, mcp_db: object) -> None:
        """Passing into=None returns status='rejected'."""
        setup = _merge_setup()
        data = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], into=None
            )
        ).to_dict()["data"]
        assert data["status"] == "rejected"

    async def test_wrong_into_returns_error_envelope_and_leaves_decision_pending(
        self, mcp_db: object
    ) -> None:
        """A mismatched `into` must refuse, not silently merge into the wrong security.

        Carries the confirming-safety-check guard (Fix 3) through the MCP
        tool: `into` must equal the decision's own candidate_security_id, not
        just be A valid security id.
        """
        setup = _merge_setup()
        other = _mint(name="Some Other Fund", created_by="user")

        parsed = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], into=other
            )
        ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        with get_database(read_only=True) as db:
            row = db.execute(
                "SELECT status FROM app.security_link_decisions WHERE decision_id = ?",
                [setup["decision_id"]],
            ).fetchone()
        assert row is not None and row[0] == "pending"

    async def test_set_sensitivity_is_low(self, mcp_db: object) -> None:
        """investments_securities_links_set response carries low sensitivity (ids + status only)."""
        setup = _merge_setup()
        parsed = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], into=None
            )
        ).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    async def test_set_actions_point_back_to_pending(self, mcp_db: object) -> None:
        """actions[] after set points back at investments_securities_links_pending."""
        setup = _merge_setup()
        result = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], into=None
            )
        ).to_dict()
        actions_text = " ".join(result["actions"])
        assert "investments_securities_links_pending" in actions_text


# ---------------------------------------------------------------------------
# investments_securities_links_history
# ---------------------------------------------------------------------------


class TestInvestmentsSecuritiesLinksHistory:
    """Tests for investments_securities_links_history."""

    async def test_empty_history(self) -> None:
        """Empty history returns decisions=[]."""
        data = (await investments_securities_links_history()).to_dict()["data"]
        assert data["decisions"] == []

    async def test_returns_envelope(self) -> None:
        """investments_securities_links_history returns a valid ResponseEnvelope."""
        parsed = (await investments_securities_links_history()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_history_row_fields(self, mcp_db: object) -> None:
        """History rows carry decision_id, status, decided_by, match_reason."""
        setup = _merge_setup()
        await investments_securities_links_set(
            decision_id=setup["decision_id"], into=setup["survivor"]
        )

        data = (await investments_securities_links_history()).to_dict()["data"]
        assert len(data["decisions"]) == 1
        row = data["decisions"][0]
        assert row["decision_id"] == setup["decision_id"]
        assert row["status"] == "accepted"
        assert row["decided_by"] == "user"
        assert row["match_reason"] == "fuzzy_name"

    async def test_history_limit(self, mcp_db: object) -> None:
        """Limit parameter is respected."""
        for i in range(3):
            candidate = _mint(name=f"Fund {i}", created_by="user")
            _insert_decision(
                decision_id=f"dhlim000{i:03d}",
                candidate_security_id=candidate,
                ref_value=f"sec_lim{i}",
            )

        data = (await investments_securities_links_history(limit=2)).to_dict()["data"]
        assert len(data["decisions"]) <= 2

    async def test_history_actions_point_to_pending(self) -> None:
        """History actions[] hint points back at investments_securities_links_pending."""
        result = (await investments_securities_links_history()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "investments_securities_links_pending" in actions_text


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestInvestmentsSecuritiesLinksRegistration:
    """Verify investments_securities_links_* tools are registered with the FastMCP server."""

    async def test_tools_registered(self) -> None:
        """register_investments_tools includes all three security-links tools."""
        srv = FastMCP("test")
        register_investments_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert "investments_securities_links_pending" in names
        assert "investments_securities_links_set" in names
        assert "investments_securities_links_history" in names


# ---------------------------------------------------------------------------
# Actor threading
# ---------------------------------------------------------------------------


class TestInvestmentsSecuritiesLinksActor:
    """Verify the MCP actor is passed through to the service."""

    @patch("moneybin.mcp.tools.investments.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.reject_merge")
    async def test_set_uses_mcp_actor(
        self, mock_reject: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """investments_securities_links_set calls SecurityLinksService with actor='mcp'."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        await investments_securities_links_set(decision_id="d_actor", into=None)

        mock_reject.assert_called_once_with("d_actor", decided_by="user")
