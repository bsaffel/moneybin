"""Tests for investments_securities_links_* MCP tools (M1G.4 review-surface fix wave).

Mirrors test_merchants_links_tools.py for the security-links review surface.
Seeds via the real repos (SecuritiesRepo/SecurityLinksRepo/
SecurityLinkDecisionsRepo) rather than raw SQL — matches
test_security_links_service.py's fixture shape. All tests use the mcp_db
fixture (session-template DB + monkeypatch for get_settings/SecretStore).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import FastMCP
from fastmcp.server.elicitation import (
    AcceptedElicitation,
    CancelledElicitation,
    DeclinedElicitation,
)

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


def _fake_ctx(*, supports_elicit: bool, elicit_result: Any = None) -> MagicMock:
    """Build a fake fastmcp Context with session + elicit wired.

    Mirrors test_first_run_setup.py's fixture: the only elicitation contract
    the tool depends on is check_client_capability + elicit.
    """
    ctx = MagicMock()
    ctx.session.check_client_capability.return_value = supports_elicit
    ctx.elicit = AsyncMock(return_value=elicit_result)
    return ctx


def _decision_status(decision_id: str) -> str:
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT status FROM app.security_link_decisions WHERE decision_id = ?",
            [decision_id],
        ).fetchone()
    assert row is not None
    return str(row[0])


def _security_exists(security_id: str) -> bool:
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT 1 FROM app.securities WHERE security_id = ?", [security_id]
        ).fetchone()
    return row is not None


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
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )
        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            parsed = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    into=setup["survivor"],
                )
            ).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_accept_payload_status_accepted(self, mcp_db: object) -> None:
        """Accepting a decision (after a confirmed elicitation) returns status='accepted'."""
        setup = _merge_setup()
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )
        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            data = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    into=setup["survivor"],
                )
            ).to_dict()["data"]
        assert data["decision_id"] == setup["decision_id"]
        assert data["status"] == "accepted"
        assert _decision_status(setup["decision_id"]) == "accepted"

    async def test_reject_returns_rejected(self, mcp_db: object) -> None:
        """action='reject' returns status='rejected' with no elicitation."""
        setup = _merge_setup()
        data = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], action="reject"
            )
        ).to_dict()["data"]
        assert data["status"] == "rejected"

    async def test_wrong_into_returns_error_envelope_and_leaves_decision_pending(
        self, mcp_db: object
    ) -> None:
        """A mismatched `into` must refuse, not silently merge into the wrong security.

        Carries the confirming-safety-check guard (Fix 3) through the MCP
        tool: `into` must equal the decision's own candidate_security_id, not
        just be A valid security id. The refusal must land BEFORE the human is
        asked to confirm — a doomed merge is never worth an elicitation.
        """
        setup = _merge_setup()
        other = _mint(name="Some Other Fund", created_by="user")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )

        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            parsed = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"], action="accept", into=other
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"
        ctx.elicit.assert_not_called()

    async def test_set_sensitivity_is_low(self, mcp_db: object) -> None:
        """investments_securities_links_set response carries low sensitivity (ids + status only)."""
        setup = _merge_setup()
        parsed = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], action="reject"
            )
        ).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    async def test_set_actions_point_back_to_pending(self, mcp_db: object) -> None:
        """actions[] after set points back at investments_securities_links_pending."""
        setup = _merge_setup()
        result = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], action="reject"
            )
        ).to_dict()
        actions_text = " ".join(result["actions"])
        assert "investments_securities_links_pending" in actions_text


# ---------------------------------------------------------------------------
# investments_securities_links_set — accept gating (D1) + explicit action (D2)
# ---------------------------------------------------------------------------


class TestInvestmentsSecuritiesLinksSetAcceptGate:
    """Accepting a merge requires explicit human agreement via MCP elicitation.

    Every pending security decision is BY CONSTRUCTION a weak inference (the
    resolver only proposes when it is ambiguous), and accepting one fuses two
    instruments' tax lots — irreversible in effect on cost basis until undone.
    `.claude/rules/design-principles.md` ("Magic stays visible") forbids agent
    self-accept of a weak inference at any confidence score.
    """

    async def test_accept_hard_fails_when_client_cannot_elicit(
        self, mcp_db: object
    ) -> None:
        """A tools-only client MUST NOT be able to accept — no fall-through."""
        setup = _merge_setup()
        ctx = _fake_ctx(supports_elicit=False)

        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            parsed = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    into=setup["survivor"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_required"
        # The error must name the CLI equivalent so the user has a way through.
        hint = parsed["error"]["hint"]
        assert "moneybin investments securities links set" in hint
        assert "--accept" in hint and "--into" in hint
        # Nothing was written: still pending, provisional security still alive.
        assert _decision_status(setup["decision_id"]) == "pending"
        assert _security_exists(setup["provisional"])
        ctx.elicit.assert_not_called()

    async def test_accept_hard_fails_when_no_active_context(
        self, mcp_db: object
    ) -> None:
        """No MCP request context at all (no client to confirm) must not accept."""
        setup = _merge_setup()

        with patch(
            "moneybin.mcp.elicitation.get_context",
            side_effect=RuntimeError("No active context found."),
        ):
            parsed = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    into=setup["survivor"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_required"
        assert _decision_status(setup["decision_id"]) == "pending"
        assert _security_exists(setup["provisional"])

    async def test_declined_elicitation_does_not_accept(self, mcp_db: object) -> None:
        """A declined confirmation leaves the decision pending — never accepts."""
        setup = _merge_setup()
        ctx = _fake_ctx(supports_elicit=True, elicit_result=DeclinedElicitation())

        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            parsed = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    into=setup["survivor"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_required"
        assert _decision_status(setup["decision_id"]) == "pending"
        assert _security_exists(setup["provisional"])
        ctx.elicit.assert_awaited_once()

    async def test_cancelled_elicitation_does_not_accept(self, mcp_db: object) -> None:
        """A cancelled confirmation is not agreement either."""
        setup = _merge_setup()
        ctx = _fake_ctx(supports_elicit=True, elicit_result=CancelledElicitation())

        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            parsed = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    into=setup["survivor"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_required"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_elicitation_message_names_both_securities_and_reason(
        self, mcp_db: object
    ) -> None:
        """The human must see BOTH sides of the fusion and why it was proposed."""
        setup = _merge_setup()
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )

        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            await investments_securities_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                into=setup["survivor"],
            )

        message = ctx.elicit.await_args.args[0]
        # Provisional (what is being merged away): ticker, name, provider ref.
        assert "VTI" in message
        assert "Vanguard Total Stock Mkt ETF" in message
        assert _REF_VALUE in message
        # Survivor (what it merges into): id + name.
        assert setup["survivor"] in message
        assert "Vanguard Total Stock Market ETF" in message
        # Why the resolver could not decide on its own.
        assert "fuzzy_name" in message

    async def test_accept_after_confirmation_records_decided_by_user(
        self, mcp_db: object
    ) -> None:
        """decided_by='user' is only truthful once a human actually confirmed."""
        setup = _merge_setup()
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )

        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            await investments_securities_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                into=setup["survivor"],
            )

        with get_database(read_only=True) as db:
            row = db.execute(
                "SELECT decided_by FROM app.security_link_decisions "
                "WHERE decision_id = ?",
                [setup["decision_id"]],
            ).fetchone()
        assert row is not None and row[0] == "user"


class TestInvestmentsSecuritiesLinksSetActionInput:
    """`action` is explicit; accept/reject is never inferred from `into` (D2)."""

    async def test_empty_into_is_an_input_error_not_a_reject(
        self, mcp_db: object
    ) -> None:
        """An empty-string `into` must NOT silently become a permanent reject."""
        setup = _merge_setup()
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )

        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            parsed = (
                await investments_securities_links_set(
                    decision_id=setup["decision_id"], action="accept", into=""
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_accept_without_into_is_an_input_error(self, mcp_db: object) -> None:
        """action='accept' with no `into` is an input error, never a reject."""
        setup = _merge_setup()
        parsed = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], action="accept"
            )
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_reject_with_into_is_an_input_error(self, mcp_db: object) -> None:
        """`into` alongside action='reject' is contradictory input, not a reject."""
        setup = _merge_setup()
        parsed = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"],
                action="reject",
                into=setup["survivor"],
            )
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_unknown_action_is_an_input_error_listing_valid_values(
        self, mcp_db: object
    ) -> None:
        """An unrecognized action names the valid values instead of guessing."""
        setup = _merge_setup()
        parsed = (
            await investments_securities_links_set(
                decision_id=setup["decision_id"], action="merge"
            )
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert "accept" in parsed["error"]["message"]
        assert "reject" in parsed["error"]["message"]
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_unknown_decision_id_is_not_found(self, mcp_db: object) -> None:
        """A decision_id with no pending decision cannot be accepted."""
        _merge_setup()
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )
        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            parsed = (
                await investments_securities_links_set(
                    decision_id="dnotthere01", action="accept", into="whatever"
                )
            ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_not_found"
        ctx.elicit.assert_not_called()


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
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=None)
        )
        with patch("moneybin.mcp.elicitation.get_context", return_value=ctx):
            await investments_securities_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                into=setup["survivor"],
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

    async def test_set_description_names_the_undo_tool(self) -> None:
        """The description must name system_audit_undo as the recovery path (D6).

        Claiming "no undo tool yet" tells the agent an accidental merge is
        irreversible when a single system_audit_undo(operation_id) call
        reverses the whole cascade atomically.
        """
        srv = FastMCP("test")
        register_investments_tools(srv)
        tool = next(
            t
            for t in await srv._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
            if t.name == "investments_securities_links_set"
        )
        description = tool.description or ""
        assert "system_audit_undo" in description
        assert "no undo tool yet" not in description
        # The elicitation gate is part of the contract the agent must know.
        assert "confirm" in description.lower()

    async def test_set_docstring_names_the_undo_tool(self) -> None:
        """The docstring (the second prose surface) carries the same recovery path."""
        doc = investments_securities_links_set.__doc__ or ""
        assert "system_audit_undo" in doc
        assert "no undo tool yet" not in doc


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
        """An agent-driven reject records decided_by='auto' — no human ratified it.

        The decision table's CHECK admits only 'auto' | 'user'; recording
        'user' for a decision no human made is the falsehood D1 calls out.
        The MCP channel itself is captured by app.audit_log's actor='mcp'.
        """
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        await investments_securities_links_set(decision_id="d_actor", action="reject")

        mock_reject.assert_called_once_with("d_actor", decided_by="auto")
