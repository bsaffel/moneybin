"""Tests for merchants_links_* MCP tools (M1T).

Mirrors test_accounts_links.py for the merchant-links review surface. All
tests use the mcp_db fixture (session-template DB + monkeypatch for
get_settings/SecretStore).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
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


def _fake_ctx(*, supports_elicit: bool, elicit_result: Any = None) -> MagicMock:
    """Build a fake fastmcp Context with session + elicit wired.

    Mirrors test_investments_securities_links_tools.py: the only elicitation
    contract the tool depends on is check_client_capability + elicit.
    """
    ctx = MagicMock()
    ctx.session.check_client_capability.return_value = supports_elicit
    ctx.elicit = AsyncMock(return_value=elicit_result)
    return ctx


def _decision_row(decision_id: str) -> tuple[str, str]:
    """Return (status, decided_by) for one decision."""
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT status, decided_by FROM app.merchant_link_decisions "
            "WHERE decision_id = ?",
            [decision_id],
        ).fetchone()
    assert row is not None
    return str(row[0]), str(row[1])


def _decision_status(decision_id: str) -> str:
    return _decision_row(decision_id)[0]


def _bind_setup(
    *,
    decision_id: str = "mg001",
    ref_value: str = "entity_gate",
    merchant_id: str = "merch000001",
    provider_merchant_name: str = "STARBUCKS #4412",
) -> dict[str, str]:
    """A pending bind proposal with both sides named and the target resolvable."""
    _seed_merchant(merchant_id)
    _insert_decision(
        decision_id=decision_id,
        ref_value=ref_value,
        provider_merchant_name=provider_merchant_name,
        candidate_merchant_id=merchant_id,
        confidence_score=0.62,
    )
    return {
        "decision_id": decision_id,
        "ref_value": ref_value,
        "merchant_id": merchant_id,
    }


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
        setup = _bind_setup(decision_id="ms001", ref_value="entity_S1")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_accept_payload_status_accepted(self, mcp_db: object) -> None:
        """Accepting a decision (after a confirmed elicitation) returns status='accepted'."""
        setup = _bind_setup(decision_id="ms010", ref_value="entity_S2")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            data = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()["data"]
        assert data["decision_id"] == "ms010"
        assert data["status"] == "accepted"
        assert _decision_status("ms010") == "accepted"

    async def test_reject_returns_rejected(self, mcp_db: object) -> None:
        """action='reject' returns status='rejected' with no elicitation."""
        _insert_decision(decision_id="ms020", ref_value="entity_S3")
        data = (
            await merchants_links_set(decision_id="ms020", action="reject")
        ).to_dict()["data"]
        assert data["status"] == "rejected"

    async def test_wrong_target_refuses_before_prompting(self, mcp_db: object) -> None:
        """A mismatched target must refuse, not bind the entity to the wrong merchant."""
        setup = _bind_setup(decision_id="ms025", ref_value="entity_S25")
        _seed_merchant("merch000009")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id="merch000009",
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"
        ctx.elicit.assert_not_called()

    async def test_set_sensitivity_is_low(self, mcp_db: object) -> None:
        """merchants_links_set response carries low sensitivity (ids + status only)."""
        _insert_decision(decision_id="ms030", ref_value="entity_S4")
        parsed = (
            await merchants_links_set(decision_id="ms030", action="reject")
        ).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    async def test_set_actions_point_back_to_pending(self, mcp_db: object) -> None:
        """actions[] after set points back at merchants_links_pending."""
        _insert_decision(decision_id="ms040", ref_value="entity_S5")
        result = (
            await merchants_links_set(decision_id="ms040", action="reject")
        ).to_dict()
        actions_text = " ".join(result["actions"])
        assert "merchants_links_pending" in actions_text


# ---------------------------------------------------------------------------
# merchants_links_set — accept gating + explicit action
# ---------------------------------------------------------------------------


class TestMerchantsLinksSetAcceptGate:
    """Accepting a merchant bind requires explicit human agreement via elicitation.

    A pending decision is BY CONSTRUCTION a weak inference (the resolver only
    queues a conflict it cannot resolve), and accepting one attributes every
    transaction carrying that provider entity id to the chosen merchant.
    `.claude/rules/design-principles.md` ("Magic stays visible") forbids agent
    self-accept of a weak inference at any confidence score.
    """

    async def test_accept_returns_bound_token_when_elicitation_is_unavailable(
        self, mcp_db: object
    ) -> None:
        """A degraded client receives an opaque token bound to the live binding."""
        setup = _bind_setup(decision_id="mg005", ref_value="entity_G05")
        ctx = _fake_ctx(supports_elicit=False)

        with patch(
            "moneybin.mcp.confirmation._active_context",
            return_value=ctx,
        ):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()

        assert parsed["error"]["code"] == "mutation_confirmation_required"
        details = parsed["error"]["details"]
        assert details["confirmation_token"]
        assert details["operation_kind"] == "merchant_identity_bind"
        assert details["blast_radius"] == {
            "merchants": 1,
            "merchant_links": 1,
            "merchant_link_decisions": 1,
        }
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_accept_refuses_when_proposal_impact_changes_after_token(
        self, mcp_db: object
    ) -> None:
        """A new sibling decision changes the exact binding and invalidates approval."""
        setup = _bind_setup(decision_id="mg006", ref_value="entity_G06")
        with patch("moneybin.mcp.confirmation._active_context", return_value=None):
            required = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()
        token = required["error"]["details"]["confirmation_token"]
        _insert_decision(
            decision_id="mg006_sibling",
            ref_value=setup["ref_value"],
            candidate_merchant_id="merch000002",
        )

        parsed = (
            await merchants_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_merchant_id=setup["merchant_id"],
                confirmation_token=token,
            )
        ).to_dict()

        assert parsed["error"]["code"] == "mutation_confirmation_mismatch"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_changed_proposal_consumes_token_against_replay(
        self, mcp_db: object
    ) -> None:
        """A mismatched token cannot be reused after the proposal is restored."""
        setup = _bind_setup(decision_id="mg007", ref_value="entity_G07")
        with patch("moneybin.mcp.confirmation._active_context", return_value=None):
            required = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()
        token = required["error"]["details"]["confirmation_token"]
        _insert_decision(
            decision_id="mg007_sibling",
            ref_value=setup["ref_value"],
            candidate_merchant_id="merch000002",
        )
        await merchants_links_set(
            decision_id=setup["decision_id"],
            action="accept",
            target_merchant_id=setup["merchant_id"],
            confirmation_token=token,
        )
        with get_database(read_only=False) as db:
            db.execute(
                "DELETE FROM app.merchant_link_decisions WHERE decision_id = ?",
                ["mg007_sibling"],
            )

        replay = (
            await merchants_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_merchant_id=setup["merchant_id"],
                confirmation_token=token,
            )
        ).to_dict()

        assert replay["error"]["code"] == "mutation_confirmation_replayed"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_accept_hard_fails_when_client_cannot_elicit(
        self, mcp_db: object
    ) -> None:
        """A tools-only client MUST NOT be able to accept — no fall-through."""
        setup = _bind_setup(decision_id="mg010", ref_value="entity_G10")
        ctx = _fake_ctx(supports_elicit=False)

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_required"
        assert parsed["error"]["details"]["confirmation_token"]
        assert _decision_status(setup["decision_id"]) == "pending"
        ctx.elicit.assert_not_called()

    async def test_accept_hard_fails_when_no_active_context(
        self, mcp_db: object
    ) -> None:
        """No MCP request context at all (no client to confirm) must not accept."""
        setup = _bind_setup(decision_id="mg020", ref_value="entity_G20")

        with patch("moneybin.mcp.confirmation._active_context", return_value=None):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_required"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_declined_elicitation_does_not_accept(self, mcp_db: object) -> None:
        """A declined confirmation leaves the decision pending — never accepts."""
        setup = _bind_setup(decision_id="mg030", ref_value="entity_G30")
        ctx = _fake_ctx(supports_elicit=True, elicit_result=DeclinedElicitation())

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_declined"
        assert _decision_status(setup["decision_id"]) == "pending"
        ctx.elicit.assert_awaited_once()

    async def test_cancelled_elicitation_does_not_accept(self, mcp_db: object) -> None:
        """A cancelled confirmation is not agreement either."""
        setup = _bind_setup(decision_id="mg040", ref_value="entity_G40")
        ctx = _fake_ctx(supports_elicit=True, elicit_result=CancelledElicitation())

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id=setup["merchant_id"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_declined"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_elicitation_message_names_both_sides_and_reason(
        self, mcp_db: object
    ) -> None:
        """The human must see BOTH the provider entity and the merchant, plus why."""
        setup = _bind_setup(decision_id="mg050", ref_value="entity_G50")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            await merchants_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_merchant_id=setup["merchant_id"],
            )

        message = ctx.elicit.await_args.args[0]
        # Provider side: the entity id + the provider's merchant name.
        assert setup["ref_value"] in message
        assert "STARBUCKS #4412" in message
        # Canonical side: merchant id + canonical name.
        assert setup["merchant_id"] in message
        assert f"Name {setup['merchant_id']}" in message
        # Why the resolver could not decide on its own.
        assert "0.62" in message

    async def test_accept_after_confirmation_records_decided_by_user(
        self, mcp_db: object
    ) -> None:
        """decided_by='user' is only truthful once a human actually confirmed."""
        setup = _bind_setup(decision_id="mg060", ref_value="entity_G60")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            await merchants_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_merchant_id=setup["merchant_id"],
            )

        assert _decision_row(setup["decision_id"]) == ("accepted", "user")

    async def test_reject_records_decided_by_auto(self, mcp_db: object) -> None:
        """An MCP reject no human ratified must NOT be recorded as decided_by='user'."""
        _insert_decision(decision_id="mg070", ref_value="entity_G70")

        await merchants_links_set(decision_id="mg070", action="reject")

        assert _decision_row("mg070") == ("rejected", "auto")


class TestMerchantsLinksSetActionInput:
    """`action` is explicit; accept/reject is never inferred from the target."""

    async def test_empty_target_is_an_input_error_not_a_reject(
        self, mcp_db: object
    ) -> None:
        """An empty-string target must NOT silently become a permanent reject.

        A rejected proposal is never re-proposed, so a malformed argument would
        permanently suppress a correct binding with no error to the user.
        """
        setup = _bind_setup(decision_id="mi010", ref_value="entity_I10")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await merchants_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_merchant_id="",
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_accept_without_target_is_an_input_error(
        self, mcp_db: object
    ) -> None:
        """action='accept' with no target is an input error, never a reject."""
        setup = _bind_setup(decision_id="mi020", ref_value="entity_I20")
        parsed = (
            await merchants_links_set(decision_id=setup["decision_id"], action="accept")
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_reject_with_target_is_an_input_error(self, mcp_db: object) -> None:
        """A target alongside action='reject' is contradictory input, not a reject."""
        setup = _bind_setup(decision_id="mi030", ref_value="entity_I30")
        parsed = (
            await merchants_links_set(
                decision_id=setup["decision_id"],
                action="reject",
                target_merchant_id=setup["merchant_id"],
            )
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_unknown_action_is_an_input_error_listing_valid_values(
        self, mcp_db: object
    ) -> None:
        """An unrecognized action names the valid values instead of guessing."""
        setup = _bind_setup(decision_id="mi040", ref_value="entity_I40")
        parsed = (
            await merchants_links_set(decision_id=setup["decision_id"], action="bind")
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert "accept" in parsed["error"]["message"]
        assert "reject" in parsed["error"]["message"]
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_unknown_decision_id_is_nothing_to_do(self, mcp_db: object) -> None:
        """A decision_id with no pending decision cannot be accepted."""
        _bind_setup(decision_id="mi050", ref_value="entity_I50")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await merchants_links_set(
                    decision_id="mnotthere01",
                    action="accept",
                    target_merchant_id="merch000001",
                )
            ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_nothing_to_do"
        ctx.elicit.assert_not_called()


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
        """merchants_links_set calls MerchantLinksService with actor='mcp'.

        The reject path records decided_by='auto': no human ratified it, and
        the column's CHECK admits only 'auto' | 'user'. The MCP channel itself
        is preserved in app.audit_log (actor='mcp').
        """
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        await merchants_links_set(decision_id="d_actor", action="reject")

        mock_set.assert_called_once_with(
            "d_actor", target_merchant_id=None, decided_by="auto"
        )
