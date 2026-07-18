"""Tests for accounts_links_* MCP tools.

Mirrors test_transactions_tools.py for the matches surface. All tests use the
mcp_db fixture (session-template DB + monkeypatch for get_settings/SecretStore).
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
from moneybin.mcp.tools.accounts import (
    accounts_links_history,
    accounts_links_pending,
    accounts_links_run,
    accounts_links_set,
    register_accounts_tools,
)

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
            "SELECT status, decided_by FROM app.account_link_decisions "
            "WHERE decision_id = ?",
            [decision_id],
        ).fetchone()
    assert row is not None
    return str(row[0]), str(row[1])


def _decision_status(decision_id: str) -> str:
    return _decision_row(decision_id)[0]


def _seed_account(account_id: str, display_name: str) -> None:
    """Make ``account_id`` resolvable (with a display name) in core.dim_accounts."""
    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO core.dim_accounts "
            "(account_id, account_type, institution_name, display_name) "
            "VALUES (?, 'CHECKING', 'Test Bank', ?)",
            [account_id, display_name],
        )


def _seed_source_native_link(account_id: str, link_id: str) -> None:
    """Accepted source_native link — without one the merge has nothing to re-point."""
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.account_links
                (link_id, account_id, ref_kind, ref_value,
                 source_type, source_origin, status, decided_by, decided_at)
            VALUES (?, ?, 'source_native', ?, 'csv', 'bank_a', 'accepted', 'auto', ?)
            """,  # noqa: S608  # test input, not executing SQL
            [link_id, account_id, f"key_{account_id}", _NOW],
        )


def _merge_setup(
    *,
    decision_id: str = "dg001",
    provisional: str = "PROV_GATE",
    candidate: str = "ACC_GATE",
) -> dict[str, str]:
    """A mergeable pending proposal: both accounts named, provisional re-pointable."""
    _seed_account(provisional, "Chase Checking (imported)")
    _seed_account(candidate, "Chase Checking")
    _seed_source_native_link(provisional, f"lnk_{decision_id}")
    _insert_decision(
        decision_id=decision_id,
        provisional_account_id=provisional,
        candidate_account_id=candidate,
        confidence=0.62,
        signal="name",
    )
    return {
        "decision_id": decision_id,
        "provisional": provisional,
        "candidate": candidate,
    }


def _insert_decision(
    *,
    decision_id: str,
    provisional_account_id: str,
    candidate_account_id: str,
    status: str = "pending",
    confidence: float = 0.85,
    signal: str = "institution_last4",
    decided_by: str = "auto",
) -> None:
    """Insert one row into app.account_link_decisions via a write connection."""
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO app.account_link_decisions (
                decision_id, provisional_account_id, candidate_account_id,
                confidence_score, match_signals, status, decided_by,
                match_reason, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,  # noqa: S608  # test input, not executing SQL
            [
                decision_id,
                provisional_account_id,
                candidate_account_id,
                confidence,
                json.dumps({"signal": signal}),
                status,
                decided_by,
                None,
                _NOW,
            ],
        )


# ---------------------------------------------------------------------------
# accounts_links_pending
# ---------------------------------------------------------------------------


class TestAccountsLinksPending:
    """Tests for accounts_links_pending."""

    async def test_returns_envelope(self) -> None:
        """accounts_links_pending returns a valid ResponseEnvelope."""
        parsed = (await accounts_links_pending()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed

    async def test_sensitivity_is_medium(self) -> None:
        """accounts_links_pending is medium (it surfaces account display_name).

        display_name is USER_NOTE — matching accounts_summary/accounts_get — so
        the proposal labels sit behind the same consent bar.
        """
        parsed = (await accounts_links_pending()).to_dict()
        assert parsed["summary"]["sensitivity"] == "medium"

    async def test_empty_queue(self) -> None:
        """Empty queue returns groups=[], n_pending=0."""
        data = (await accounts_links_pending()).to_dict()["data"]
        assert data["groups"] == []
        assert data["n_pending"] == 0

    async def test_pending_grouped_by_provisional(self, mcp_db: object) -> None:
        """Pending decisions are grouped by provisional_account_id."""
        _insert_decision(
            decision_id="d001",
            provisional_account_id="PROV1",
            candidate_account_id="ACC001",
        )
        _insert_decision(
            decision_id="d002",
            provisional_account_id="PROV1",
            candidate_account_id="ACC002",
        )
        _insert_decision(
            decision_id="d003",
            provisional_account_id="PROV2",
            candidate_account_id="ACC001",
        )

        data = (await accounts_links_pending()).to_dict()["data"]
        groups = data["groups"]
        assert len(groups) == 2  # PROV1 and PROV2

        prov1 = next(g for g in groups if g["provisional_account_id"] == "PROV1")
        assert len(prov1["candidates"]) == 2

        prov2 = next(g for g in groups if g["provisional_account_id"] == "PROV2")
        assert len(prov2["candidates"]) == 1

    async def test_candidate_fields_present(self, mcp_db: object) -> None:
        """Each candidate carries decision_id, account_id, confidence, signal."""
        _insert_decision(
            decision_id="d010",
            provisional_account_id="PROV_A",
            candidate_account_id="ACC001",
            confidence=0.9,
            signal="institution_last4",
        )

        data = (await accounts_links_pending()).to_dict()["data"]
        groups = data["groups"]
        assert len(groups) == 1
        cand = groups[0]["candidates"][0]
        assert cand["decision_id"] == "d010"
        assert cand["candidate_account_id"] == "ACC001"
        assert "confidence" in cand
        assert cand["signal"] == "institution_last4"

    async def test_no_ref_value_in_payload(self, mcp_db: object) -> None:
        """ref_value (account number) is never present in the payload."""
        _insert_decision(
            decision_id="d020",
            provisional_account_id="PROV_B",
            candidate_account_id="ACC001",
        )

        data = (await accounts_links_pending()).to_dict()["data"]
        raw = json.dumps(data)
        assert "ref_value" not in raw

    async def test_n_pending_counts_provisional_accounts(self, mcp_db: object) -> None:
        """n_pending counts distinct provisional accounts, not decision rows."""
        _insert_decision(
            decision_id="d030",
            provisional_account_id="PROV_C",
            candidate_account_id="ACC001",
        )
        _insert_decision(
            decision_id="d031",
            provisional_account_id="PROV_C",
            candidate_account_id="ACC002",
        )

        data = (await accounts_links_pending()).to_dict()["data"]
        # Two rows but one provisional account → n_pending = 1
        assert data["n_pending"] == 1

    async def test_actions_point_to_set(self) -> None:
        """actions[] hint points the agent at accounts_links_set."""
        result = (await accounts_links_pending()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "accounts_links_set" in actions_text


# ---------------------------------------------------------------------------
# accounts_links_set
# ---------------------------------------------------------------------------


class TestAccountsLinksSet:
    """Tests for accounts_links_set."""

    async def test_accept_returns_envelope(self, mcp_db: object) -> None:
        """accounts_links_set (accept) returns a valid ResponseEnvelope."""
        setup = _merge_setup(decision_id="ds001")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_accept_payload_status_accepted(self, mcp_db: object) -> None:
        """Accepting a decision (after a confirmed elicitation) returns status='accepted'."""
        setup = _merge_setup(decision_id="ds010", provisional="PROV_S2")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            data = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()["data"]
        assert data["decision_id"] == "ds010"
        assert data["status"] == "accepted"
        assert _decision_status("ds010") == "accepted"

    async def test_reject_returns_rejected(self, mcp_db: object) -> None:
        """action='reject' returns status='rejected' with no elicitation."""
        _insert_decision(
            decision_id="ds020",
            provisional_account_id="PROV_S3",
            candidate_account_id="ACC001",
        )

        data = (
            await accounts_links_set(decision_id="ds020", action="reject")
        ).to_dict()["data"]
        assert data["status"] == "rejected"

    async def test_wrong_target_refuses_before_prompting(self, mcp_db: object) -> None:
        """A mismatched target must refuse, not merge into the wrong account.

        The refusal must land BEFORE the human is asked — a doomed merge is
        never worth a confirmation.
        """
        setup = _merge_setup(decision_id="ds025", provisional="PROV_S25")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id="ACC002",
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"
        ctx.elicit.assert_not_called()

    async def test_set_sensitivity_is_low(self, mcp_db: object) -> None:
        """accounts_links_set response carries low sensitivity."""
        _insert_decision(
            decision_id="ds030",
            provisional_account_id="PROV_S4",
            candidate_account_id="ACC001",
        )
        parsed = (
            await accounts_links_set(decision_id="ds030", action="reject")
        ).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    async def test_set_actions_point_back_to_pending(self, mcp_db: object) -> None:
        """actions[] after set points back at accounts_links_pending."""
        _insert_decision(
            decision_id="ds040",
            provisional_account_id="PROV_S5",
            candidate_account_id="ACC001",
        )
        result = (
            await accounts_links_set(decision_id="ds040", action="reject")
        ).to_dict()
        actions_text = " ".join(result["actions"])
        assert "accounts_links_pending" in actions_text


# ---------------------------------------------------------------------------
# accounts_links_set — accept gating + explicit action
# ---------------------------------------------------------------------------


class TestAccountsLinksSetAcceptGate:
    """Accepting an account merge requires explicit human agreement via elicitation.

    Every pending decision is BY CONSTRUCTION a weak inference (the resolver
    only proposes when it cannot bind unambiguously), and accepting one fuses
    two accounts' transaction histories and balances.
    `.claude/rules/design-principles.md` ("Magic stays visible") forbids agent
    self-accept of a weak inference at any confidence score.
    """

    async def test_accept_returns_bound_token_when_elicitation_is_unavailable(
        self, mcp_db: object
    ) -> None:
        """A degraded client receives an opaque token bound to the live merge."""
        setup = _merge_setup(decision_id="dg005", provisional="PROV_G05")
        ctx = _fake_ctx(supports_elicit=False)

        with patch(
            "moneybin.mcp.confirmation._active_context",
            return_value=ctx,
        ):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()

        assert parsed["error"]["code"] == "mutation_confirmation_required"
        details = parsed["error"]["details"]
        assert details["confirmation_token"]
        assert details["operation_kind"] == "account_identity_merge"
        assert details["blast_radius"] == {
            "accounts": 2,
            "account_links": 1,
            "account_link_decisions": 1,
        }
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_accept_refuses_when_proposal_impact_changes_after_token(
        self, mcp_db: object
    ) -> None:
        """A new sibling decision changes the exact merge and invalidates approval."""
        setup = _merge_setup(decision_id="dg006", provisional="PROV_G06")
        with patch("moneybin.mcp.confirmation._active_context", return_value=None):
            required = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()
        token = required["error"]["details"]["confirmation_token"]
        _insert_decision(
            decision_id="dg006_sibling",
            provisional_account_id=setup["provisional"],
            candidate_account_id="ACC002",
        )

        parsed = (
            await accounts_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_account_id=setup["candidate"],
                confirmation_token=token,
            )
        ).to_dict()

        assert parsed["error"]["code"] == "mutation_confirmation_mismatch"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_changed_proposal_consumes_token_against_replay(
        self, mcp_db: object
    ) -> None:
        """A mismatched token cannot be reused after the proposal is restored."""
        setup = _merge_setup(decision_id="dg007", provisional="PROV_G07")
        with patch("moneybin.mcp.confirmation._active_context", return_value=None):
            required = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()
        token = required["error"]["details"]["confirmation_token"]
        _insert_decision(
            decision_id="dg007_sibling",
            provisional_account_id=setup["provisional"],
            candidate_account_id="ACC002",
        )
        await accounts_links_set(
            decision_id=setup["decision_id"],
            action="accept",
            target_account_id=setup["candidate"],
            confirmation_token=token,
        )
        with get_database(read_only=False) as db:
            db.execute(
                "DELETE FROM app.account_link_decisions WHERE decision_id = ?",
                ["dg007_sibling"],
            )

        replay = (
            await accounts_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_account_id=setup["candidate"],
                confirmation_token=token,
            )
        ).to_dict()

        assert replay["error"]["code"] == "mutation_confirmation_replayed"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_accept_hard_fails_when_client_cannot_elicit(
        self, mcp_db: object
    ) -> None:
        """A tools-only client MUST NOT be able to accept — no fall-through."""
        setup = _merge_setup(decision_id="dg010", provisional="PROV_G10")
        ctx = _fake_ctx(supports_elicit=False)

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
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
        setup = _merge_setup(decision_id="dg020", provisional="PROV_G20")

        with patch("moneybin.mcp.confirmation._active_context", return_value=None):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_required"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_declined_elicitation_does_not_accept(self, mcp_db: object) -> None:
        """A declined confirmation leaves the decision pending — never accepts."""
        setup = _merge_setup(decision_id="dg030", provisional="PROV_G30")
        ctx = _fake_ctx(supports_elicit=True, elicit_result=DeclinedElicitation())

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_declined"
        assert _decision_status(setup["decision_id"]) == "pending"
        ctx.elicit.assert_awaited_once()

    async def test_cancelled_elicitation_does_not_accept(self, mcp_db: object) -> None:
        """A cancelled confirmation is not agreement either."""
        setup = _merge_setup(decision_id="dg040", provisional="PROV_G40")
        ctx = _fake_ctx(supports_elicit=True, elicit_result=CancelledElicitation())

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id=setup["candidate"],
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_confirmation_declined"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_elicitation_message_names_both_accounts_and_reason(
        self, mcp_db: object
    ) -> None:
        """The human must see BOTH accounts being fused and why it was proposed."""
        setup = _merge_setup(decision_id="dg050", provisional="PROV_G50")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            await accounts_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_account_id=setup["candidate"],
            )

        message = ctx.elicit.await_args.args[0]
        # Provisional (merged away): id + display name.
        assert setup["provisional"] in message
        assert "Chase Checking (imported)" in message
        # Survivor (merged into): id + display name.
        assert setup["candidate"] in message
        assert "Chase Checking" in message
        # Why the resolver could not decide on its own.
        assert "name" in message
        assert "0.62" in message

    async def test_accept_after_confirmation_records_decided_by_user(
        self, mcp_db: object
    ) -> None:
        """decided_by='user' is only truthful once a human actually confirmed."""
        setup = _merge_setup(decision_id="dg060", provisional="PROV_G60")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            await accounts_links_set(
                decision_id=setup["decision_id"],
                action="accept",
                target_account_id=setup["candidate"],
            )

        assert _decision_row(setup["decision_id"]) == ("accepted", "user")

    async def test_reject_records_decided_by_auto(self, mcp_db: object) -> None:
        """An MCP reject no human ratified must NOT be recorded as decided_by='user'."""
        _insert_decision(
            decision_id="dg070",
            provisional_account_id="PROV_G70",
            candidate_account_id="ACC001",
        )

        await accounts_links_set(decision_id="dg070", action="reject")

        assert _decision_row("dg070") == ("rejected", "auto")


class TestAccountsLinksSetActionInput:
    """`action` is explicit; accept/reject is never inferred from the target."""

    async def test_empty_target_is_an_input_error_not_a_reject(
        self, mcp_db: object
    ) -> None:
        """An empty-string target must NOT silently become a permanent reject."""
        setup = _merge_setup(decision_id="di010", provisional="PROV_I10")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )

        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await accounts_links_set(
                    decision_id=setup["decision_id"],
                    action="accept",
                    target_account_id="",
                )
            ).to_dict()

        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_accept_without_target_is_an_input_error(
        self, mcp_db: object
    ) -> None:
        """action='accept' with no target is an input error, never a reject."""
        setup = _merge_setup(decision_id="di020", provisional="PROV_I20")
        parsed = (
            await accounts_links_set(decision_id=setup["decision_id"], action="accept")
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_reject_with_target_is_an_input_error(self, mcp_db: object) -> None:
        """A target alongside action='reject' is contradictory input, not a reject."""
        setup = _merge_setup(decision_id="di030", provisional="PROV_I30")
        parsed = (
            await accounts_links_set(
                decision_id=setup["decision_id"],
                action="reject",
                target_account_id=setup["candidate"],
            )
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_unknown_action_is_an_input_error_listing_valid_values(
        self, mcp_db: object
    ) -> None:
        """An unrecognized action names the valid values instead of guessing."""
        setup = _merge_setup(decision_id="di040", provisional="PROV_I40")
        parsed = (
            await accounts_links_set(decision_id=setup["decision_id"], action="merge")
        ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        assert "accept" in parsed["error"]["message"]
        assert "reject" in parsed["error"]["message"]
        assert _decision_status(setup["decision_id"]) == "pending"

    async def test_unknown_decision_id_is_nothing_to_do(self, mcp_db: object) -> None:
        """A decision_id with no pending decision cannot be accepted."""
        _merge_setup(decision_id="di050", provisional="PROV_I50")
        ctx = _fake_ctx(
            supports_elicit=True, elicit_result=AcceptedElicitation(data=True)
        )
        with patch("moneybin.mcp.confirmation._active_context", return_value=ctx):
            parsed = (
                await accounts_links_set(
                    decision_id="dnotthere01",
                    action="accept",
                    target_account_id="ACC001",
                )
            ).to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_nothing_to_do"
        ctx.elicit.assert_not_called()


# ---------------------------------------------------------------------------
# accounts_links_history
# ---------------------------------------------------------------------------


class TestAccountsLinksHistory:
    """Tests for accounts_links_history."""

    async def test_empty_history(self) -> None:
        """Empty history returns decisions=[]."""
        data = (await accounts_links_history()).to_dict()["data"]
        assert data["decisions"] == []

    async def test_returns_envelope(self) -> None:
        """accounts_links_history returns a valid ResponseEnvelope."""
        parsed = (await accounts_links_history()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_sensitivity_is_low(self) -> None:
        """accounts_links_history has low sensitivity."""
        parsed = (await accounts_links_history()).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    async def test_history_row_fields(self, mcp_db: object) -> None:
        """History rows carry decision_id, account ids, status, decided_by, confidence, signal."""
        _insert_decision(
            decision_id="dh001",
            provisional_account_id="PROV_H1",
            candidate_account_id="ACC001",
            status="accepted",
            confidence=0.75,
            signal="name",
            decided_by="user",
        )

        data = (await accounts_links_history()).to_dict()["data"]
        assert len(data["decisions"]) == 1
        row = data["decisions"][0]
        assert row["decision_id"] == "dh001"
        assert row["provisional_account_id"] == "PROV_H1"
        assert row["candidate_account_id"] == "ACC001"
        assert row["status"] == "accepted"
        assert row["decided_by"] == "user"
        assert "confidence" in row
        assert row["signal"] == "name"

    async def test_history_newest_first(self, mcp_db: object) -> None:
        """History is ordered newest-first by decided_at."""
        with get_database(read_only=False) as db:
            db.execute(
                """
                INSERT INTO app.account_link_decisions (
                    decision_id, provisional_account_id, candidate_account_id,
                    confidence_score, match_signals, status, decided_by,
                    match_reason, decided_at
                ) VALUES
                ('dh_old', 'PROV_HA', 'ACC001', 0.8, '{"signal":"name"}',
                 'accepted', 'user', NULL, '2025-01-01T10:00:00'),
                ('dh_new', 'PROV_HB', 'ACC002', 0.9, '{"signal":"institution_last4"}',
                 'rejected', 'user', NULL, '2025-06-01T10:00:00')
                """,  # noqa: S608  # test input, not executing SQL
            )

        data = (await accounts_links_history()).to_dict()["data"]
        ids = [r["decision_id"] for r in data["decisions"]]
        assert ids.index("dh_new") < ids.index("dh_old")

    async def test_history_limit(self, mcp_db: object) -> None:
        """Limit parameter is respected."""
        for i in range(5):
            _insert_decision(
                decision_id=f"dlim{i:03d}",
                provisional_account_id=f"PROV_LIM{i}",
                candidate_account_id="ACC001",
                status="accepted",
            )

        data = (await accounts_links_history(limit=2)).to_dict()["data"]
        assert len(data["decisions"]) <= 2

    async def test_history_actions_point_to_pending(self) -> None:
        """History actions[] hint points back at accounts_links_pending."""
        result = (await accounts_links_history()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "accounts_links_pending" in actions_text


# ---------------------------------------------------------------------------
# accounts_links_run
# ---------------------------------------------------------------------------


class TestAccountsLinksRun:
    """Tests for accounts_links_run."""

    @patch("moneybin.mcp.tools.accounts.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    async def test_run_returns_envelope(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """accounts_links_run returns a valid ResponseEnvelope."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = 3

        parsed = (await accounts_links_run()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed

    @patch("moneybin.mcp.tools.accounts.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    async def test_run_payload_contains_count(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """data.new_proposals reflects the count returned by service.run()."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = 5

        data = (await accounts_links_run()).to_dict()["data"]
        assert data["new_proposals"] == 5

    @patch("moneybin.mcp.tools.accounts.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    async def test_run_sensitivity_is_low(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """accounts_links_run has low sensitivity (counts only)."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = 0

        parsed = (await accounts_links_run()).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

    @patch("moneybin.mcp.tools.accounts.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    async def test_run_actions_point_to_pending(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """actions[] after run points at accounts_links_pending."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_run.return_value = 2

        result = (await accounts_links_run()).to_dict()
        actions_text = " ".join(result["actions"])
        assert "accounts_links_pending" in actions_text


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestAccountsLinksRegistration:
    """Verify accounts_links_* tools are registered with the FastMCP server."""

    async def test_tools_registered(self) -> None:
        """register_accounts_tools includes all four accounts_links_* tools."""
        srv = FastMCP("test")
        register_accounts_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert "accounts_links_pending" in names
        assert "accounts_links_set" in names
        assert "accounts_links_history" in names
        assert "accounts_links_run" in names


# ---------------------------------------------------------------------------
# Actor threading
# ---------------------------------------------------------------------------


class TestAccountsLinksActor:
    """Verify the MCP actor is passed through to the service."""

    @patch("moneybin.mcp.tools.accounts.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    async def test_set_uses_mcp_actor(
        self, mock_set: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """accounts_links_set calls AccountLinksService with actor='mcp'.

        The reject path records decided_by='auto': no human ratified it, and
        the column's CHECK admits only 'auto' | 'user'. The MCP channel itself
        is preserved in app.audit_log (actor='mcp').
        """
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        await accounts_links_set(decision_id="d_actor", action="reject")

        mock_set.assert_called_once_with(
            "d_actor", target_account_id=None, decided_by="auto"
        )
