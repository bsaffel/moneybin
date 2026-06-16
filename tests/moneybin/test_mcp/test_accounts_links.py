"""Tests for accounts_links_* MCP tools.

Mirrors test_transactions_tools.py for the matches surface. All tests use the
mcp_db fixture (session-template DB + monkeypatch for get_settings/SecretStore).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.accounts import (
    accounts_links_history,
    accounts_links_pending,
    accounts_links_set,
    register_accounts_tools,
)

pytestmark = pytest.mark.usefixtures("mcp_db")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime.now(tz=UTC).isoformat()


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

    async def test_sensitivity_is_low(self) -> None:
        """accounts_links_pending is sensitivity low (no amounts/descriptions)."""
        parsed = (await accounts_links_pending()).to_dict()
        assert parsed["summary"]["sensitivity"] == "low"

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
        _insert_decision(
            decision_id="ds001",
            provisional_account_id="PROV_S1",
            candidate_account_id="ACC001",
        )
        parsed = (
            await accounts_links_set(decision_id="ds001", target_account_id="ACC001")
        ).to_dict()
        assert "summary" in parsed
        assert "data" in parsed

    async def test_accept_payload_status_accepted(self, mcp_db: object) -> None:
        """Accepting a decision returns status='accepted' in payload."""
        _insert_decision(
            decision_id="ds010",
            provisional_account_id="PROV_S2",
            candidate_account_id="ACC001",
        )
        # Also need an account_links row so the repoint doesn't error
        with get_database(read_only=False) as db:
            db.execute(
                """
                INSERT INTO app.account_links
                    (link_id, account_id, ref_kind, ref_value,
                     source_type, source_origin, status, decided_by, decided_at)
                VALUES ('lnk001', 'PROV_S2', 'source_native', 'key_s2',
                        'csv', 'bank_a', 'accepted', 'auto', ?)
                """,  # noqa: S608  # test input, not executing SQL
                [_NOW],
            )

        data = (
            await accounts_links_set(decision_id="ds010", target_account_id="ACC001")
        ).to_dict()["data"]
        assert data["decision_id"] == "ds010"
        assert data["status"] == "accepted"

    async def test_standalone_null_returns_rejected(self, mcp_db: object) -> None:
        """Passing target_account_id=None returns status='rejected'."""
        _insert_decision(
            decision_id="ds020",
            provisional_account_id="PROV_S3",
            candidate_account_id="ACC001",
        )

        data = (
            await accounts_links_set(decision_id="ds020", target_account_id=None)
        ).to_dict()["data"]
        assert data["status"] == "rejected"

    async def test_set_sensitivity_is_low(self, mcp_db: object) -> None:
        """accounts_links_set response carries low sensitivity."""
        _insert_decision(
            decision_id="ds030",
            provisional_account_id="PROV_S4",
            candidate_account_id="ACC001",
        )
        parsed = (
            await accounts_links_set(decision_id="ds030", target_account_id=None)
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
            await accounts_links_set(decision_id="ds040", target_account_id=None)
        ).to_dict()
        actions_text = " ".join(result["actions"])
        assert "accounts_links_pending" in actions_text


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
# Registration
# ---------------------------------------------------------------------------


class TestAccountsLinksRegistration:
    """Verify accounts_links_* tools are registered with the FastMCP server."""

    async def test_tools_registered(self) -> None:
        """register_accounts_tools includes all three accounts_links_* tools."""
        srv = FastMCP("test")
        register_accounts_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert "accounts_links_pending" in names
        assert "accounts_links_set" in names
        assert "accounts_links_history" in names


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
        """accounts_links_set calls AccountLinksService with actor='mcp'."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        await accounts_links_set(decision_id="d_actor", target_account_id=None)

        mock_set.assert_called_once_with(
            "d_actor", target_account_id=None, decided_by="user"
        )
