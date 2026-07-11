"""Tests for `investments securities links` CLI commands.

Mirrors test_merchants_links.py for the security-links surface. CLI tests mock
the service layer and test argument parsing, exit codes, and output shape.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.investments.security_links import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pending_group(
    *,
    ref_kind: str = "plaid_security_id",
    ref_value: str = "sec_1",
    source_type: str = "plaid",
    provider_ticker: str | None = "VTI",
    provider_name: str | None = "Vanguard Total Stock Mkt ETF",
    decision_id: str = "dec001",
    candidate_id: str = "sec001aabbcc",
    candidate_ticker: str | None = "VTI",
    candidate_name: str | None = "Vanguard Total Stock Market ETF",
    confidence: float = 0.50,
) -> MagicMock:
    """Build a mock PendingSecurityLinkGroup with sensible defaults."""
    candidate = MagicMock()
    candidate.decision_id = decision_id
    candidate.candidate_security_id = candidate_id
    candidate.candidate_ticker = candidate_ticker
    candidate.candidate_name = candidate_name
    candidate.confidence = confidence
    candidate.match_reason = "fuzzy_name"

    group = MagicMock()
    group.ref_kind = ref_kind
    group.ref_value = ref_value
    group.source_type = source_type
    group.provider_ticker = provider_ticker
    group.provider_name = provider_name
    group.candidates = [candidate]
    return group


# ---------------------------------------------------------------------------
# links pending
# ---------------------------------------------------------------------------


class TestSecurityLinksPending:
    """Tests for `investments securities links pending`."""

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.pending")
    @patch(
        "moneybin.services.security_links_service.SecurityLinksService.count_pending"
    )
    def test_pending_empty(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Empty queue exits 0 with no output."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = []
        mock_count.return_value = 0

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.pending")
    @patch(
        "moneybin.services.security_links_service.SecurityLinksService.count_pending"
    )
    def test_pending_shows_ref_and_candidate_identity(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Text output includes the ref, decision id, AND the candidate's ticker/name.

        The reviewer cannot judge a merge from a bare candidate_security_id —
        the whole point of Task 12's enrichment.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            ref_value="sec_1",
            decision_id="dec001",
            candidate_id="sec001aabbcc",
            candidate_ticker="VTI",
            candidate_name="Vanguard Total Stock Market ETF",
        )
        mock_pending.return_value = [group]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "sec_1" in result.output
        assert "dec001" in result.output
        assert "VTI" in result.output
        assert "Vanguard Total Stock Market ETF" in result.output

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.pending")
    @patch(
        "moneybin.services.security_links_service.SecurityLinksService.count_pending"
    )
    def test_pending_json_output_shape(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--output json emits groups[] with candidates[] and n_pending."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            ref_value="sec_j",
            decision_id="dec_j",
            candidate_id="sec_j_cand01",
        )
        mock_pending.return_value = [group]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending", "--output", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        # Same envelope as MCP: summary + data + actions
        assert "data" in parsed
        assert "groups" in parsed["data"]
        groups = parsed["data"]["groups"]
        assert len(groups) == 1
        assert groups[0]["ref_value"] == "sec_j"
        assert len(groups[0]["candidates"]) == 1
        assert groups[0]["candidates"][0]["decision_id"] == "dec_j"
        assert groups[0]["candidates"][0]["candidate_ticker"] == "VTI"
        assert "n_pending" in parsed["data"]


# ---------------------------------------------------------------------------
# links set
# ---------------------------------------------------------------------------


class TestSecurityLinksSet:
    """Tests for `investments securities links set`."""

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.accept_merge")
    def test_set_accept_calls_accept_merge(
        self,
        mock_accept: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--accept calls accept_merge with decided_by='user'."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()

        result = runner.invoke(app, ["set", "dec001", "--accept"])
        assert result.exit_code == 0
        mock_accept.assert_called_once_with("dec001", decided_by="user")

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.reject_merge")
    def test_set_reject_calls_reject_merge(
        self,
        mock_reject: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--reject calls reject_merge with decided_by='user'."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()

        result = runner.invoke(app, ["set", "dec001", "--reject"])
        assert result.exit_code == 0
        mock_reject.assert_called_once_with("dec001", decided_by="user")

    def test_set_requires_accept_or_reject(self) -> None:
        """Invoking set without --accept or --reject exits 2."""
        result = runner.invoke(app, ["set", "dec001"])
        assert result.exit_code == 2

    def test_set_rejects_both_flags(self) -> None:
        """--accept and --reject are mutually exclusive → exit 2."""
        result = runner.invoke(app, ["set", "dec001", "--accept", "--reject"])
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# links history
# ---------------------------------------------------------------------------


class TestSecurityLinksHistory:
    """Tests for `investments securities links history`."""

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.history")
    def test_history_empty(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Empty history exits 0."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.history")
    def test_history_json_output(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--output json returns an envelope with decisions[]."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = [
            {
                "decision_id": "dh001",
                "ref_kind": "plaid_security_id",
                "ref_value": "sec_h",
                "source_type": "plaid",
                "provider_ticker": "VTI",
                "provider_name": "Vanguard Total Stock Mkt ETF",
                "candidate_security_id": "sec001aabbcc",
                "status": "accepted",
                "decided_by": "user",
                "decided_at": "2025-06-01T10:00:00",
                "confidence_score": 0.5,
                "match_signals": {"signal": "fuzzy_name"},
                "match_reason": "fuzzy_name",
                "reversed_at": None,
                "reversed_by": None,
            }
        ]

        result = runner.invoke(app, ["history", "--output", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "data" in parsed
        assert "decisions" in parsed["data"]
        decisions = parsed["data"]["decisions"]
        assert len(decisions) == 1
        assert decisions[0]["decision_id"] == "dh001"
        assert decisions[0]["match_reason"] == "fuzzy_name"

    @patch("moneybin.cli.commands.investments.security_links.get_database")
    @patch("moneybin.services.security_links_service.SecurityLinksService.history")
    def test_history_limit_option(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--limit is forwarded to the service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        runner.invoke(app, ["history", "--limit", "10"])
        mock_history.assert_called_once_with(limit=10)
