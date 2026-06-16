"""Tests for `accounts links` CLI commands.

Mirrors test_matches.py for the transactions surface. CLI tests mock the
service layer and test argument parsing, exit codes, and output shape.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.accounts.links import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pending_group(
    *,
    provisional_id: str = "PROV1",
    provisional_name: str = "Provisional Account",
    decision_id: str = "dec001",
    candidate_id: str = "CAND001",
    candidate_name: str = "Candidate Account",
    confidence: float = 0.85,
    signal: str = "institution_last4",
) -> MagicMock:
    """Build a mock PendingLinkGroup with sensible defaults."""
    candidate = MagicMock()
    candidate.decision_id = decision_id
    candidate.candidate_account_id = candidate_id
    candidate.candidate_display_name = candidate_name
    candidate.confidence = confidence
    candidate.signal = signal

    group = MagicMock()
    group.provisional_account_id = provisional_id
    group.provisional_display_name = provisional_name
    group.candidates = [candidate]
    return group


# ---------------------------------------------------------------------------
# links pending
# ---------------------------------------------------------------------------


class TestLinksPending:
    """Tests for `accounts links pending`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
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

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_shows_provisional_and_candidates(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Text output includes provisional account id and candidate decision ids."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            provisional_id="PROV1",
            decision_id="dec001",
            candidate_id="CAND001",
        )
        mock_pending.return_value = [group]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "PROV1" in result.output
        assert "dec001" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_json_output_shape(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--output json emits groups[] with candidates[] and n_pending."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            provisional_id="PROV_J",
            decision_id="dec_j",
            candidate_id="CAND_J",
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
        assert groups[0]["provisional_account_id"] == "PROV_J"
        assert len(groups[0]["candidates"]) == 1
        assert groups[0]["candidates"][0]["decision_id"] == "dec_j"
        assert "n_pending" in parsed["data"]

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_json_no_ref_value(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """JSON output never includes ref_value."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [_make_pending_group()]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending", "--output", "json"])
        assert result.exit_code == 0
        assert "ref_value" not in result.output


# ---------------------------------------------------------------------------
# links set
# ---------------------------------------------------------------------------


class TestLinksSet:
    """Tests for `accounts links set`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_calls_service_with_target(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--into <account_id> passes target_account_id to service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"])
        assert result.exit_code == 0
        mock_set.assert_called_once_with(
            "dec001", target_account_id="CAND001", decided_by="user"
        )

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_standalone_calls_service_with_none(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--standalone passes target_account_id=None to service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()

        result = runner.invoke(app, ["set", "dec001", "--standalone"])
        assert result.exit_code == 0
        mock_set.assert_called_once_with(
            "dec001", target_account_id=None, decided_by="user"
        )

    def test_set_requires_into_or_standalone(self) -> None:
        """Invoking set without --into or --standalone exits 2."""
        result = runner.invoke(app, ["set", "dec001"])
        assert result.exit_code == 2

    def test_set_rejects_both_flags(self) -> None:
        """--into and --standalone are mutually exclusive → exit 2."""
        result = runner.invoke(
            app, ["set", "dec001", "--into", "CAND001", "--standalone"]
        )
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# links history
# ---------------------------------------------------------------------------


class TestLinksRun:
    """Tests for `accounts links run`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_exits_0(self, mock_run: MagicMock, mock_get_db: MagicMock) -> None:
        """Run exits 0 and prints the new-proposal count."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 3

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        assert "3" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_mentions_pending_command(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Run output hints the user toward `accounts links pending`."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 2

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        assert "pending" in result.output.lower()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_zero_proposals_exits_0(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Run with 0 new proposals still exits 0."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 0

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_json_output_shape(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--output json returns an envelope with new_proposals in data."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 7

        result = runner.invoke(app, ["run", "--output", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "data" in parsed
        assert parsed["data"]["new_proposals"] == 7


class TestLinksHistory:
    """Tests for `accounts links history`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_empty(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Empty history exits 0."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_json_output(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--output json returns an envelope with decisions[]."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = [
            {
                "decision_id": "dh001",
                "provisional_account_id": "PROV_H",
                "candidate_account_id": "CAND_H",
                "status": "accepted",
                "decided_by": "user",
                "decided_at": "2025-06-01T10:00:00",
                "confidence_score": 0.85,
                "match_signals": {"signal": "name"},
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
        assert decisions[0]["signal"] == "name"

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_limit_option(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--limit is forwarded to the service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        runner.invoke(app, ["history", "--limit", "10"])
        mock_history.assert_called_once_with(limit=10)
