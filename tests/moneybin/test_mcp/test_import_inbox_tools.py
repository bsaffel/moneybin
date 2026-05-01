"""Tests for import.inbox_sync / import.inbox_list MCP tools."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.mcp.tools.import_inbox import inbox_list as inbox_list_tool
from moneybin.mcp.tools.import_inbox import inbox_sync as inbox_sync_tool
from moneybin.services.inbox_service import InboxListResult, InboxSyncResult


@pytest.fixture
def patch_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Patch _build_service to return a MagicMock."""
    fake = MagicMock()
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr("moneybin.mcp.tools.import_inbox._build_service", lambda: fake)
    return fake


class TestInboxSyncTool:
    """import.inbox_sync envelope shape and actions."""

    def test_returns_low_sensitivity_envelope(self, patch_service) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 3}],
        )
        envelope = inbox_sync_tool()
        assert envelope.summary.sensitivity == "low"
        assert envelope.data["processed"][0]["filename"] == "a.csv"

    def test_failure_includes_actions_hint(self, patch_service) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            failed=[{"filename": "x.csv", "error_code": "needs_account_name"}],
        )
        envelope = inbox_sync_tool()
        assert any("inbox/<account-slug>" in a for a in envelope.actions)

    def test_no_failure_no_resolution_hint(self, patch_service) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 1}],
        )
        envelope = inbox_sync_tool()
        assert not any("inbox/<account-slug>" in a for a in envelope.actions)


class TestInboxListTool:
    """import.inbox_list envelope shape."""

    def test_returns_would_process_shape(self, patch_service) -> None:
        patch_service.enumerate.return_value = InboxListResult(
            would_process=[{"filename": "a.csv", "account_hint": None}],
        )
        envelope = inbox_list_tool()
        assert envelope.summary.sensitivity == "low"
        assert envelope.data["would_process"][0]["filename"] == "a.csv"
