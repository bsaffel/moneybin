"""Tests for import_inbox_sync / import_inbox_list MCP tools."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.mcp.tools.import_inbox import inbox_list as inbox_list_tool
from moneybin.mcp.tools.import_inbox import inbox_sync as inbox_sync_tool
from moneybin.services.inbox_service import (
    InboxListResult,
    InboxService,
    InboxSyncResult,
)


@pytest.fixture
def patch_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> MagicMock:
    """Patch InboxService factories so MCP tool tests don't open a real DB."""
    fake = MagicMock()
    fake.root = tmp_path / "inbox-root"

    def _factory(cls: type[InboxService]) -> MagicMock:
        return fake

    monkeypatch.setattr(
        "moneybin.services.inbox_service.InboxService.for_active_profile",
        classmethod(_factory),
    )
    monkeypatch.setattr(
        "moneybin.services.inbox_service.InboxService.for_active_profile_no_db",
        classmethod(_factory),
    )
    return fake


class TestInboxSyncTool:
    """import_inbox_sync envelope shape and actions."""

    async def test_returns_low_sensitivity_envelope(
        self, patch_service: MagicMock
    ) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 3}],
        )
        envelope = await inbox_sync_tool()
        assert envelope.summary.sensitivity == "low"
        assert envelope.data["processed"][0]["filename"] == "a.csv"

    async def test_failure_includes_actions_hint(
        self, patch_service: MagicMock
    ) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            failed=[{"filename": "x.csv", "error_code": "needs_account_name"}],
        )
        envelope = await inbox_sync_tool()
        assert any("inbox/<account-slug>" in a for a in envelope.actions)

    async def test_no_failure_no_resolution_hint(
        self, patch_service: MagicMock
    ) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 1}],
        )
        envelope = await inbox_sync_tool()
        assert not any("inbox/<account-slug>" in a for a in envelope.actions)

    async def test_categorize_hint_appears_when_above_threshold(
        self, patch_service: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Hint referencing categorize_assist is appended when uncategorized >= threshold."""
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 5}],
        )
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_inbox._uncategorized_count",
            lambda: 50,
        )
        envelope = await inbox_sync_tool()
        assert any("categorize_assist" in a for a in envelope.actions)
        assert any("50" in a for a in envelope.actions)

    async def test_categorize_hint_absent_below_threshold(
        self, patch_service: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No hint when uncategorized count is below the configured threshold."""
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 1}],
        )
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_inbox._uncategorized_count",
            lambda: 0,
        )
        envelope = await inbox_sync_tool()
        assert not any("categorize_assist" in a for a in envelope.actions)


class TestInboxListTool:
    """import_inbox_list envelope shape."""

    async def test_returns_would_process_shape(self, patch_service: MagicMock) -> None:
        patch_service.enumerate.return_value = InboxListResult(
            would_process=[{"filename": "a.csv", "account_hint": None}],
        )
        envelope = await inbox_list_tool()
        assert envelope.summary.sensitivity == "low"
        assert envelope.data["would_process"][0]["filename"] == "a.csv"
