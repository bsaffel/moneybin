"""Shared stubs for PDF disclosure-consent tests (MCP host context, inbox settings)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from pytest import MonkeyPatch

import moneybin.mcp.pdf_disclosure as disclosure_module
from moneybin.config import ImportSettings, MoneyBinSettings


def mcp_host_ctx(
    *, name: str | None = "claude-ai", elicit: AsyncMock | None = None
) -> MagicMock:
    """A stand-in FastMCP context reporting the given ``clientInfo`` name."""
    ctx = MagicMock()
    ctx.session.client_params.clientInfo.name = name
    ctx.session.client_params.clientInfo.version = "1.0.0"
    ctx.transport = "stdio"
    ctx.elicit = elicit or AsyncMock()
    return ctx


def use_mcp_host(
    monkeypatch: MonkeyPatch, ctx: MagicMock | None, *, elicits: bool = False
) -> None:
    """Make the disclosure module see ``ctx`` as the active MCP context."""

    def _supports(_ctx: object) -> bool:
        return elicits

    monkeypatch.setattr(disclosure_module, "_active_context", lambda: ctx)
    monkeypatch.setattr(disclosure_module, "supports_elicitation", _supports)


def make_inbox_settings(tmp_path: Path) -> MoneyBinSettings:
    return MoneyBinSettings(
        profile="test",
        import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
    )
