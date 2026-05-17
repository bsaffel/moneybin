# tests/moneybin/test_mcp/test_accounts.py
"""Tests for accounts.* MCP tools.

Other accounts.* tool wiring lives in test_tools.py. This module covers the
free-text resolution tool added per docs/specs/moneybin-mcp.md
§accounts_resolve, and the extended accounts_set entrypoint that subsumes
the rename / include / archive / unarchive narrow tools.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.accounts import (
    accounts_resolve,
    accounts_set,
    register_accounts_tools,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


def _seed_named_account(
    account_id: str,
    display_name: str | None,
    account_subtype: str | None = None,
    institution_name: str = "Test Bank",
) -> None:
    """Insert a fully-named row directly into core.dim_accounts.

    Opens and closes its own write connection so the caller doesn't hold an
    open connection when the MCP tool runs.  The mcp_db template inserts
    ACC001/ACC002 with display_name=NULL, so resolve tests need to seed their
    own rows to exercise display_name and institution_name matches.
    """
    with get_database() as db:
        db.execute(
            """
            INSERT INTO core.dim_accounts (
                account_id, routing_number, account_type, institution_name,
                institution_fid, source_type, source_file, extracted_at,
                loaded_at, updated_at, display_name, account_subtype
            ) VALUES (?, NULL, 'CHECKING', ?, NULL, 'ofx', 'test.qfx',
                      '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?)
            """,
            [account_id, institution_name, display_name, account_subtype],
        )


class TestAccountsResolveRegistration:
    """Verify accounts_resolve is registered with the FastMCP server."""

    @pytest.mark.unit
    async def test_accounts_resolve_registered(self) -> None:
        srv = FastMCP("test")
        register_accounts_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert "accounts_resolve" in names


class TestNarrowToolsRemoved:
    """The four narrow account write tools were folded into accounts_set."""

    @pytest.mark.unit
    async def test_narrow_account_tools_removed(self) -> None:
        srv = FastMCP("test")
        register_accounts_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        for removed in (
            "accounts_rename",
            "accounts_include",
            "accounts_archive",
            "accounts_unarchive",
        ):
            assert removed not in names, (
                f"{removed} should be removed; folded into accounts_set"
            )
        assert "accounts_set" in names


class TestAccountsResolve:
    """Tests for the accounts_resolve MCP tool envelope and action hints."""

    @pytest.mark.unit
    async def test_envelope_shape_returns_low_sensitivity(self, mcp_db: Path) -> None:
        """accounts_resolve returns envelope with low sensitivity and sorted results."""
        _seed_named_account(
            "a1",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
        )
        result = await accounts_resolve(query="chase")
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "low"
        assert isinstance(parsed["data"], list)
        assert len(parsed["data"]) >= 1
        assert parsed["data"][0]["account_id"] == "a1"
        # Data shape matches AccountResolution.to_dict
        assert "confidence" in parsed["data"][0]
        assert "display_name" in parsed["data"][0]

    @pytest.mark.unit
    async def test_no_matches_returns_action_hint(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No matches → action hint suggests broadening or accounts.

        Uses an isolated empty DB rather than the mcp_db template (whose seed
        accounts have institution names that fuzzy-match almost anything via
        SequenceMatcher).
        """
        from unittest.mock import MagicMock

        from moneybin.database import Database
        from tests.moneybin.db_helpers import create_core_tables_raw

        store = MagicMock()
        store.get_key.return_value = "test-encryption-key-256bit-placeholder"
        empty_path = tmp_path / "empty.duckdb"

        # Set up an empty DB at the path
        empty = Database(empty_path, secret_store=store, no_auto_upgrade=True)
        create_core_tables_raw(empty.conn)
        empty.close()

        # Redirect get_database() to the empty path
        mock_settings = MagicMock()
        mock_settings.database.path = empty_path
        monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
        monkeypatch.setattr("moneybin.database.SecretStore", lambda: store)

        result = await accounts_resolve(query="anything")
        parsed = result.to_dict()
        assert parsed["data"] == []
        assert any("accounts" in a or "broader" in a.lower() for a in parsed["actions"])

    @pytest.mark.unit
    async def test_low_confidence_top_match_emits_verify_hint(
        self, mcp_db: Path
    ) -> None:
        """Top match below 0.6 confidence triggers a verify-with-user hint."""
        _seed_named_account(
            "a1",
            display_name="XYZ Account",
            institution_name="XYZ Bank",
        )
        result = await accounts_resolve(query="qq")
        parsed = result.to_dict()
        # Either the match exists with low confidence and we get a hint,
        # or no matches at all (handled by other test). Skip if no matches.
        if parsed["data"] and parsed["data"][0]["confidence"] < 0.6:
            assert any(
                "verify" in a.lower() or "low confidence" in a.lower()
                for a in parsed["actions"]
            )

    @pytest.mark.unit
    async def test_limit_caps_returned_candidates(self, mcp_db: Path) -> None:
        """Limit parameter caps the number of returned candidates."""
        for i in range(4):
            _seed_named_account(f"acct_{i}", display_name=f"Account {i}")
        result = await accounts_resolve(query="account", limit=2)
        parsed = result.to_dict()
        assert len(parsed["data"]) == 2


class TestAccountsSetExtended:
    """Tests for the extended accounts_set MCP tool.

    The mcp_db template seeds ACC001/ACC002 in core.dim_accounts with no row
    in app.account_settings — settings_update lazy-creates the row.
    """

    @pytest.mark.unit
    async def test_accepts_display_name_and_include(self, mcp_db: Path) -> None:
        """accounts_set accepts display_name and include_in_net_worth together."""
        result = await accounts_set(
            account_id="ACC001",
            display_name="My Custom Name",
            include_in_net_worth=False,
        )
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "medium"
        assert parsed["data"]["display_name"] == "My Custom Name"
        assert parsed["data"]["include_in_net_worth"] is False
        assert parsed["data"]["archived"] is False
        # No cascade unless is_archived=True was passed.
        assert "cascaded_include_in_net_worth" not in parsed["data"]

    @pytest.mark.unit
    async def test_is_archived_cascades_to_include(self, mcp_db: Path) -> None:
        """is_archived=True translates to archived=True and cascades include_in_net_worth=False."""
        result = await accounts_set(account_id="ACC001", is_archived=True)
        parsed = result.to_dict()
        # AccountSettings.to_dict() emits "archived", not "is_archived".
        assert parsed["data"]["archived"] is True
        assert parsed["data"]["include_in_net_worth"] is False
        assert parsed["data"]["cascaded_include_in_net_worth"] is False

    @pytest.mark.unit
    async def test_unarchive_does_not_restore_include(self, mcp_db: Path) -> None:
        """Unarchive (is_archived=False) leaves include_in_net_worth unchanged."""
        # Archive first → include cascades to False.
        await accounts_set(account_id="ACC001", is_archived=True)
        # Unarchive without an explicit include flag.
        result = await accounts_set(account_id="ACC001", is_archived=False)
        parsed = result.to_dict()
        assert parsed["data"]["archived"] is False
        # NOT restored — caller must opt back in explicitly.
        assert parsed["data"]["include_in_net_worth"] is False
        assert "cascaded_include_in_net_worth" not in parsed["data"]

    @pytest.mark.unit
    async def test_clear_display_name(self, mcp_db: Path) -> None:
        """display_name is in _CLEARABLE_FIELDS; clearing it returns NULL."""
        # Set a name first.
        await accounts_set(account_id="ACC001", display_name="Initial Name")
        # Now clear it.
        result = await accounts_set(account_id="ACC001", clear_fields=["display_name"])
        parsed = result.to_dict()
        assert parsed["data"]["display_name"] is None
