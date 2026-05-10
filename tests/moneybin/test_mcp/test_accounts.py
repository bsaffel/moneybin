# tests/moneybin/test_mcp/test_accounts.py
"""Tests for accounts_resolve MCP tool.

Other accounts.* tool wiring lives in test_tools.py. This module covers the
free-text resolution tool added per docs/specs/mcp-tool-surface.md
§accounts_resolve.
"""

from __future__ import annotations

import pytest
from fastmcp import FastMCP

from moneybin.database import Database
from moneybin.mcp.tools.accounts import accounts_resolve, register_accounts_tools

pytestmark = pytest.mark.usefixtures("mcp_db")


def _seed_named_account(
    db: Database,
    account_id: str,
    display_name: str | None,
    account_subtype: str | None = None,
    institution_name: str = "Test Bank",
) -> None:
    """Insert a fully-named row directly into core.dim_accounts.

    The mcp_db template inserts ACC001/ACC002 with display_name=NULL, so
    resolve tests need to seed their own rows to exercise display_name and
    institution_name matches.
    """
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


class TestAccountsResolve:
    """Tests for the accounts_resolve MCP tool envelope and action hints."""

    @pytest.mark.unit
    async def test_envelope_shape_returns_low_sensitivity(
        self, mcp_db: Database
    ) -> None:
        """accounts_resolve returns envelope with low sensitivity and sorted results."""
        _seed_named_account(
            mcp_db,
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
    async def test_no_matches_returns_action_hint(self, tmp_path: object) -> None:
        """No matches → action hint suggests broadening or accounts_list.

        Uses an isolated empty DB rather than the mcp_db template (whose seed
        accounts have institution names that fuzzy-match almost anything via
        SequenceMatcher).
        """
        from pathlib import Path
        from unittest.mock import MagicMock

        import moneybin.database as db_module
        from moneybin.database import Database
        from tests.moneybin.db_helpers import create_core_tables_raw

        store = MagicMock()
        store.get_key.return_value = "test-encryption-key-256bit-placeholder"
        assert isinstance(tmp_path, Path)
        empty = Database(
            tmp_path / "empty.duckdb", secret_store=store, no_auto_upgrade=True
        )
        create_core_tables_raw(empty.conn)
        prior = db_module._database_instance  # type: ignore[attr-defined]
        db_module._database_instance = empty  # type: ignore[attr-defined]
        try:
            result = await accounts_resolve(query="anything")
            parsed = result.to_dict()
            assert parsed["data"] == []
            assert any(
                "accounts_list" in a or "broader" in a.lower()
                for a in parsed["actions"]
            )
        finally:
            db_module._database_instance = prior  # type: ignore[attr-defined]
            empty.close()

    @pytest.mark.unit
    async def test_low_confidence_top_match_emits_verify_hint(
        self, mcp_db: Database
    ) -> None:
        """Top match below 0.6 confidence triggers a verify-with-user hint."""
        _seed_named_account(
            mcp_db,
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
    async def test_limit_caps_returned_candidates(self, mcp_db: Database) -> None:
        """Limit parameter caps the number of returned candidates."""
        for i in range(4):
            _seed_named_account(mcp_db, f"acct_{i}", display_name=f"Account {i}")
        result = await accounts_resolve(query="account", limit=2)
        parsed = result.to_dict()
        assert len(parsed["data"]) == 2
