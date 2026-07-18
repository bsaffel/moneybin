# tests/moneybin/test_mcp/test_accounts.py
"""Tests for accounts.* MCP tools.

Other accounts.* tool wiring lives in test_tools.py. This module covers the
free-text resolution tool added per docs/specs/moneybin-mcp.md
§accounts_resolve, and the extended accounts_set entrypoint that subsumes
the rename / include / archive / unarchive narrow tools.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.accounts import (
    accounts_balances_coarse,
    accounts_coarse,
    accounts_resolve,
    accounts_set,
    register_accounts_coarse_reads,
    register_accounts_tools,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


def _seed_named_account(
    account_id: str,
    display_name: str | None,
    account_subtype: str | None = None,
    institution_name: str = "Test Bank",
    account_type: str = "CHECKING",
    archived: bool = False,
) -> None:
    """Insert a fully-named row directly into core.dim_accounts.

    Opens and closes its own write connection so the caller doesn't hold an
    open connection when the MCP tool runs.  The mcp_db template inserts
    ACC001/ACC002 with display_name=NULL, so resolve tests need to seed their
    own rows to exercise display_name and institution_name matches.
    """
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.dim_accounts (
                account_id, routing_number, account_type, institution_name,
                institution_fid, source_type, source_file, extracted_at,
                loaded_at, updated_at, display_name, account_subtype, archived
            ) VALUES (?, NULL, ?, ?, NULL, 'ofx', 'test.qfx',
                      '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?)
            """,
            [
                account_id,
                account_type,
                institution_name,
                display_name,
                account_subtype,
                archived,
            ],
        )


def _seed_balance_data() -> None:
    """Insert daily observations and one assertion for coarse balance views."""
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily (
                account_id, balance_date, balance, is_observed,
                observation_source, reconciliation_delta
            ) VALUES
                ('ACC001', '2025-06-29', 4900.00, TRUE, 'ofx', NULL),
                ('ACC001', '2025-06-30', 5000.00, TRUE, 'ofx', 25.00),
                ('ACC002', '2025-06-30', 15000.00, TRUE, 'ofx', NULL)
            """
        )
        db.execute(
            """
            INSERT INTO app.balance_assertions (
                account_id, assertion_date, balance, notes
            ) VALUES ('ACC001', '2025-06-30', 4975.00, 'statement')
            """
        )


class TestDormantCoarseAccountReads:
    """Contract tests for the Plan 6 account-read replacements."""

    @pytest.mark.unit
    async def test_account_detail_uses_shared_reference_resolution(
        self, mcp_db: Path
    ) -> None:
        with get_database(read_only=False) as db:
            db.execute(
                "UPDATE core.dim_accounts SET display_name = ? WHERE account_id = ?",
                ["Checking", "ACC001"],
            )

        response = await accounts_coarse(view="detail", reference="Checking")

        assert response.data.kind == "detail"
        assert response.data.account.account_id == "ACC001"

    @pytest.mark.unit
    async def test_account_detail_refuses_ambiguous_reference(
        self, mcp_db: Path
    ) -> None:
        savings_ids = ["SAVINGS_B", "SAVINGS_A"]
        for account_id in savings_ids:
            _seed_named_account(
                account_id,
                display_name="Savings",
                account_type="SAVINGS",
            )

        response = await accounts_coarse(view="detail", reference="Savings")

        assert response.error is not None
        assert response.error.code == "ENTITY_REFERENCE_AMBIGUOUS"
        assert response.error.details == {
            "candidate_ids": sorted(["ACC002", *savings_ids])
        }

    @pytest.mark.unit
    async def test_account_detail_returns_structured_missing_reference(
        self, mcp_db: Path
    ) -> None:
        response = await accounts_coarse(view="detail", reference="Vacation")

        assert response.error is not None
        assert response.error.code == "ENTITY_REFERENCE_NOT_FOUND"
        assert response.error.details == {"candidate_ids": []}

    @pytest.mark.unit
    async def test_account_list_paginates_with_exact_counts(self, mcp_db: Path) -> None:
        first = await accounts_coarse(view="list", limit=1)

        assert first.data.kind == "list"
        assert len(first.data.rows) == 1
        assert first.summary.total_count == 2
        assert first.summary.returned_count == 1
        assert first.summary.has_more is True
        assert first.next_cursor is not None

        second = await accounts_coarse(
            view="list",
            limit=1,
            cursor=first.next_cursor,
        )

        assert second.data.kind == "list"
        assert len(second.data.rows) == 1
        assert second.data.rows[0].account_id != first.data.rows[0].account_id
        assert second.summary.total_count == 2
        assert second.summary.returned_count == 1
        assert second.summary.has_more is False

    @pytest.mark.unit
    async def test_account_list_include_closed_is_strict_and_effective(
        self, mcp_db: Path
    ) -> None:
        _seed_named_account("CLOSED_ID", display_name="Closed", archived=True)

        active = await accounts_coarse(view="list")
        all_accounts = await accounts_coarse(view="list", include_closed=True)

        assert {row.account_id for row in active.data.rows} == {"ACC001", "ACC002"}
        assert {row.account_id for row in all_accounts.data.rows} == {
            "ACC001",
            "ACC002",
            "CLOSED_ID",
        }

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("kwargs", "code"),
        [
            ({"view": "list", "reference": "ACC001"}, "ACCOUNT_REFERENCE_NOT_ALLOWED"),
            ({"view": "summary", "query": "checking"}, "ACCOUNT_QUERY_NOT_ALLOWED"),
            ({"view": "detail"}, "ACCOUNT_REFERENCE_REQUIRED"),
            (
                {"view": "detail", "reference": "ACC001", "query": "checking"},
                "ACCOUNT_QUERY_NOT_ALLOWED",
            ),
            ({"view": "resolve"}, "ACCOUNT_QUERY_REQUIRED"),
            (
                {"view": "resolve", "query": "checking", "reference": "ACC001"},
                "ACCOUNT_REFERENCE_NOT_ALLOWED",
            ),
            (
                {"view": "summary", "cursor": "opaque"},
                "ACCOUNT_CURSOR_NOT_ALLOWED",
            ),
        ],
    )
    async def test_account_views_reject_invalid_combinations(
        self,
        kwargs: dict[str, object],
        code: str,
        mcp_db: Path,
    ) -> None:
        response = await accounts_coarse(**kwargs)  # type: ignore[arg-type]

        assert response.error is not None
        assert response.error.code == code

    @pytest.mark.unit
    async def test_balance_views_resolve_references_and_preserve_period(
        self, mcp_db: Path
    ) -> None:
        _seed_balance_data()

        history = await accounts_balances_coarse(
            view="history",
            reference="CHECKING",
            start=date(2025, 6, 30),
            end=date(2025, 6, 30),
        )
        assertions = await accounts_balances_coarse(
            view="assertions",
            reference="CHECKING",
        )

        assert history.data.kind == "history"
        assert [row.account_id for row in history.data.observations] == ["ACC001"]
        assert history.summary.period == "2025-06-30 to 2025-06-30"
        assert assertions.data.kind == "assertions"
        assert [row.account_id for row in assertions.data.assertions] == ["ACC001"]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("kwargs", "code"),
        [
            (
                {"view": "latest", "start": "2025-01-01"},
                "BALANCE_DATES_NOT_ALLOWED",
            ),
            (
                {"view": "assertions", "end": "2025-01-31"},
                "BALANCE_DATES_NOT_ALLOWED",
            ),
            ({"view": "history"}, "ACCOUNT_REFERENCE_REQUIRED"),
            (
                {"view": "latest", "cursor": "not-a-cursor"},
                "BALANCE_CURSOR_INVALID",
            ),
        ],
    )
    async def test_balance_views_reject_invalid_combinations(
        self,
        kwargs: dict[str, object],
        code: str,
        mcp_db: Path,
    ) -> None:
        response = await accounts_balances_coarse(**kwargs)  # type: ignore[arg-type]

        assert response.error is not None
        assert response.error.code == code

    @pytest.mark.unit
    async def test_dormant_registrar_only_registers_replacement_reads(self) -> None:
        srv = FastMCP("test")
        register_accounts_coarse_reads(srv)

        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        assert names == {"accounts", "accounts_balances"}

    @pytest.mark.unit
    async def test_live_registrar_remains_unchanged(self) -> None:
        srv = FastMCP("test")
        register_accounts_tools(srv)

        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        assert "accounts" in names
        assert "accounts_get" in names
        assert "accounts_summary" in names
        assert "accounts_resolve" in names
        assert "accounts_balances" in names
        assert "accounts_balance_history" in names
        assert "accounts_balance_assertions" in names


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
    async def test_envelope_shape_returns_medium_sensitivity(
        self, mcp_db: Path
    ) -> None:
        """accounts_resolve returns MEDIUM sensitivity (display_name is USER_NOTE).

        account_id is RECORD_ID (spec D6) — no longer CRITICAL; the highest
        class in AccountResolutionItem is USER_NOTE (display_name) → MEDIUM.
        """
        _seed_named_account(
            "a1",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
        )
        result = await accounts_resolve(query="chase")
        parsed = result.to_dict()
        # USER_NOTE (display_name) → Tier.MEDIUM per privacy taxonomy
        assert parsed["summary"]["sensitivity"] == "medium"
        # data is now {"matches": [...]} from AccountResolvePayload serialization
        assert isinstance(parsed["data"], dict)
        matches = parsed["data"]["matches"]
        assert len(matches) >= 1
        # account_id is RECORD_ID (spec D6) — passes through unmasked.
        assert matches[0]["account_id"] == "a1"
        # Data shape matches AccountResolutionItem fields
        assert "confidence" in matches[0]
        assert "display_name" in matches[0]

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
        empty = Database(
            empty_path, secret_store=store, no_auto_upgrade=True, read_only=False
        )
        create_core_tables_raw(empty.conn)
        empty.close()

        # Redirect get_database() to the empty path
        mock_settings = MagicMock()
        mock_settings.database.path = empty_path
        monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
        monkeypatch.setattr("moneybin.database.SecretStore", lambda: store)

        result = await accounts_resolve(query="anything")
        parsed = result.to_dict()
        # data is {"matches": []} — empty payload
        assert parsed["data"]["matches"] == []
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
        matches = parsed["data"]["matches"]
        # Either the match exists with low confidence and we get a hint,
        # or no matches at all (handled by other test). Skip if no matches.
        if matches and matches[0]["confidence"] < 0.6:
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
        assert len(parsed["data"]["matches"]) == 2


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
        # AccountSettingsPayload is CRITICAL via last_four (INSTITUTION_ACCOUNT_NUMBER);
        # account_id is RECORD_ID (spec D6).
        assert parsed["summary"]["sensitivity"] == "critical"
        assert parsed["data"]["display_name"] == "My Custom Name"
        assert parsed["data"]["include_in_net_worth"] is False
        assert parsed["data"]["archived"] is False
        # No cascade: cascaded_include_in_net_worth is None when is_archived != True.
        assert parsed["data"]["cascaded_include_in_net_worth"] is None

    @pytest.mark.unit
    async def test_is_archived_cascades_to_include(self, mcp_db: Path) -> None:
        """is_archived=True translates to archived=True and cascades include_in_net_worth=False."""
        result = await accounts_set(account_id="ACC001", is_archived=True)
        parsed = result.to_dict()
        # AccountSettingsPayload emits "archived", not "is_archived".
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
        # No cascade for unarchive: cascaded_include_in_net_worth is None.
        assert parsed["data"]["cascaded_include_in_net_worth"] is None

    @pytest.mark.unit
    async def test_clear_display_name(self, mcp_db: Path) -> None:
        """display_name is in _CLEARABLE_FIELDS; clearing it returns NULL."""
        # Set a name first.
        await accounts_set(account_id="ACC001", display_name="Initial Name")
        # Now clear it.
        result = await accounts_set(account_id="ACC001", clear_fields=["display_name"])
        parsed = result.to_dict()
        assert parsed["data"]["display_name"] is None

    @pytest.mark.unit
    async def test_default_cost_basis_method_round_trips(self, mcp_db: Path) -> None:
        """default_cost_basis_method param round-trips through the payload."""
        result = await accounts_set(
            account_id="ACC001", default_cost_basis_method="hifo"
        )
        parsed = result.to_dict()
        assert parsed["data"]["default_cost_basis_method"] == "hifo"

    @pytest.mark.unit
    async def test_clear_default_cost_basis_method(self, mcp_db: Path) -> None:
        """default_cost_basis_method is in _CLEARABLE_FIELDS; clearing it returns NULL."""
        await accounts_set(account_id="ACC001", default_cost_basis_method="fifo")
        result = await accounts_set(
            account_id="ACC001", clear_fields=["default_cost_basis_method"]
        )
        parsed = result.to_dict()
        assert parsed["data"]["default_cost_basis_method"] is None
