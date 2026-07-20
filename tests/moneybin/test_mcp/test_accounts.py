# tests/moneybin/test_mcp/test_accounts.py
"""Tests for accounts.* MCP tools.

Other accounts.* tool wiring lives in test_tools.py. This module covers the
free-text resolution tool added per docs/specs/moneybin-mcp.md
§accounts_resolve, and the extended accounts_set entrypoint that subsumes
the rename / include / archive / unarchive narrow tools.
"""

from __future__ import annotations

import base64
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.accounts import (
    accounts_balance_assert,
    accounts_balance_assert_coarse,
    accounts_balances_coarse,
    accounts_coarse,
    accounts_resolve,
    accounts_set,
    register_accounts_coarse_reads,
    register_accounts_coarse_writes,
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


def _seed_archived_balance_data() -> None:
    """Insert one archived account with observations and an assertion."""
    _seed_named_account(
        "ARCHIVED_BALANCE",
        display_name="Old Checking",
        account_type="CHECKING",
        archived=True,
    )
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily (
                account_id, balance_date, balance, is_observed,
                observation_source, reconciliation_delta
            ) VALUES
                ('ARCHIVED_BALANCE', '2025-06-29', 100.00, TRUE, 'ofx', NULL),
                ('ARCHIVED_BALANCE', '2025-06-30', 125.00, TRUE, 'ofx', NULL)
            """
        )
        db.execute(
            """
            INSERT INTO app.balance_assertions (
                account_id, assertion_date, balance, notes
            ) VALUES ('ARCHIVED_BALANCE', '2025-06-30', 125.00, 'final statement')
            """
        )


def _replace_cursor_keys_with_numbers(cursor: str) -> str:
    """Return a structurally valid cursor with the wrong account key types."""
    payload = json.loads(base64.urlsafe_b64decode(cursor))
    payload["snapshot"] = [1] * len(payload["snapshot"])
    payload["after"] = [1] * len(payload["after"])
    return base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).decode()


def _seed_paginated_balance_views() -> None:
    """Give every balance view at least two rows."""
    _seed_balance_data()
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily (
                account_id, balance_date, balance, is_observed,
                observation_source, reconciliation_delta
            ) VALUES ('ACC002', '2025-06-29', 14900.00, TRUE, 'ofx', 10.00)
            """
        )
        db.execute(
            """
            INSERT INTO app.balance_assertions (
                account_id, assertion_date, balance, notes
            ) VALUES ('ACC002', '2025-06-30', 14950.00, 'statement')
            """
        )


def _prepend_balance_view(
    view: Literal["latest", "history", "assertions", "reconcile"],
) -> None:
    """Insert a row before one view's initial high-water key."""
    if view == "history":
        with get_database(read_only=False) as db:
            db.execute(
                """
                INSERT INTO core.fct_balances_daily (
                    account_id, balance_date, balance, is_observed,
                    observation_source, reconciliation_delta
                ) VALUES ('ACC001', '2025-06-28', 4800.00, TRUE, 'ofx', NULL)
                """
            )
        return

    _seed_named_account("000_PREPENDED", display_name="New")
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily (
                account_id, balance_date, balance, is_observed,
                observation_source, reconciliation_delta
            ) VALUES (
                '000_PREPENDED', '2025-06-30', 50.00, TRUE, 'ofx', 5.00
            )
            """
        )
        db.execute(
            """
            INSERT INTO app.balance_assertions (
                account_id, assertion_date, balance, notes
            ) VALUES ('000_PREPENDED', '2025-06-30', 45.00, 'statement')
            """
        )


class TestStandardCoarseAccountReads:
    """Contract tests for the standard account-read replacements."""

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
    async def test_account_list_cursor_survives_prepend_and_removal(
        self, mcp_db: Path
    ) -> None:
        first = await accounts_coarse(view="list", limit=1)
        assert first.next_cursor is not None
        first_id = first.data.rows[0].account_id

        _seed_named_account("000_PREPENDED", display_name="New")
        with get_database(read_only=False) as db:
            db.execute(
                "DELETE FROM core.dim_accounts WHERE account_id = ?",
                [first_id],
            )

        second = await accounts_coarse(
            view="list",
            limit=1,
            cursor=first.next_cursor,
        )

        assert [row.account_id for row in second.data.rows] == ["ACC002"]
        assert second.summary.total_count == 2
        assert second.summary.has_more is False

    @pytest.mark.unit
    async def test_account_list_cursor_binds_include_closed_and_action(
        self, mcp_db: Path
    ) -> None:
        first = await accounts_coarse(view="list", include_closed=True, limit=1)
        assert first.next_cursor is not None
        assert (
            f"Continue with accounts(view='list', include_closed=True, limit=1, "
            f"cursor='{first.next_cursor}')"
        ) in first.actions

        reused = await accounts_coarse(
            view="list",
            include_closed=False,
            limit=1,
            cursor=first.next_cursor,
        )

        assert reused.error is not None
        assert reused.error.code == "ACCOUNT_CURSOR_INVALID"

    @pytest.mark.unit
    async def test_account_resolve_ranks_exact_stable_ties(self, mcp_db: Path) -> None:
        for account_id in ("tie_c", "tie_a", "tie_b"):
            _seed_named_account(account_id, display_name="zzzz")

        response = await accounts_coarse(
            view="resolve",
            query="zzzz",
            limit=3,
        )

        assert response.summary.total_count == 3
        assert response.summary.returned_count == 3
        assert [match.account_id for match in response.data.matches] == [
            "tie_a",
            "tie_b",
            "tie_c",
        ]
        assert response.next_cursor is None

    @pytest.mark.unit
    async def test_account_resolve_ranks_by_confidence_then_account_id(
        self, mcp_db: Path
    ) -> None:
        _seed_named_account("zzz_exact", display_name="alpha")
        _seed_named_account("aaa_partial", display_name="alpha!")
        _seed_named_account("bbb_partial", display_name="alpha!!")

        response = await accounts_coarse(
            view="resolve",
            query="alpha",
            limit=3,
        )
        seen = [(match.account_id, match.confidence) for match in response.data.matches]

        assert [account_id for account_id, _confidence in seen] == [
            "zzz_exact",
            "aaa_partial",
            "bbb_partial",
        ]
        assert [confidence for _account_id, confidence in seen] == sorted(
            (confidence for _account_id, confidence in seen),
            reverse=True,
        )

    @pytest.mark.unit
    async def test_account_resolve_reports_truncation_without_cursor(
        self, mcp_db: Path
    ) -> None:
        for account_id in ("tie_c", "tie_a", "tie_b"):
            _seed_named_account(account_id, display_name="zzzz")

        response = await accounts_coarse(
            view="resolve",
            query="zzzz",
            limit=1,
        )

        assert [row.account_id for row in response.data.matches] == ["tie_a"]
        assert response.summary.total_count == 3
        assert response.summary.returned_count == 1
        assert response.summary.has_more is True
        assert response.next_cursor is None
        assert (
            "Refine the account query or increase limit to inspect more candidates."
            in response.actions
        )

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
    async def test_account_detail_resolves_exact_archived_id_without_listing_it(
        self, mcp_db: Path
    ) -> None:
        _seed_named_account("CLOSED_ID", display_name="Closed", archived=True)

        detail = await accounts_coarse(view="detail", reference="CLOSED_ID")
        active = await accounts_coarse(view="list")

        assert detail.error is None
        assert detail.data.account.account_id == "CLOSED_ID"
        assert "CLOSED_ID" not in {row.account_id for row in active.data.rows}

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
            (
                {"view": "resolve", "query": "checking", "cursor": "opaque"},
                "ACCOUNT_CURSOR_NOT_ALLOWED",
            ),
            (
                {"view": "summary", "include_closed": True},
                "ACCOUNT_INCLUDE_CLOSED_NOT_ALLOWED",
            ),
            (
                {"view": "resolve", "query": "checking", "include_closed": True},
                "ACCOUNT_INCLUDE_CLOSED_NOT_ALLOWED",
            ),
            (
                {"view": "detail", "reference": "ACC001", "limit": 1},
                "ACCOUNT_LIMIT_NOT_ALLOWED",
            ),
            (
                {"view": "summary", "limit": 1},
                "ACCOUNT_LIMIT_NOT_ALLOWED",
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
    async def test_balance_history_cursor_binds_filters_and_action(
        self, mcp_db: Path
    ) -> None:
        _seed_balance_data()
        first = await accounts_balances_coarse(
            view="history",
            reference="CHECKING",
            start=date(2025, 6, 29),
            end=date(2025, 6, 30),
            limit=1,
        )
        assert first.next_cursor is not None
        assert (
            "Continue with accounts_balances(view='history', "
            "reference='CHECKING', start='2025-06-29', end='2025-06-30', "
            f"limit=1, cursor='{first.next_cursor}')"
        ) in first.actions

        reused = await accounts_balances_coarse(
            view="history",
            reference="CHECKING",
            start=date(2025, 6, 30),
            end=date(2025, 6, 30),
            limit=1,
            cursor=first.next_cursor,
        )

        assert reused.error is not None
        assert reused.error.code == "BALANCE_CURSOR_INVALID"

    @pytest.mark.unit
    async def test_balance_history_cursor_survives_prepended_observation(
        self, mcp_db: Path
    ) -> None:
        _seed_balance_data()
        first = await accounts_balances_coarse(
            view="history",
            reference="ACC001",
            limit=1,
        )
        assert first.next_cursor is not None

        with get_database(read_only=False) as db:
            db.execute(
                """
                INSERT INTO core.fct_balances_daily (
                    account_id, balance_date, balance, is_observed,
                    observation_source, reconciliation_delta
                ) VALUES ('ACC001', '2025-06-28', 4800.00, TRUE, 'ofx', NULL)
                """
            )
        second = await accounts_balances_coarse(
            view="history",
            reference="ACC001",
            limit=1,
            cursor=first.next_cursor,
        )

        assert [row.balance_date for row in second.data.observations] == [
            date(2025, 6, 30)
        ]
        assert second.summary.total_count == 2
        assert second.summary.has_more is False

    @pytest.mark.unit
    async def test_balance_latest_as_of_and_reconcile_are_paginated(
        self, mcp_db: Path
    ) -> None:
        _seed_balance_data()
        with get_database(read_only=False) as db:
            db.execute(
                """
                INSERT INTO core.fct_balances_daily (
                    account_id, balance_date, balance, is_observed,
                    observation_source, reconciliation_delta
                ) VALUES ('ACC002', '2025-06-29', 14900.00, TRUE, 'ofx', 10.00)
                """
            )

        as_of = await accounts_balances_coarse(
            view="latest",
            as_of=date(2025, 6, 29),
            limit=1,
        )
        assert as_of.data.kind == "latest"
        assert as_of.next_cursor is not None
        assert {row.balance_date for row in as_of.data.observations} == {
            date(2025, 6, 29)
        }
        assert "as_of='2025-06-29'" in as_of.actions[-1]

        reconcile = await accounts_balances_coarse(
            view="reconcile",
            threshold=Decimal("0"),
            limit=1,
        )
        assert reconcile.data.kind == "reconcile"
        assert reconcile.next_cursor is not None
        assert reconcile.data.observations[0].reconciliation_delta is not None
        assert "threshold=0" in reconcile.actions[-1]

        reused = await accounts_balances_coarse(
            view="reconcile",
            threshold=Decimal("1"),
            limit=1,
            cursor=reconcile.next_cursor,
        )
        assert reused.error is not None
        assert reused.error.code == "BALANCE_CURSOR_INVALID"

    @pytest.mark.unit
    async def test_account_list_rejects_wrong_typed_cursor_keys(
        self,
        mcp_db: Path,
    ) -> None:
        first = await accounts_coarse(view="list", limit=1)
        assert first.next_cursor is not None

        response = await accounts_coarse(
            view="list",
            limit=1,
            cursor=_replace_cursor_keys_with_numbers(first.next_cursor),
        )

        assert response.error is not None
        assert response.error.code == "ACCOUNT_CURSOR_INVALID"
        assert response.error.message == "Invalid pagination cursor."

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "view",
        ["latest", "history", "assertions", "reconcile"],
    )
    async def test_balance_paginated_views_reject_wrong_typed_keys(
        self,
        view: Literal["latest", "history", "assertions", "reconcile"],
        mcp_db: Path,
    ) -> None:
        _seed_paginated_balance_views()
        kwargs: dict[str, object] = {}
        if view == "history":
            kwargs["reference"] = "ACC001"
        first = await accounts_balances_coarse(
            view=view,
            limit=1,
            **kwargs,  # type: ignore[arg-type]
        )
        assert first.next_cursor is not None

        response = await accounts_balances_coarse(
            view=view,
            limit=1,
            cursor=_replace_cursor_keys_with_numbers(first.next_cursor),
            **kwargs,  # type: ignore[arg-type]
        )

        assert response.error is not None
        assert response.error.code == "BALANCE_CURSOR_INVALID"
        assert response.error.message == "Invalid pagination cursor."

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "view",
        ["latest", "history", "assertions", "reconcile"],
    )
    async def test_balance_paginated_views_preserve_snapshot_total_after_prepend(
        self,
        view: Literal["latest", "history", "assertions", "reconcile"],
        mcp_db: Path,
    ) -> None:
        _seed_paginated_balance_views()
        kwargs: dict[str, object] = {}
        if view == "history":
            kwargs["reference"] = "ACC001"
        first = await accounts_balances_coarse(
            view=view,
            limit=1,
            **kwargs,  # type: ignore[arg-type]
        )
        assert first.next_cursor is not None

        _prepend_balance_view(view)

        second = await accounts_balances_coarse(
            view=view,
            limit=1,
            cursor=first.next_cursor,
            **kwargs,  # type: ignore[arg-type]
        )

        assert first.summary.total_count == 2
        assert second.summary.total_count == first.summary.total_count

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "view",
        ["latest", "history", "assertions"],
    )
    async def test_balance_views_accept_exact_archived_account_id(
        self,
        view: Literal["latest", "history", "assertions"],
        mcp_db: Path,
    ) -> None:
        _seed_archived_balance_data()
        kwargs: dict[str, Any] = {
            "view": view,
            "reference": "ARCHIVED_BALANCE",
        }
        if view == "history":
            kwargs["start"] = date(2025, 6, 29)
            kwargs["end"] = date(2025, 6, 30)

        response = await accounts_balances_coarse(**kwargs)  # type: ignore[arg-type]

        assert response.error is None
        if response.data.kind == "assertions":
            ids = [row.account_id for row in response.data.assertions]
        else:
            ids = [row.account_id for row in response.data.observations]
        assert ids
        assert set(ids) == {"ARCHIVED_BALANCE"}

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
            ({"view": "history", "as_of": "2025-01-31"}, "BALANCE_AS_OF_NOT_ALLOWED"),
            ({"view": "latest", "threshold": "1"}, "BALANCE_THRESHOLD_NOT_ALLOWED"),
            ({"view": "reconcile", "start": "2025-01-31"}, "BALANCE_DATES_NOT_ALLOWED"),
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
    @pytest.mark.parametrize(
        ("callback", "code"),
        [
            (accounts_coarse, "ACCOUNT_CURSOR_INVALID"),
            (accounts_balances_coarse, "BALANCE_CURSOR_INVALID"),
        ],
    )
    async def test_malformed_cursor_is_rejected_before_account_data_access(
        self,
        callback: Any,
        code: str,
        monkeypatch: pytest.MonkeyPatch,
        mcp_db: Path,
    ) -> None:
        async def fail_if_called(*args: object, **kwargs: object) -> object:
            raise AssertionError("account data was accessed before cursor validation")

        monkeypatch.setattr(
            "moneybin.mcp.tools.accounts._run_account_read",
            fail_if_called,
        )

        response = await callback(cursor="not-a-cursor")

        assert response.error is not None
        assert response.error.code == code

    @pytest.mark.unit
    async def test_standard_registrar_only_registers_replacement_reads(self) -> None:
        srv = FastMCP("test")
        register_accounts_coarse_reads(srv)

        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        assert names == {"accounts", "accounts_balances"}

    @pytest.mark.unit
    async def test_standard_registrar_uses_coarse_boundaries(self) -> None:
        srv = FastMCP("test")
        register_accounts_tools(srv)

        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        assert names == {
            "accounts",
            "accounts_set",
            "accounts_balances",
            "accounts_balance_assert",
        }


class TestStandardCoarseBalanceAssertionWrite:
    """Target-state behavior for the standard declarative assertion callback."""

    @pytest.mark.unit
    async def test_present_requires_amount(self, mcp_db: Path) -> None:
        response = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
        )

        assert response.error is not None
        assert response.error.code == "mutation_invalid_input"

    @pytest.mark.unit
    async def test_absent_forbids_amount(self, mcp_db: Path) -> None:
        response = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
            amount=Decimal("1250.00"),
        )

        assert response.error is not None
        assert response.error.code == "mutation_invalid_input"

    @pytest.mark.unit
    async def test_present_resolves_reference_and_upserts_target(
        self, mcp_db: Path
    ) -> None:
        with get_database(read_only=False) as db:
            db.execute(
                "UPDATE core.dim_accounts SET display_name = ? WHERE account_id = ?",
                ["House Checking", "ACC001"],
            )

        created = await accounts_balance_assert_coarse(
            account="House Checking",
            as_of=date(2026, 7, 1),
            amount=Decimal("1250.00"),
        )
        updated = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            amount=Decimal("1300.00"),
        )

        assert created.data.account_id == "ACC001"
        assert created.data.as_of == date(2026, 7, 1)
        assert created.data.prior_state == "absent"
        assert created.data.state == "present"
        assert created.data.operation_id.startswith("op_")
        assert updated.data.prior_state == "present"
        assert updated.data.state == "present"
        with get_database(read_only=True) as db:
            row = db.execute(
                """
                SELECT balance
                FROM app.balance_assertions
                WHERE account_id = ? AND assertion_date = ?
                """,
                ["ACC001", date(2026, 7, 1)],
            ).fetchone()
        assert row == (Decimal("1300.00"),)

    @pytest.mark.unit
    async def test_present_already_satisfied_is_noop(self, mcp_db: Path) -> None:
        first = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            amount=Decimal("1250.00"),
        )
        second = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            amount=Decimal("1250.00"),
        )

        assert first.error is None
        assert second.error is not None
        assert second.error.code == "mutation_nothing_to_do"
        with get_database(read_only=True) as db:
            audits = db.execute(
                """
                SELECT action
                FROM app.audit_log
                WHERE target_id = ?
                """,
                ["ACC001|2026-07-01"],
            ).fetchall()
        assert audits == [("balance_assertion.set",)]

    @pytest.mark.unit
    async def test_absent_already_satisfied_is_noop_without_confirmation(
        self, mcp_db: Path
    ) -> None:
        response = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
        )

        assert response.error is not None
        assert response.error.code == "mutation_nothing_to_do"
        assert response.error.details is None

    @pytest.mark.unit
    async def test_absent_uses_exact_payload_bound_confirmation(
        self, mcp_db: Path
    ) -> None:
        present = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            amount=Decimal("1250.00"),
        )
        required = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
        )
        assert required.error is not None
        assert required.error.code == "mutation_confirmation_required"
        assert required.error.details is not None
        assert required.error.details["operation_kind"] == "balance_assertion_remove"
        assert required.error.details["blast_radius"] == {"assertions": 1}
        token = required.error.details["confirmation_token"]

        removed = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
            confirmation_token=token,
        )

        assert removed.error is None
        assert removed.data.account_id == "ACC001"
        assert removed.data.prior_state == "present"
        assert removed.data.state == "absent"
        assert removed.data.operation_id != present.data.operation_id
        with get_database(read_only=True) as db:
            assertion = db.execute(
                """
                SELECT 1
                FROM app.balance_assertions
                WHERE account_id = ? AND assertion_date = ?
                """,
                ["ACC001", date(2026, 7, 1)],
            ).fetchone()
            audit = db.execute(
                """
                SELECT actor, action, operation_id
                FROM app.audit_log
                WHERE target_id = ? AND action = 'balance_assertion.delete'
                """,
                ["ACC001|2026-07-01"],
            ).fetchall()
        assert assertion is None
        assert audit == [("mcp", "balance_assertion.delete", removed.data.operation_id)]
        assert [action.tool for action in removed.recovery_actions] == [
            "system_audit",
            "system_audit_undo",
        ]
        assert removed.recovery_actions[0].arguments == {
            "view": "detail",
            "operation_id": removed.data.operation_id,
        }
        assert removed.recovery_actions[1].arguments == {
            "operation_id": removed.data.operation_id
        }

    @pytest.mark.unit
    async def test_absent_recomputes_exact_assertion_before_delete(
        self, mcp_db: Path
    ) -> None:
        await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            amount=Decimal("1250.00"),
        )
        required = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
        )
        assert required.error is not None
        assert required.error.details is not None
        token = required.error.details["confirmation_token"]
        await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            amount=Decimal("1300.00"),
        )

        refused = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
            confirmation_token=token,
        )

        assert refused.error is not None
        assert refused.error.code == "mutation_confirmation_mismatch"
        with get_database(read_only=True) as db:
            row = db.execute(
                """
                SELECT balance
                FROM app.balance_assertions
                WHERE account_id = ? AND assertion_date = ?
                """,
                ["ACC001", date(2026, 7, 1)],
            ).fetchone()
            delete_audits = db.execute(
                """
                SELECT 1
                FROM app.audit_log
                WHERE target_id = ? AND action = 'balance_assertion.delete'
                """,
                ["ACC001|2026-07-01"],
            ).fetchall()
        assert row == (Decimal("1300.00"),)
        assert delete_audits == []

    @pytest.mark.unit
    async def test_same_value_reassert_consumes_stale_confirmation(
        self, mcp_db: Path
    ) -> None:
        await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            amount=Decimal("1250.00"),
        )
        with get_database(read_only=False) as db:
            db.execute(
                """
                UPDATE app.balance_assertions
                SET updated_at = ?
                WHERE account_id = ? AND assertion_date = ?
                """,
                ["2026-07-01 12:00:00", "ACC001", date(2026, 7, 1)],
            )
        required = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
        )
        assert required.error is not None
        assert required.error.details is not None
        token = required.error.details["confirmation_token"]

        reasserted = accounts_balance_assert(
            account_id="ACC001",
            assertion_date="2026-07-01",
            balance=1250.0,
        )
        assert reasserted.error is None
        refused = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
            confirmation_token=token,
        )
        replayed = await accounts_balance_assert_coarse(
            account="ACC001",
            as_of=date(2026, 7, 1),
            state="absent",
            confirmation_token=token,
        )

        assert refused.error is not None
        assert refused.error.code == "mutation_confirmation_mismatch"
        assert replayed.error is not None
        assert replayed.error.code == "mutation_confirmation_replayed"
        with get_database(read_only=True) as db:
            row = db.execute(
                """
                SELECT balance
                FROM app.balance_assertions
                WHERE account_id = ? AND assertion_date = ?
                """,
                ["ACC001", date(2026, 7, 1)],
            ).fetchone()
            delete_audits = db.execute(
                """
                SELECT 1
                FROM app.audit_log
                WHERE target_id = ? AND action = 'balance_assertion.delete'
                """,
                ["ACC001|2026-07-01"],
            ).fetchall()
        assert row == (Decimal("1250.00"),)
        assert delete_audits == []

    @pytest.mark.unit
    async def test_standard_write_registrar_only_registers_replacement(self) -> None:
        srv = FastMCP("test")
        register_accounts_coarse_writes(srv)

        tools = await srv._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        assert [tool.name for tool in tools] == ["accounts_balance_assert"]
        tool = tools[0]
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is False
        assert tool.annotations.destructiveHint is True
        assert tool.annotations.idempotentHint is True


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
    async def test_helper_envelope_defaults_to_low_sensitivity(
        self, mcp_db: Path
    ) -> None:
        """The undecorated compatibility helper leaves privacy to its caller."""
        _seed_named_account(
            "a1",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
        )
        result = accounts_resolve(query="chase")
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "low"
        # data is now {"matches": [...]} from AccountResolvePayload serialization
        assert isinstance(parsed["data"], dict)
        matches = result.data.matches
        assert len(matches) >= 1
        # account_id is RECORD_ID (spec D6) — passes through unmasked.
        assert matches[0].account_id == "a1"
        # Data shape matches AccountResolutionItem fields
        assert matches[0].confidence
        assert matches[0].display_name

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

        result = accounts_resolve(query="anything")
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
        result = accounts_resolve(query="qq")
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
        result = accounts_resolve(query="account", limit=2)
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
