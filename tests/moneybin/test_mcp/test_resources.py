"""Tests for MCP v1 resource definitions."""

from __future__ import annotations

import json
from typing import Any

import pytest

from moneybin.mcp.resources import (
    resource_accounts,
    resource_privacy,
    resource_status,
    resource_tools,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


# ---------------------------------------------------------------------------
# moneybin://status
# ---------------------------------------------------------------------------


class TestResourceStatus:
    """Tests for moneybin://status resource."""

    @pytest.mark.unit
    def test_returns_accounts_count(self) -> None:
        result = resource_status()
        data: dict[str, Any] = json.loads(result)
        assert "accounts" in data
        assert data["accounts"]["total"] == 2

    @pytest.mark.unit
    def test_transactions_absent_when_empty(self) -> None:
        # No transactions inserted — key should be absent or total 0
        result = resource_status()
        data: dict[str, Any] = json.loads(result)
        transactions = data.get("transactions", {})
        assert transactions.get("total", 0) == 0

    @pytest.mark.unit
    def test_transactions_present_after_insert(self, mcp_db: object) -> None:
        from moneybin.mcp import server

        server.get_db().execute("""
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description, memo,
                transaction_type, is_pending, currency_code, source_type,
                source_extracted_at, loaded_at,
                transaction_year, transaction_month, transaction_day,
                transaction_day_of_week, transaction_year_month,
                transaction_year_quarter
            ) VALUES
            ('TXN_S01', 'ACC001', '2025-06-15', -42.50, 42.50, 'expense',
             'Grocery Store', 'Weekly groceries', 'DEBIT', false, 'USD', 'ofx',
             '2025-01-24', CURRENT_TIMESTAMP,
             2025, 6, 15, 0, '2025-06', '2025-Q2')
        """)
        result = resource_status()
        data: dict[str, Any] = json.loads(result)
        assert data["transactions"]["total"] == 1
        assert data["transactions"]["date_range_start"] == "2025-06-15"
        assert data["transactions"]["date_range_end"] == "2025-06-15"


# ---------------------------------------------------------------------------
# moneybin://accounts
# ---------------------------------------------------------------------------


class TestResourceAccounts:
    """Tests for moneybin://accounts resource."""

    @pytest.mark.unit
    def test_returns_accounts_list(self) -> None:
        result = resource_accounts()
        data: dict[str, Any] = json.loads(result)
        assert "accounts" in data
        assert len(data["accounts"]) == 2

    @pytest.mark.unit
    def test_account_fields_present(self) -> None:
        result = resource_accounts()
        data: dict[str, Any] = json.loads(result)
        account = data["accounts"][0]
        assert "account_id" in account
        assert "account_type" in account
        assert "institution_name" in account
        assert "source_type" in account

    @pytest.mark.unit
    def test_no_balance_fields(self) -> None:
        result = resource_accounts()
        data: dict[str, Any] = json.loads(result)
        for account in data["accounts"]:
            assert "ledger_balance" not in account
            assert "available_balance" not in account

    @pytest.mark.unit
    def test_institution_names_present(self) -> None:
        result = resource_accounts()
        data: dict[str, Any] = json.loads(result)
        names = {a["institution_name"] for a in data["accounts"]}
        assert "Test Bank" in names
        assert "Other Bank" in names


# ---------------------------------------------------------------------------
# moneybin://privacy
# ---------------------------------------------------------------------------


class TestResourcePrivacy:
    """Tests for moneybin://privacy resource."""

    @pytest.mark.unit
    def test_returns_consent_grants(self) -> None:
        result = resource_privacy()
        data: dict[str, Any] = json.loads(result)
        assert "consent_grants" in data
        assert isinstance(data["consent_grants"], list)

    @pytest.mark.unit
    def test_consent_mode_opt_in(self) -> None:
        result = resource_privacy()
        data: dict[str, Any] = json.loads(result)
        assert data["consent_mode"] == "opt-in"

    @pytest.mark.unit
    def test_unmask_critical_false(self) -> None:
        result = resource_privacy()
        data: dict[str, Any] = json.loads(result)
        assert data["unmask_critical"] is False


# ---------------------------------------------------------------------------
# moneybin://tools
# ---------------------------------------------------------------------------


class TestResourceTools:
    """Tests for moneybin://tools resource."""

    @pytest.fixture(autouse=True)
    def _ensure_tools_registered(self) -> None:
        """Ensure the real FastMCP server has core tools registered."""
        from moneybin.mcp.server import register_core_tools

        register_core_tools()

    def _read(self) -> dict[str, Any]:
        import asyncio

        return json.loads(asyncio.run(resource_tools()))

    @pytest.mark.unit
    def test_returns_core_namespaces(self) -> None:
        data = self._read()
        assert "core" in data
        assert isinstance(data["core"], list)
        core: list[dict[str, Any]] = data["core"]
        assert len(core) > 0

    @pytest.mark.unit
    def test_core_namespaces_have_required_fields(self) -> None:
        data = self._read()
        for entry in data["core"]:
            assert "namespace" in entry
            assert "loaded" in entry
            assert "description" in entry

    @pytest.mark.unit
    def test_core_namespaces_loaded_true(self) -> None:
        data = self._read()
        core_entries: list[dict[str, Any]] = data["core"]
        assert all(entry["loaded"] is True for entry in core_entries)

    @pytest.mark.unit
    def test_discover_tool_present(self) -> None:
        data = self._read()
        assert data["discover_tool"] == "moneybin_discover"

    @pytest.mark.unit
    def test_known_namespaces_present(self) -> None:
        data = self._read()
        namespaces = {e["namespace"] for e in data["core"]}
        assert "spending" in namespaces
        assert "accounts" in namespaces
