"""Tests for MCP server helper functions."""

from pathlib import Path

import pytest

from moneybin.mcp import server
from moneybin.tables import DIM_ACCOUNTS, TableRef

pytestmark = pytest.mark.usefixtures("mcp_db")


class TestGetDbPath:
    """Tests for get_db_path()."""

    @pytest.mark.unit
    def test_returns_path(self, mcp_db: Path) -> None:
        """get_db_path() returns a Path object pointing to the database file."""
        path = server.get_db_path()
        assert isinstance(path, Path)
        assert path.name == "test.duckdb"


class TestTableRef:
    """Tests for the TableRef named tuple."""

    @pytest.mark.unit
    def test_qualified_name(self) -> None:
        ref = TableRef("core", "dim_accounts")
        assert ref.full_name == "core.dim_accounts"

    @pytest.mark.unit
    def test_schema_and_name_accessible(self) -> None:
        ref = TableRef("core", "fct_transactions")
        assert ref.schema == "core"
        assert ref.name == "fct_transactions"

    @pytest.mark.unit
    def test_module_constants_defined(self) -> None:
        assert DIM_ACCOUNTS.full_name == "core.dim_accounts"


class TestTableExists:
    """Tests for the table_exists function."""

    @pytest.mark.unit
    def test_existing_table_returns_true(self, mcp_db: Path) -> None:
        assert server.table_exists(DIM_ACCOUNTS) is True

    @pytest.mark.unit
    def test_nonexistent_table_returns_false(self, mcp_db: Path) -> None:
        assert server.table_exists(TableRef("core", "no_such_table")) is False


class TestCloseDb:
    """Tests for close_db()."""

    @pytest.mark.unit
    def test_close_db_logs_without_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """close_db() completes without raising when no DB was accessed."""
        import logging

        with caplog.at_level(logging.INFO, logger="moneybin.mcp.server"):
            server.close_db()
        assert "MCP session closing" in caplog.text

    @pytest.mark.unit
    def test_close_db_flushes_metrics_when_accessed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """close_db() always calls flush_metrics(); flush_metrics guards on database_was_written()."""
        from unittest.mock import patch

        with patch("moneybin.observability.flush_metrics") as mock_flush:
            server.close_db()
        mock_flush.assert_called_once()
