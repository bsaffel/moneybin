"""Tests for MCP server lifecycle and DuckDB connection management."""

from pathlib import Path

import duckdb
import pytest

from moneybin.mcp import server
from moneybin.mcp.server import DIM_ACCOUNTS, TableRef


@pytest.fixture
def test_db_path(mcp_db: duckdb.DuckDBPyConnection, tmp_path: Path) -> Path:
    """Return path to the pre-populated test DB, closing the active connection.

    The shared mcp_db fixture creates schemas and base data.  This fixture
    closes that writable connection so server lifecycle tests can call
    init_db() / close_db() themselves.
    """
    server.close_db()
    return tmp_path / "test.duckdb"


class TestDatabaseLifecycle:
    """Tests for init_db / get_db / close_db."""

    @pytest.mark.unit
    def test_get_db_before_init_raises(self) -> None:
        """get_db() should raise if init_db() hasn't been called."""
        server.close_db()
        with pytest.raises(RuntimeError, match="not initialized"):
            server.get_db()

    @pytest.mark.unit
    def test_init_db_opens_connection(self, test_db_path: Path) -> None:
        """init_db() should open a working read-only connection."""
        try:
            server.init_db(test_db_path)
            conn = server.get_db()
            result = conn.execute(
                """SELECT COUNT(*) FROM core.dim_accounts"""
            ).fetchone()
            assert result is not None
            assert result[0] == 2
        finally:
            server.close_db()

    @pytest.mark.unit
    def test_init_db_read_only(self, test_db_path: Path) -> None:
        """Database should be opened in read-only mode."""
        try:
            server.init_db(test_db_path)
            conn = server.get_db()
            with pytest.raises(duckdb.InvalidInputException):
                conn.execute(
                    "INSERT INTO core.dim_accounts VALUES "
                    "('X', 'X', 'X', 'X', 'X', 'X', 'X', "
                    "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "
                    "CURRENT_TIMESTAMP)"
                )
        finally:
            server.close_db()

    @pytest.mark.unit
    def test_init_db_missing_file_raises(self, tmp_path: Path) -> None:
        """init_db() should raise FileNotFoundError for missing database."""
        server.close_db()
        with pytest.raises(FileNotFoundError, match="not found"):
            server.init_db(tmp_path / "nonexistent.duckdb")

    @pytest.mark.unit
    def test_close_db_clears_connection(self, test_db_path: Path) -> None:
        """close_db() should set _db back to None."""
        server.init_db(test_db_path)
        server.close_db()
        assert server._db is None  # type: ignore[reportPrivateUsage] — test verification


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
    def test_existing_table_returns_true(self) -> None:
        assert server.table_exists(DIM_ACCOUNTS) is True

    @pytest.mark.unit
    def test_nonexistent_table_returns_false(self) -> None:
        assert server.table_exists(TableRef("core", "no_such_table")) is False
