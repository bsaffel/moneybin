"""Tests for MCP server helper functions."""

import duckdb
import pytest

from moneybin.mcp import server
from moneybin.tables import DIM_ACCOUNTS, TableRef

pytestmark = pytest.mark.usefixtures("mcp_db")


class TestGetDb:
    """Tests for get_db()."""

    @pytest.mark.unit
    def test_returns_duckdb_connection(self, mcp_db: object) -> None:
        """get_db() returns the underlying DuckDB connection."""
        conn = server.get_db()
        assert isinstance(conn, duckdb.DuckDBPyConnection)

    @pytest.mark.unit
    def test_connection_is_read_write(self, mcp_db: object) -> None:
        """Connection allows writes (single r/w connection via Database class)."""
        conn = server.get_db()
        # Should be able to write — no InvalidInputException
        conn.execute(  # noqa: S608  # building test input string, not executing SQL
            "INSERT INTO core.dim_accounts VALUES "
            "('RWTEST', NULL, 'CHECKING', 'Test Bank', NULL, 'ofx', 'test.ofx', "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "
            "'Test Bank CHECKING ...TEST', NULL, NULL, NULL, NULL, 'USD', "
            "NULL, FALSE, TRUE)"
        )
        result = conn.execute(
            "SELECT COUNT(*) FROM core.dim_accounts WHERE account_id = 'RWTEST'"
        ).fetchone()
        assert result is not None
        assert result[0] == 1


class TestGetDbPath:
    """Tests for get_db_path()."""

    @pytest.mark.unit
    def test_returns_path(self, mcp_db: object) -> None:
        """get_db_path() returns a Path object pointing to the database file."""
        from pathlib import Path

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
    def test_existing_table_returns_true(self, mcp_db: object) -> None:
        assert server.table_exists(DIM_ACCOUNTS) is True

    @pytest.mark.unit
    def test_nonexistent_table_returns_false(self, mcp_db: object) -> None:
        assert server.table_exists(TableRef("core", "no_such_table")) is False


class TestCloseDb:
    """Tests for close_db()."""

    @pytest.mark.unit
    def test_close_db_clears_singleton(self) -> None:
        """close_db() resets the database singleton."""
        import moneybin.database as db_module

        # Ensure we have an active singleton
        conn = server.get_db()
        assert conn is not None

        # close_db() should clear the singleton
        server.close_db()
        assert db_module._database_instance is None  # type: ignore[reportPrivateUsage] — test verification
