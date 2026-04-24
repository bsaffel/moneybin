# ruff: noqa: S101
"""Tests for sqlmesh_context — encrypted DB injection into SQLMesh."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestSQLMeshContext:
    """sqlmesh_context injects an encrypted adapter into SQLMesh's cache."""

    @patch("moneybin.database.SecretStore")
    @patch("sqlmesh.core.engine_adapter.duckdb.DuckDBEngineAdapter")
    @patch("sqlmesh.Context")
    @patch("duckdb.connect")
    def test_attaches_encrypted_database(
        self,
        mock_duckdb_connect: MagicMock,
        mock_ctx_cls: MagicMock,
        mock_adapter_cls: MagicMock,
        mock_store_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify the DuckDB connection ATTACHes with ENCRYPTION_KEY."""
        mock_store = mock_store_cls.return_value
        mock_store.get_key.return_value = "deadbeef" * 8  # 64-char hex key

        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        db_path = tmp_path / "test.duckdb"

        with (
            patch(
                "sqlmesh.core.config.connection.BaseDuckDBConnectionConfig._data_file_to_adapter",
                {},
            ),
            patch("moneybin.database.get_settings") as mock_settings,
        ):
            mock_settings.return_value.database.path = db_path

            from moneybin.database import sqlmesh_context

            with sqlmesh_context() as ctx:
                ctx.plan(auto_apply=True, no_prompts=True)

        mock_store.get_key.assert_called_once_with("DATABASE__ENCRYPTION_KEY")

        # Verify ATTACH was called with encryption key
        execute_calls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("ENCRYPTION_KEY" in c for c in execute_calls), (
            f"Expected ATTACH with ENCRYPTION_KEY, got: {execute_calls}"
        )

        # Verify SQLMesh plan was run
        mock_ctx_cls.return_value.plan.assert_called_once_with(
            auto_apply=True, no_prompts=True
        )
        # Connection closed after context manager exits
        mock_conn.close.assert_called_once()

    @patch("moneybin.database.SecretStore")
    @patch("sqlmesh.core.engine_adapter.duckdb.DuckDBEngineAdapter")
    @patch("sqlmesh.Context")
    @patch("duckdb.connect")
    def test_cleans_up_on_failure(
        self,
        mock_duckdb_connect: MagicMock,
        mock_ctx_cls: MagicMock,
        mock_adapter_cls: MagicMock,
        mock_store_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Adapter cache and connection are cleaned up even if SQLMesh fails."""
        mock_store = mock_store_cls.return_value
        mock_store.get_key.return_value = "deadbeef" * 8

        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        mock_ctx_cls.return_value.plan.side_effect = RuntimeError("SQLMesh boom")
        db_path = tmp_path / "test.duckdb"

        with (
            patch(
                "sqlmesh.core.config.connection.BaseDuckDBConnectionConfig._data_file_to_adapter",
                {},
            ) as cache,
            patch("moneybin.database.get_settings") as mock_settings,
        ):
            mock_settings.return_value.database.path = db_path

            from moneybin.database import sqlmesh_context

            with pytest.raises(RuntimeError, match="SQLMesh boom"):
                with sqlmesh_context() as ctx:
                    ctx.plan(auto_apply=True, no_prompts=True)

            # Cache cleaned up despite error
            assert len(cache) == 0

        # Connection closed despite error
        mock_conn.close.assert_called_once()
