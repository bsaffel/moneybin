# ruff: noqa: S101
"""Tests for sqlmesh_context — encrypted DB injection into SQLMesh."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.database import DatabaseKeyError


class TestSQLMeshContext:
    """sqlmesh_context injects the singleton's adapter into SQLMesh's cache."""

    def test_raises_when_no_singleton(self) -> None:
        """sqlmesh_context requires an active Database singleton."""
        from moneybin.database import sqlmesh_context

        with (
            patch("moneybin.database._database_instance", None),
            pytest.raises(DatabaseKeyError, match="Database not initialized"),
        ):
            with sqlmesh_context():
                pass

    def test_raises_when_conn_is_none(self) -> None:
        """sqlmesh_context raises if the singleton's connection was closed."""
        from moneybin.database import sqlmesh_context

        mock_db = MagicMock()
        mock_db._conn = None

        with (
            patch("moneybin.database._database_instance", mock_db),
            pytest.raises(DatabaseKeyError, match="Database not initialized"),
        ):
            with sqlmesh_context():
                pass

    @patch("sqlmesh.core.engine_adapter.duckdb.DuckDBEngineAdapter")
    @patch("sqlmesh.Context")
    def test_injects_and_cleans_adapter_cache(
        self,
        mock_ctx_cls: MagicMock,
        mock_adapter_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Adapter is injected into cache on entry and removed on exit."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn = mock_conn
        db_path = tmp_path / "test.duckdb"

        cache: dict[str, object] = {}

        with (
            patch("moneybin.database._database_instance", mock_db),
            patch(
                "sqlmesh.core.config.connection.BaseDuckDBConnectionConfig._data_file_to_adapter",
                cache,
            ),
            patch("moneybin.database.get_settings") as mock_settings,
        ):
            mock_settings.return_value.database.path = db_path

            from moneybin.database import sqlmesh_context

            with sqlmesh_context() as ctx:
                # Adapter is in cache during context
                assert str(db_path) in cache
                ctx.plan(auto_apply=True, no_prompts=True)

            # Adapter removed after exit
            assert str(db_path) not in cache

        mock_ctx_cls.return_value.plan.assert_called_once_with(
            auto_apply=True, no_prompts=True
        )

    @patch("sqlmesh.core.engine_adapter.duckdb.DuckDBEngineAdapter")
    @patch("sqlmesh.Context")
    def test_cleans_up_on_failure(
        self,
        mock_ctx_cls: MagicMock,
        mock_adapter_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Adapter cache is cleaned up even if SQLMesh fails."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn = mock_conn
        mock_ctx_cls.return_value.plan.side_effect = RuntimeError("SQLMesh boom")
        db_path = tmp_path / "test.duckdb"

        cache: dict[str, object] = {}

        with (
            patch("moneybin.database._database_instance", mock_db),
            patch(
                "sqlmesh.core.config.connection.BaseDuckDBConnectionConfig._data_file_to_adapter",
                cache,
            ),
            patch("moneybin.database.get_settings") as mock_settings,
        ):
            mock_settings.return_value.database.path = db_path

            from moneybin.database import sqlmesh_context

            with pytest.raises(RuntimeError, match="SQLMesh boom"):
                with sqlmesh_context() as ctx:
                    ctx.plan(auto_apply=True, no_prompts=True)

            # Cache cleaned up despite error
            assert len(cache) == 0
