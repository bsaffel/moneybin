# ruff: noqa: S101
"""Tests for sqlmesh_context — encrypted DB injection into SQLMesh."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.database import DatabaseKeyError


class TestSQLMeshContext:
    """sqlmesh_context injects the explicit db's adapter into SQLMesh's cache."""

    def test_raises_when_conn_is_none(self) -> None:
        """sqlmesh_context raises if the db's connection was closed."""
        from moneybin.database import sqlmesh_context

        mock_db = MagicMock()
        mock_db._conn = None

        with pytest.raises(DatabaseKeyError, match="closed"):
            with sqlmesh_context(mock_db):
                pass

    def test_rejects_mock_db_with_non_path_db_path(self) -> None:
        """A mock db that slips past the conn check fails loudly, never littering.

        Regression for the #11 junk-dir leak: a test driving the real
        ``sqlmesh_context`` with a bare ``MagicMock`` (``_db_path`` left as an
        auto-mock) and ``sqlmesh.Context`` un-patched silently mkdir'd
        ``sqlmesh/<MagicMock ...>/`` under the project root — because
        ``cache_dir=str(db._db_path.parent / ...)`` stringified the mock. The
        guard converts that into an immediate ``TypeError`` (raised before any
        ``Context`` is built) whose traceback names the offending test, so the
        intermittent leak can never recur silently.
        """
        from moneybin.database import sqlmesh_context

        mock_db = MagicMock()
        mock_db._conn = MagicMock()  # truthy → passes the closed-connection check
        # _db_path deliberately left as an auto-mock — the exact bug condition.

        with pytest.raises(TypeError, match="requires an open Database"):
            with sqlmesh_context(mock_db):
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
        mock_db._db_path = db_path

        cache: dict[str, object] = {}

        with patch(
            "sqlmesh.core.config.connection.BaseDuckDBConnectionConfig._data_file_to_adapter",
            cache,
        ):
            from moneybin.database import sqlmesh_context

            with sqlmesh_context(mock_db) as ctx:
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
    def test_cache_dir_isolated_per_db(
        self,
        mock_ctx_cls: MagicMock,
        mock_adapter_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """cache_dir is derived from the db path, not the shared project cache.

        SQLMesh's on-disk cache is keyed by model fingerprint, independent of
        which DB/environment a plan targets. Sharing one cache across concurrent
        restate plans on different DBs (xdist scenario workers) poisons snapshots
        and raises ConflictingPlanError. Pinning cache_dir under the db's own
        directory isolates it per-profile (prod) and per-test tmpdir (tests).
        """
        mock_db = MagicMock()
        mock_db._conn = MagicMock()
        db_path = tmp_path / "profile_a" / "moneybin.duckdb"
        mock_db._db_path = db_path

        cache: dict[str, object] = {}
        with patch(
            "sqlmesh.core.config.connection.BaseDuckDBConnectionConfig._data_file_to_adapter",
            cache,
        ):
            from moneybin.database import sqlmesh_context

            with sqlmesh_context(mock_db):
                pass

        config = mock_ctx_cls.call_args.kwargs["config"]
        assert config.cache_dir == str(db_path.parent / ".sqlmesh-cache")

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
        mock_db._db_path = db_path

        cache: dict[str, object] = {}

        with patch(
            "sqlmesh.core.config.connection.BaseDuckDBConnectionConfig._data_file_to_adapter",
            cache,
        ):
            from moneybin.database import sqlmesh_context

            with pytest.raises(RuntimeError, match="SQLMesh boom"):
                with sqlmesh_context(mock_db) as ctx:
                    ctx.plan(auto_apply=True, no_prompts=True)

            # Cache cleaned up despite error
            assert len(cache) == 0
