"""Tests for MCP server helper functions."""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from moneybin.database import DatabaseNotInitializedError, get_database
from moneybin.db_lock import OperationType
from moneybin.mcp import server
from moneybin.repositories.import_previews_repo import ImportPreviewsRepo
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


def test_startup_purges_expired_import_preview_snapshots() -> None:
    """Configured server boot deletes expired staged financial-file bytes."""
    now = datetime.now(UTC)
    with get_database(read_only=False) as db:
        repo = ImportPreviewsRepo(db)
        preview_id = repo.issue(
            file_path="/Users/example/expired.csv",
            file_sha256="a" * 64,
            file_size_bytes=13,
            channel="tabular",
            source_bytes=b"expired bytes",
            snapshot={"data": {}, "actions": [], "sensitivity": "medium"},
            issued_at=now - timedelta(minutes=10),
            expires_at=now - timedelta(minutes=5),
            actor="mcp",
        )
        live_id = repo.issue(
            file_path="/Users/example/live.csv",
            file_sha256="b" * 64,
            file_size_bytes=10,
            channel="tabular",
            source_bytes=b"live bytes",
            snapshot={"data": {}, "actions": [], "sensitivity": "medium"},
            issued_at=now,
            expires_at=now + timedelta(minutes=5),
            actor="mcp",
        )
        consumed_id = repo.issue(
            file_path="/Users/example/consumed.csv",
            file_sha256="c" * 64,
            file_size_bytes=14,
            channel="tabular",
            source_bytes=b"consumed bytes",
            snapshot={"data": {}, "actions": [], "sensitivity": "medium"},
            issued_at=now - timedelta(minutes=10),
            expires_at=now - timedelta(minutes=5),
            actor="mcp",
        )
        repo.consume(
            consumed_id,
            file_sha256="c" * 64,
            file_size_bytes=14,
            now=now - timedelta(minutes=9),
            actor="mcp",
        )

    assert server.purge_expired_import_previews_at_boot() == 1

    with get_database(read_only=True) as db:
        repo = ImportPreviewsRepo(db)
        assert repo.get(preview_id) is None
        assert repo.get_source_bytes(preview_id) is None
        assert repo.get(live_id) is not None
        assert repo.get_source_bytes(live_id) == b"live bytes"
        assert repo.get(consumed_id) is not None
        assert repo.get_source_bytes(consumed_id) is None
        audits = db.execute(
            """
            SELECT actor
            FROM app.audit_log
            WHERE action = 'import_preview.expire' AND target_id = ?
            """,
            [preview_id],
        ).fetchall()
        assert audits == [("system",)]

    assert server.purge_expired_import_previews_at_boot() == 0

    with get_database(read_only=True) as db:
        audit_count = db.execute(
            """
            SELECT COUNT(*)
            FROM app.audit_log
            WHERE action = 'import_preview.expire' AND target_id = ?
            """,
            [preview_id],
        ).fetchone()
        assert audit_count == (1,)


def test_startup_purge_does_not_create_missing_database(mcp_db: Path) -> None:
    """Retention maintenance preserves the explicit db-init lifecycle."""
    mcp_db.unlink()

    assert server.purge_expired_import_previews_at_boot() == 0
    assert not mcp_db.exists()


def test_require_existing_rechecks_after_writer_lock(
    mcp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Maintenance never recreates a database deleted while waiting for its lock."""

    @contextmanager
    def deleting_lock(
        _db_path: Path,
        *,
        deadline: float,
        operation_type: OperationType,
    ) -> Generator[None, None, None]:
        del deadline, operation_type
        mcp_db.unlink()
        yield

    monkeypatch.setattr("moneybin.db_lock.write_lock", deleting_lock)

    with pytest.raises(DatabaseNotInitializedError):
        get_database(read_only=False, require_existing=True)
    assert not mcp_db.exists()


def test_startup_purge_stays_read_only_when_nothing_needs_cleanup() -> None:
    """A clean startup does not contend for the exclusive writer lock."""
    with patch("moneybin.database.get_database", wraps=get_database) as open_db:
        assert server.purge_expired_import_previews_at_boot() == 0

    assert [call.kwargs["read_only"] for call in open_db.call_args_list] == [True]


@pytest.mark.parametrize(
    ("missing_table", "purged"),
    [
        ("app.import_previews", 0),
        ("raw.import_preview_snapshots", 1),
    ],
)
def test_startup_repairs_partial_preview_schema_and_removes_orphan(
    missing_table: str,
    purged: int,
) -> None:
    """A crash between schema files cannot strand staged financial bytes."""
    now = datetime.now(UTC)
    with get_database(read_only=False) as db:
        repo = ImportPreviewsRepo(db)
        repo.issue(
            file_path="/Users/example/partial.csv",
            file_sha256="d" * 64,
            file_size_bytes=13,
            channel="tabular",
            source_bytes=b"partial bytes",
            snapshot={"data": {}, "actions": [], "sensitivity": "medium"},
            issued_at=now,
            expires_at=now + timedelta(minutes=5),
            actor="mcp",
        )
        db.execute(f"DROP TABLE {missing_table}")  # noqa: S608  # parametrized code-owned table names

    assert server.purge_expired_import_previews_at_boot() == purged

    with get_database(read_only=True) as db:
        app_count = db.execute("SELECT COUNT(*) FROM app.import_previews").fetchone()
        raw_count = db.execute(
            "SELECT COUNT(*) FROM raw.import_preview_snapshots"
        ).fetchone()
        assert app_count == (0,)
        assert raw_count == (0,)


def test_server_instructions_orient_to_standard_surface() -> None:
    """Session onboarding names final workflows and shared trust semantics."""
    assert server.mcp.instructions is not None
    instructions = server.mcp.instructions.lower()

    for required in (
        "system_status",
        "negative = expense",
        "positive = income",
        "summary.display_currency",
        "degrade",
        "confirmation",
        "system_audit",
        "system_audit_undo",
        "reports",
        "report_id",
        "sql_query",
        "read-only",
    ):
        assert required in instructions
    for deprecated in (
        "reports_networth",
        "accounts_summary",
        "transactions_get",
        "pack",
    ):
        assert deprecated not in instructions
