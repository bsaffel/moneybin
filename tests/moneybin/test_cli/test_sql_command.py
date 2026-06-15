"""Tests for `moneybin sql query` — the privacy-safe ad-hoc SQL CLI command.

Verifies the CLI wrapper inherits the shared primitive's enforcement:
CRITICAL columns are masked in both text and JSON output, and rejected
queries exit non-zero. Deep lineage behavior is covered at the primitive
level in tests/privacy/test_sql_query.py.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.sql import app
from moneybin.database import Database
from tests.moneybin.db_helpers import (
    apply_core_table_comments,
    create_core_dim_stub_views,
    create_core_tables_raw,
)

runner = CliRunner()


@pytest.fixture()
def seeded_db(tmp_path: Path) -> Generator[Database, None, None]:
    """A Database with core.* tables and one account row to mask."""
    store = MagicMock()
    store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path / "sql_cli.duckdb",
        secret_store=store,
        no_auto_upgrade=True,
        read_only=False,
    )
    create_core_tables_raw(db.conn)
    apply_core_table_comments(db)
    create_core_dim_stub_views(db)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, routing_number, account_type) "
        "VALUES ('ACC000123456789', '021000021', 'checking')"
    )
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def _patched(  # pyright: ignore[reportUnusedFunction]  # used via @pytest.mark.usefixtures
    seeded_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Route the command at the seeded DB and isolate it from config + audit IO."""

    @contextmanager
    def _fake_get_database(*_args: object, **_kwargs: object) -> Generator[Database]:
        yield seeded_db

    def _noop_event(_event: dict[str, Any]) -> None:
        return None

    monkeypatch.setattr("moneybin.cli.commands.sql.get_database", _fake_get_database)
    monkeypatch.setattr("moneybin.mcp.privacy.get_max_rows", lambda: 100)
    # Keep the test hermetic — don't write to a real privacy.log.jsonl.
    monkeypatch.setattr("moneybin.cli.output.write_privacy_event", _noop_event)


def test_query_help_lists_output_flag() -> None:
    """`sql query --help` wires and advertises the read-only output flags."""
    result = runner.invoke(app, ["query", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output


@pytest.mark.usefixtures("_patched")
def test_query_json_masks_critical() -> None:
    """JSON output masks the CRITICAL routing_number — parity with the MCP tool."""
    result = runner.invoke(
        app, ["query", "SELECT routing_number FROM core.dim_accounts", "-o", "json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert str(payload["data"][0]["routing_number"]).startswith("****")
    assert "021000021" not in result.output
    assert payload["summary"]["sensitivity"] == "critical"


@pytest.mark.usefixtures("_patched")
def test_query_text_masks_critical() -> None:
    """Text output masks the CRITICAL routing_number and never prints the raw value."""
    result = runner.invoke(
        app, ["query", "SELECT routing_number FROM core.dim_accounts"]
    )
    assert result.exit_code == 0, result.output
    assert "****" in result.output
    assert "021000021" not in result.output


@pytest.mark.usefixtures("_patched")
def test_query_write_exits_nonzero() -> None:
    """A write statement is rejected by the read-only gate (exit non-zero)."""
    result = runner.invoke(app, ["query", "INSERT INTO core.dim_accounts VALUES ('x')"])
    assert result.exit_code != 0


@pytest.mark.usefixtures("_patched")
def test_query_disallowed_schema_exits_nonzero() -> None:
    """A query outside core/app is refused by the schema gate (exit non-zero)."""
    result = runner.invoke(
        app, ["query", "SELECT account_id FROM raw.ofx_transactions"]
    )
    assert result.exit_code != 0
