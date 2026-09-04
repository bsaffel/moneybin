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

from moneybin import error_codes
from moneybin.cli.commands.sql import app
from moneybin.database import Database
from moneybin.tables import FCT_TRANSACTION_PROVENANCE
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
    monkeypatch.setattr("moneybin.privacy.sensitivity.get_max_rows", lambda: 100)
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
def test_query_write_is_refused_and_writes_nothing(seeded_db: Database) -> None:
    """A write is refused before execution, and the row count proves it.

    The statement is a **valid** insert against a table the fixture creates, so
    nothing but the gates stands between it and a successful write. It used to
    be ``INSERT INTO core.dim_accounts VALUES ('x')`` — one value for a
    multi-column table — asserted with ``exit_code != 0``, which a binder error
    satisfies just as well as a refusal.

    Two assertions, and it is worth being exact about which one carries which
    risk, because probing them said something the code alone does not.

    **The error code is what goes red.** It proves a *write-refusal* fired
    rather than a catalog or binder accident. It does not isolate *which* gate:
    two independent ones refuse a write and both raise ``SQL_INVALID_QUERY`` —
    the read-only prefix check in ``validate_read_only_query``, and the
    statement-shape routing in ``execute_sql_query`` that admits only data and
    metadata queries. Disabling the first leaves the insert refused by the
    second ("Only SELECT queries and DESCRIBE/SHOW are supported."). That
    redundancy is the design, and neither gate is reachable without the other,
    so no fixture can separate them (``.claude/rules/testing.md``, "A Fixture
    That Trips Two Guards Isolates Neither").

    **The row count covers what the code cannot.** ``seeded_db`` inserts exactly
    one row, so a write that landed makes it two whatever the process exited
    with. Disabling both gates does not reach it today — the data path then
    fails to bind a non-``SELECT`` and returns ``SQL_UNKNOWN_TABLE`` — so this
    assertion is standing cover for a future path that refuses a statement
    *after* executing it, which is the failure no exit code or error code would
    reveal.
    """
    result = runner.invoke(
        app,
        [
            "query",
            "INSERT INTO core.dim_accounts (account_id) VALUES ('x')",
            "--output",
            "json",
        ],
    )

    assert result.exit_code != 0, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == error_codes.SQL_INVALID_QUERY
    rows = seeded_db.execute("SELECT COUNT(*) FROM core.dim_accounts").fetchone()
    assert rows is not None
    assert rows[0] == 1, "the refused INSERT must not have landed"


@pytest.mark.usefixtures("_patched")
def test_query_disallowed_schema_is_refused_by_the_schema_gate() -> None:
    """The refusal must be the *gate's*, not any non-zero exit that resembles it.

    ``exit_code != 0`` alone proved nothing here. The fixture used to name
    ``meta.internal_only``, which exists nowhere, so deleting the schema gate
    outright left the query failing on an unknown table — still non-zero, still
    green. Two changes make it real:

    - **A table that exists.** ``meta.fct_transaction_provenance``, through the
      ``moneybin.tables`` constant, so there is a successful query underneath
      for the gate to be the only thing preventing.
    - **The error code, not the exit code.** A schema refusal is
      ``SQL_SCHEMA_NOT_ALLOWED``; every unknown table, column, or binder failure
      is ``SQL_UNKNOWN_TABLE``. Asserting the code is what distinguishes "the
      gate refused this" from "something else went wrong first".

    The query names one column rather than ``*``: ``expand_star`` runs *before*
    the gate on the data path, so a star over a table outside the lineage
    snapshot would raise a binder error and never reach the refusal under test.

    Re-aimed from ``raw.ofx_transactions`` when M2O.2 admitted ``raw``/``prep``
    through this same gate. ``meta`` is a real MoneyBin schema that stays
    internal.
    """
    result = runner.invoke(
        app,
        [
            "query",
            f"SELECT transaction_id FROM {FCT_TRANSACTION_PROVENANCE.full_name}",  # noqa: S608  # TableRef constant; the query is the test input, and the gate refuses it
            "--output",
            "json",
        ],
    )

    assert result.exit_code != 0, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == error_codes.SQL_SCHEMA_NOT_ALLOWED
