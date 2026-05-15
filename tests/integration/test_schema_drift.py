# ruff: noqa: S101
"""Integration test: boot check raises when materialized columns drift.

Builds a SQLMesh-applied DB via ImportService, drops one column from
core.dim_accounts, then verifies the MCP boot-time schema check fires.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, SchemaDriftError

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "tabular"


@pytest.mark.integration
def test_boot_check_raises_on_dropped_column(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_check_schema_at_boot() raises SchemaDriftError when a column is missing."""
    # Leading underscore preserved: function is wired in by cli.commands.mcp
    # only. Importing it here is intentional to verify boot-path behavior.
    from moneybin.mcp.server import (
        _check_schema_at_boot,  # pyright: ignore[reportPrivateUsage]
    )
    from moneybin.services.import_service import ImportService

    secret_store = MagicMock()
    secret_store.get_key.return_value = "integration-test-key-0123456789abcdef"

    db_path = tmp_path / "drift.duckdb"
    db = Database(db_path, secret_store=secret_store)

    # sqlmesh_context() reads get_settings().database.path to key the
    # adapter cache. Point it at this test DB so SQLMesh reuses the
    # encrypted connection. get_database() (called by the boot helper)
    # also reads get_settings() for the path and instantiates a fresh
    # SecretStore() — patch SecretStore so the boot helper finds the same
    # encryption key without hitting the real keyring.
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
    monkeypatch.setattr("moneybin.database.SecretStore", lambda: secret_store)

    fixture = FIXTURES_DIR / "standard.csv"
    assert fixture.exists(), f"missing fixture: {fixture}"

    result = ImportService(db).import_file(
        fixture, account_name="checking", auto_accept=True
    )
    assert result.core_tables_rebuilt, "transforms must run to materialize core.*"

    # SQLMesh exposes core.dim_accounts as a view over a versioned physical
    # table in the sqlmesh__core schema. ALTER TABLE/VIEW DROP COLUMN won't
    # work on a view, so simulate drift by replacing the view with a copy
    # that omits display_name. The drift check reads duckdb_columns() and
    # only cares about column-set membership.
    db.execute(
        "CREATE OR REPLACE TABLE core._dim_accounts_drift AS "
        "SELECT * EXCLUDE (display_name) FROM core.dim_accounts"
    )
    db.execute("DROP VIEW core.dim_accounts")
    db.execute(
        "CREATE VIEW core.dim_accounts AS SELECT * FROM core._dim_accounts_drift"
    )
    db.close()

    with pytest.raises(SchemaDriftError) as exc_info:
        _check_schema_at_boot()

    assert "dim_accounts" in str(exc_info.value)
