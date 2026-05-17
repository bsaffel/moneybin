"""Privacy-test fixtures: a populated DB with core.* and app.* present."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from tests.moneybin.db_helpers import (
    apply_core_table_comments,
    create_core_tables_raw,
)


@pytest.fixture()
def populated_db(tmp_path: Path) -> Generator[Database, None, None]:
    """A Database with init_schemas() done and core.* test tables present.

    Mirrors the shape `tests/moneybin/conftest.py:schema_catalog_db` uses,
    pared down to what the completeness checks need: app.* from
    init_schemas() and core.* from the test-helper DDL set. The
    dim_categories / dim_merchants stub views are added here because
    create_core_tables_raw doesn't create them but the CLASSIFICATION
    registry covers them (they're SQLMesh views in production).
    """
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    database = Database(
        tmp_path / "privacy.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    create_core_tables_raw(database.conn)
    apply_core_table_comments(database)
    # core.dim_categories / core.dim_merchants are SQLMesh-managed views in
    # production. Stub them here with the column shape the registry expects
    # so the completeness check covers them too.
    database.execute(
        "CREATE OR REPLACE VIEW core.dim_categories AS "
        "SELECT CAST(NULL AS VARCHAR) AS category_id, "
        "CAST(NULL AS VARCHAR) AS category, "
        "CAST(NULL AS VARCHAR) AS subcategory, "
        "CAST(NULL AS VARCHAR) AS description, "
        "CAST(NULL AS VARCHAR) AS plaid_detailed, "
        "CAST(NULL AS BOOLEAN) AS is_default, "
        "CAST(NULL AS BOOLEAN) AS is_active, "
        "CAST(NULL AS TIMESTAMP) AS created_at, "
        "CAST(NULL AS TIMESTAMP) AS updated_at "
        "WHERE FALSE"
    )
    database.execute(
        "CREATE OR REPLACE VIEW core.dim_merchants AS "
        "SELECT CAST(NULL AS VARCHAR) AS merchant_id, "
        "CAST(NULL AS VARCHAR) AS raw_pattern, "
        "CAST(NULL AS VARCHAR) AS match_type, "
        "CAST(NULL AS VARCHAR) AS canonical_name, "
        "CAST(NULL AS VARCHAR) AS category, "
        "CAST(NULL AS VARCHAR) AS subcategory, "
        "CAST(NULL AS VARCHAR) AS created_by, "
        "CAST(NULL AS VARCHAR[]) AS exemplars, "
        "CAST(NULL AS TIMESTAMP) AS created_at, "
        "CAST(NULL AS TIMESTAMP) AS updated_at "
        "WHERE FALSE"
    )
    try:
        yield database
    finally:
        database.close()
