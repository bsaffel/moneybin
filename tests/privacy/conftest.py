"""Privacy-test fixtures: a populated DB with core.* and app.* present."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from tests.moneybin.db_helpers import (
    apply_core_table_comments,
    create_core_dim_stub_views,
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
        read_only=False,
    )
    create_core_tables_raw(database.conn)
    apply_core_table_comments(database)
    create_core_dim_stub_views(database)
    try:
        yield database
    finally:
        database.close()
