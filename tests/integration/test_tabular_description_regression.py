# ruff: noqa: S101
"""Regression test: tabular descriptions must survive transforms into core.

A previous followup speculated tabular-imported rows landed in
``core.fct_transactions`` with ``description = NULL``, which would silently
break rule matching (``POSITION(LOWER(?) IN LOWER(t.description))`` fails
on NULL) and prevent auto-rule back-fill. The bug did not reproduce on
investigation, but the contract is load-bearing — this test pins it down
so a future ARG_MIN-tiebreaker regression can't go undetected.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "tabular"


@pytest.mark.integration
def test_tabular_import_preserves_description(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing a CSV must round-trip every description into core.fct_transactions."""
    from moneybin.services.import_service import ImportService

    secret_store = MagicMock()
    secret_store.get_key.return_value = "integration-test-key-0123456789abcdef"

    db_path = tmp_path / "tabular_desc.duckdb"
    db = Database(db_path, secret_store=secret_store)

    # run_transforms() resolves the active Database via the module singleton
    # plus get_settings().database.path; wire both to this test DB so
    # sqlmesh_context() reuses our encrypted connection.
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    monkeypatch.setattr("moneybin.database._database_instance", db)
    monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)

    fixture = FIXTURES_DIR / "standard.csv"
    assert fixture.exists(), f"missing fixture: {fixture}"

    result = ImportService(db).import_file(
        fixture, account_name="checking", auto_accept=True
    )
    assert result.transactions > 0, "fixture should yield transactions"
    assert result.core_tables_rebuilt, "transforms should have run"

    null_in_core = db.execute(
        "SELECT COUNT(*) FROM core.fct_transactions WHERE description IS NULL"
    ).fetchone()
    assert null_in_core is not None
    assert null_in_core[0] == 0, (
        f"{null_in_core[0]} tabular rows landed in core with description=NULL"
    )

    # core rewrites transaction_id (SHA256 of source key), so compare on
    # description values: every distinct raw description must appear in core.
    raw_descriptions = {
        r[0]
        for r in db.execute(
            "SELECT description FROM raw.tabular_transactions "
            "WHERE description IS NOT NULL"
        ).fetchall()
    }
    assert raw_descriptions, "fixture must produce descriptions"

    core_descriptions = {
        r[0]
        for r in db.execute(
            "SELECT description FROM core.fct_transactions "
            "WHERE description IS NOT NULL"
        ).fetchall()
    }
    missing = raw_descriptions - core_descriptions
    assert not missing, f"descriptions dropped during transform: {missing}"
