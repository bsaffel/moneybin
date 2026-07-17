"""End-to-end proof that multi-currency capture (M1K.1 Part A) holds together.

Drives it through the real import path -- no raw.* seeding shortcuts. Covers
multi-currency.md Requirements 1, 2, 3, 8: a non-USD OFX statement's CURDEF is
captured, survives the union without being relabeled USD, and lands correctly
on core.fct_transactions / core.fct_balances via ImportService.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.import_service import ImportService

pytestmark = pytest.mark.integration

_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def _build_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    """Real encrypted Database wired for ImportService's internal refresh.

    The ``db`` fixture in tests/moneybin/conftest.py is not visible here --
    pytest fixtures don't cross sibling package boundaries. This mirrors the
    established tests/integration/ pattern (test_import_service_batch.py,
    test_schema_drift.py): build a real Database directly, then point
    get_settings() at its path so sqlmesh_context() (invoked internally by
    ImportService's refresh=True) reuses this same encrypted connection
    instead of opening an unencrypted one at the default path.
    """
    secret_store = MagicMock()
    secret_store.get_key.return_value = _ENCRYPTION_KEY
    db_path = tmp_path / "multi_currency_eur.duckdb"
    db = Database(db_path, secret_store=secret_store, read_only=False)
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
    return db


@pytest.mark.slow
def test_eur_ofx_statement_currency_survives_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A EUR OFX import's currency reaches core.fct_transactions/fct_balances unmangled."""
    db = _build_db(tmp_path, monkeypatch)
    fixture = _FIXTURES_DIR / "multi_currency_eur.qfx"

    ImportService(db).import_file(fixture, refresh=True)

    # The real account resolver mints an opaque canonical account_id, unknown
    # to this test in advance — but this is a fresh db with exactly one
    # import in it, so an unfiltered query is unambiguous by construction.
    txn_currencies = db.execute(
        "SELECT DISTINCT currency_code FROM core.fct_transactions"
    ).fetchall()
    assert txn_currencies == [("EUR",)], (
        "EUR transactions must not be relabeled USD anywhere in the pipeline"
    )

    balance_currencies = db.execute(
        "SELECT DISTINCT currency_code FROM core.fct_balances "
        "WHERE source_type = 'ofx' AND balance = 5000.00"
    ).fetchall()
    assert balance_currencies == [("EUR",)]
