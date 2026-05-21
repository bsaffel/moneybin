"""Shared fixtures for gsheet adapter and service tests."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.connectors.gsheet.adapters.base import GSheetConnection
from moneybin.database import Database


@pytest.fixture()
def in_memory_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """Test Database with the full base schema (raw + app + core scaffolding)."""
    db_path = tmp_path / "gsheet_test.duckdb"
    database = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
    yield database
    database.close()


@pytest.fixture()
def sample_connection() -> GSheetConnection:
    """A transactions-adapter connection with a pinned Tiller-style mapping."""
    return GSheetConnection(
        connection_id="conn_abc123",
        spreadsheet_id="ss1",
        sheet_gid=12345,
        sheet_name="Transactions",
        workbook_name="Tiller Foundation",
        adapter="transactions",
        alias=None,
        account_id="acct_chase",
        account_name="Chase Checking",
        # column_mapping is source_header → dest_field (per base.py).
        column_mapping={
            "Date": "transaction_date",
            "Amount": "amount",
            "Description": "description",
            "Category": "category",
            "Account": "account_name",
        },
        header_signature=[
            "Date",
            "Description",
            "Category",
            "Amount",
            "Account",
            "Tags",
        ],
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        skip_rows=0,
        skip_trailing_patterns=[],
        status="healthy",
        last_pull_at=None,
        last_pull_import_id=None,
        last_success_at=None,
        last_drift_reason=None,
        consecutive_failure_count=0,
    )
