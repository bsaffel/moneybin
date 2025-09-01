# ruff: noqa: S101,S106
"""Tests for PlaidExtractor.

Includes unit tests with stubs and an optional integration test that
calls Plaid's Sandbox APIs directly. The integration test is skipped
unless `PLAID_CLIENT_ID` and `PLAID_SECRET` are set in the environment.
"""

from __future__ import annotations

# Ensure project root is on sys.path so 'src' namespace is importable
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from pathlib import Path as _Path
from types import SimpleNamespace
from typing import Any, cast

sys.path.append(str(_Path(__file__).resolve().parents[1]))

import polars as pl
import pytest

from src.moneybin.extractors.plaid_extractor import (
    PlaidExtractionConfig,
    PlaidExtractor,
)

# ------------------------------
# Unit tests (using a local fixture that patches PlaidApi)
# ------------------------------


@pytest.fixture
def mocked_plaid_client(mocker: Any) -> Any:
    """Patch PlaidApi constructor and return the injected mock client."""
    client = mocker.MagicMock()
    mocker.patch(
        "src.extractors.plaid_extractor.plaid_api.PlaidApi", return_value=client
    )
    return client


@pytest.mark.unit
def test_get_accounts_returns_dataframe_and_saves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocked_plaid_client: Any,
) -> None:
    """It returns a non-empty DataFrame and writes a parquet file."""
    # Arrange: fake env for credentials (values unused due to stubs)
    monkeypatch.setenv("PLAID_CLIENT_ID", "dummy")
    monkeypatch.setenv("PLAID_SECRET", "dummy")
    monkeypatch.setenv("PLAID_ENV", "sandbox")

    config = PlaidExtractionConfig(save_raw_data=True, raw_data_path=tmp_path)

    # Mock Plaid client.accounts_get to return a realistic structure
    mocked_client: Any = mocked_plaid_client
    accounts = [
        {
            "account_id": "acc_123",
            "balances": {
                "available": 1000.0,
                "current": 1000.0,
                "iso_currency_code": "USD",
            },
            "mask": "0000",
            "name": "Checking",
            "official_name": "Checking Account",
            "persistent_account_id": None,
            "subtype": "checking",
            "type": "depository",
        }
    ]
    mocked_client.accounts_get.return_value = SimpleNamespace(
        accounts=accounts, item=SimpleNamespace(institution_id="ins_stub")
    )
    extractor = PlaidExtractor(config=config)

    # Act
    df = extractor.get_accounts(access_token="access-dummy")

    # Assert
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 1
    # A parquet file should have been written
    assert any(p.suffix == ".parquet" for p in tmp_path.iterdir())


@pytest.mark.unit
def test_get_transactions_paginates_and_saves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocked_plaid_client: Any,
) -> None:
    """It paginates through transactions and writes a parquet file."""
    # Arrange: fake env for credentials (values unused due to stubs)
    monkeypatch.setenv("PLAID_CLIENT_ID", "dummy")
    monkeypatch.setenv("PLAID_SECRET", "dummy")
    monkeypatch.setenv("PLAID_ENV", "sandbox")

    # Build a pool of 750 transactions to force pagination with default batch_size=500
    transactions_pool: list[dict[str, Any]] = []
    for i in range(750):
        transactions_pool.append({
            "transaction_id": f"tx_{i}",
            "account_id": "acc_123",
            "amount": Decimal("12.34"),
            "iso_currency_code": "USD",
            "date": datetime.now().date().isoformat(),
            "name": "Test Transaction",
            "category": ["Shops"],
            "pending": False,
        })

    config = PlaidExtractionConfig(save_raw_data=True, raw_data_path=tmp_path)

    # Mock Plaid client.transactions_get with a pagination-aware side effect
    mocked_client: Any = mocked_plaid_client

    def _transactions_get_side_effect(request: Any) -> Any:
        options = getattr(request, "options", getattr(request, "_options", None))
        count = cast(int, getattr(options, "count", 500))
        offset = cast(int, getattr(options, "offset", 0))
        page = transactions_pool[offset : offset + count]
        return SimpleNamespace(
            transactions=page, total_transactions=len(transactions_pool)
        )

    mocked_client.transactions_get.side_effect = _transactions_get_side_effect
    extractor = PlaidExtractor(config=config)

    # Act
    start = datetime.now() - timedelta(days=30)
    end = datetime.now()
    df = extractor.get_transactions(
        access_token="access-dummy", start_date=start, end_date=end
    )

    # Assert
    assert isinstance(df, pl.DataFrame)
    assert df.height == 750
    assert {"transaction_id", "amount", "transaction_date"}.issubset(set(df.columns))
    # A parquet file should have been written
    assert any(p.suffix == ".parquet" for p in tmp_path.iterdir())


# ---------------------------------------------
# Integration test (real Plaid Sandbox requests)
# ---------------------------------------------


@pytest.mark.integration
@pytest.mark.slow
def test_integration_plaid_sandbox_extracts_accounts_and_transactions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """It calls Plaid Sandbox APIs and returns DataFrames for accounts and transactions."""
    # Ensure we use sandbox
    monkeypatch.setenv("PLAID_ENV", "sandbox")

    # Use extractor helper to create a sandbox access token
    extractor_for_token = PlaidExtractor()
    access_token = extractor_for_token.create_sandbox_access_token()

    # Do not write files during integration run
    config = PlaidExtractionConfig(save_raw_data=False)
    extractor = PlaidExtractor(config=config)

    accounts_df = extractor.get_accounts(access_token)
    assert isinstance(accounts_df, pl.DataFrame)
    assert accounts_df.height >= 1

    # Limit range to 30 days for speed
    start = datetime.now() - timedelta(days=30)
    end = datetime.now()
    tx_df = extractor.get_transactions(access_token, start_date=start, end_date=end)
    assert isinstance(tx_df, pl.DataFrame)
    assert tx_df.height >= 0  # may be 0 depending on sandbox data freshness
    assert {"transaction_id", "amount", "transaction_date"}.issubset(set(tx_df.columns))
