"""Shared pytest fixtures for moneybin tests.

This module provides common fixtures and test utilities used across
the test suite, including profile cleanup and configuration management.
"""

import shutil
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.config import (
    clear_settings_cache,
    get_base_dir,
    register_profile_resolver,
    set_current_profile,
)
from moneybin.database import Database
from tests.moneybin.db_helpers import apply_core_table_comments, create_core_tables_raw


@contextmanager
def temp_profile(profile: str) -> Generator[str, None, None]:
    """Context manager for automatic profile cleanup.

    Use this in tests to automatically clean up profile directories:

    Example:
        def test_something():
            with temp_profile("alice"):
                set_current_profile("alice")
                # ... test code ...
                # automatic cleanup happens when exiting the with block

    Args:
        profile: Profile name to clean up (will be normalized)

    Yields:
        The normalized profile name

    The context manager handles:
    - Profile name normalization (using the same normalization as the config system)
    - Cleanup of data/ and logs/ directories after the with block
    - Works with any profile name, no central list needed
    """
    from moneybin.utils.user_config import normalize_profile_name

    # Use the project-wide normalization function for profile names
    normalized = normalize_profile_name(profile)

    try:
        # Yield the normalized profile name to the test
        yield normalized
    finally:
        # Clean up profile directory
        # Don't check exists() because tests may mock it - just try to remove
        base = get_base_dir()
        profile_dir = base / "profiles" / normalized
        # ignore_errors handles ENOTEMPTY when xdist workers race teardown
        # of the shared profiles/ dir for tests that don't isolate
        # MONEYBIN_HOME.
        shutil.rmtree(profile_dir, ignore_errors=True)


@pytest.fixture(autouse=True)
def clean_profile_state() -> Generator[None, None, None]:
    """Automatically clean up profile state before and after each test.

    This fixture:
    - Runs for every test automatically (autouse=True)
    - Clears the settings cache to prevent test pollution
    - Resets current profile to 'test'
    - Resets the module-level ``_CLIFlags`` singleton in ``cli.utils`` so
      a stale ``--profile`` value from one test cannot leak into the
      next via ``resolve_profile()``.

    This ensures tests are isolated and don't affect each other.

    For profile directory cleanup, use the temp_profile() context manager.
    """
    from moneybin.cli import utils as cli_utils

    # Setup: clean state before test
    register_profile_resolver(None)
    clear_settings_cache()
    set_current_profile("test")
    cli_utils._flags.profile = None  # pyright: ignore[reportPrivateUsage]
    cli_utils._flags.verbose = False  # pyright: ignore[reportPrivateUsage]

    # Yield to run the test
    yield

    # Cleanup after test
    register_profile_resolver(None)
    clear_settings_cache()
    set_current_profile("test")
    cli_utils._flags.profile = None  # pyright: ignore[reportPrivateUsage]
    cli_utils._flags.verbose = False  # pyright: ignore[reportPrivateUsage]


@pytest.fixture()
def mock_secret_store() -> MagicMock:
    """Mock SecretStore that returns a test encryption key.

    Use this fixture when you need to create Database instances in tests
    without requiring actual keyring/system secret storage.
    """
    store = MagicMock()
    store.get_key.return_value = "test-encryption-key-for-unit-tests"
    return store


@pytest.fixture()
def schema_catalog_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """Database with core tables + the core.dim_categories view stub.

    `Database.__init__` runs `init_schemas()` which creates every base
    `app.*` interface table (budgets, merchants, categorization_rules,
    transaction_categories, transaction_notes, etc.). It does NOT create
    `core.dim_categories`, which is a view normally materialized by SQLMesh.
    We stub it here so drift tests cover the full interface surface,
    not just core.*.
    """
    database = Database(
        tmp_path / "schema_catalog.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    create_core_tables_raw(database.conn)
    apply_core_table_comments(database)
    database.execute(
        "CREATE OR REPLACE VIEW core.dim_categories AS "
        "SELECT CAST(NULL AS VARCHAR) AS category_id, "
        "CAST(NULL AS VARCHAR) AS category, "
        "CAST(NULL AS VARCHAR) AS subcategory, "
        "CAST(NULL AS VARCHAR) AS description, "
        "CAST(NULL AS VARCHAR) AS plaid_detailed, "
        "CAST(NULL AS BOOLEAN) AS is_default, "
        "CAST(NULL AS BOOLEAN) AS is_active, "
        "CAST(NULL AS TIMESTAMP) AS created_at "
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
        "CAST(NULL AS TIMESTAMP) AS created_at, "
        "CAST(NULL AS BOOLEAN) AS is_user "
        "WHERE FALSE"
    )
    # Reports.* views — production builds these via SQLMesh; tests stub the
    # shape so schema-catalog interface tests can resolve the names.
    database.execute(
        "CREATE OR REPLACE VIEW reports.cash_flow AS "
        "SELECT CAST(NULL AS DATE) AS year_month, "
        "CAST(NULL AS VARCHAR) AS account_id, "
        "CAST(NULL AS VARCHAR) AS account_name, "
        "CAST(NULL AS VARCHAR) AS category, "
        "CAST(NULL AS DECIMAL(18,2)) AS inflow, "
        "CAST(NULL AS DECIMAL(18,2)) AS outflow, "
        "CAST(NULL AS DECIMAL(18,2)) AS net, "
        "CAST(NULL AS BIGINT) AS txn_count "
        "WHERE FALSE"
    )
    database.execute(
        "CREATE OR REPLACE VIEW reports.spending_trend AS "
        "SELECT CAST(NULL AS DATE) AS year_month, "
        "CAST(NULL AS VARCHAR) AS category, "
        "CAST(NULL AS DECIMAL(18,2)) AS total_spend, "
        "CAST(NULL AS BIGINT) AS txn_count, "
        "CAST(NULL AS DECIMAL(18,2)) AS prev_month_spend, "
        "CAST(NULL AS DECIMAL(18,2)) AS mom_delta, "
        "CAST(NULL AS DOUBLE) AS mom_pct, "
        "CAST(NULL AS DECIMAL(18,2)) AS prev_year_spend, "
        "CAST(NULL AS DECIMAL(18,2)) AS yoy_delta, "
        "CAST(NULL AS DOUBLE) AS yoy_pct, "
        "CAST(NULL AS DECIMAL(18,2)) AS trailing_3mo_avg "
        "WHERE FALSE"
    )
    database.execute(
        "CREATE OR REPLACE VIEW reports.recurring_subscriptions AS "
        "SELECT CAST(NULL AS VARCHAR) AS merchant_normalized, "
        "CAST(NULL AS DECIMAL(18,2)) AS avg_amount, "
        "CAST(NULL AS VARCHAR) AS cadence, "
        "CAST(NULL AS DOUBLE) AS interval_days_avg, "
        "CAST(NULL AS DOUBLE) AS interval_days_stddev, "
        "CAST(NULL AS BIGINT) AS occurrence_count, "
        "CAST(NULL AS DATE) AS first_seen, "
        "CAST(NULL AS DATE) AS last_seen, "
        "CAST(NULL AS VARCHAR) AS status, "
        "CAST(NULL AS DECIMAL(18,2)) AS annualized_cost, "
        "CAST(NULL AS DOUBLE) AS confidence "
        "WHERE FALSE"
    )
    database.execute(
        "CREATE OR REPLACE VIEW reports.uncategorized_queue AS "
        "SELECT CAST(NULL AS VARCHAR) AS transaction_id, "
        "CAST(NULL AS VARCHAR) AS account_id, "
        "CAST(NULL AS VARCHAR) AS account_name, "
        "CAST(NULL AS DATE) AS txn_date, "
        "CAST(NULL AS DECIMAL(18,2)) AS amount, "
        "CAST(NULL AS VARCHAR) AS description, "
        "CAST(NULL AS VARCHAR) AS merchant_normalized, "
        "CAST(NULL AS INTEGER) AS age_days, "
        "CAST(NULL AS DECIMAL(18,2)) AS priority_score, "
        "CAST(NULL AS VARCHAR) AS source_type, "
        "CAST(NULL AS VARCHAR) AS source_id "
        "WHERE FALSE"
    )
    database.execute(
        "CREATE OR REPLACE VIEW reports.merchant_activity AS "
        "SELECT CAST(NULL AS VARCHAR) AS merchant_normalized, "
        "CAST(NULL AS DECIMAL(18,2)) AS total_spend, "
        "CAST(NULL AS DECIMAL(18,2)) AS total_inflow, "
        "CAST(NULL AS DECIMAL(18,2)) AS total_outflow, "
        "CAST(NULL AS BIGINT) AS txn_count, "
        "CAST(NULL AS DECIMAL(18,2)) AS avg_amount, "
        "CAST(NULL AS DECIMAL(18,2)) AS median_amount, "
        "CAST(NULL AS DATE) AS first_seen, "
        "CAST(NULL AS DATE) AS last_seen, "
        "CAST(NULL AS BIGINT) AS active_months, "
        "CAST(NULL AS VARCHAR) AS top_category, "
        "CAST(NULL AS BIGINT) AS account_count "
        "WHERE FALSE"
    )
    database.execute(
        "CREATE OR REPLACE VIEW reports.large_transactions AS "
        "SELECT CAST(NULL AS VARCHAR) AS transaction_id, "
        "CAST(NULL AS VARCHAR) AS account_id, "
        "CAST(NULL AS VARCHAR) AS account_name, "
        "CAST(NULL AS DATE) AS txn_date, "
        "CAST(NULL AS DECIMAL(18,2)) AS amount, "
        "CAST(NULL AS VARCHAR) AS description, "
        "CAST(NULL AS VARCHAR) AS merchant_normalized, "
        "CAST(NULL AS VARCHAR) AS category, "
        "CAST(NULL AS DOUBLE) AS amount_zscore_account, "
        "CAST(NULL AS DOUBLE) AS amount_zscore_category, "
        "CAST(NULL AS BOOLEAN) AS is_top_100 "
        "WHERE FALSE"
    )
    database.execute(
        "CREATE OR REPLACE VIEW reports.balance_drift AS "
        "SELECT CAST(NULL AS VARCHAR) AS account_id, "
        "CAST(NULL AS VARCHAR) AS account_name, "
        "CAST(NULL AS DATE) AS assertion_date, "
        "CAST(NULL AS DECIMAL(18,2)) AS asserted_balance, "
        "CAST(NULL AS DECIMAL(18,2)) AS computed_balance, "
        "CAST(NULL AS DECIMAL(18,2)) AS drift, "
        "CAST(NULL AS DECIMAL(18,2)) AS drift_abs, "
        "CAST(NULL AS DOUBLE) AS drift_pct, "
        "CAST(NULL AS INTEGER) AS days_since_assertion, "
        "CAST(NULL AS VARCHAR) AS status "
        "WHERE FALSE"
    )
    db_module._database_instance = database  # type: ignore[attr-defined]
    try:
        yield database
    finally:
        db_module._database_instance = None  # type: ignore[attr-defined]
        database.close()


@pytest.fixture(scope="module")
def module_db(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[Database, None, None]:
    """Module-scoped read-only Database. Tests must NOT mutate this fixture.

    Shared across every test in a module to amortize Database() + init_schemas
    cost. Only safe for modules that exclusively read — any INSERT/UPDATE/DELETE
    will pollute downstream tests in the same module.
    """
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-for-unit-tests"

    db_path = tmp_path_factory.mktemp("module_db") / "test.duckdb"
    database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
    yield database
    database.close()


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a test Database instance with encryption.

    Creates a temporary encrypted database suitable for unit and integration
    tests. The database is initialized with all base schemas (raw, core, app)
    but contains no pre-populated data.

    For tests that need specific core tables (dim_accounts, fct_transactions),
    use db_helpers.create_core_tables(db) or create_core_tables_raw(db.conn).

    Args:
        tmp_path: pytest temporary directory fixture.
        mock_secret_store: Mocked SecretStore that provides a test key.

    Yields:
        A Database instance ready for test queries.
    """
    db_path = tmp_path / "test.duckdb"
    database = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
    yield database
    database.close()
