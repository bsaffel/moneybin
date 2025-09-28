"""Dagster assets for moneybin financial data processing.

This module defines CLI-based assets that use the MoneyBin CLI commands
for simpler orchestration scenarios.
"""

import logging
import subprocess  # noqa: S404

from dagster import AssetExecutionContext, asset

logger = logging.getLogger(__name__)


@asset(group_name="raw_data")
def plaid_raw_data(context: AssetExecutionContext):
    """Extract Plaid data using CLI command."""
    result = subprocess.run(
        ["uv", "run", "moneybin", "extract", "plaid", "--verbose"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(f"Plaid extraction completed: {result.stdout}")
    return {"status": "success", "output": result.stdout}


@asset(group_name="staging", deps=[plaid_raw_data])
def loaded_staging_data(context: AssetExecutionContext):
    """Load Parquet files into DuckDB using CLI command."""
    result = subprocess.run(
        ["uv", "run", "moneybin", "load", "parquet", "--verbose"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(f"Data loading completed: {result.stdout}")
    return {"status": "success", "output": result.stdout}


@asset(group_name="analytics", deps=[loaded_staging_data])
def dbt_transformed_data(context: AssetExecutionContext):
    """Run dbt transformations using CLI command."""
    result = subprocess.run(
        ["uv", "run", "moneybin", "transform", "run", "--verbose"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(f"dbt transformations completed: {result.stdout}")
    return {"status": "success", "output": result.stdout}


# Export assets for Dagster to discover
__all__ = ["plaid_raw_data", "loaded_staging_data", "dbt_transformed_data"]
