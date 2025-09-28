"""Dagster assets for dbt integration.

This module provides native Dagster-dbt integration for rich asset tracking,
lineage, and orchestration capabilities.
"""

from collections.abc import Iterator
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from moneybin.loaders import ParquetLoader
from moneybin.loaders.parquet_loader import LoadingConfig

# Define the dbt project
dbt_project = DbtProject(
    project_dir="dbt",
    packaged_project_dir="dbt",
)

# Create dbt resource
dbt_resource = DbtCliResource(project_dir="dbt")


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
)
def moneybin_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
) -> Iterator[Any]:
    """All dbt models as Dagster assets with rich lineage and monitoring.

    This provides:
    - Asset lineage visualization
    - Incremental execution
    - Rich monitoring and alerting
    - Dependency tracking
    """
    yield from dbt.cli(["build"], context=context).stream()  # type: ignore[reportUnknownMemberType]


# Raw data loading function (separate from dbt assets)
def load_raw_data(context: AssetExecutionContext) -> dict[str, int]:
    """Load raw Parquet files into DuckDB staging tables.

    This uses your existing ParquetLoader for consistency with CLI operations.
    """
    config = LoadingConfig()
    loader = ParquetLoader(config)
    results = loader.load_all_parquet_files()

    context.log.info(f"Loaded data: {results}")
    return results
