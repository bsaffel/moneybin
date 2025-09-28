"""Dagster definitions for moneybin financial data processing.

This module contains the main Definitions object that Dagster uses to discover
and run all assets, jobs, schedules, and sensors in the project.

Two approaches are available:
1. CLI-based assets (assets.py) - Simple orchestration using CLI commands
2. Native dbt assets (dbt_assets.py) - Rich dbt integration with lineage
"""

from dagster import Definitions, load_assets_from_modules

from pipelines import assets  # noqa: TID252

# Uncomment for native dbt integration:
# from pipelines import dbt_assets

# CLI-based approach (current)
cli_assets = load_assets_from_modules([assets])

# Native dbt approach (future)
# native_assets = load_assets_from_modules([dbt_assets])

defs = Definitions(
    assets=cli_assets,
    # For native dbt integration, use:
    # assets=native_assets,
    # Or combine both:
    # assets=[*cli_assets, *native_assets],
)
