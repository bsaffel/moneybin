"""Dagster definitions for moneybin financial data processing.

This module contains the main Definitions object that Dagster uses to discover
and run all assets, jobs, schedules, and sensors in the project.
"""

from dagster import Definitions, load_assets_from_modules

from pipelines import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)
