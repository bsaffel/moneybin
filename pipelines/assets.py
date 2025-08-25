"""Dagster assets for moneybin financial data processing.

This module defines all data assets that represent tables, files, or other
data objects that are produced and consumed by the data pipeline.
"""

from dagster import AssetKey, asset


@asset(
    key=AssetKey("placeholder_asset"),
    ins={},
)
def placeholder_asset():
    """Create a placeholder asset to ensure the module has content.

    This is a temporary asset that will be replaced with actual data assets
    as the pipeline development progresses.

    Returns:
        str: A placeholder string value.
    """
    return "placeholder"


# Export assets for Dagster to discover
__all__ = ["placeholder_asset"]
