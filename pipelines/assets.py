from dagster import AssetKey, asset


@asset(
    key=AssetKey("placeholder_asset"),
    ins={},
)
def placeholder_asset():
    """Placeholder asset to ensure the module has content."""
    return "placeholder"


# Export assets for Dagster to discover
__all__ = ["placeholder_asset"]
