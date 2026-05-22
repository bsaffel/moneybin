"""The _framework package re-exports its public API surface."""

import moneybin.packages._framework as framework


def test_public_surface_importable() -> None:
    """Every name in __all__ resolves to a real attribute."""
    for name in framework.__all__:
        assert hasattr(framework, name), f"{name} missing from framework surface"


def test_key_symbols_present() -> None:
    """Spot-check the load-bearing entry points are exported."""
    assert framework.register_package is not None
    assert framework.PackageManifest is not None
    assert framework.discover_packages is not None
