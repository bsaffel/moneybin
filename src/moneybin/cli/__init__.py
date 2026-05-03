"""MoneyBin CLI package.

This package provides a unified command-line interface for all MoneyBin operations,
including data extraction, credential management, and system utilities.

Note: the `main()` function is intentionally NOT re-exported here. The console
script entry point (`moneybin.cli.main:main`) imports it directly from the
submodule, and re-exporting it would shadow the submodule of the same name —
forcing callers into `sys.modules` workarounds.
"""

from .main import app

__all__ = ["app"]
