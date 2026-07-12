"""MoneyBin: Personal financial data aggregation and analysis tool.

This package provides secure, local-first financial data management with support for:
- Plaid API integration for bank account data
- DuckDB-based analytics and storage
- SQLMesh transformations for data modeling
- Modern CLI interface for all operations

All financial data is stored locally with user-controlled encryption and backup.
"""

from typing import Any


def __getattr__(name: str) -> Any:
    # Single-source the version from installed distribution metadata (same as
    # cli.main.get_version), rather than a literal that drifts from pyproject on
    # a release bump. Lazy via PEP 562 so `import moneybin` pays nothing unless
    # `moneybin.__version__` is actually read — keeps the cold-start path light.
    if name == "__version__":
        import importlib.metadata

        return importlib.metadata.version("moneybin")
    raise AttributeError(f"module {__name__!r} has no attribute {name}")
