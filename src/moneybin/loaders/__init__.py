"""Data loaders package for MoneyBin.

This package contains loaders for different data formats that load raw data
files into DuckDB for further processing by dbt transformations.
"""

from .ofx_loader import OFXLoader
from .parquet_loader import ParquetLoader
from .w2_loader import W2Loader

__all__ = ["OFXLoader", "ParquetLoader", "W2Loader"]
