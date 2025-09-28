"""Data loaders package for MoneyBin.

This package contains loaders for different data formats that load raw data
files into DuckDB for further processing by dbt transformations.
"""

from .parquet_loader import ParquetLoader

__all__ = ["ParquetLoader"]
