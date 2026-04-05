"""Data loaders package for MoneyBin.

This package contains loaders for different data formats that load raw data
files into DuckDB for further processing by SQLMesh transforms.
"""

from .csv_loader import CSVLoader
from .ofx_loader import OFXLoader
from .parquet_loader import ParquetLoader
from .w2_loader import W2Loader

__all__ = ["CSVLoader", "OFXLoader", "ParquetLoader", "W2Loader"]
