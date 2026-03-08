"""SQLMesh configuration that reads the database path from MoneyBin settings.

This replaces config.yaml so that the --profile CLI option is respected
when running sqlmesh commands directly.
"""

import os
import sys

from sqlmesh.core.config import (  # type: ignore[import-untyped]
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

# Add project root to path so moneybin is importable
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, os.path.join(_project_root, "src"))

from moneybin.config import get_database_path  # noqa: E402

_sqlmesh_dir = os.path.dirname(os.path.abspath(__file__))

config = Config(
    gateways={
        "local": GatewayConfig(
            connection=DuckDBConnectionConfig(
                database=str(get_database_path()),
            ),
        ),
    },
    default_gateway="local",
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    cache_dir=os.path.join(_sqlmesh_dir, ".cache"),
)
