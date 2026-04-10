"""SQLMesh configuration that reads the database path from MoneyBin settings.

This replaces config.yaml so that the --profile CLI option is respected
when running sqlmesh commands directly.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from sqlmesh.core.config import (  # type: ignore[import-untyped]
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

from sqlmesh import LOG_FILENAME_PREFIX, LOG_FORMAT  # type: ignore[import-untyped]

# Add project root to path so moneybin is importable
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, os.path.join(_project_root, "src"))

# Anchor MONEYBIN_HOME to the project root so get_base_dir() resolves paths
# correctly regardless of the working directory SQLMesh was invoked from.
if "MONEYBIN_HOME" not in os.environ:
    os.environ["MONEYBIN_HOME"] = _project_root

from moneybin.config import get_database_path, get_settings  # noqa: E402

_sqlmesh_dir = os.path.dirname(os.path.abspath(__file__))

# Route SQLMesh file logging to the active profile's log directory.
#
# The SQLMesh CLI calls configure_logging() (which adds a file handler pointing to
# logs/ relative to CWD) *before* config.py is loaded.  The Python API never calls
# configure_logging() at all.  In both cases we redirect to logs/{profile}/ here.
#
# config.py is re-executed on every Context creation (SQLMesh clears it from
# sys.modules), so the logic below must be idempotent.
_profile_log_dir = get_settings().logging.log_file_path.parent
_profile_log_dir.mkdir(parents=True, exist_ok=True)

_root_logger = logging.getLogger()

# Drop any file handlers that don't already point to the profile log dir.
for _h in _root_logger.handlers[:]:
    if isinstance(_h, logging.FileHandler) and not Path(_h.baseFilename).is_relative_to(
        _profile_log_dir
    ):
        _root_logger.removeHandler(_h)
        _h.close()

# Add a profile-specific file handler when none exists yet.
if not any(
    isinstance(_h, logging.FileHandler)
    and Path(_h.baseFilename).is_relative_to(_profile_log_dir)
    for _h in _root_logger.handlers
):
    _log_file = (
        _profile_log_dir
        / f"{LOG_FILENAME_PREFIX}{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"
    )
    _fh = logging.FileHandler(str(_log_file), mode="w", encoding="utf-8")
    _fh.setLevel(logging.INFO)
    _fh.setFormatter(logging.Formatter(LOG_FORMAT))
    _root_logger.addHandler(_fh)

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
