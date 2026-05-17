"""SQLMesh configuration that reads the database path from MoneyBin settings.

This replaces config.yaml so that the --profile CLI option is respected
when running sqlmesh commands directly.
"""

import logging
import os
import sys
from pathlib import Path

from sqlmesh.core.config import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

# Add project root to path so moneybin is importable
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, os.path.join(_project_root, "src"))

# Anchor MONEYBIN_HOME to the project root so get_base_dir() resolves paths
# correctly regardless of the working directory SQLMesh was invoked from.
if "MONEYBIN_HOME" not in os.environ:
    os.environ["MONEYBIN_HOME"] = _project_root

from moneybin.config import (  # noqa: E402 — must follow sys.path setup above
    get_current_profile,
    get_database_path,
    get_settings,
    set_current_profile,
)

# Initialize a profile only when none is set in-process. This keeps config.py
# self-sufficient for non-CLI entry points (SQLMesh VSCode extension, direct
# `sqlmesh` shell invocations, the language server) which never run moneybin's
# CLI callback, while preserving the CLI-selected profile. The CLI sets the
# profile via --profile before SQLMesh loads config.py; SQLMesh then re-executes
# this file on every Context creation (see comment below), so the gate must be
# idempotent — set_current_profile() invalidates caches whenever the name
# differs, so blindly calling it would clobber the CLI's profile back to
# "default" mid-process.
try:
    get_current_profile(auto_resolve=False)
except RuntimeError:
    set_current_profile(os.environ.get("MONEYBIN_PROFILE", "default"))

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
# Resolve to absolute so is_relative_to comparisons work correctly against
# handler.baseFilename (which is always absolute).
_profile_log_dir_abs = _profile_log_dir.resolve()

_root_logger = logging.getLogger()

# Drop any file handlers that don't already point to the profile log dir.
for _h in _root_logger.handlers[:]:
    if isinstance(_h, logging.FileHandler) and not Path(_h.baseFilename).is_relative_to(
        _profile_log_dir_abs
    ):
        _root_logger.removeHandler(_h)
        _h.close()


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
