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
from sqlmesh.core.config.format import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    FormatConfig,
)


def _repo_root_or_none(config_file: Path) -> Path | None:
    """The source-checkout repo root to anchor MONEYBIN_HOME to, or None.

    SQLMesh re-executes this config on every ``Context`` creation — including
    the in-process ``sqlmesh_context()`` path every transform/demo run uses — so
    whatever this module writes to ``os.environ`` runs on every such call.

    In a source checkout, ``<repo>/src/moneybin/sqlmesh/config.py`` sits four
    levels below the repo root; anchoring MONEYBIN_HOME there lets a bare
    ``sqlmesh`` CLI invoked from any CWD resolve the repo's data. In an
    installed wheel the package lives under ``site-packages`` with no repo root
    above it, so return None and leave MONEYBIN_HOME to ``get_base_dir()``'s
    ``~/.moneybin`` default. (The pre-fix code walked up too few levels after
    the package move and anchored MONEYBIN_HOME to the *package* directory,
    scattering profile/log/DB state under ``src/moneybin`` / ``site-packages``.)
    """
    repo_root = config_file.resolve().parents[3]
    looks_like_checkout = (repo_root / "pyproject.toml").is_file() and (
        repo_root / "src" / "moneybin"
    ).is_dir()
    return repo_root if looks_like_checkout else None


_repo_root = _repo_root_or_none(Path(__file__))
if _repo_root is not None:
    # Bare `sqlmesh` / LSP invocations in a checkout may run without moneybin
    # installed; put <repo>/src on the path so the import below resolves. In a
    # wheel moneybin is already importable, so _repo_root is None and this is
    # skipped.
    _src_dir = str(_repo_root / "src")
    if _src_dir not in sys.path:
        sys.path.insert(0, _src_dir)
    # Anchor MONEYBIN_HOME to the repo root so get_base_dir() resolves the
    # repo's data dir regardless of CWD. Never override an explicit value.
    os.environ.setdefault("MONEYBIN_HOME", str(_repo_root))

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
    # sqlglot emits no trailing newline by default, which fights the repo-wide
    # end-of-file-fixer hook (sqlmesh strips it, the hook re-adds it). append_newline
    # makes the formatter emit it itself, so formatted SQL is already EOF-clean.
    format=FormatConfig(append_newline=True),
)
