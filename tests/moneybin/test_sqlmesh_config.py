"""The packaged SQLMesh config must not repoint MONEYBIN_HOME at the package dir.

Regression guard for the relocation bug (codex P1 on PR #316): after moving
config.py from ``<repo>/sqlmesh/config.py`` into
``<repo>/src/moneybin/sqlmesh/config.py``, the old two-level ``dirname`` walk
resolved the "project root" to the package directory. Because SQLMesh
re-executes this config on every ``Context`` creation, that exported
``MONEYBIN_HOME=<...>/src/moneybin`` (checkout) or ``<...>/site-packages/moneybin``
(wheel) whenever the var was unset — scattering profile/log/DB state under the
package tree (which is why ``src/moneybin/profiles/`` had to be gitignored).
"""

from pathlib import Path

import pytest

from moneybin.sqlmesh.config import (
    _repo_root_or_none,  # pyright: ignore[reportPrivateUsage]  # private resolver under test
)

pytestmark = pytest.mark.unit


def _make_config_file(root: Path) -> Path:
    """Create ``<root>/src/moneybin/sqlmesh/config.py`` and return its path."""
    config = root / "src" / "moneybin" / "sqlmesh" / "config.py"
    config.parent.mkdir(parents=True)
    config.write_text("# stub\n")
    return config


def test_source_checkout_resolves_to_repo_root(tmp_path: Path) -> None:
    """A checkout layout (pyproject.toml + src/moneybin) anchors to the repo root."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'moneybin'\n")
    config_file = _make_config_file(tmp_path)

    assert _repo_root_or_none(config_file) == tmp_path


def test_installed_wheel_returns_none(tmp_path: Path) -> None:
    """A wheel layout (no repo root above the package) must NOT anchor anywhere.

    Returning None leaves MONEYBIN_HOME to get_base_dir()'s ~/.moneybin default;
    anchoring it to the package dir is the exact bug this guards.
    """
    # site-packages/moneybin/sqlmesh/config.py — no pyproject.toml four levels up.
    config_file = tmp_path / "site-packages" / "moneybin" / "sqlmesh" / "config.py"
    config_file.parent.mkdir(parents=True)
    config_file.write_text("# stub\n")

    assert _repo_root_or_none(config_file) is None


def test_never_resolves_to_the_package_directory(tmp_path: Path) -> None:
    """When it does anchor, the root is the repo root — never the package dir."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'moneybin'\n")
    config_file = _make_config_file(tmp_path)

    resolved = _repo_root_or_none(config_file)
    package_dir = config_file.resolve().parent.parent  # <...>/moneybin
    assert resolved != package_dir
    assert resolved == tmp_path
