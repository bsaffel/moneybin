"""The built wheel must contain every resource the runtime loads from disk.

Regression guard for the packaging defect where ``package-data`` globs used
``../../sqlmesh/...`` paths that escape the package dir — setuptools silently
ignores those, so the wheel shipped no SQL, no models, and no synthetic data.
A ``pip install moneybin`` could not create a profile or run a transform.

Two complementary checks, one built wheel:

* :func:`test_wheel_contains_every_required_resource` — a hand-written map of
  glob -> why it matters. Good failure messages, states intent.
* :func:`test_wheel_ships_every_runtime_resource_on_disk` — a completeness
  check against the source tree. The glob map alone would go green on a wheel
  missing a resource nobody remembered to list; this one cannot.
"""

import fnmatch
import subprocess  # noqa: S404  # building a real wheel is the point of this test
import zipfile
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PKG_ROOT = _REPO_ROOT / "src" / "moneybin"

# Each entry: a glob the wheel MUST match at least once, and why it exists.
REQUIRED_WHEEL_CONTENTS = {
    "moneybin/sql/schema/*.sql": "schema DDL — profile create fails without it",
    "moneybin/sql/migrations/*.sql": "SQL migrations — schema upgrades fail",
    "moneybin/sql/migrations/*.py": "Python migrations — most of the ladder is .py",
    "moneybin/sqlmesh/config.py": "SQLMesh project config",
    "moneybin/sqlmesh/external_models.yaml": "SQLMesh external model defs",
    "moneybin/sqlmesh/models/**/*.sql": "SQL models — no transforms without them",
    "moneybin/sqlmesh/models/**/*.py": "Python models — core fct_* tables",
    "moneybin/sqlmesh/models/seeds/*.csv": "seed data for the category models",
    "moneybin/sqlmesh/audits/**/*.sql": "named audits — system doctor needs them",
    "moneybin/synthetic/data/personas/*.yaml": "moneybin demo fails without personas",
    "moneybin/synthetic/data/merchants/*.yaml": "moneybin demo fails without merchants",
    "moneybin/data/tabular_formats/*.yaml": "--format tiller/mint/ynab",
    "moneybin/extractors/*/schema/*.sql": "extractor raw-table DDL",
}

# Suffixes the runtime reads from disk rather than imports.
_DATA_SUFFIXES = frozenset({".sql", ".yaml", ".yml", ".csv"})

# Directories whose .py files are *data*, not importable modules. Neither is a
# package (no __init__.py), so `packages.find` never sees them; they reach the
# wheel only as package-data. SQLMesh scans its project dir by path, and
# migrations.py loads .py migrations via spec_from_file_location + exec_module.
_DATA_PY_DIRS = ("sqlmesh/", "sql/migrations/")

# Build/runtime droppings that must never be packaged.
_EXCLUDED_DIRS = frozenset({"__pycache__", ".cache", "logs"})


def _wheel_matches(names: list[str], pattern: str) -> bool:
    """True if any archive member matches the glob.

    ``fnmatch`` has no ``**`` semantics, so expand it: everything under the
    prefix whose basename matches the tail.
    """
    if "**" in pattern:
        head, tail = pattern.split("**", 1)
        tail = tail.lstrip("/")
        return any(
            name.startswith(head) and fnmatch.fnmatch(name.rsplit("/", 1)[-1], tail)
            for name in names
        )
    return any(fnmatch.fnmatch(name, pattern) for name in names)


def _runtime_resources_on_disk() -> list[str]:
    """Every non-importable file the runtime loads, as wheel-relative paths."""
    found: list[str] = []
    for path in _PKG_ROOT.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(_PKG_ROOT)
        if _EXCLUDED_DIRS.intersection(relative.parts):
            continue
        posix = relative.as_posix()
        is_data_py = path.suffix == ".py" and posix.startswith(_DATA_PY_DIRS)
        if path.suffix in _DATA_SUFFIXES or is_data_py:
            found.append(f"moneybin/{posix}")
    return sorted(found)


@pytest.fixture(scope="module")
def wheel_members(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """Build a real wheel once and return its archive member names."""
    out_dir = tmp_path_factory.mktemp("wheel")

    subprocess.run(  # noqa: S603  # fixed argv, no shell, no user input
        ["uv", "build", "--wheel", "--out-dir", str(out_dir)],  # noqa: S607  # uv resolved from PATH by design
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
    )

    wheels = list(out_dir.glob("*.whl"))
    assert len(wheels) == 1, f"expected exactly one wheel, got {wheels}"

    with zipfile.ZipFile(wheels[0]) as archive:
        return archive.namelist()


@pytest.mark.e2e
@pytest.mark.slow
def test_wheel_contains_every_required_resource(wheel_members: list[str]):
    missing = {
        pattern: reason
        for pattern, reason in REQUIRED_WHEEL_CONTENTS.items()
        if not _wheel_matches(wheel_members, pattern)
    }
    assert not missing, f"wheel is missing required resources: {missing}"


@pytest.mark.e2e
@pytest.mark.slow
def test_wheel_ships_every_runtime_resource_on_disk(wheel_members: list[str]):
    """No runtime resource may be left behind by an under-specified glob."""
    on_disk = _runtime_resources_on_disk()
    assert on_disk, "found no runtime resources on disk — the walk is broken"

    missing = sorted(set(on_disk) - set(wheel_members))
    assert not missing, (
        f"{len(missing)} of {len(on_disk)} runtime resources are absent from the "
        f"wheel; add a package-data glob covering them: {missing}"
    )


@pytest.mark.e2e
@pytest.mark.slow
def test_wheel_excludes_build_artifacts(wheel_members: list[str]):
    """A lazy `**/*` glob would sweep caches and logs into the distribution."""
    leaked = sorted(
        name
        for name in wheel_members
        if _EXCLUDED_DIRS.intersection(Path(name).parts) or name.endswith(".pyc")
    )
    assert not leaked, f"wheel ships build artifacts that must be excluded: {leaked}"
