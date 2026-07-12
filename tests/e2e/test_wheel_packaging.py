"""The built wheel must contain every file the installed package needs.

Regression guard for the packaging defect where ``package-data`` globs used
``../../sqlmesh/...`` paths that escape the package dir — setuptools silently
ignores those, so the wheel shipped no SQL, no models, and no synthetic data.
A ``pip install moneybin`` could not create a profile or run a transform.

Two complementary checks, one built wheel:

* :func:`test_wheel_contains_every_required_resource` — a hand-written map of
  glob -> why it matters. Good failure messages, states intent.
* :func:`test_wheel_ships_every_packaged_file_on_disk` — a completeness check
  against the source tree, framed as a *deny*-list. An allow-list of
  "resource-ish" suffixes goes green on a resource type nobody thought to list;
  a deny-list fails until the new type is globbed.
"""

import fnmatch
import shutil
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

# Every file under src/moneybin/ must reach the wheel. Two mechanisms carry it:
# `packages.find` (namespaces = true) ships every .py under the package root —
# including the SQLMesh models and the .py migrations, which are *data* read by
# path rather than imported. `[tool.setuptools.package-data]` ships everything
# else, and also lists those data .py explicitly so that a future
# `namespaces = false` cannot silently drop them.
#
# The completeness walk below is therefore a DENY-list, not an allow-list of
# "resource-ish" suffixes: a new resource type (a .json, a .j2, a .toml) fails
# this test until someone adds a glob for it, instead of being quietly omitted
# from the distribution. Quiet omission is how the SQLMesh project dir, the
# SQLMesh .py models, and the .py migrations were each missed in turn.

# Build and runtime droppings. Never distributed, and their presence in a wheel
# is itself a bug — so both the completeness walk and the junk check use this.
_EXCLUDED_DIRS = frozenset({
    "__pycache__",  # bytecode; regenerated on import
    ".cache",  # SQLMesh query cache (src/moneybin/sqlmesh/.cache/)
    "logs",  # logs written when the CLI is run from the source tree
    "profiles",  # profile state (DBs, config) if MONEYBIN_HOME resolves in-tree
})

# Suffixes deliberately left out of the wheel.
_UNSHIPPED_SUFFIXES = frozenset({
    ".pyc",  # bytecode; a stray one outside __pycache__
    ".md",  # contributor docs on source layout — nothing in an install reads them
})


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


def _files_that_must_ship() -> list[str]:
    """Every file under the package root the wheel is required to carry."""
    found: list[str] = []
    for path in _PKG_ROOT.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(_PKG_ROOT)
        if _EXCLUDED_DIRS.intersection(relative.parts):
            continue
        if path.suffix in _UNSHIPPED_SUFFIXES:
            continue
        found.append(f"moneybin/{relative.as_posix()}")
    return sorted(found)


def _purge_build_caches() -> None:
    """Delete setuptools' two never-pruned caches so the wheel is built from source.

    Both make a stale file outlive the glob that once matched it, so both would
    let this guard pass on exactly the edit it exists to catch — a deleted or
    narrowed ``package-data`` glob — and the guard's own first run populates them:

    * ``build/lib/`` — setuptools stages package-data here and never prunes, so a
      file that stops matching any glob is still swept into the next wheel.
    * ``src/*.egg-info/SOURCES.txt`` — ``include-package-data`` defaults to *true*
      under ``pyproject.toml``, and ``build_py`` re-reads this file list and ships
      everything in it as package-data. It is self-perpetuating: ``egg_info``
      re-reads the existing SOURCES.txt when regenerating it, so once a path lands
      there it survives every later build regardless of the globs.
    """
    shutil.rmtree(_REPO_ROOT / "build", ignore_errors=True)
    for egg_info in (_REPO_ROOT / "src").glob("*.egg-info"):
        shutil.rmtree(egg_info, ignore_errors=True)


@pytest.fixture(scope="module")
def wheel_members(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """Build a real wheel from a clean tree and return its archive member names."""
    # Safe under xdist: --dist=loadscope keeps this module on one worker, and no
    # other test builds. Both caches are gitignored build artifacts, regenerated
    # by the build below.
    _purge_build_caches()

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
def test_wheel_ships_every_packaged_file_on_disk(wheel_members: list[str]):
    """No file may be left behind by an under-specified or deleted glob."""
    on_disk = _files_that_must_ship()
    assert on_disk, "found no files on disk — the walk is broken"

    missing = sorted(set(on_disk) - set(wheel_members))
    assert not missing, (
        f"{len(missing)} of {len(on_disk)} packaged files are absent from the "
        f"wheel; add a package-data glob covering them: {missing}"
    )


@pytest.mark.e2e
@pytest.mark.slow
def test_wheel_excludes_build_artifacts(wheel_members: list[str]):
    """A lazy `**/*` glob would sweep caches, logs and profile state into the wheel."""
    leaked = sorted(
        name
        for name in wheel_members
        if _EXCLUDED_DIRS.intersection(Path(name).parts) or name.endswith(".pyc")
    )
    assert not leaked, f"wheel ships build artifacts that must be excluded: {leaked}"
