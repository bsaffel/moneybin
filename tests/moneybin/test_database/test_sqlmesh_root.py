"""The SQLMesh project must resolve inside the installed package.

Regression guard: it previously resolved via ``parents[2]``, which only works
in a source checkout — a wheel install found nothing and could not transform.
``SQLMESH_ROOT`` is public precisely so tests and callers locate the project
through one constant instead of re-deriving the path at every call site.
"""

from pathlib import Path

import pytest

import moneybin
from moneybin.database import SQLMESH_ROOT

pytestmark = pytest.mark.unit


def test_sqlmesh_root_is_inside_the_package() -> None:
    package_dir = Path(moneybin.__file__).resolve().parent
    assert SQLMESH_ROOT.is_relative_to(package_dir)


def test_sqlmesh_root_carries_the_whole_project() -> None:
    assert (SQLMESH_ROOT / "config.py").is_file()
    assert (SQLMESH_ROOT / "external_models.yaml").is_file()
    assert list((SQLMESH_ROOT / "models").rglob("*.sql")), "no models found"
    assert list((SQLMESH_ROOT / "audits").rglob("*.sql")), "no audits found"
    assert list((SQLMESH_ROOT / "models" / "seeds").glob("*.csv")), "no seeds found"
