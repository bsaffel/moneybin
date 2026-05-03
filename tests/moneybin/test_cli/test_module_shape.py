"""Locks in the package-level shape of moneybin.cli.

Specifically: `moneybin.cli.main` must resolve to the *module*, not the
`main()` function. Re-exporting the function at the package level shadows
the submodule and forces test code into `sys.modules` workarounds.
"""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest

pytestmark = pytest.mark.unit


def test_cli_main_attribute_is_module() -> None:
    cli = importlib.import_module("moneybin.cli")
    assert isinstance(cli.main, ModuleType), (
        "moneybin.cli.main is shadowed by a non-module attribute "
        "(likely a function re-export). The submodule must remain accessible."
    )


def test_cli_package_does_not_export_main_function() -> None:
    cli = importlib.import_module("moneybin.cli")
    assert "main" not in cli.__all__, (
        "`main` is in moneybin.cli.__all__; remove it to keep the submodule "
        "name unshadowed."
    )
