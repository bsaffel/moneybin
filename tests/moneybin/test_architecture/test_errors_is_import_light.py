"""Structural guardrail: `moneybin.errors` is a leaf of the import graph.

`errors.py` carries the cross-cutting error taxonomy — `UserError`,
`ErrorDetail`, `RecoveryAction` — that every layer raises and every surface
catches. If importing it drags in `moneybin.database`, then `database.py` can
no longer import `UserError` without minting an import cycle, and neither can
anything `database.py` itself imports. That is how the pre-MB-51 tangle formed:
`errors -> database` at module level forced `db_lock` and the service layer
into deferred imports to get back.

The classifier still recognizes those families; it imports them inside
`classify_user_error`, on the failure path, where a one-time module load costs
nothing. This test pins the module-load footprint so the module-level import
cannot creep back.

Measured in a fresh interpreter: `sys.modules` in this process is already
polluted by everything the test suite imported.
"""

from __future__ import annotations

import json
import subprocess  # noqa: S404 — clean-interpreter import check
import sys

# Everything `import moneybin.errors` may pull in: the package root, the
# constant table, and itself. Nothing else — `error_codes` is a flat list of
# string constants, so this set cannot grow without a new module-level import.
ALLOWED_MODULE_LOADS = frozenset({
    "moneybin",
    "moneybin.error_codes",
    "moneybin.errors",
})

_PROBE = """
import json
import sys

import moneybin.errors  # noqa: F401

print(json.dumps(sorted(m for m in sys.modules if m.startswith("moneybin"))))
"""


def _modules_loaded_by_importing_errors() -> frozenset[str]:
    result = subprocess.run(  # noqa: S603  # fixed argv, no shell, no user input
        [sys.executable, "-c", _PROBE],
        capture_output=True,
        text=True,
        check=True,
    )
    return frozenset(json.loads(result.stdout))


def test_importing_errors_loads_only_leaf_modules() -> None:
    loaded = _modules_loaded_by_importing_errors()
    unexpected = loaded - ALLOWED_MODULE_LOADS
    assert not unexpected, (
        "importing moneybin.errors now loads "
        f"{sorted(unexpected)}. Keep the taxonomy a leaf: move the import "
        "inside the function that needs it, or add the module here only if "
        "it is genuinely a leaf with no MoneyBin imports of its own."
    )


def test_importing_errors_does_not_load_the_database_or_service_layers() -> None:
    """The two directions that reintroduce the cycle, named explicitly.

    `test_importing_errors_loads_only_leaf_modules` already covers these; this
    states the failure mode so the diff that breaks it reads as a cycle, not as
    an allowlist that needs one more entry.
    """
    loaded = _modules_loaded_by_importing_errors()
    assert "moneybin.database" not in loaded
    assert not any(m.startswith("moneybin.services") for m in loaded)
