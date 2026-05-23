"""Repository layer — audited writes to protected app.* tables.

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
a protected ``app.*`` table flows through a ``*Repo`` class that pairs the
write with an ``app.audit_log`` row in the same DuckDB transaction. Services
compose repositories instead of executing raw ``INSERT``/``UPDATE``/``DELETE``
against ``app.*``.
"""

from __future__ import annotations

import importlib
import pkgutil

from moneybin.repositories.base import BaseRepo


def concrete_repo_classes() -> list[type[BaseRepo]]:
    """Every concrete ``BaseRepo`` subclass defined under this package.

    Imports each repo module so the subclasses register, then filters
    ``BaseRepo.__subclasses__()`` to this package (excludes test-only fakes
    elsewhere). The single source of truth for "all repos" — the undo dispatch
    registry and the metadata/coverage tests all derive from it, so a new repo is
    discoverable everywhere the moment it is defined.
    """
    for mod in pkgutil.iter_modules(__path__):
        if mod.name != "base":
            importlib.import_module(f"{__name__}.{mod.name}")
    return [
        cls
        for cls in BaseRepo.__subclasses__()
        if cls.__module__.startswith(f"{__name__}.")
    ]
