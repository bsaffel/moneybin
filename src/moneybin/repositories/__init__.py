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

    Abstract repository bases are excluded while their concrete descendants are
    included, so shared repository mechanics do not hide an owned table from
    undo dispatch.
    """
    for mod in pkgutil.iter_modules(__path__):
        if mod.name != "base":
            importlib.import_module(f"{__name__}.{mod.name}")

    def descendants(parent: type[BaseRepo]) -> list[type[BaseRepo]]:
        """Return every descendant, including repos behind an abstract base."""
        children = parent.__subclasses__()
        return children + [child for cls in children for child in descendants(cls)]

    return [
        cls
        for cls in descendants(BaseRepo)
        if cls.__module__.startswith(f"{__name__}.") and not cls.abstract
    ]
