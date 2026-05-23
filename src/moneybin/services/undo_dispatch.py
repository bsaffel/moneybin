"""Resolve an audit row's ``(schema, table)`` to the repo that owns it (REC-PR3).

The undo consumer reads each audit row's ``target_schema``/``target_table`` and
needs the ``*Repo`` whose generic :meth:`BaseRepo.undo_event` reverses it. Rather
than a hand-maintained map — a second place to update for every new repo, exactly
the parallel pattern coherence forbids — the registry is built by discovery:
import every module under ``moneybin.repositories`` and key each ``BaseRepo``
subclass by its declared ``table_ref``. ``test_undo_dispatch`` guards that the
registry covers every concrete repo, so a new repo is dispatchable the moment it
declares its metadata.
"""

from __future__ import annotations

import importlib
import pkgutil

from moneybin import repositories as _repos_pkg
from moneybin.database import Database
from moneybin.repositories.base import BaseRepo


def _discover_repo_classes() -> list[type[BaseRepo]]:
    """Import every repo module and return the concrete ``BaseRepo`` subclasses.

    ``__subclasses__`` only sees imported classes, so the package walk is what
    makes discovery exhaustive. Test-only fakes in other packages are excluded.
    """
    for mod in pkgutil.iter_modules(_repos_pkg.__path__):
        if mod.name != "base":
            importlib.import_module(f"{_repos_pkg.__name__}.{mod.name}")
    return [
        c
        for c in BaseRepo.__subclasses__()
        if c.__module__.startswith("moneybin.repositories.")
    ]


def _build_registry() -> dict[tuple[str, str], type[BaseRepo]]:
    registry: dict[tuple[str, str], type[BaseRepo]] = {}
    for cls in _discover_repo_classes():
        key = (cls.table_ref.schema, cls.table_ref.name)
        existing = registry.get(key)
        if existing is not None and existing is not cls:
            raise RuntimeError(
                f"Two repos own {key[0]}.{key[1]}: "
                f"{existing.__name__} and {cls.__name__}"
            )
        registry[key] = cls
    return registry


#: ``(schema, table) -> RepoClass`` for every audited ``app.*`` table.
_REGISTRY: dict[tuple[str, str], type[BaseRepo]] = _build_registry()


def repo_for(schema: str, table: str, db: Database) -> BaseRepo:
    """Return a repo instance that owns ``schema.table``, bound to ``db``.

    Raises ``KeyError`` when no repo owns the table — the audit row targets a
    table outside the undoable ``app.*`` surface (e.g. ``raw.manual_transactions``
    from manual entry, which is out of undo scope).
    """
    cls = _REGISTRY.get((schema, table))
    if cls is None:
        raise KeyError(f"No repo registered for {schema}.{table}")
    return cls(db)
