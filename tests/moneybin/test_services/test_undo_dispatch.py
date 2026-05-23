"""Audit-row → owning-repo resolution: ``undo_dispatch.repo_for`` (REC-PR3 Phase 5).

The undo consumer reads each audit row's ``target_schema``/``target_table`` and
needs the ``*Repo`` whose generic ``undo_event`` reverses it. The registry is
built by discovery, so this test guards two properties: every concrete repo
resolves to itself, and an unregistered table raises rather than silently
returning the wrong repo.
"""

# This module verifies the registry's internals (_REGISTRY coverage) directly.
# pyright: reportPrivateUsage=false
from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.repositories import concrete_repo_classes
from moneybin.services.undo_dispatch import _REGISTRY, repo_for


def test_every_repo_resolves_to_itself(db: Database) -> None:
    for cls in concrete_repo_classes():
        ref = cls.table_ref
        repo = repo_for(ref.schema, ref.name, db)
        assert isinstance(repo, cls), f"{ref.full_name} resolved to {type(repo)}"


def test_resolved_repo_is_bound_to_given_db(db: Database) -> None:
    repo = repo_for("app", "transaction_notes", db)
    assert repo._db is db  # pyright: ignore[reportPrivateUsage]


def test_registry_covers_every_concrete_repo() -> None:
    registered = set(_REGISTRY.values())
    discovered = set(concrete_repo_classes())
    assert discovered, "no repos discovered"
    assert discovered == registered, (
        f"registry out of sync: missing {discovered - registered}, "
        f"extra {registered - discovered}"
    )


def test_unknown_table_raises(db: Database) -> None:
    with pytest.raises(KeyError):
        repo_for("app", "does_not_exist", db)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
