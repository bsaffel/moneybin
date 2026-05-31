"""Doctor recipe registry — ``(audit_name) -> Recipe | None`` lookup.

A *recipe* converts an audit's ``affected_ids`` into a list of
``RecoveryAction`` an agent can execute directly. Recipes are pure callables
over their inputs; ``RecipeContext`` carries the database handle for the rare
recipe that needs to query for additional state, but most recipes ignore it.

Recipes self-register via :func:`register` from each recipe module; importing
the parent ``moneybin.audits.recipes`` package triggers that registration.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from moneybin.errors import RecoveryAction

if TYPE_CHECKING:
    from moneybin.database import Database


@dataclass(frozen=True)
class RecipeContext:
    """Context passed to every recipe.

    ``db`` is optional so recipes that don't need DB access can be unit-tested
    without spinning one up. Recipes that DO query the DB must defend against
    ``None`` (or the test must supply a real handle).
    """

    db: Database | None


Recipe = Callable[[list[str], RecipeContext], list[RecoveryAction]]


_REGISTRY: dict[str, Recipe] = {}


def register(audit_name: str, recipe: Recipe) -> None:
    """Bind ``audit_name`` to ``recipe`` in the module-level registry."""
    _REGISTRY[audit_name] = recipe


def get(audit_name: str) -> Recipe | None:
    """Return the recipe for ``audit_name``, or ``None`` if unregistered."""
    return _REGISTRY.get(audit_name)
