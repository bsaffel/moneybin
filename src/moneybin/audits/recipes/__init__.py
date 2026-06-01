"""Doctor recipe registry — recipes registered by ``__init__.py`` at import.

Adding a new recipe is a two-step change: (1) create the recipe module
under ``moneybin/audits/recipes/<audit_name>.py``, (2) add an explicit
``register("<audit_name>", <module>.recipe)`` call below. The recipe
modules themselves do NOT self-register — `test_round_trip_executable`'s
``test_every_explicit_recipe_module_is_registered`` catches the omission
if you forget step (2). ``DoctorService`` imports this package (rather
than just ``.registry``) so all ``register(...)`` calls have fired
before any lookup.
"""

from __future__ import annotations

from moneybin.audits.recipes import (
    categorization_coverage,
    dedup_reconciliation,
    orphan_app_state,
)
from moneybin.audits.recipes.registry import (
    Recipe,
    RecipeContext,
    get,
    register,
)

register("orphan_app_state", orphan_app_state.recipe)
register("categorization_coverage", categorization_coverage.recipe)
register("dedup_reconciliation", dedup_reconciliation.recipe)


__all__ = ["Recipe", "RecipeContext", "get", "register"]
