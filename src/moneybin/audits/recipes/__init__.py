"""Doctor recipe registry — recipes self-register at package import.

Importing this package triggers each recipe module's ``register(...)`` call.
``DoctorService`` imports this package (rather than just ``.registry``) to
ensure registrations have fired before lookup.
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
