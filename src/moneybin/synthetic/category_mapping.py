"""Map synthetic category labels to the canonical taxonomy.

Synthetic personas use machine-friendly lowercase labels (``grocery``,
``utilities``, ``income``). The canonical taxonomy in
``sqlmesh/models/seeds/categories.csv`` uses Title Case (``Food & Drink``,
``Housing & Utilities``, ``Income``). Without alignment, categorization
evaluations and sign-convention assertions compare values from different
naming conventions and always fail.

This mapping lives at the synthetic boundary so ground-truth labels and
user-created merchant rules both speak the canonical vocabulary.
"""

from __future__ import annotations

_SYNTHETIC_TO_CANONICAL: dict[str, str] = {
    "income": "Income",
    "grocery": "Food & Drink",
    "dining": "Food & Drink",
    "transport": "Transportation",
    "shopping": "Shopping",
    "entertainment": "Entertainment",
    "personal_care": "Personal Care",
    "housing": "Housing & Utilities",
    "utilities": "Housing & Utilities",
    "insurance": "Services",
    "subscriptions": "Entertainment",
    "taxes": "Government & Nonprofit",
    "gifts": "Other",
    "kids": "Other",
    "kids_activities": "Other",
    "health": "Healthcare",
    "education": "Services",
    "travel": "Travel",
}


def to_canonical(synthetic_category: str | None) -> str | None:
    """Return the canonical category for a synthetic label, or ``None`` unchanged.

    Falls back to ``"Other"`` for unknown labels so unmapped data still
    satisfies the sign-convention assertion (which only special-cases
    ``Income`` and ``Transfer``).
    """
    if synthetic_category is None:
        return None
    return _SYNTHETIC_TO_CANONICAL.get(synthetic_category, "Other")
