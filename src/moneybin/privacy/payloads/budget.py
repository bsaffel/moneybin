"""Typed payload dataclasses for the budget surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Tier derivation summary:
  - ``BudgetCategoryStatusRow``  → Tier.HIGH (TXN_AMOUNT fields)
  - ``BudgetStatusPayload``      → Tier.HIGH (via BudgetCategoryStatusRow)
  - ``BudgetSetPayload``         → Tier.HIGH (monthly_amount = TXN_AMOUNT)
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Literal

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class BudgetCategoryStatusRow:
    """Per-category budget status row."""

    category: Annotated[str, DataClass.CATEGORY]
    budget: Annotated[Decimal, DataClass.TXN_AMOUNT]
    spent: Annotated[Decimal, DataClass.TXN_AMOUNT]
    remaining: Annotated[Decimal, DataClass.TXN_AMOUNT]
    status: Annotated[Literal["OK", "WARNING", "OVER"], DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class BudgetStatusPayload:
    """Result of reports_budget — per-category status + month."""

    month: Annotated[str, DataClass.TXN_DATE]
    categories: list[BudgetCategoryStatusRow]


@dataclass(frozen=True, slots=True)
class BudgetSetPayload:
    """Result of ``budget_set`` — confirmation of created/updated budget.

    ``monthly_amount`` is TXN_AMOUNT (Tier.HIGH) — it is a user-provided
    spending target and classified consistently with other amount fields
    in the budget surface.
    ``start_month`` is TXN_DATE (Tier.MEDIUM) — the month boundary for
    this budget target.
    """

    category: Annotated[str, DataClass.CATEGORY]
    monthly_amount: Annotated[Decimal, DataClass.TXN_AMOUNT]
    action: Annotated[Literal["created", "updated"], DataClass.TXN_TYPE]
    start_month: Annotated[str, DataClass.TXN_DATE]
