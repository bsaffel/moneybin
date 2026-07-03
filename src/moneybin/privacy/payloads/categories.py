"""Typed payload dataclasses for the categories + merchants surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Tier derivation summary:
  - ``CategoryRow``              → Tier.LOW (CATEGORY fields only)
  - ``CategoriesPayload``        → Tier.LOW (via CategoryRow)
  - ``CategoryCreatePayload``    → Tier.LOW (CATEGORY fields + RECORD_ID)
  - ``CategorySetPayload``       → Tier.LOW (CATEGORY + TXN_TYPE)
  - ``CategoryDeletePayload``    → Tier.LOW (CATEGORY + TXN_TYPE)
  - ``MerchantRow``              → Tier.MEDIUM (MERCHANT_NAME fields)
  - ``MerchantsPayload``         → Tier.MEDIUM (via MerchantRow)
  - ``MerchantsCreatePayload``   → Tier.LOW (AGGREGATE only — counts + opaque dicts)

``merchants`` returns raw_pattern and canonical_name, both MERCHANT_NAME
(Tier.MEDIUM). Write-result payloads (create, set, delete) carry only counts
and IDs — all Tier.LOW.

Design note: ``MerchantsCreatePayload.error_details`` uses ``list[dict[str, str]]``
(not a typed ``ErrorDetailRow``) so the introspection walker does not recurse
into MERCHANT_NAME fields inside the nested class, keeping the write-result
payload at Tier.LOW — matching the ``CategorizeCommitPayload`` pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# categories tool — list all categories
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategoryRow:
    """One row from core.dim_categories / app.user_categories.

    Fields follow the taxonomy for ``("core", "dim_categories")``:
    ``category_id`` is CATEGORY (not RECORD_ID) in that table's mapping.
    """

    # CATEGORY — keeps CategoryRow at Tier.LOW (no MERCHANT_NAME, ACCOUNT_IDENTIFIER)
    category_id: Annotated[str, DataClass.CATEGORY]
    category: Annotated[str | None, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    description: Annotated[str | None, DataClass.CATEGORY]
    # class_: trailing underscore because `class` is a Python keyword; maps to
    # the DB column `class` (accounting class: income | expense | transfer | debt).
    class_: Annotated[str | None, DataClass.TXN_TYPE]
    is_default: Annotated[bool | None, DataClass.TXN_TYPE]
    is_active: Annotated[bool | None, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class CategoriesPayload:
    """Payload for the ``categories`` list tool."""

    categories: list[CategoryRow]


# ---------------------------------------------------------------------------
# categories_create — write result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategoryCreatePayload:
    """Payload for ``categories_create`` — creation confirmation.

    All fields are Tier.LOW: category_id follows ``dim_categories`` taxonomy
    (CATEGORY), display is a formatted label (CATEGORY), action is a status
    string (TXN_TYPE).
    """

    category_id: Annotated[str, DataClass.CATEGORY]
    category: Annotated[str, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    action: Annotated[str, DataClass.TXN_TYPE]
    display: Annotated[str, DataClass.CATEGORY]


# ---------------------------------------------------------------------------
# categories_set — partial-update result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategorySetPayload:
    """Payload for ``categories_set`` — is_active toggle confirmation."""

    category_id: Annotated[str, DataClass.CATEGORY]
    action: Annotated[str, DataClass.TXN_TYPE]


# ---------------------------------------------------------------------------
# categories_delete — hard-delete result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategoryDeletePayload:
    """Payload for ``categories_delete`` — delete confirmation."""

    category_id: Annotated[str, DataClass.CATEGORY]
    action: Annotated[str, DataClass.TXN_TYPE]
    force: Annotated[bool, DataClass.TXN_TYPE]


# ---------------------------------------------------------------------------
# merchants tool — list all merchant mappings
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MerchantRow:
    """One row from core.dim_merchants.

    ``raw_pattern`` and ``canonical_name`` are MERCHANT_NAME (Tier.MEDIUM),
    which drives ``MerchantsPayload`` to Tier.MEDIUM.
    """

    merchant_id: Annotated[str, DataClass.RECORD_ID]
    # MERCHANT_NAME — drives MerchantsPayload to Tier.MEDIUM
    raw_pattern: Annotated[str | None, DataClass.MERCHANT_NAME]
    match_type: Annotated[str | None, DataClass.TXN_TYPE]
    canonical_name: Annotated[str, DataClass.MERCHANT_NAME]
    category: Annotated[str | None, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]


@dataclass(frozen=True, slots=True)
class MerchantsPayload:
    """Payload for the ``merchants`` list tool."""

    merchants: list[MerchantRow]


# ---------------------------------------------------------------------------
# merchants_create — batch creation result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MerchantsCreatePayload:
    """Payload for ``merchants_create`` — batch creation result.

    Top-level fields are AGGREGATE (Tier.LOW). ``error_details`` uses
    ``list[dict[str, str]]`` (not a typed nested dataclass) so the
    introspection walker does not recurse into MERCHANT_NAME fields,
    keeping derive_tier at Tier.LOW — matching CategorizeCommitPayload.
    """

    created: Annotated[int, DataClass.AGGREGATE]
    skipped: Annotated[int, DataClass.AGGREGATE]
    error_details: Annotated[list[dict[str, str]], DataClass.AGGREGATE]
