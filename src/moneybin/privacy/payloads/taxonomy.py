"""Typed payloads for the dormant normalized taxonomy read."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict

from moneybin.privacy.payloads.categories import CategoryRow, MerchantRow
from moneybin.privacy.taxonomy import DataClass


class TaxonomyCategoriesView(BaseModel):
    """Paginated category taxonomy."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["categories"], DataClass.TXN_TYPE] = "categories"
    rows: list[CategoryRow]


class TaxonomyMerchantsView(BaseModel):
    """Paginated merchant mappings."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["merchants"], DataClass.TXN_TYPE] = "merchants"
    rows: list[MerchantRow]


TaxonomyCoarsePayload = TaxonomyCategoriesView | TaxonomyMerchantsView
