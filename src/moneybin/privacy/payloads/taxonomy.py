"""Typed payloads for the dormant normalized taxonomy read."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class TaxonomyStateResult:
    """One applied taxonomy target in request order."""

    kind: Annotated[Literal["category", "merchant"], DataClass.TXN_TYPE]
    target_id: Annotated[str | None, DataClass.RECORD_ID]
    state: Annotated[
        Literal["present", "inactive", "absent"],
        DataClass.TXN_TYPE,
    ]
    changed: Annotated[bool, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class TaxonomySetPayload:
    """Result of one atomic taxonomy target-state batch."""

    results: list[TaxonomyStateResult]
    operation_id: Annotated[str, DataClass.RECORD_ID]
