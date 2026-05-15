"""Pydantic models for YAML config validation and runtime data types.

Config models validate persona and merchant catalog YAML files at load time.
Runtime dataclasses represent generated data flowing through the pipeline.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ---------------------------------------------------------------------------
# YAML config models
# ---------------------------------------------------------------------------


class AmountDistribution(BaseModel):
    """An amount that can be sampled from a distribution.

    Used for variable charges like utilities (mean=$145, stddev=$35)
    and per-merchant transaction amounts.
    """

    mean: float = Field(gt=0)
    stddev: float = Field(ge=0, default=0.0)


class MerchantEntry(BaseModel):
    """A single merchant in a catalog with weighted selection and amount distribution."""

    name: str = Field(min_length=1)
    weight: int = Field(ge=1)
    amount: AmountDistribution
    description_prefix: str | None = None


class MerchantCatalog(BaseModel):
    """A category-level merchant catalog loaded from YAML."""

    category: str = Field(min_length=1)
    merchants: list[MerchantEntry] = Field(min_length=1)


class AccountConfig(BaseModel):
    """A single account in a persona definition."""

    name: str = Field(min_length=1)
    type: Literal["checking", "savings", "credit_card"]
    source_type: Literal["ofx", "csv"]
    institution: str = Field(min_length=1)
    opening_balance: float = 0.0


class PriceIncrease(BaseModel):
    """A scheduled price increase for a recurring charge."""

    after_months: int = Field(ge=1)
    new_amount: float = Field(gt=0)


class IncomeConfig(BaseModel):
    """An income source in a persona definition."""

    type: Literal["salary", "freelance"]
    account: str
    amount: float | AmountDistribution
    schedule: Literal["biweekly", "monthly", "irregular"]
    pay_day: str | None = None
    annual_raise_pct: float = 0.0
    description_template: str = ""
    employer: str | None = None
    count_per_month: AmountDistribution | None = None


class RecurringConfig(BaseModel):
    """A recurring charge in a persona definition."""

    category: str
    description: str
    account: str
    amount: float | AmountDistribution
    day_of_month: int = Field(ge=1, le=28)
    price_increases: list[PriceIncrease] = Field(default_factory=list)
    months: list[int] | None = None


class SpendingCategoryConfig(BaseModel):
    """A discretionary spending category in a persona definition."""

    name: str
    merchant_catalog: str
    monthly_budget: AmountDistribution
    transactions_per_month: AmountDistribution
    accounts: list[str] = Field(min_length=1)
    account_weights: list[float] | None = None
    seasonal_modifiers: dict[str, float] = Field(default_factory=dict)
    day_of_week_weights: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_account_weights(self) -> SpendingCategoryConfig:
        """Ensure account_weights length matches accounts length."""
        if self.account_weights is not None and len(self.account_weights) != len(
            self.accounts
        ):
            raise ValueError(
                f"account_weights length ({len(self.account_weights)}) must match "
                f"accounts length ({len(self.accounts)})"
            )
        return self


class SpendingConfig(BaseModel):
    """The spending section of a persona definition."""

    categories: list[SpendingCategoryConfig]


class TransferConfig(BaseModel):
    """An account-to-account transfer in a persona definition.

    YAML uses ``from``/``to`` keys which are Python reserved words,
    so we use Field aliases.
    """

    model_config = ConfigDict(populate_by_name=True)

    from_account: str = Field(alias="from")
    to_account: str = Field(alias="to")
    amount: float | Literal["statement_balance"]
    schedule: Literal["monthly", "biweekly"]
    day_of_month: int = Field(ge=1, le=28)
    description_template: str = ""


class PersonaConfig(BaseModel):
    """A complete persona definition loaded from YAML.

    Validates that all account references in income, recurring, spending,
    and transfers actually exist in the accounts list.
    """

    persona: str
    profile: str
    description: str
    years_default: int = 3
    accounts: list[AccountConfig] = Field(min_length=1)
    income: list[IncomeConfig]
    recurring: list[RecurringConfig] = Field(default_factory=list)
    spending: SpendingConfig
    transfers: list[TransferConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_account_references(self) -> PersonaConfig:
        """Ensure all account references point to defined accounts."""
        account_names = {a.name for a in self.accounts}

        for inc in self.income:
            if inc.account not in account_names:
                raise ValueError(f"Income references unknown account: {inc.account!r}")

        for rec in self.recurring:
            if rec.account not in account_names:
                raise ValueError(
                    f"Recurring references unknown account: {rec.account!r}"
                )

        for cat in self.spending.categories:
            for acct in cat.accounts:
                if acct not in account_names:
                    raise ValueError(
                        f"Spending category {cat.name!r} references "
                        f"unknown account: {acct!r}"
                    )

        for xfer in self.transfers:
            if xfer.from_account not in account_names:
                raise ValueError(
                    f"Transfer references unknown account: {xfer.from_account!r}"
                )
            if xfer.to_account not in account_names:
                raise ValueError(
                    f"Transfer references unknown account: {xfer.to_account!r}"
                )

        return self


# ---------------------------------------------------------------------------
# YAML loading helpers
# ---------------------------------------------------------------------------

_DATA_DIR = Path(__file__).resolve().parent / "data"


def load_persona(persona_name: str) -> PersonaConfig:
    """Load and validate a persona YAML file.

    Args:
        persona_name: Name of the persona (matches filename without extension).

    Returns:
        Validated PersonaConfig.

    Raises:
        FileNotFoundError: If the persona YAML file doesn't exist.
    """
    import yaml

    path = _DATA_DIR / "personas" / f"{persona_name}.yaml"
    if not path.resolve().is_relative_to(_DATA_DIR.resolve()):
        raise FileNotFoundError(f"Invalid persona name: {persona_name!r}")
    if not path.exists():
        available = sorted(p.stem for p in (_DATA_DIR / "personas").glob("*.yaml"))
        raise FileNotFoundError(
            f"Unknown persona: {persona_name!r}. "
            f"Available: {', '.join(available) or '(none)'}"
        )
    raw = yaml.safe_load(path.read_text())
    return PersonaConfig.model_validate(raw)


def load_merchant_catalog(category: str) -> MerchantCatalog:
    """Load and validate a merchant catalog YAML file.

    Args:
        category: Category name (matches filename without extension).

    Returns:
        Validated MerchantCatalog.

    Raises:
        FileNotFoundError: If the merchant catalog YAML file doesn't exist.
    """
    import yaml

    path = _DATA_DIR / "merchants" / f"{category}.yaml"
    if not path.resolve().is_relative_to(_DATA_DIR.resolve()):
        raise FileNotFoundError(f"Invalid merchant catalog name: {category!r}")
    if not path.exists():
        available = sorted(p.stem for p in (_DATA_DIR / "merchants").glob("*.yaml"))
        raise FileNotFoundError(
            f"Unknown merchant catalog: {category!r}. "
            f"Available: {', '.join(available) or '(none)'}"
        )
    raw = yaml.safe_load(path.read_text())
    return MerchantCatalog.model_validate(raw)


# ---------------------------------------------------------------------------
# Runtime dataclasses (generated data flowing through the pipeline)
# ---------------------------------------------------------------------------


@dataclass
class GeneratedAccount:
    """An account created by the generator with synthetic IDs."""

    name: str
    account_id: str
    account_type: str
    source_type: str
    institution: str
    opening_balance: Decimal


@dataclass
class GeneratedTransaction:
    """A single generated transaction.

    Generators produce these; the engine assigns ``transaction_id``
    after deterministic sorting; the writer maps them to raw table columns.
    """

    date: datetime.date
    amount: Decimal
    description: str
    account_name: str
    category: str | None = None
    transfer_pair_id: str | None = None
    transaction_type: Literal["DEBIT", "DEP", "DIRECTDEP", "XFER"] = "DEBIT"
    transaction_id: str = ""


@dataclass
class GenerationResult:
    """Complete output of a generation run, ready for the writer."""

    persona: str
    seed: int
    accounts: list[GeneratedAccount]
    transactions: list[GeneratedTransaction]
    start_date: datetime.date
    end_date: datetime.date
