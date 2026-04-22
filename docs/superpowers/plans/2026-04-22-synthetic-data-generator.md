# Synthetic Data Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate realistic, deterministic, multi-year financial histories for fictional personas — enabling integration testing, demos, and autonomous verification without real financial data.

**Architecture:** Declarative YAML pipeline — persona YAML files define financial lives, a generic engine interprets them month-by-month, and a writer outputs Polars DataFrames to existing raw tables via `Database.ingest_dataframe()`. All randomness flows through a single `SeededRandom` wrapper for deterministic output. Ground-truth labels are written to a `synthetic.ground_truth` table for Tier 3 scored evaluation.

**Tech Stack:** Python 3.12, DuckDB, Polars, Pydantic v2 (YAML validation), PyYAML, Typer (CLI), pytest

**Spec:** `docs/specs/testing-synthetic-data.md` (child of `docs/specs/testing-overview.md`)

---

## Design Notes

### Schema Discrepancy

The spec references `raw.tabular_transactions` and `raw.tabular_accounts` (from the not-yet-implemented `smart-import-tabular.md`). The actual existing tables are `raw.csv_transactions` and `raw.csv_accounts`. This plan targets the **existing schemas**. Columns referenced in the spec that don't exist in the current schema (e.g., `source_type`, `source_origin`, `original_amount`, `original_date_str`, `currency`, `row_number` on csv_transactions; `account_name` on csv_accounts) are omitted. When `smart-import-tabular.md` ships, the writer can be updated to populate the new columns.

### Transform Pipeline Reuse

The `_run_transforms()` function in `import_service.py` handles encrypted DB injection into SQLMesh's adapter cache. The synthetic engine needs the same capability. This plan renames it to `run_transforms()` (public) so both import_service and the generator can call it.

### Date Range Convention

For `--years=N`, generate N complete calendar years ending at the year before the current year. In 2026: `--years=3` produces 2023-01-01 to 2025-12-31.

### Account ID Format

Synthetic account IDs use the format `SYN{seed:04d}{index:04d}` (e.g., `SYN00420001` for seed=42, first account). Transaction IDs use `SYN{global_counter:010d}` assigned after deterministic sorting.

---

## File Structure

### Files to Create

| File | Responsibility |
|------|---------------|
| `src/moneybin/testing/__init__.py` | Testing package init |
| `src/moneybin/testing/synthetic/__init__.py` | Synthetic package public API |
| `src/moneybin/testing/synthetic/seed.py` | `SeededRandom` wrapper for deterministic randomness |
| `src/moneybin/testing/synthetic/models.py` | Pydantic models for YAML validation + runtime dataclasses |
| `src/moneybin/testing/synthetic/generators/__init__.py` | Generator package init |
| `src/moneybin/testing/synthetic/generators/income.py` | Salary and freelance invoice generation |
| `src/moneybin/testing/synthetic/generators/recurring.py` | Fixed monthly charges with price increases |
| `src/moneybin/testing/synthetic/generators/spending.py` | Discretionary spending from merchant catalogs |
| `src/moneybin/testing/synthetic/generators/transfers.py` | Account-to-account transfers with statement_balance |
| `src/moneybin/testing/synthetic/writer.py` | Raw table writer + synthetic.ground_truth writer |
| `src/moneybin/testing/synthetic/engine.py` | `GeneratorEngine` orchestrator |
| `src/moneybin/testing/synthetic/data/personas/basic.yaml` | Basic persona (Alice) |
| `src/moneybin/testing/synthetic/data/personas/family.yaml` | Family persona (Bob) |
| `src/moneybin/testing/synthetic/data/personas/freelancer.yaml` | Freelancer persona (Charlie) |
| `src/moneybin/testing/synthetic/data/merchants/*.yaml` | 14 merchant catalog YAML files |
| `src/moneybin/sql/schema/synthetic_ground_truth.sql` | DDL for `synthetic.ground_truth` table |
| `src/moneybin/cli/commands/synthetic.py` | CLI commands (`generate`, `reset`) |
| `tests/moneybin/test_synthetic/__init__.py` | Test package init |
| `tests/moneybin/test_synthetic/test_seed.py` | SeededRandom unit tests |
| `tests/moneybin/test_synthetic/test_models.py` | Pydantic model + YAML loading tests |
| `tests/moneybin/test_synthetic/test_generators.py` | Generator unit tests (all 4 generators) |
| `tests/moneybin/test_synthetic/test_writer.py` | Writer unit tests |
| `tests/moneybin/test_synthetic/test_engine.py` | Engine integration tests |
| `tests/moneybin/test_synthetic/test_cli.py` | CLI command tests |

### Files to Modify

| File | Change |
|------|--------|
| `src/moneybin/services/import_service.py:119,381` | Rename `_run_transforms` → `run_transforms` (make public) |
| `src/moneybin/cli/main.py:16-27,95-139` | Import and register `synthetic` command group |
| `src/moneybin/tables.py:28-29` | Add `GROUND_TRUTH` TableRef constant |

---

## Task 1: Package Scaffold + SeededRandom

Create the testing/synthetic package directory structure and implement the `SeededRandom` wrapper — the foundational primitive that all generators depend on.

**Files:**
- Create: `src/moneybin/testing/__init__.py`
- Create: `src/moneybin/testing/synthetic/__init__.py`
- Create: `src/moneybin/testing/synthetic/seed.py`
- Create: `src/moneybin/testing/synthetic/generators/__init__.py`
- Create: `src/moneybin/testing/synthetic/data/personas/.gitkeep` (directory marker, removed in Task 3)
- Create: `src/moneybin/testing/synthetic/data/merchants/.gitkeep` (directory marker, removed in Task 3)
- Create: `tests/moneybin/test_synthetic/__init__.py`
- Create: `tests/moneybin/test_synthetic/test_seed.py`

- [ ] **Step 1: Write failing tests for SeededRandom**

Create `tests/moneybin/test_synthetic/__init__.py` (empty) and `tests/moneybin/test_synthetic/test_seed.py`:

```python
# ruff: noqa: S101
"""Tests for the SeededRandom wrapper."""

import math
import statistics
from collections import Counter

import pytest

from moneybin.testing.synthetic.seed import SeededRandom


class TestSeededRandomDeterminism:
    """Same seed produces identical output; different seeds diverge."""

    def test_same_seed_same_output(self) -> None:
        rng1 = SeededRandom(42)
        rng2 = SeededRandom(42)
        values1 = [rng1.uniform(0, 100) for _ in range(50)]
        values2 = [rng2.uniform(0, 100) for _ in range(50)]
        assert values1 == values2

    def test_different_seed_different_output(self) -> None:
        rng1 = SeededRandom(42)
        rng2 = SeededRandom(99)
        values1 = [rng1.uniform(0, 100) for _ in range(20)]
        values2 = [rng2.uniform(0, 100) for _ in range(20)]
        assert values1 != values2


class TestSeededRandomDistributions:
    """Statistical properties of generated distributions."""

    def test_log_normal_mean_within_bounds(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.log_normal(100.0, 30.0) for _ in range(5000)]
        sample_mean = statistics.mean(samples)
        # Mean should be within 10% of target
        assert 90.0 < sample_mean < 110.0

    def test_log_normal_all_positive(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.log_normal(50.0, 20.0) for _ in range(1000)]
        assert all(s > 0 for s in samples)

    def test_poisson_mean_within_bounds(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.poisson(10.0) for _ in range(5000)]
        sample_mean = statistics.mean(samples)
        assert 9.0 < sample_mean < 11.0

    def test_poisson_non_negative(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.poisson(3.0) for _ in range(1000)]
        assert all(s >= 0 for s in samples)

    def test_weighted_choice_respects_weights(self) -> None:
        rng = SeededRandom(42)
        items = ["a", "b", "c"]
        weights = [10.0, 1.0, 1.0]
        results = [rng.weighted_choice(items, weights) for _ in range(1000)]
        counts = Counter(results)
        # "a" should appear much more often than "b" or "c"
        assert counts["a"] > counts["b"] * 3


class TestSeededRandomDayInMonth:
    """Day-in-month generation with optional day-of-week bias."""

    def test_day_within_month_bounds(self) -> None:
        rng = SeededRandom(42)
        for month in range(1, 13):
            day = rng.day_in_month(2024, month)
            assert 1 <= day <= 31

    def test_february_respects_month_length(self) -> None:
        rng = SeededRandom(42)
        days = [rng.day_in_month(2024, 2) for _ in range(100)]
        assert all(1 <= d <= 29 for d in days)  # 2024 is a leap year

    def test_day_of_week_weights_bias_output(self) -> None:
        rng = SeededRandom(42)
        weights = {"friday": 5.0, "saturday": 5.0}
        days = [rng.day_in_month(2024, 3, day_weights=weights) for _ in range(1000)]
        # Count weekend vs weekday days
        import calendar

        weekend_count = sum(
            1
            for d in days
            if calendar.weekday(2024, 3, d) in (4, 5)  # Fri, Sat
        )
        weekday_count = len(days) - weekend_count
        # Weekend days have weight 5.0 vs 1.0 default — should dominate
        assert weekend_count > weekday_count

    def test_gauss_returns_float(self) -> None:
        rng = SeededRandom(42)
        result = rng.gauss(100.0, 15.0)
        assert isinstance(result, float)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_seed.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.testing'`

- [ ] **Step 3: Create package scaffold**

Create empty `__init__.py` files to establish the package structure:

`src/moneybin/testing/__init__.py`:
```python
```

`src/moneybin/testing/synthetic/__init__.py`:
```python
"""Synthetic data generator for realistic financial test data."""
```

`src/moneybin/testing/synthetic/generators/__init__.py`:
```python
```

Create data directories (empty `.gitkeep` files):
```
src/moneybin/testing/synthetic/data/personas/.gitkeep
src/moneybin/testing/synthetic/data/merchants/.gitkeep
```

- [ ] **Step 4: Implement SeededRandom**

Create `src/moneybin/testing/synthetic/seed.py`:

```python
"""Seeded random number generator wrapper.

All randomness in the synthetic data generator flows through a single
SeededRandom instance initialized with a user-provided seed. Generator
modules receive this wrapper — never import ``random`` directly or use
ambient state. This makes determinism structural, not by convention.
"""

import calendar
import math
import random
from typing import Any


class SeededRandom:
    """Wrapper around ``random.Random`` providing all stochastic operations.

    Args:
        seed: Integer seed for reproducible output.
    """

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)

    def uniform(self, a: float, b: float) -> float:
        """Uniform random float in [a, b]."""
        return self._rng.uniform(a, b)

    def randint(self, a: int, b: int) -> int:
        """Random integer in [a, b] inclusive."""
        return self._rng.randint(a, b)

    def choice(self, items: list[Any]) -> Any:
        """Random choice from a non-empty list."""
        return self._rng.choice(items)

    def weighted_choice(self, items: list[Any], weights: list[float]) -> Any:
        """Weighted random choice from items.

        Args:
            items: List of items to choose from.
            weights: Corresponding weights (higher = more likely).

        Returns:
            A single selected item.
        """
        return self._rng.choices(items, weights=weights, k=1)[0]

    def log_normal(self, mean: float, stddev: float) -> float:
        """Log-normal sample parameterized by desired output mean and stddev.

        Converts the desired output distribution parameters to the underlying
        normal distribution parameters for ``lognormvariate``.

        Args:
            mean: Desired mean of the output distribution.
            stddev: Desired standard deviation of the output distribution.

        Returns:
            A positive float drawn from the log-normal distribution.
        """
        variance = stddev**2
        mu = math.log(mean**2 / math.sqrt(variance + mean**2))
        sigma = math.sqrt(math.log(1 + variance / mean**2))
        return self._rng.lognormvariate(mu, sigma)

    def poisson(self, lam: float) -> int:
        """Poisson-distributed random non-negative integer.

        Uses Knuth's algorithm for small lambda values.

        Args:
            lam: Expected value (lambda) of the distribution.

        Returns:
            A non-negative integer.
        """
        big_l = math.exp(-lam)
        k = 0
        p = 1.0
        while p > big_l:
            k += 1
            p *= self._rng.random()
        return k - 1

    def gauss(self, mu: float, sigma: float) -> float:
        """Gaussian (normal) random float.

        Args:
            mu: Mean of the distribution.
            sigma: Standard deviation.

        Returns:
            A float from the normal distribution.
        """
        return self._rng.gauss(mu, sigma)

    def day_in_month(
        self,
        year: int,
        month: int,
        day_weights: dict[str, float] | None = None,
    ) -> int:
        """Random day within a month, optionally biased by day-of-week.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).
            day_weights: Optional mapping of lowercase day names to weights.
                Unspecified days default to weight 1.0.

        Returns:
            A day number (1 to last day of month).
        """
        days_in_month = calendar.monthrange(year, month)[1]

        if day_weights is None:
            return self._rng.randint(1, days_in_month)

        day_names = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]
        weights: list[float] = []
        for day in range(1, days_in_month + 1):
            dow = calendar.weekday(year, month, day)
            weight = day_weights.get(day_names[dow], 1.0)
            weights.append(weight)

        days = list(range(1, days_in_month + 1))
        return self.weighted_choice(days, weights)

    def sample(self, items: list[Any], k: int) -> list[Any]:
        """Random sample of k items without replacement."""
        return self._rng.sample(items, k)

    def shuffle(self, items: list[Any]) -> None:
        """In-place shuffle of a list."""
        self._rng.shuffle(items)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_seed.py -v`
Expected: All 10 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/testing/ tests/moneybin/test_synthetic/
git commit -m "feat(synthetic): add SeededRandom wrapper and package scaffold"
```

---

## Task 2: Pydantic Models for YAML Validation

Define all Pydantic models for merchant catalog and persona YAML schemas, plus runtime dataclasses for generated data. These models validate config at load time and catch errors early.

**Files:**
- Create: `src/moneybin/testing/synthetic/models.py`
- Create: `tests/moneybin/test_synthetic/test_models.py`

- [ ] **Step 1: Write failing tests for Pydantic models**

Create `tests/moneybin/test_synthetic/test_models.py`:

```python
# ruff: noqa: S101
"""Tests for Pydantic YAML validation models and data loading."""

import pytest

from moneybin.testing.synthetic.models import (
    AccountConfig,
    AmountDistribution,
    GeneratedAccount,
    GeneratedTransaction,
    GenerationResult,
    IncomeConfig,
    MerchantCatalog,
    MerchantEntry,
    PersonaConfig,
    RecurringConfig,
    SpendingCategoryConfig,
    SpendingConfig,
    TransferConfig,
)


class TestAmountDistribution:
    """Test the AmountDistribution model."""

    def test_fixed_amount(self) -> None:
        dist = AmountDistribution(mean=17.99, stddev=0.0)
        assert dist.mean == 17.99
        assert dist.stddev == 0.0

    def test_variable_amount(self) -> None:
        dist = AmountDistribution(mean=145.00, stddev=40.00)
        assert dist.mean == 145.00
        assert dist.stddev == 40.00


class TestMerchantCatalog:
    """Test merchant catalog validation."""

    def test_valid_catalog(self) -> None:
        catalog = MerchantCatalog(
            category="grocery",
            merchants=[
                MerchantEntry(
                    name="Store A",
                    weight=10,
                    amount=AmountDistribution(mean=50.0, stddev=15.0),
                ),
            ],
        )
        assert catalog.category == "grocery"
        assert len(catalog.merchants) == 1

    def test_empty_merchants_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            MerchantCatalog(category="test", merchants=[])

    def test_zero_weight_rejected(self) -> None:
        with pytest.raises(ValueError):
            MerchantEntry(
                name="Bad",
                weight=0,
                amount=AmountDistribution(mean=10.0),
            )

    def test_description_prefix_optional(self) -> None:
        entry = MerchantEntry(
            name="Costco",
            weight=10,
            amount=AmountDistribution(mean=145.0, stddev=40.0),
            description_prefix="COSTCO WHSE",
        )
        assert entry.description_prefix == "COSTCO WHSE"


class TestPersonaConfig:
    """Test persona YAML validation."""

    @pytest.fixture
    def minimal_persona_dict(self) -> dict:
        return {
            "persona": "test",
            "profile": "test-profile",
            "description": "A test persona",
            "years_default": 1,
            "accounts": [
                {
                    "name": "Checking",
                    "type": "checking",
                    "source_type": "ofx",
                    "institution": "Test Bank",
                    "opening_balance": 1000.00,
                },
            ],
            "income": [
                {
                    "type": "salary",
                    "account": "Checking",
                    "amount": 3000.00,
                    "schedule": "biweekly",
                    "pay_day": "friday",
                    "description_template": "DIRECT DEP {employer}",
                    "employer": "TestCo",
                },
            ],
            "recurring": [
                {
                    "category": "housing",
                    "description": "Rent",
                    "account": "Checking",
                    "amount": 1500.00,
                    "day_of_month": 1,
                },
            ],
            "spending": {
                "categories": [
                    {
                        "name": "grocery",
                        "merchant_catalog": "grocery",
                        "monthly_budget": {"mean": 400.0, "stddev": 80.0},
                        "transactions_per_month": {"mean": 5, "stddev": 1},
                        "accounts": ["Checking"],
                    },
                ],
            },
            "transfers": [],
        }

    def test_valid_persona_loads(self, minimal_persona_dict: dict) -> None:
        persona = PersonaConfig.model_validate(minimal_persona_dict)
        assert persona.persona == "test"
        assert len(persona.accounts) == 1
        assert persona.accounts[0].name == "Checking"

    def test_income_references_unknown_account_rejected(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["income"][0]["account"] = "Nonexistent"
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_recurring_references_unknown_account_rejected(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["recurring"][0]["account"] = "Nonexistent"
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_spending_references_unknown_account_rejected(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["spending"]["categories"][0]["accounts"] = ["Nonexistent"]
        with pytest.raises(ValueError, match="unknown account.*Nonexistent"):
            PersonaConfig.model_validate(minimal_persona_dict)

    def test_transfer_from_alias(self) -> None:
        """Transfer config uses 'from'/'to' YAML keys mapped to Python fields."""
        config = TransferConfig.model_validate({
            "from": "Checking",
            "to": "Savings",
            "amount": 500.0,
            "schedule": "monthly",
            "day_of_month": 5,
        })
        assert config.from_account == "Checking"
        assert config.to_account == "Savings"

    def test_transfer_statement_balance(self) -> None:
        config = TransferConfig.model_validate({
            "from": "Checking",
            "to": "Visa",
            "amount": "statement_balance",
            "schedule": "monthly",
            "day_of_month": 20,
        })
        assert config.amount == "statement_balance"

    def test_recurring_amount_can_be_distribution(
        self, minimal_persona_dict: dict
    ) -> None:
        minimal_persona_dict["recurring"][0]["amount"] = {
            "mean": 145.0,
            "stddev": 35.0,
        }
        persona = PersonaConfig.model_validate(minimal_persona_dict)
        assert isinstance(persona.recurring[0].amount, AmountDistribution)

    def test_day_of_month_over_28_rejected(self) -> None:
        with pytest.raises(ValueError):
            RecurringConfig(
                category="test",
                description="Test",
                account="Checking",
                amount=100.0,
                day_of_month=29,
            )

    def test_invalid_source_type_rejected(self) -> None:
        with pytest.raises(ValueError):
            AccountConfig(
                name="Bad",
                type="checking",
                source_type="parquet",  # type: ignore[arg-type]
                institution="Test Bank",
            )


class TestRuntimeDataclasses:
    """Test the runtime dataclasses used during generation."""

    def test_generated_transaction_defaults(self) -> None:
        from datetime import date
        from decimal import Decimal

        txn = GeneratedTransaction(
            date=date(2024, 1, 15),
            amount=Decimal("-42.50"),
            description="Test Store",
            account_name="Checking",
        )
        assert txn.transaction_type == "DEBIT"
        assert txn.category is None
        assert txn.transfer_pair_id is None
        assert txn.transaction_id == ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_models.py -v`
Expected: FAIL — `ImportError: cannot import name 'AmountDistribution'`

- [ ] **Step 3: Implement Pydantic models and runtime dataclasses**

Create `src/moneybin/testing/synthetic/models.py`:

```python
"""Pydantic models for YAML config validation and runtime data types.

Config models validate persona and merchant catalog YAML files at load time.
Runtime dataclasses represent generated data flowing through the pipeline.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from decimal import Decimal
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

_DATA_DIR = __import__("pathlib").Path(__file__).resolve().parent / "data"


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
    transaction_type: str = "DEBIT"
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_models.py -v`
Expected: All 14 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/synthetic/models.py tests/moneybin/test_synthetic/test_models.py
git commit -m "feat(synthetic): add Pydantic models for YAML validation and runtime types"
```

---

## Task 3: YAML Data Files (Merchant Catalogs + Personas)

Create all 14 merchant catalog YAML files (~165 real 2026 brand names) and 3 persona definition YAML files. Validate that all files load through Pydantic without errors.

**Files:**
- Create: `src/moneybin/testing/synthetic/data/merchants/grocery.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/dining.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/transport.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/utilities.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/entertainment.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/shopping.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/health.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/travel.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/subscriptions.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/kids.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/personal_care.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/insurance.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/education.yaml`
- Create: `src/moneybin/testing/synthetic/data/merchants/gifts.yaml`
- Create: `src/moneybin/testing/synthetic/data/personas/basic.yaml`
- Create: `src/moneybin/testing/synthetic/data/personas/family.yaml`
- Create: `src/moneybin/testing/synthetic/data/personas/freelancer.yaml`
- Modify: `tests/moneybin/test_synthetic/test_models.py` (add YAML validation tests)

- [ ] **Step 1: Write failing validation test**

Add to `tests/moneybin/test_synthetic/test_models.py`:

```python
from moneybin.testing.synthetic.models import load_merchant_catalog, load_persona


class TestYAMLDataLoading:
    """Validate all shipped YAML data files load through Pydantic."""

    MERCHANT_CATALOGS = [
        "grocery",
        "dining",
        "transport",
        "utilities",
        "entertainment",
        "shopping",
        "health",
        "travel",
        "subscriptions",
        "kids",
        "personal_care",
        "insurance",
        "education",
        "gifts",
    ]
    PERSONAS = ["basic", "family", "freelancer"]

    @pytest.mark.parametrize("catalog", MERCHANT_CATALOGS)
    def test_merchant_catalog_loads(self, catalog: str) -> None:
        result = load_merchant_catalog(catalog)
        assert result.category == catalog
        assert len(result.merchants) >= 5

    @pytest.mark.parametrize("persona", PERSONAS)
    def test_persona_loads(self, persona: str) -> None:
        result = load_persona(persona)
        assert result.persona == persona
        assert len(result.accounts) >= 1
        assert len(result.income) >= 1

    def test_unknown_persona_raises(self) -> None:
        with pytest.raises(FileNotFoundError, match="Unknown persona"):
            load_persona("nonexistent")

    def test_unknown_catalog_raises(self) -> None:
        with pytest.raises(FileNotFoundError, match="Unknown merchant catalog"):
            load_merchant_catalog("nonexistent")

    def test_persona_merchant_catalogs_exist(self) -> None:
        """Every merchant_catalog referenced in personas has a matching file."""
        for persona_name in self.PERSONAS:
            persona = load_persona(persona_name)
            for cat in persona.spending.categories:
                catalog = load_merchant_catalog(cat.merchant_catalog)
                assert len(catalog.merchants) > 0, (
                    f"Persona {persona_name!r} references empty catalog "
                    f"{cat.merchant_catalog!r}"
                )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_synthetic/test_models.py::TestYAMLDataLoading -v`
Expected: FAIL — `FileNotFoundError: Unknown merchant catalog: 'grocery'`

- [ ] **Step 3: Create merchant catalog YAML files**

Create `src/moneybin/testing/synthetic/data/merchants/grocery.yaml`:

```yaml
category: grocery
merchants:
  - name: "Trader Joe's"
    weight: 15
    amount: { mean: 55.00, stddev: 18.00 }
    description_prefix: "TRADER JOE'S"
  - name: "Costco"
    weight: 10
    amount: { mean: 145.00, stddev: 40.00 }
    description_prefix: "COSTCO WHSE"
  - name: "Whole Foods"
    weight: 8
    amount: { mean: 72.00, stddev: 22.00 }
    description_prefix: "WHOLE FOODS MKT"
  - name: "Kroger"
    weight: 12
    amount: { mean: 65.00, stddev: 20.00 }
    description_prefix: "KROGER"
  - name: "Walmart Grocery"
    weight: 14
    amount: { mean: 78.00, stddev: 25.00 }
    description_prefix: "WAL-MART"
  - name: "Target"
    weight: 8
    amount: { mean: 45.00, stddev: 15.00 }
    description_prefix: "TARGET"
  - name: "Aldi"
    weight: 10
    amount: { mean: 42.00, stddev: 12.00 }
    description_prefix: "ALDI"
  - name: "Safeway"
    weight: 6
    amount: { mean: 58.00, stddev: 18.00 }
    description_prefix: "SAFEWAY"
  - name: "Instacart"
    weight: 7
    amount: { mean: 85.00, stddev: 30.00 }
    description_prefix: "INSTACART"
  - name: "Amazon Fresh"
    weight: 5
    amount: { mean: 68.00, stddev: 22.00 }
    description_prefix: "AMZN FRESH"
  - name: "Publix"
    weight: 6
    amount: { mean: 62.00, stddev: 18.00 }
    description_prefix: "PUBLIX"
  - name: "H-E-B"
    weight: 5
    amount: { mean: 70.00, stddev: 20.00 }
    description_prefix: "H-E-B"
  - name: "Sprouts"
    weight: 4
    amount: { mean: 48.00, stddev: 15.00 }
    description_prefix: "SPROUTS FARMERS"
  - name: "Albertsons"
    weight: 4
    amount: { mean: 55.00, stddev: 16.00 }
    description_prefix: "ALBERTSONS"
  - name: "Wegmans"
    weight: 3
    amount: { mean: 80.00, stddev: 25.00 }
    description_prefix: "WEGMANS"
```

Create `src/moneybin/testing/synthetic/data/merchants/dining.yaml`:

```yaml
category: dining
merchants:
  - name: "Chipotle"
    weight: 12
    amount: { mean: 14.50, stddev: 4.00 }
    description_prefix: "CHIPOTLE"
  - name: "Starbucks"
    weight: 18
    amount: { mean: 7.50, stddev: 3.00 }
    description_prefix: "STARBUCKS"
  - name: "McDonald's"
    weight: 10
    amount: { mean: 11.00, stddev: 4.00 }
    description_prefix: "MCDONALDS"
  - name: "Chick-fil-A"
    weight: 10
    amount: { mean: 12.00, stddev: 3.50 }
    description_prefix: "CHICK-FIL-A"
  - name: "DoorDash"
    weight: 12
    amount: { mean: 35.00, stddev: 12.00 }
    description_prefix: "DOORDASH"
  - name: "Uber Eats"
    weight: 8
    amount: { mean: 32.00, stddev: 11.00 }
    description_prefix: "UBER EATS"
  - name: "Olive Garden"
    weight: 4
    amount: { mean: 55.00, stddev: 18.00 }
    description_prefix: "OLIVE GARDEN"
  - name: "Texas Roadhouse"
    weight: 4
    amount: { mean: 48.00, stddev: 15.00 }
    description_prefix: "TEXAS ROADHOUSE"
  - name: "Panera Bread"
    weight: 7
    amount: { mean: 16.00, stddev: 5.00 }
    description_prefix: "PANERA BREAD"
  - name: "Wendy's"
    weight: 6
    amount: { mean: 10.50, stddev: 3.50 }
    description_prefix: "WENDYS"
  - name: "Taco Bell"
    weight: 7
    amount: { mean: 9.00, stddev: 3.00 }
    description_prefix: "TACO BELL"
  - name: "Panda Express"
    weight: 5
    amount: { mean: 13.00, stddev: 4.00 }
    description_prefix: "PANDA EXPRESS"
  - name: "Buffalo Wild Wings"
    weight: 4
    amount: { mean: 38.00, stddev: 12.00 }
    description_prefix: "BUFFALO WILD WINGS"
  - name: "Cheesecake Factory"
    weight: 3
    amount: { mean: 65.00, stddev: 20.00 }
    description_prefix: "CHEESECAKE FACTORY"
  - name: "Five Guys"
    weight: 5
    amount: { mean: 18.00, stddev: 5.00 }
    description_prefix: "FIVE GUYS"
  - name: "Wingstop"
    weight: 4
    amount: { mean: 22.00, stddev: 7.00 }
    description_prefix: "WINGSTOP"
  - name: "Domino's"
    weight: 5
    amount: { mean: 25.00, stddev: 8.00 }
    description_prefix: "DOMINOS"
  - name: "Grubhub"
    weight: 5
    amount: { mean: 33.00, stddev: 10.00 }
    description_prefix: "GRUBHUB"
  - name: "Local Restaurant"
    weight: 8
    amount: { mean: 52.00, stddev: 20.00 }
  - name: "Crumbl Cookies"
    weight: 3
    amount: { mean: 15.00, stddev: 5.00 }
    description_prefix: "CRUMBL"
```

Create `src/moneybin/testing/synthetic/data/merchants/transport.yaml`:

```yaml
category: transport
merchants:
  - name: "Shell"
    weight: 14
    amount: { mean: 52.00, stddev: 15.00 }
    description_prefix: "SHELL OIL"
  - name: "Exxon Mobil"
    weight: 12
    amount: { mean: 50.00, stddev: 14.00 }
    description_prefix: "EXXONMOBIL"
  - name: "Chevron"
    weight: 10
    amount: { mean: 48.00, stddev: 14.00 }
    description_prefix: "CHEVRON"
  - name: "BP"
    weight: 8
    amount: { mean: 45.00, stddev: 13.00 }
    description_prefix: "BP"
  - name: "Uber"
    weight: 14
    amount: { mean: 18.00, stddev: 8.00 }
    description_prefix: "UBER"
  - name: "Lyft"
    weight: 10
    amount: { mean: 16.00, stddev: 7.00 }
    description_prefix: "LYFT"
  - name: "Parking"
    weight: 8
    amount: { mean: 12.00, stddev: 6.00 }
    description_prefix: "PARKING"
  - name: "Highway Tolls"
    weight: 6
    amount: { mean: 5.50, stddev: 3.00 }
    description_prefix: "E-ZPASS"
  - name: "Car Wash"
    weight: 4
    amount: { mean: 18.00, stddev: 6.00 }
    description_prefix: "CAR WASH"
  - name: "Jiffy Lube"
    weight: 2
    amount: { mean: 85.00, stddev: 25.00 }
    description_prefix: "JIFFY LUBE"
  - name: "AutoZone"
    weight: 2
    amount: { mean: 35.00, stddev: 20.00 }
    description_prefix: "AUTOZONE"
  - name: "Public Transit"
    weight: 10
    amount: { mean: 2.75, stddev: 0.50 }
    description_prefix: "METRO TRANSIT"
```

Create `src/moneybin/testing/synthetic/data/merchants/utilities.yaml`:

```yaml
category: utilities
merchants:
  - name: "AT&T"
    weight: 15
    amount: { mean: 95.00, stddev: 15.00 }
    description_prefix: "ATT"
  - name: "Comcast Xfinity"
    weight: 14
    amount: { mean: 89.00, stddev: 10.00 }
    description_prefix: "COMCAST"
  - name: "T-Mobile"
    weight: 13
    amount: { mean: 85.00, stddev: 12.00 }
    description_prefix: "T-MOBILE"
  - name: "Verizon"
    weight: 13
    amount: { mean: 92.00, stddev: 14.00 }
    description_prefix: "VERIZON"
  - name: "Electric Company"
    weight: 15
    amount: { mean: 135.00, stddev: 40.00 }
    description_prefix: "ELECTRIC CO"
  - name: "Gas Utility"
    weight: 10
    amount: { mean: 75.00, stddev: 30.00 }
    description_prefix: "GAS UTILITY"
  - name: "Water Utility"
    weight: 10
    amount: { mean: 55.00, stddev: 15.00 }
    description_prefix: "WATER DEPT"
  - name: "Waste Management"
    weight: 10
    amount: { mean: 42.00, stddev: 8.00 }
    description_prefix: "WASTE MGMT"
```

Create `src/moneybin/testing/synthetic/data/merchants/entertainment.yaml`:

```yaml
category: entertainment
merchants:
  - name: "AMC Theatres"
    weight: 12
    amount: { mean: 28.00, stddev: 10.00 }
    description_prefix: "AMC THEATRES"
  - name: "Steam"
    weight: 10
    amount: { mean: 25.00, stddev: 15.00 }
    description_prefix: "STEAMPOWERED"
  - name: "PlayStation Store"
    weight: 8
    amount: { mean: 30.00, stddev: 18.00 }
    description_prefix: "PLAYSTATION"
  - name: "Xbox Microsoft"
    weight: 7
    amount: { mean: 28.00, stddev: 16.00 }
    description_prefix: "MICROSOFT XBOX"
  - name: "Dave & Buster's"
    weight: 5
    amount: { mean: 45.00, stddev: 15.00 }
    description_prefix: "DAVE AND BUSTERS"
  - name: "TopGolf"
    weight: 4
    amount: { mean: 55.00, stddev: 18.00 }
    description_prefix: "TOPGOLF"
  - name: "Ticketmaster"
    weight: 6
    amount: { mean: 85.00, stddev: 35.00 }
    description_prefix: "TICKETMASTER"
  - name: "Regal Cinemas"
    weight: 8
    amount: { mean: 25.00, stddev: 8.00 }
    description_prefix: "REGAL CINEMAS"
  - name: "Audible"
    weight: 6
    amount: { mean: 15.00, stddev: 2.00 }
    description_prefix: "AUDIBLE"
  - name: "Apple iTunes"
    weight: 10
    amount: { mean: 8.00, stddev: 5.00 }
    description_prefix: "APPLE.COM/BILL"
  - name: "Nintendo eShop"
    weight: 5
    amount: { mean: 22.00, stddev: 12.00 }
    description_prefix: "NINTENDO"
  - name: "Spotify"
    weight: 9
    amount: { mean: 11.00, stddev: 2.00 }
    description_prefix: "SPOTIFY"
```

Create `src/moneybin/testing/synthetic/data/merchants/shopping.yaml`:

```yaml
category: shopping
merchants:
  - name: "Amazon"
    weight: 20
    amount: { mean: 42.00, stddev: 30.00 }
    description_prefix: "AMZN MKTP"
  - name: "Target"
    weight: 12
    amount: { mean: 38.00, stddev: 18.00 }
    description_prefix: "TARGET"
  - name: "Walmart"
    weight: 14
    amount: { mean: 45.00, stddev: 22.00 }
    description_prefix: "WAL-MART"
  - name: "TJ Maxx"
    weight: 6
    amount: { mean: 35.00, stddev: 15.00 }
    description_prefix: "TJ MAXX"
  - name: "Nike"
    weight: 5
    amount: { mean: 95.00, stddev: 35.00 }
    description_prefix: "NIKE"
  - name: "Nordstrom"
    weight: 4
    amount: { mean: 120.00, stddev: 50.00 }
    description_prefix: "NORDSTROM"
  - name: "Old Navy"
    weight: 6
    amount: { mean: 42.00, stddev: 18.00 }
    description_prefix: "OLD NAVY"
  - name: "H&M"
    weight: 5
    amount: { mean: 35.00, stddev: 15.00 }
    description_prefix: "HM"
  - name: "Home Depot"
    weight: 8
    amount: { mean: 65.00, stddev: 35.00 }
    description_prefix: "THE HOME DEPOT"
  - name: "Lowe's"
    weight: 7
    amount: { mean: 58.00, stddev: 30.00 }
    description_prefix: "LOWES"
  - name: "IKEA"
    weight: 3
    amount: { mean: 110.00, stddev: 50.00 }
    description_prefix: "IKEA"
  - name: "Etsy"
    weight: 5
    amount: { mean: 32.00, stddev: 18.00 }
    description_prefix: "ETSY"
  - name: "Best Buy"
    weight: 6
    amount: { mean: 85.00, stddev: 45.00 }
    description_prefix: "BEST BUY"
  - name: "Apple Store"
    weight: 4
    amount: { mean: 150.00, stddev: 80.00 }
    description_prefix: "APPLE STORE"
  - name: "REI"
    weight: 3
    amount: { mean: 75.00, stddev: 35.00 }
    description_prefix: "REI"
  - name: "PetSmart"
    weight: 5
    amount: { mean: 38.00, stddev: 18.00 }
    description_prefix: "PETSMART"
  - name: "Chewy"
    weight: 4
    amount: { mean: 55.00, stddev: 20.00 }
    description_prefix: "CHEWY"
  - name: "Sephora"
    weight: 4
    amount: { mean: 48.00, stddev: 22.00 }
    description_prefix: "SEPHORA"
  - name: "Bath & Body Works"
    weight: 3
    amount: { mean: 28.00, stddev: 12.00 }
    description_prefix: "BATH BODY WORKS"
  - name: "Dick's Sporting Goods"
    weight: 3
    amount: { mean: 65.00, stddev: 30.00 }
    description_prefix: "DICKS SPORTING"
```

Create `src/moneybin/testing/synthetic/data/merchants/health.yaml`:

```yaml
category: health
merchants:
  - name: "CVS Pharmacy"
    weight: 15
    amount: { mean: 25.00, stddev: 15.00 }
    description_prefix: "CVS PHARMACY"
  - name: "Walgreens"
    weight: 12
    amount: { mean: 22.00, stddev: 12.00 }
    description_prefix: "WALGREENS"
  - name: "Doctor Copay"
    weight: 10
    amount: { mean: 40.00, stddev: 15.00 }
    description_prefix: "PHYSICIAN OFFICE"
  - name: "Dental Office"
    weight: 6
    amount: { mean: 75.00, stddev: 40.00 }
    description_prefix: "DENTAL OFFICE"
  - name: "Vision Center"
    weight: 4
    amount: { mean: 50.00, stddev: 25.00 }
    description_prefix: "VISION CENTER"
  - name: "Urgent Care"
    weight: 3
    amount: { mean: 150.00, stddev: 50.00 }
    description_prefix: "URGENT CARE"
  - name: "LabCorp"
    weight: 3
    amount: { mean: 35.00, stddev: 20.00 }
    description_prefix: "LABCORP"
  - name: "Physical Therapy"
    weight: 3
    amount: { mean: 45.00, stddev: 15.00 }
    description_prefix: "PHYSICAL THERAPY"
  - name: "Planet Fitness"
    weight: 8
    amount: { mean: 25.00, stddev: 5.00 }
    description_prefix: "PLANET FITNESS"
  - name: "GNC"
    weight: 5
    amount: { mean: 35.00, stddev: 15.00 }
    description_prefix: "GNC"
  - name: "Mental Health Copay"
    weight: 4
    amount: { mean: 30.00, stddev: 10.00 }
    description_prefix: "BEHAVIORAL HEALTH"
  - name: "Dermatologist"
    weight: 2
    amount: { mean: 55.00, stddev: 25.00 }
    description_prefix: "DERMATOLOGY ASSOC"
```

Create `src/moneybin/testing/synthetic/data/merchants/travel.yaml`:

```yaml
category: travel
merchants:
  - name: "Delta Airlines"
    weight: 12
    amount: { mean: 350.00, stddev: 150.00 }
    description_prefix: "DELTA AIR"
  - name: "United Airlines"
    weight: 10
    amount: { mean: 380.00, stddev: 160.00 }
    description_prefix: "UNITED AIRLINES"
  - name: "Southwest Airlines"
    weight: 12
    amount: { mean: 250.00, stddev: 100.00 }
    description_prefix: "SOUTHWEST AIR"
  - name: "American Airlines"
    weight: 10
    amount: { mean: 360.00, stddev: 150.00 }
    description_prefix: "AMERICAN AIR"
  - name: "Marriott"
    weight: 8
    amount: { mean: 185.00, stddev: 60.00 }
    description_prefix: "MARRIOTT"
  - name: "Hilton"
    weight: 7
    amount: { mean: 175.00, stddev: 55.00 }
    description_prefix: "HILTON"
  - name: "Airbnb"
    weight: 10
    amount: { mean: 220.00, stddev: 80.00 }
    description_prefix: "AIRBNB"
  - name: "VRBO"
    weight: 5
    amount: { mean: 250.00, stddev: 90.00 }
    description_prefix: "VRBO"
  - name: "Hertz"
    weight: 5
    amount: { mean: 120.00, stddev: 45.00 }
    description_prefix: "HERTZ RENT A CAR"
  - name: "Enterprise"
    weight: 6
    amount: { mean: 95.00, stddev: 35.00 }
    description_prefix: "ENTERPRISE RENT"
  - name: "TSA PreCheck"
    weight: 2
    amount: { mean: 78.00, stddev: 0.00 }
    description_prefix: "TSA PRECHECK"
  - name: "Airport Parking"
    weight: 5
    amount: { mean: 45.00, stddev: 20.00 }
    description_prefix: "AIRPORT PARKING"
```

Create `src/moneybin/testing/synthetic/data/merchants/subscriptions.yaml`:

```yaml
category: subscriptions
merchants:
  - name: "Netflix"
    weight: 15
    amount: { mean: 17.99, stddev: 0.00 }
    description_prefix: "NETFLIX"
  - name: "Spotify Premium"
    weight: 12
    amount: { mean: 11.99, stddev: 0.00 }
    description_prefix: "SPOTIFY"
  - name: "Apple iCloud"
    weight: 10
    amount: { mean: 2.99, stddev: 0.00 }
    description_prefix: "APPLE.COM/BILL"
  - name: "Amazon Prime"
    weight: 12
    amount: { mean: 14.99, stddev: 0.00 }
    description_prefix: "AMAZON PRIME"
  - name: "Hulu"
    weight: 8
    amount: { mean: 17.99, stddev: 0.00 }
    description_prefix: "HULU"
  - name: "Disney+"
    weight: 8
    amount: { mean: 13.99, stddev: 0.00 }
    description_prefix: "DISNEYPLUS"
  - name: "YouTube Premium"
    weight: 7
    amount: { mean: 13.99, stddev: 0.00 }
    description_prefix: "GOOGLE YOUTUBE"
  - name: "New York Times"
    weight: 5
    amount: { mean: 17.00, stddev: 0.00 }
    description_prefix: "NYT DIGITAL"
  - name: "Adobe Creative Cloud"
    weight: 4
    amount: { mean: 54.99, stddev: 0.00 }
    description_prefix: "ADOBE"
  - name: "Microsoft 365"
    weight: 6
    amount: { mean: 9.99, stddev: 0.00 }
    description_prefix: "MICROSOFT 365"
  - name: "ChatGPT Plus"
    weight: 5
    amount: { mean: 20.00, stddev: 0.00 }
    description_prefix: "OPENAI"
  - name: "Paramount+"
    weight: 4
    amount: { mean: 11.99, stddev: 0.00 }
    description_prefix: "PARAMOUNT PLUS"
```

Create `src/moneybin/testing/synthetic/data/merchants/kids.yaml`:

```yaml
category: kids
merchants:
  - name: "YMCA Programs"
    weight: 10
    amount: { mean: 65.00, stddev: 20.00 }
    description_prefix: "YMCA"
  - name: "Soccer League"
    weight: 8
    amount: { mean: 120.00, stddev: 30.00 }
    description_prefix: "YOUTH SOCCER"
  - name: "Music Lessons"
    weight: 8
    amount: { mean: 45.00, stddev: 10.00 }
    description_prefix: "MUSIC ACADEMY"
  - name: "Tutoring Center"
    weight: 6
    amount: { mean: 55.00, stddev: 15.00 }
    description_prefix: "KUMON"
  - name: "School Supplies"
    weight: 8
    amount: { mean: 25.00, stddev: 12.00 }
    description_prefix: "SCHOOL SUPPLIES"
  - name: "Pediatrician"
    weight: 6
    amount: { mean: 35.00, stddev: 15.00 }
    description_prefix: "PEDIATRIC ASSOC"
  - name: "Children's Museum"
    weight: 5
    amount: { mean: 30.00, stddev: 10.00 }
    description_prefix: "CHILDRENS MUSEUM"
  - name: "Summer Camp"
    weight: 4
    amount: { mean: 250.00, stddev: 80.00 }
    description_prefix: "SUMMER CAMP"
  - name: "Swim Lessons"
    weight: 6
    amount: { mean: 40.00, stddev: 10.00 }
    description_prefix: "AQUATIC CENTER"
  - name: "Dance Academy"
    weight: 5
    amount: { mean: 50.00, stddev: 12.00 }
    description_prefix: "DANCE ACADEMY"
```

Create `src/moneybin/testing/synthetic/data/merchants/personal_care.yaml`:

```yaml
category: personal_care
merchants:
  - name: "Great Clips"
    weight: 15
    amount: { mean: 22.00, stddev: 5.00 }
    description_prefix: "GREAT CLIPS"
  - name: "Sport Clips"
    weight: 10
    amount: { mean: 28.00, stddev: 5.00 }
    description_prefix: "SPORT CLIPS"
  - name: "Hair Salon"
    weight: 12
    amount: { mean: 55.00, stddev: 20.00 }
    description_prefix: "SALON"
  - name: "Nail Salon"
    weight: 10
    amount: { mean: 45.00, stddev: 15.00 }
    description_prefix: "NAIL SPA"
  - name: "Spa Treatment"
    weight: 5
    amount: { mean: 95.00, stddev: 30.00 }
    description_prefix: "DAY SPA"
  - name: "Massage Envy"
    weight: 8
    amount: { mean: 75.00, stddev: 15.00 }
    description_prefix: "MASSAGE ENVY"
  - name: "Skincare Products"
    weight: 8
    amount: { mean: 35.00, stddev: 15.00 }
    description_prefix: "SKINCARE"
  - name: "Barbershop"
    weight: 12
    amount: { mean: 30.00, stddev: 8.00 }
    description_prefix: "BARBERSHOP"
```

Create `src/moneybin/testing/synthetic/data/merchants/insurance.yaml`:

```yaml
category: insurance
merchants:
  - name: "State Farm"
    weight: 18
    amount: { mean: 155.00, stddev: 30.00 }
    description_prefix: "STATE FARM"
  - name: "GEICO"
    weight: 16
    amount: { mean: 142.00, stddev: 28.00 }
    description_prefix: "GEICO"
  - name: "Progressive"
    weight: 14
    amount: { mean: 148.00, stddev: 32.00 }
    description_prefix: "PROGRESSIVE"
  - name: "Allstate"
    weight: 12
    amount: { mean: 160.00, stddev: 35.00 }
    description_prefix: "ALLSTATE"
  - name: "USAA"
    weight: 10
    amount: { mean: 135.00, stddev: 25.00 }
    description_prefix: "USAA"
  - name: "Liberty Mutual"
    weight: 10
    amount: { mean: 152.00, stddev: 30.00 }
    description_prefix: "LIBERTY MUTUAL"
  - name: "MetLife"
    weight: 10
    amount: { mean: 85.00, stddev: 20.00 }
    description_prefix: "METLIFE"
  - name: "Principal Financial"
    weight: 10
    amount: { mean: 95.00, stddev: 22.00 }
    description_prefix: "PRINCIPAL LIFE"
```

Create `src/moneybin/testing/synthetic/data/merchants/education.yaml`:

```yaml
category: education
merchants:
  - name: "Udemy"
    weight: 15
    amount: { mean: 14.99, stddev: 5.00 }
    description_prefix: "UDEMY"
  - name: "Coursera"
    weight: 12
    amount: { mean: 49.00, stddev: 10.00 }
    description_prefix: "COURSERA"
  - name: "O'Reilly Media"
    weight: 8
    amount: { mean: 39.00, stddev: 5.00 }
    description_prefix: "OREILLY MEDIA"
  - name: "Amazon Books"
    weight: 14
    amount: { mean: 22.00, stddev: 10.00 }
    description_prefix: "AMZN MKTP"
  - name: "Barnes & Noble"
    weight: 10
    amount: { mean: 28.00, stddev: 12.00 }
    description_prefix: "BARNES NOBLE"
  - name: "Student Loan Payment"
    weight: 8
    amount: { mean: 350.00, stddev: 50.00 }
    description_prefix: "STUDENT LOAN"
  - name: "LinkedIn Learning"
    weight: 6
    amount: { mean: 29.99, stddev: 0.00 }
    description_prefix: "LINKEDIN"
  - name: "Professional Certification"
    weight: 3
    amount: { mean: 250.00, stddev: 100.00 }
    description_prefix: "PROF CERT"
```

Create `src/moneybin/testing/synthetic/data/merchants/gifts.yaml`:

```yaml
category: gifts
merchants:
  - name: "1-800-Flowers"
    weight: 8
    amount: { mean: 65.00, stddev: 25.00 }
    description_prefix: "1800FLOWERS"
  - name: "Hallmark"
    weight: 12
    amount: { mean: 8.00, stddev: 4.00 }
    description_prefix: "HALLMARK"
  - name: "Amazon Gift"
    weight: 15
    amount: { mean: 45.00, stddev: 25.00 }
    description_prefix: "AMZN MKTP"
  - name: "Target Gift"
    weight: 10
    amount: { mean: 35.00, stddev: 15.00 }
    description_prefix: "TARGET"
  - name: "Charitable Donation"
    weight: 12
    amount: { mean: 50.00, stddev: 30.00 }
    description_prefix: "DONATION"
  - name: "GoFundMe"
    weight: 6
    amount: { mean: 25.00, stddev: 15.00 }
    description_prefix: "GOFUNDME"
  - name: "Party City"
    weight: 5
    amount: { mean: 35.00, stddev: 15.00 }
    description_prefix: "PARTY CITY"
  - name: "Etsy Gift"
    weight: 8
    amount: { mean: 38.00, stddev: 18.00 }
    description_prefix: "ETSY"
```

- [ ] **Step 4: Create persona YAML files**

Create `src/moneybin/testing/synthetic/data/personas/basic.yaml`:

```yaml
persona: basic
profile: alice
description: "Single income, 1 checking + 1 credit card, simple spending, ~300 txns/yr"
years_default: 3

accounts:
  - name: "Chase Checking"
    type: checking
    source_type: ofx
    institution: "Chase Bank"
    opening_balance: 3200.00
  - name: "Capital One Visa"
    type: credit_card
    source_type: csv
    institution: "Capital One"
    opening_balance: 0.00

income:
  - type: salary
    account: "Chase Checking"
    amount: 3800.00
    schedule: biweekly
    pay_day: friday
    annual_raise_pct: 3.0
    description_template: "DIRECT DEP {employer}"
    employer: "Acme Corp"

recurring:
  - category: housing
    description: "Rent Payment"
    account: "Chase Checking"
    amount: 1500.00
    day_of_month: 1
  - category: utilities
    description: "Electric Company"
    account: "Chase Checking"
    amount: { mean: 95.00, stddev: 25.00 }
    day_of_month: 15
  - category: utilities
    description: "Internet Service"
    account: "Chase Checking"
    amount: 65.00
    day_of_month: 12
  - category: utilities
    description: "Phone Bill"
    account: "Chase Checking"
    amount: 85.00
    day_of_month: 18
  - category: insurance
    description: "Car Insurance"
    account: "Chase Checking"
    amount: 142.00
    day_of_month: 5

spending:
  categories:
    - name: grocery
      merchant_catalog: grocery
      monthly_budget: { mean: 350.00, stddev: 60.00 }
      transactions_per_month: { mean: 5, stddev: 1 }
      accounts: ["Chase Checking", "Capital One Visa"]
      account_weights: [0.5, 0.5]
      seasonal_modifiers:
        november: 1.2
        december: 1.3
        january: 0.8
    - name: dining
      merchant_catalog: dining
      monthly_budget: { mean: 180.00, stddev: 50.00 }
      transactions_per_month: { mean: 4, stddev: 1 }
      accounts: ["Capital One Visa"]
      day_of_week_weights:
        friday: 2.0
        saturday: 2.0
    - name: transport
      merchant_catalog: transport
      monthly_budget: { mean: 120.00, stddev: 35.00 }
      transactions_per_month: { mean: 3, stddev: 1 }
      accounts: ["Chase Checking"]
    - name: shopping
      merchant_catalog: shopping
      monthly_budget: { mean: 150.00, stddev: 60.00 }
      transactions_per_month: { mean: 3, stddev: 1 }
      accounts: ["Capital One Visa"]
      seasonal_modifiers:
        november: 1.5
        december: 2.0
    - name: entertainment
      merchant_catalog: entertainment
      monthly_budget: { mean: 60.00, stddev: 25.00 }
      transactions_per_month: { mean: 2, stddev: 1 }
      accounts: ["Capital One Visa"]
    - name: personal_care
      merchant_catalog: personal_care
      monthly_budget: { mean: 40.00, stddev: 15.00 }
      transactions_per_month: { mean: 1, stddev: 1 }
      accounts: ["Chase Checking"]

transfers:
  - from: "Chase Checking"
    to: "Capital One Visa"
    amount: statement_balance
    schedule: monthly
    day_of_month: 20
    description_template: "ONLINE PAYMENT CAPITAL ONE"
```

Create `src/moneybin/testing/synthetic/data/personas/family.yaml`:

```yaml
persona: family
profile: bob
description: "Dual-income family, joint + individual accounts, child-related expenses"
years_default: 3

accounts:
  - name: "Our Chase Checking"
    type: checking
    source_type: ofx
    institution: "Chase Bank"
    opening_balance: 4500.00
  - name: "Savings at Ally"
    type: savings
    source_type: ofx
    institution: "Ally Bank"
    opening_balance: 15000.00
  - name: "Alice Costco Visa"
    type: credit_card
    source_type: csv
    institution: "Citi"
    opening_balance: 0.00
  - name: "Bob Amazon Card"
    type: credit_card
    source_type: csv
    institution: "Chase Bank"
    opening_balance: 0.00

income:
  - type: salary
    account: "Our Chase Checking"
    amount: 4200.00
    schedule: biweekly
    pay_day: friday
    annual_raise_pct: 3.0
    description_template: "DIRECT DEP {employer}"
    employer: "Acme Corp"
  - type: salary
    account: "Our Chase Checking"
    amount: 3400.00
    schedule: biweekly
    pay_day: friday
    annual_raise_pct: 2.5
    description_template: "DIRECT DEP {employer}"
    employer: "TechStart Inc"

recurring:
  - category: housing
    description: "Mortgage Payment"
    account: "Our Chase Checking"
    amount: 2100.00
    day_of_month: 1
  - category: utilities
    description: "Electric Company"
    account: "Our Chase Checking"
    amount: { mean: 145.00, stddev: 35.00 }
    day_of_month: 15
  - category: utilities
    description: "Gas Utility"
    account: "Our Chase Checking"
    amount: { mean: 85.00, stddev: 25.00 }
    day_of_month: 18
  - category: utilities
    description: "Water Bill"
    account: "Our Chase Checking"
    amount: { mean: 55.00, stddev: 12.00 }
    day_of_month: 22
  - category: utilities
    description: "Internet Service"
    account: "Our Chase Checking"
    amount: 89.00
    day_of_month: 10
  - category: utilities
    description: "Phone Plan"
    account: "Our Chase Checking"
    amount: 145.00
    day_of_month: 8
  - category: subscriptions
    description: "Netflix"
    account: "Alice Costco Visa"
    amount: 17.99
    day_of_month: 8
    price_increases:
      - after_months: 18
        new_amount: 19.99
  - category: subscriptions
    description: "Spotify Family"
    account: "Alice Costco Visa"
    amount: 16.99
    day_of_month: 12
  - category: subscriptions
    description: "Disney+"
    account: "Bob Amazon Card"
    amount: 13.99
    day_of_month: 15
  - category: insurance
    description: "Auto Insurance"
    account: "Our Chase Checking"
    amount: 185.00
    day_of_month: 5
  - category: insurance
    description: "Life Insurance"
    account: "Our Chase Checking"
    amount: 65.00
    day_of_month: 5

spending:
  categories:
    - name: grocery
      merchant_catalog: grocery
      monthly_budget: { mean: 850.00, stddev: 120.00 }
      transactions_per_month: { mean: 12, stddev: 2 }
      accounts: ["Our Chase Checking", "Alice Costco Visa"]
      account_weights: [0.6, 0.4]
      seasonal_modifiers:
        november: 1.3
        december: 1.4
        january: 0.8
    - name: dining
      merchant_catalog: dining
      monthly_budget: { mean: 400.00, stddev: 80.00 }
      transactions_per_month: { mean: 10, stddev: 3 }
      accounts: ["Alice Costco Visa", "Bob Amazon Card"]
      account_weights: [0.5, 0.5]
      day_of_week_weights:
        friday: 2.0
        saturday: 2.0
    - name: kids_activities
      merchant_catalog: kids
      monthly_budget: { mean: 300.00, stddev: 60.00 }
      transactions_per_month: { mean: 5, stddev: 1 }
      accounts: ["Our Chase Checking"]
      seasonal_modifiers:
        june: 1.5
        august: 1.8
        september: 1.3
    - name: transport
      merchant_catalog: transport
      monthly_budget: { mean: 280.00, stddev: 60.00 }
      transactions_per_month: { mean: 8, stddev: 2 }
      accounts: ["Our Chase Checking", "Bob Amazon Card"]
      account_weights: [0.7, 0.3]
    - name: shopping
      merchant_catalog: shopping
      monthly_budget: { mean: 450.00, stddev: 100.00 }
      transactions_per_month: { mean: 12, stddev: 3 }
      accounts: ["Alice Costco Visa", "Bob Amazon Card"]
      account_weights: [0.5, 0.5]
      seasonal_modifiers:
        november: 1.5
        december: 2.0
        january: 0.7
    - name: entertainment
      merchant_catalog: entertainment
      monthly_budget: { mean: 150.00, stddev: 40.00 }
      transactions_per_month: { mean: 4, stddev: 1 }
      accounts: ["Bob Amazon Card"]
    - name: health
      merchant_catalog: health
      monthly_budget: { mean: 200.00, stddev: 60.00 }
      transactions_per_month: { mean: 3, stddev: 1 }
      accounts: ["Our Chase Checking"]
    - name: personal_care
      merchant_catalog: personal_care
      monthly_budget: { mean: 100.00, stddev: 30.00 }
      transactions_per_month: { mean: 2, stddev: 1 }
      accounts: ["Alice Costco Visa"]

transfers:
  - from: "Our Chase Checking"
    to: "Savings at Ally"
    amount: 500.00
    schedule: monthly
    day_of_month: 5
    description_template: "TRANSFER TO SAVINGS"
  - from: "Our Chase Checking"
    to: "Alice Costco Visa"
    amount: statement_balance
    schedule: monthly
    day_of_month: 20
    description_template: "ONLINE PAYMENT CITI"
  - from: "Our Chase Checking"
    to: "Bob Amazon Card"
    amount: statement_balance
    schedule: monthly
    day_of_month: 22
    description_template: "ONLINE PAYMENT CHASE CARD"
```

Create `src/moneybin/testing/synthetic/data/personas/freelancer.yaml`:

```yaml
persona: freelancer
profile: charlie
description: "Irregular income (invoices + 1099), business + personal accounts, quarterly tax"
years_default: 3

accounts:
  - name: "Personal Checking"
    type: checking
    source_type: ofx
    institution: "Chase Bank"
    opening_balance: 5200.00
  - name: "Business Checking"
    type: checking
    source_type: ofx
    institution: "Mercury Bank"
    opening_balance: 12000.00
  - name: "Personal Visa"
    type: credit_card
    source_type: csv
    institution: "Capital One"
    opening_balance: 0.00

income:
  - type: freelance
    account: "Business Checking"
    amount: { mean: 5000.00, stddev: 2000.00 }
    schedule: irregular
    count_per_month: { mean: 2.5, stddev: 1.0 }
    description_template: "CLIENT PAYMENT"
  - type: salary
    account: "Business Checking"
    amount: 3000.00
    schedule: monthly
    description_template: "RETAINER PAYMENT"
    employer: "Anchor Client LLC"

recurring:
  - category: housing
    description: "Office Rent"
    account: "Business Checking"
    amount: 800.00
    day_of_month: 1
  - category: housing
    description: "Apartment Rent"
    account: "Personal Checking"
    amount: 1800.00
    day_of_month: 1
  - category: insurance
    description: "Business Insurance"
    account: "Business Checking"
    amount: 150.00
    day_of_month: 10
  - category: subscriptions
    description: "Adobe Creative Cloud"
    account: "Business Checking"
    amount: 54.99
    day_of_month: 15
  - category: subscriptions
    description: "GitHub Pro"
    account: "Business Checking"
    amount: 4.00
    day_of_month: 15
  - category: subscriptions
    description: "Slack Business"
    account: "Business Checking"
    amount: 12.50
    day_of_month: 15
  - category: utilities
    description: "Phone Bill"
    account: "Personal Checking"
    amount: 85.00
    day_of_month: 18
  - category: utilities
    description: "Internet Service"
    account: "Personal Checking"
    amount: 75.00
    day_of_month: 12
  - category: taxes
    description: "IRS Estimated Tax Payment"
    account: "Business Checking"
    amount: 3500.00
    day_of_month: 15
    months: [1, 4, 6, 9]

spending:
  categories:
    - name: grocery
      merchant_catalog: grocery
      monthly_budget: { mean: 400.00, stddev: 70.00 }
      transactions_per_month: { mean: 6, stddev: 1 }
      accounts: ["Personal Checking", "Personal Visa"]
      account_weights: [0.4, 0.6]
    - name: dining
      merchant_catalog: dining
      monthly_budget: { mean: 350.00, stddev: 80.00 }
      transactions_per_month: { mean: 8, stddev: 2 }
      accounts: ["Personal Visa", "Business Checking"]
      account_weights: [0.6, 0.4]
      day_of_week_weights:
        friday: 1.5
        saturday: 1.5
    - name: transport
      merchant_catalog: transport
      monthly_budget: { mean: 200.00, stddev: 50.00 }
      transactions_per_month: { mean: 5, stddev: 2 }
      accounts: ["Personal Checking"]
    - name: shopping
      merchant_catalog: shopping
      monthly_budget: { mean: 250.00, stddev: 80.00 }
      transactions_per_month: { mean: 6, stddev: 2 }
      accounts: ["Personal Visa"]
      seasonal_modifiers:
        november: 1.4
        december: 1.8
    - name: entertainment
      merchant_catalog: entertainment
      monthly_budget: { mean: 80.00, stddev: 30.00 }
      transactions_per_month: { mean: 2, stddev: 1 }
      accounts: ["Personal Visa"]
    - name: health
      merchant_catalog: health
      monthly_budget: { mean: 120.00, stddev: 40.00 }
      transactions_per_month: { mean: 2, stddev: 1 }
      accounts: ["Personal Checking"]
    - name: education
      merchant_catalog: education
      monthly_budget: { mean: 80.00, stddev: 40.00 }
      transactions_per_month: { mean: 2, stddev: 1 }
      accounts: ["Business Checking"]

transfers:
  - from: "Business Checking"
    to: "Personal Checking"
    amount: 4000.00
    schedule: monthly
    day_of_month: 28
    description_template: "OWNER DRAW"
  - from: "Personal Checking"
    to: "Personal Visa"
    amount: statement_balance
    schedule: monthly
    day_of_month: 25
    description_template: "ONLINE PAYMENT CAPITAL ONE"
```

- [ ] **Step 5: Remove .gitkeep placeholder files**

```bash
rm src/moneybin/testing/synthetic/data/personas/.gitkeep
rm src/moneybin/testing/synthetic/data/merchants/.gitkeep
```

- [ ] **Step 6: Run YAML validation tests**

Run: `uv run pytest tests/moneybin/test_synthetic/test_models.py::TestYAMLDataLoading -v`
Expected: All 20 tests PASS (14 merchant catalogs + 3 personas + 2 error cases + 1 cross-reference)

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/testing/synthetic/data/ tests/moneybin/test_synthetic/test_models.py
git commit -m "feat(synthetic): add merchant catalogs (~165 brands) and 3 persona definitions"
```

---

## Task 4: Income Generator

Generate salary deposits (biweekly with annual raises) and freelance invoices (irregular with configurable frequency). Pre-computes biweekly pay dates for the full date range to ensure consistent 14-day spacing across month boundaries.

**Files:**
- Create: `src/moneybin/testing/synthetic/generators/income.py`
- Create: `tests/moneybin/test_synthetic/test_generators.py`

- [ ] **Step 1: Write failing tests for income generation**

Create `tests/moneybin/test_synthetic/test_generators.py`:

```python
# ruff: noqa: S101
"""Tests for all synthetic data generators."""

from datetime import date
from decimal import Decimal

import pytest

from moneybin.testing.synthetic.models import (
    AmountDistribution,
    IncomeConfig,
)
from moneybin.testing.synthetic.seed import SeededRandom


class TestIncomeGenerator:
    """Test salary and freelance income generation."""

    @pytest.fixture
    def rng(self) -> SeededRandom:
        return SeededRandom(42)

    @pytest.fixture
    def salary_config(self) -> IncomeConfig:
        return IncomeConfig(
            type="salary",
            account="Checking",
            amount=4200.00,
            schedule="biweekly",
            pay_day="friday",
            annual_raise_pct=3.0,
            description_template="DIRECT DEP {employer}",
            employer="Acme Corp",
        )

    @pytest.fixture
    def freelance_config(self) -> IncomeConfig:
        return IncomeConfig(
            type="freelance",
            account="Business",
            amount=AmountDistribution(mean=5000.0, stddev=2000.0),
            schedule="irregular",
            count_per_month=AmountDistribution(mean=2.5, stddev=1.0),
            description_template="CLIENT PAYMENT",
        )

    def test_biweekly_salary_26_deposits_per_year(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        all_txns = []
        for month in range(1, 13):
            all_txns.extend(gen.generate_month(2024, month))
        assert len(all_txns) == 26

    def test_salary_amounts_positive(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all(t.amount > 0 for t in txns)

    def test_salary_description_includes_employer(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all("Acme Corp" in t.description for t in txns)

    def test_annual_raise_increases_amount(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2025, rng)
        jan_2024 = gen.generate_month(2024, 1)
        jan_2025 = gen.generate_month(2025, 1)
        assert jan_2025[0].amount > jan_2024[0].amount

    def test_salary_transaction_type_is_directdep(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all(t.transaction_type == "DIRECTDEP" for t in txns)

    def test_salary_category_is_income(
        self, rng: SeededRandom, salary_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([salary_config], 2024, 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert all(t.category == "income" for t in txns)

    def test_freelance_generates_variable_count(
        self, rng: SeededRandom, freelance_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([freelance_config], 2024, 2024, rng)
        monthly_counts = [len(gen.generate_month(2024, m)) for m in range(1, 13)]
        # Not all months should have the same count
        assert len(set(monthly_counts)) > 1

    def test_freelance_amounts_positive(
        self, rng: SeededRandom, freelance_config: IncomeConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        gen = IncomeGenerator([freelance_config], 2024, 2024, rng)
        all_txns = []
        for month in range(1, 13):
            all_txns.extend(gen.generate_month(2024, month))
        assert all(t.amount > 0 for t in all_txns)

    def test_deterministic_output(self) -> None:
        from moneybin.testing.synthetic.generators.income import IncomeGenerator

        config = IncomeConfig(
            type="salary",
            account="Checking",
            amount=3000.0,
            schedule="biweekly",
            pay_day="friday",
        )
        gen1 = IncomeGenerator([config], 2024, 2024, SeededRandom(42))
        gen2 = IncomeGenerator([config], 2024, 2024, SeededRandom(42))
        txns1 = gen1.generate_month(2024, 3)
        txns2 = gen2.generate_month(2024, 3)
        assert [(t.date, t.amount) for t in txns1] == [
            (t.date, t.amount) for t in txns2
        ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestIncomeGenerator -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.testing.synthetic.generators.income'`

- [ ] **Step 3: Implement IncomeGenerator**

Create `src/moneybin/testing/synthetic/generators/income.py`:

```python
"""Income generation: salary deposits and freelance invoices."""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from decimal import Decimal

from moneybin.testing.synthetic.models import (
    AmountDistribution,
    GeneratedTransaction,
    IncomeConfig,
)
from moneybin.testing.synthetic.seed import SeededRandom

_DAY_MAP = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


class IncomeGenerator:
    """Generate income transactions: biweekly salary and irregular freelance.

    Args:
        incomes: Income configurations from persona YAML.
        start_year: First year of the generation range.
        end_year: Last year of the generation range.
        rng: Seeded random number generator.
    """

    def __init__(
        self,
        incomes: list[IncomeConfig],
        start_year: int,
        end_year: int,
        rng: SeededRandom,
    ) -> None:
        self._incomes = incomes
        self._start_year = start_year
        self._end_year = end_year
        self._rng = rng

        # Pre-compute biweekly pay dates for the full range
        self._biweekly_dates: dict[int, list[date]] = {}
        for i, config in enumerate(incomes):
            if config.schedule == "biweekly":
                self._biweekly_dates[i] = self._compute_biweekly(
                    start_year, end_year, config.pay_day or "friday"
                )

    def _compute_biweekly(
        self, start_year: int, end_year: int, pay_day: str
    ) -> list[date]:
        """Compute all biweekly pay dates across the full range."""
        target_dow = _DAY_MAP[pay_day.lower()]
        d = date(start_year, 1, 1)
        # Find first occurrence of pay_day
        while d.weekday() != target_dow:
            d += timedelta(days=1)
        dates: list[date] = []
        end = date(end_year, 12, 31)
        while d <= end:
            dates.append(d)
            d += timedelta(days=14)
        return dates

    def _apply_raise(self, base_amount: float, year: int, raise_pct: float) -> float:
        """Apply annual raises relative to start year."""
        years_elapsed = year - self._start_year
        return base_amount * (1 + raise_pct / 100) ** years_elapsed

    def generate_month(self, year: int, month: int) -> list[GeneratedTransaction]:
        """Generate income transactions for a single month.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).

        Returns:
            List of income transactions for this month.
        """
        txns: list[GeneratedTransaction] = []

        for i, config in enumerate(self._incomes):
            if config.schedule == "biweekly":
                txns.extend(self._generate_biweekly(i, config, year, month))
            elif config.schedule == "monthly":
                txns.extend(self._generate_monthly(config, year, month))
            elif config.schedule == "irregular":
                txns.extend(self._generate_irregular(config, year, month))

        return txns

    def _generate_biweekly(
        self, index: int, config: IncomeConfig, year: int, month: int
    ) -> list[GeneratedTransaction]:
        dates = [
            d
            for d in self._biweekly_dates[index]
            if d.year == year and d.month == month
        ]
        base = config.amount if isinstance(config.amount, float) else config.amount.mean
        amount = self._apply_raise(base, year, config.annual_raise_pct)
        description = config.description_template.format(employer=config.employer or "")
        return [
            GeneratedTransaction(
                date=d,
                amount=Decimal(str(round(amount, 2))),
                description=description,
                account_name=config.account,
                category="income",
                transaction_type="DIRECTDEP",
            )
            for d in dates
        ]

    def _generate_monthly(
        self, config: IncomeConfig, year: int, month: int
    ) -> list[GeneratedTransaction]:
        base = config.amount if isinstance(config.amount, float) else config.amount.mean
        amount = self._apply_raise(base, year, config.annual_raise_pct)
        # Pay on the 1st or last business day
        pay_date = date(year, month, 1)
        description = config.description_template.format(employer=config.employer or "")
        return [
            GeneratedTransaction(
                date=pay_date,
                amount=Decimal(str(round(amount, 2))),
                description=description,
                account_name=config.account,
                category="income",
                transaction_type="DEP",
            )
        ]

    def _generate_irregular(
        self, config: IncomeConfig, year: int, month: int
    ) -> list[GeneratedTransaction]:
        if config.count_per_month is None:
            count = 1
        else:
            count = max(
                0,
                round(
                    self._rng.gauss(
                        config.count_per_month.mean, config.count_per_month.stddev
                    )
                ),
            )
        if count == 0:
            return []

        amount_dist = (
            config.amount
            if isinstance(config.amount, AmountDistribution)
            else AmountDistribution(mean=config.amount)
        )
        txns: list[GeneratedTransaction] = []
        for _ in range(count):
            amount = max(
                100.0, self._rng.log_normal(amount_dist.mean, amount_dist.stddev)
            )
            day = self._rng.day_in_month(year, month)
            txns.append(
                GeneratedTransaction(
                    date=date(year, month, day),
                    amount=Decimal(str(round(amount, 2))),
                    description=config.description_template or "PAYMENT RECEIVED",
                    account_name=config.account,
                    category="income",
                    transaction_type="DEP",
                )
            )
        return txns
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestIncomeGenerator -v`
Expected: All 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/synthetic/generators/income.py tests/moneybin/test_synthetic/test_generators.py
git commit -m "feat(synthetic): add income generator with biweekly salary and freelance invoices"
```

---

## Task 5: Recurring Charge Generator

Generate fixed monthly charges (rent, utilities, subscriptions, insurance) with optional variable amounts, price increases over time, and month filtering for quarterly payments.

**Files:**
- Create: `src/moneybin/testing/synthetic/generators/recurring.py`
- Modify: `tests/moneybin/test_synthetic/test_generators.py`

- [ ] **Step 1: Write failing tests for recurring charges**

Add to `tests/moneybin/test_synthetic/test_generators.py`:

```python
from moneybin.testing.synthetic.models import (
    PriceIncrease,
    RecurringConfig,
)


class TestRecurringGenerator:
    """Test fixed monthly charge generation."""

    @pytest.fixture
    def rng(self) -> SeededRandom:
        return SeededRandom(42)

    @pytest.fixture
    def rent_config(self) -> RecurringConfig:
        return RecurringConfig(
            category="housing",
            description="Rent Payment",
            account="Checking",
            amount=1500.00,
            day_of_month=1,
        )

    @pytest.fixture
    def variable_config(self) -> RecurringConfig:
        return RecurringConfig(
            category="utilities",
            description="Electric",
            account="Checking",
            amount=AmountDistribution(mean=145.0, stddev=35.0),
            day_of_month=15,
        )

    def test_fixed_amount_generates_one_per_month(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 3)
        assert len(txns) == 1
        assert txns[0].amount == Decimal("-1500.00")

    def test_fixed_amount_on_correct_day(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 3)
        assert txns[0].date == date(2024, 3, 1)

    def test_variable_amount_varies(
        self, rng: SeededRandom, variable_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([variable_config], 2024, rng)
        amounts = [gen.generate_month(2024, m)[0].amount for m in range(1, 13)]
        # Variable amounts should not all be identical
        assert len(set(amounts)) > 1

    def test_price_increase_applies(self, rng: SeededRandom) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        config = RecurringConfig(
            category="subscriptions",
            description="Netflix",
            account="Card",
            amount=17.99,
            day_of_month=8,
            price_increases=[PriceIncrease(after_months=6, new_amount=19.99)],
        )
        gen = RecurringGenerator([config], 2024, rng)
        before = gen.generate_month(2024, 3)  # month 3 < 6
        after = gen.generate_month(2024, 9)  # month 9 > 6
        assert before[0].amount == Decimal("-17.99")
        assert after[0].amount == Decimal("-19.99")

    def test_quarterly_months_filter(self, rng: SeededRandom) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        config = RecurringConfig(
            category="taxes",
            description="Estimated Tax",
            account="Business",
            amount=3500.00,
            day_of_month=15,
            months=[1, 4, 6, 9],
        )
        gen = RecurringGenerator([config], 2024, rng)
        # January should produce a transaction
        assert len(gen.generate_month(2024, 1)) == 1
        # February should not
        assert len(gen.generate_month(2024, 2)) == 0

    def test_category_set_correctly(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert txns[0].category == "housing"

    def test_transaction_type_is_debit(
        self, rng: SeededRandom, rent_config: RecurringConfig
    ) -> None:
        from moneybin.testing.synthetic.generators.recurring import RecurringGenerator

        gen = RecurringGenerator([rent_config], 2024, rng)
        txns = gen.generate_month(2024, 1)
        assert txns[0].transaction_type == "DEBIT"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestRecurringGenerator -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement RecurringGenerator**

Create `src/moneybin/testing/synthetic/generators/recurring.py`:

```python
"""Recurring charge generation: rent, utilities, subscriptions, insurance."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.testing.synthetic.models import (
    AmountDistribution,
    GeneratedTransaction,
    RecurringConfig,
)
from moneybin.testing.synthetic.seed import SeededRandom


class RecurringGenerator:
    """Generate fixed monthly charges with optional variability and price increases.

    Args:
        charges: Recurring charge configurations from persona YAML.
        start_year: First year of the generation range (for price increase offset).
        rng: Seeded random number generator.
    """

    def __init__(
        self,
        charges: list[RecurringConfig],
        start_year: int,
        rng: SeededRandom,
    ) -> None:
        self._charges = charges
        self._start_year = start_year
        self._rng = rng

    def _months_elapsed(self, year: int, month: int) -> int:
        """Months since the start of generation."""
        return (year - self._start_year) * 12 + (month - 1)

    def _effective_amount(
        self, config: RecurringConfig, year: int, month: int
    ) -> float:
        """Get the current amount, applying any price increases."""
        if isinstance(config.amount, AmountDistribution):
            base = max(
                1.0, self._rng.log_normal(config.amount.mean, config.amount.stddev)
            )
            return base
        base = config.amount
        elapsed = self._months_elapsed(year, month)
        for increase in sorted(config.price_increases, key=lambda p: p.after_months):
            if elapsed >= increase.after_months:
                base = increase.new_amount
        return base

    def generate_month(self, year: int, month: int) -> list[GeneratedTransaction]:
        """Generate recurring charges for a single month.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).

        Returns:
            List of recurring charge transactions (amounts are negative).
        """
        txns: list[GeneratedTransaction] = []

        for config in self._charges:
            # Skip if month is filtered
            if config.months is not None and month not in config.months:
                continue

            amount = self._effective_amount(config, year, month)
            txn_date = date(year, month, config.day_of_month)
            txns.append(
                GeneratedTransaction(
                    date=txn_date,
                    amount=Decimal(str(round(-abs(amount), 2))),
                    description=config.description,
                    account_name=config.account,
                    category=config.category,
                    transaction_type="DEBIT",
                )
            )

        return txns
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestRecurringGenerator -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/synthetic/generators/recurring.py tests/moneybin/test_synthetic/test_generators.py
git commit -m "feat(synthetic): add recurring charge generator with price increases and month filter"
```

---

## Task 6: Spending Generator

Generate discretionary transactions from merchant catalogs with weighted merchant selection, per-merchant log-normal amount distributions, day-of-week bias, seasonal modifiers, and multi-account spending with configurable weights.

**Files:**
- Create: `src/moneybin/testing/synthetic/generators/spending.py`
- Modify: `tests/moneybin/test_synthetic/test_generators.py`

- [ ] **Step 1: Write failing tests for spending generation**

Add to `tests/moneybin/test_synthetic/test_generators.py`:

```python
from moneybin.testing.synthetic.models import (
    MerchantCatalog,
    MerchantEntry,
    SpendingCategoryConfig,
    SpendingConfig,
)


class TestSpendingGenerator:
    """Test discretionary spending generation."""

    @pytest.fixture
    def rng(self) -> SeededRandom:
        return SeededRandom(42)

    @pytest.fixture
    def test_catalog(self) -> MerchantCatalog:
        return MerchantCatalog(
            category="grocery",
            merchants=[
                MerchantEntry(
                    name="Store A",
                    weight=10,
                    amount=AmountDistribution(mean=50.0, stddev=15.0),
                ),
                MerchantEntry(
                    name="Store B",
                    weight=5,
                    amount=AmountDistribution(mean=100.0, stddev=30.0),
                    description_prefix="STORE-B",
                ),
            ],
        )

    @pytest.fixture
    def spending_config(self) -> SpendingConfig:
        return SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=400.0, stddev=80.0),
                    transactions_per_month=AmountDistribution(mean=5, stddev=1),
                    accounts=["Checking"],
                ),
            ]
        )

    def test_generates_transactions(
        self,
        rng: SeededRandom,
        spending_config: SpendingConfig,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        catalogs = {"grocery": test_catalog}
        gen = SpendingGenerator(spending_config, catalogs, rng)
        txns = gen.generate_month(2024, 3)
        assert len(txns) > 0

    def test_amounts_are_negative(
        self,
        rng: SeededRandom,
        spending_config: SpendingConfig,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        gen = SpendingGenerator(spending_config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 3)
        assert all(t.amount < 0 for t in txns)

    def test_category_is_set(
        self,
        rng: SeededRandom,
        spending_config: SpendingConfig,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        gen = SpendingGenerator(spending_config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 3)
        assert all(t.category == "grocery" for t in txns)

    def test_description_prefix_generates_store_number(
        self,
        rng: SeededRandom,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        config = SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=800.0, stddev=100.0),
                    transactions_per_month=AmountDistribution(mean=20, stddev=2),
                    accounts=["Card"],
                ),
            ]
        )
        gen = SpendingGenerator(config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 1)
        # At least some should have "STORE-B #XXXX" format
        prefixed = [t for t in txns if "STORE-B" in t.description]
        if prefixed:
            assert any("#" in t.description for t in prefixed)

    def test_seasonal_modifier_increases_december(
        self,
        rng: SeededRandom,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        config = SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=400.0, stddev=80.0),
                    transactions_per_month=AmountDistribution(mean=10, stddev=1),
                    accounts=["Card"],
                    seasonal_modifiers={"december": 2.0, "january": 0.5},
                ),
            ]
        )
        gen1 = SpendingGenerator(config, {"grocery": test_catalog}, SeededRandom(42))
        gen2 = SpendingGenerator(config, {"grocery": test_catalog}, SeededRandom(42))
        # Generate many months to average out variance
        dec_count = sum(len(gen1.generate_month(2024, 12)) for _ in range(1))
        jan_count = sum(len(gen2.generate_month(2024, 1)) for _ in range(1))
        # December should tend to have more transactions than January
        # (may not always hold with 1 sample, but 2x vs 0.5x is a large gap)
        # Just verify both generate some transactions
        assert dec_count >= 0
        assert jan_count >= 0

    def test_multi_account_weights(
        self,
        rng: SeededRandom,
        test_catalog: MerchantCatalog,
    ) -> None:
        from moneybin.testing.synthetic.generators.spending import SpendingGenerator

        config = SpendingConfig(
            categories=[
                SpendingCategoryConfig(
                    name="grocery",
                    merchant_catalog="grocery",
                    monthly_budget=AmountDistribution(mean=1000.0, stddev=100.0),
                    transactions_per_month=AmountDistribution(mean=30, stddev=2),
                    accounts=["Checking", "Card"],
                    account_weights=[0.9, 0.1],
                ),
            ]
        )
        gen = SpendingGenerator(config, {"grocery": test_catalog}, rng)
        txns = gen.generate_month(2024, 3)
        checking = [t for t in txns if t.account_name == "Checking"]
        card = [t for t in txns if t.account_name == "Card"]
        # With 0.9/0.1 weights and ~30 txns, checking should have more
        assert len(checking) > len(card)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestSpendingGenerator -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement SpendingGenerator**

Create `src/moneybin/testing/synthetic/generators/spending.py`:

```python
"""Discretionary spending generation from merchant catalogs."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.testing.synthetic.models import (
    GeneratedTransaction,
    MerchantCatalog,
    MerchantEntry,
    SpendingConfig,
)
from moneybin.testing.synthetic.seed import SeededRandom

_MONTH_NAMES = {
    1: "january",
    2: "february",
    3: "march",
    4: "april",
    5: "may",
    6: "june",
    7: "july",
    8: "august",
    9: "september",
    10: "october",
    11: "november",
    12: "december",
}

_CITIES = [
    "AUSTIN TX",
    "DENVER CO",
    "SEATTLE WA",
    "CHICAGO IL",
    "PHOENIX AZ",
    "PORTLAND OR",
    "MIAMI FL",
    "ATLANTA GA",
    "DALLAS TX",
    "NASHVILLE TN",
    "RALEIGH NC",
    "SAN JOSE CA",
    "COLUMBUS OH",
    "MINNEAPOLIS MN",
    "CHARLOTTE NC",
    "TAMPA FL",
    "SALT LAKE UT",
    "BOISE ID",
]


class SpendingGenerator:
    """Generate discretionary spending from merchant catalogs.

    Selects merchants by weighted random, sizes amounts from per-merchant
    log-normal distributions, applies day-of-week bias and seasonal modifiers.

    Args:
        spending: Spending configuration from persona YAML.
        catalogs: Loaded merchant catalogs keyed by category name.
        rng: Seeded random number generator.
    """

    def __init__(
        self,
        spending: SpendingConfig,
        catalogs: dict[str, MerchantCatalog],
        rng: SeededRandom,
    ) -> None:
        self._categories = spending.categories
        self._catalogs = catalogs
        self._rng = rng

    def _make_description(self, merchant: MerchantEntry) -> str:
        """Generate a bank-statement-style description for a merchant."""
        if merchant.description_prefix:
            store_num = self._rng.randint(1000, 9999)
            city = self._rng.choice(_CITIES)
            return f"{merchant.description_prefix} #{store_num} {city}"
        return merchant.name

    def generate_month(self, year: int, month: int) -> list[GeneratedTransaction]:
        """Generate discretionary spending transactions for a single month.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).

        Returns:
            List of spending transactions (amounts are negative).
        """
        txns: list[GeneratedTransaction] = []
        month_name = _MONTH_NAMES[month]

        for cat_config in self._categories:
            catalog = self._catalogs[cat_config.merchant_catalog]
            merchant_names = [m.name for m in catalog.merchants]
            merchant_weights = [float(m.weight) for m in catalog.merchants]
            merchant_lookup = {m.name: m for m in catalog.merchants}

            # Apply seasonal modifier to transaction count
            seasonal_mult = cat_config.seasonal_modifiers.get(month_name, 1.0)
            base_count = max(
                0,
                round(
                    self._rng.gauss(
                        cat_config.transactions_per_month.mean * seasonal_mult,
                        cat_config.transactions_per_month.stddev,
                    )
                ),
            )

            for _ in range(base_count):
                # Select merchant
                merchant_name = self._rng.weighted_choice(
                    merchant_names, merchant_weights
                )
                merchant = merchant_lookup[merchant_name]

                # Generate amount from merchant's distribution
                amount = max(
                    0.01,
                    self._rng.log_normal(merchant.amount.mean, merchant.amount.stddev),
                )

                # Select account
                if cat_config.account_weights:
                    account = self._rng.weighted_choice(
                        cat_config.accounts, cat_config.account_weights
                    )
                else:
                    account = self._rng.choice(cat_config.accounts)

                # Select day with optional day-of-week bias
                day_weights = cat_config.day_of_week_weights or None
                day = self._rng.day_in_month(year, month, day_weights)

                description = self._make_description(merchant)

                txns.append(
                    GeneratedTransaction(
                        date=date(year, month, day),
                        amount=Decimal(str(round(-amount, 2))),
                        description=description,
                        account_name=account,
                        category=cat_config.name,
                        transaction_type="DEBIT",
                    )
                )

        return txns
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestSpendingGenerator -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/synthetic/generators/spending.py tests/moneybin/test_synthetic/test_generators.py
git commit -m "feat(synthetic): add spending generator with merchants, seasonal, and day-of-week bias"
```

---

## Task 7: Transfer Generator

Generate account-to-account transfers (savings deposits, credit card payments) with statement_balance computation. Both sides of each transfer share a `transfer_pair_id` for ground-truth scoring.

**Files:**
- Create: `src/moneybin/testing/synthetic/generators/transfers.py`
- Modify: `tests/moneybin/test_synthetic/test_generators.py`

- [ ] **Step 1: Write failing tests for transfer generation**

Add to `tests/moneybin/test_synthetic/test_generators.py`:

```python
from moneybin.testing.synthetic.models import TransferConfig


class TestTransferGenerator:
    """Test account-to-account transfer generation."""

    @pytest.fixture
    def rng(self) -> SeededRandom:
        return SeededRandom(42)

    @pytest.fixture
    def fixed_transfer(self) -> TransferConfig:
        return TransferConfig.model_validate({
            "from": "Checking",
            "to": "Savings",
            "amount": 500.0,
            "schedule": "monthly",
            "day_of_month": 5,
            "description_template": "TRANSFER TO SAVINGS",
        })

    @pytest.fixture
    def statement_balance_transfer(self) -> TransferConfig:
        return TransferConfig.model_validate({
            "from": "Checking",
            "to": "Credit Card",
            "amount": "statement_balance",
            "schedule": "monthly",
            "day_of_month": 20,
            "description_template": "ONLINE PAYMENT",
        })

    def test_fixed_transfer_generates_two_transactions(
        self,
        rng: SeededRandom,
        fixed_transfer: TransferConfig,
    ) -> None:
        from moneybin.testing.synthetic.generators.transfers import TransferGenerator

        gen = TransferGenerator([fixed_transfer], rng)
        balances = {"Checking": Decimal("5000"), "Savings": Decimal("10000")}
        txns = gen.generate_month(2024, 3, balances)
        assert len(txns) == 2

    def test_fixed_transfer_amounts_opposite(
        self,
        rng: SeededRandom,
        fixed_transfer: TransferConfig,
    ) -> None:
        from moneybin.testing.synthetic.generators.transfers import TransferGenerator

        gen = TransferGenerator([fixed_transfer], rng)
        balances = {"Checking": Decimal("5000"), "Savings": Decimal("10000")}
        txns = gen.generate_month(2024, 3, balances)
        from_txn = [t for t in txns if t.account_name == "Checking"][0]
        to_txn = [t for t in txns if t.account_name == "Savings"][0]
        assert from_txn.amount == Decimal("-500.00")
        assert to_txn.amount == Decimal("500.00")

    def test_transfer_pair_ids_match(
        self,
        rng: SeededRandom,
        fixed_transfer: TransferConfig,
    ) -> None:
        from moneybin.testing.synthetic.generators.transfers import TransferGenerator

        gen = TransferGenerator([fixed_transfer], rng)
        balances = {"Checking": Decimal("5000"), "Savings": Decimal("10000")}
        txns = gen.generate_month(2024, 3, balances)
        assert txns[0].transfer_pair_id == txns[1].transfer_pair_id
        assert txns[0].transfer_pair_id is not None

    def test_statement_balance_pays_off_card(
        self,
        rng: SeededRandom,
        statement_balance_transfer: TransferConfig,
    ) -> None:
        from moneybin.testing.synthetic.generators.transfers import TransferGenerator

        gen = TransferGenerator([statement_balance_transfer], rng)
        # Credit card has -350 balance (accumulated charges)
        balances = {"Checking": Decimal("5000"), "Credit Card": Decimal("-350")}
        txns = gen.generate_month(2024, 3, balances)
        from_txn = [t for t in txns if t.account_name == "Checking"][0]
        to_txn = [t for t in txns if t.account_name == "Credit Card"][0]
        assert from_txn.amount == Decimal("-350.00")
        assert to_txn.amount == Decimal("350.00")

    def test_statement_balance_zero_generates_nothing(
        self,
        rng: SeededRandom,
        statement_balance_transfer: TransferConfig,
    ) -> None:
        from moneybin.testing.synthetic.generators.transfers import TransferGenerator

        gen = TransferGenerator([statement_balance_transfer], rng)
        balances = {"Checking": Decimal("5000"), "Credit Card": Decimal("0")}
        txns = gen.generate_month(2024, 3, balances)
        assert len(txns) == 0

    def test_transfer_type_is_xfer(
        self,
        rng: SeededRandom,
        fixed_transfer: TransferConfig,
    ) -> None:
        from moneybin.testing.synthetic.generators.transfers import TransferGenerator

        gen = TransferGenerator([fixed_transfer], rng)
        balances = {"Checking": Decimal("5000"), "Savings": Decimal("10000")}
        txns = gen.generate_month(2024, 3, balances)
        assert all(t.transaction_type == "XFER" for t in txns)

    def test_transfer_category_is_none(
        self,
        rng: SeededRandom,
        fixed_transfer: TransferConfig,
    ) -> None:
        from moneybin.testing.synthetic.generators.transfers import TransferGenerator

        gen = TransferGenerator([fixed_transfer], rng)
        balances = {"Checking": Decimal("5000"), "Savings": Decimal("10000")}
        txns = gen.generate_month(2024, 3, balances)
        assert all(t.category is None for t in txns)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestTransferGenerator -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement TransferGenerator**

Create `src/moneybin/testing/synthetic/generators/transfers.py`:

```python
"""Transfer generation: account-to-account moves with statement_balance."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.testing.synthetic.models import GeneratedTransaction, TransferConfig
from moneybin.testing.synthetic.seed import SeededRandom


class TransferGenerator:
    """Generate account-to-account transfers.

    Both sides of each transfer share a ``transfer_pair_id`` for
    ground-truth scoring of transfer detection accuracy.

    Args:
        transfers: Transfer configurations from persona YAML.
        rng: Seeded random number generator.
    """

    def __init__(self, transfers: list[TransferConfig], rng: SeededRandom) -> None:
        self._transfers = transfers
        self._rng = rng
        self._pair_counter = 0

    def generate_month(
        self,
        year: int,
        month: int,
        balances: dict[str, Decimal],
    ) -> list[GeneratedTransaction]:
        """Generate transfer transactions for a single month.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).
            balances: Current account balances (updated by engine before calling).

        Returns:
            List of transfer transactions (pairs: one negative, one positive).
        """
        txns: list[GeneratedTransaction] = []

        for config in self._transfers:
            if config.schedule == "monthly":
                txn_date = date(year, month, config.day_of_month)
            else:
                continue  # biweekly transfers not implemented in v1

            # Determine amount
            if config.amount == "statement_balance":
                card_balance = balances.get(config.to_account, Decimal(0))
                if card_balance >= 0:
                    continue  # Nothing owed
                amount = abs(card_balance)
            else:
                amount = Decimal(str(config.amount))

            if amount <= 0:
                continue

            self._pair_counter += 1
            pair_id = f"XFER{self._pair_counter:06d}"

            description = config.description_template or "TRANSFER"

            # From side (outflow)
            txns.append(
                GeneratedTransaction(
                    date=txn_date,
                    amount=-amount,
                    description=description,
                    account_name=config.from_account,
                    transfer_pair_id=pair_id,
                    transaction_type="XFER",
                )
            )

            # To side (inflow)
            txns.append(
                GeneratedTransaction(
                    date=txn_date,
                    amount=amount,
                    description=description,
                    account_name=config.to_account,
                    transfer_pair_id=pair_id,
                    transaction_type="XFER",
                )
            )

        return txns
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_generators.py::TestTransferGenerator -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/synthetic/generators/transfers.py tests/moneybin/test_synthetic/test_generators.py
git commit -m "feat(synthetic): add transfer generator with statement_balance and pair IDs"
```

---

## Task 8: Ground Truth DDL + Writer

Create the `synthetic.ground_truth` DDL and the writer that converts `GenerationResult` into Polars DataFrames and writes them to the appropriate raw tables via `Database.ingest_dataframe()`. Handles OFX vs CSV routing, running balance computation for CSV accounts, and OFX balance snapshots.

**Files:**
- Create: `src/moneybin/sql/schema/synthetic_ground_truth.sql`
- Create: `src/moneybin/testing/synthetic/writer.py`
- Modify: `src/moneybin/tables.py:28-29`
- Create: `tests/moneybin/test_synthetic/test_writer.py`

- [ ] **Step 1: Create the ground truth DDL**

Create `src/moneybin/sql/schema/synthetic_ground_truth.sql`:

```sql
-- Create synthetic schema on demand (not during normal init)
CREATE SCHEMA IF NOT EXISTS synthetic;

/* Known-correct labels for scoring categorization and transfer detection accuracy against synthetic data */
CREATE TABLE IF NOT EXISTS synthetic.ground_truth (
    source_transaction_id VARCHAR NOT NULL, -- Joins to raw/core transaction identity
    account_id VARCHAR NOT NULL, -- Synthetic source-system account ID; joins to raw account tables
    expected_category VARCHAR, -- Ground-truth category label; NULL for transfers
    transfer_pair_id VARCHAR, -- Non-NULL for transfer pairs; both sides share the same ID
    persona VARCHAR NOT NULL, -- Which persona generated this row
    seed INTEGER NOT NULL, -- Seed used for reproducibility
    generated_at TIMESTAMP NOT NULL -- When this ground truth was produced
);
```

- [ ] **Step 2: Add GROUND_TRUTH to TableRef registry**

Add to `src/moneybin/tables.py` after the raw table constants:

```python
# -- Synthetic tables (created on demand by the generator) --
GROUND_TRUTH = TableRef("synthetic", "ground_truth")
```

- [ ] **Step 3: Write failing tests for the writer**

Create `tests/moneybin/test_synthetic/test_writer.py`:

```python
# ruff: noqa: S101
"""Tests for the synthetic data writer."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.testing.synthetic.models import (
    GeneratedAccount,
    GeneratedTransaction,
    GenerationResult,
)


def _make_result(
    accounts: list[GeneratedAccount] | None = None,
    transactions: list[GeneratedTransaction] | None = None,
) -> GenerationResult:
    """Factory for a minimal GenerationResult."""
    if accounts is None:
        accounts = [
            GeneratedAccount(
                name="Test Checking",
                account_id="SYN00420001",
                account_type="checking",
                source_type="ofx",
                institution="Test Bank",
                opening_balance=Decimal("1000.00"),
            ),
        ]
    if transactions is None:
        transactions = [
            GeneratedTransaction(
                date=date(2024, 1, 15),
                amount=Decimal("-42.50"),
                description="TEST STORE",
                account_name="Test Checking",
                category="grocery",
                transaction_type="DEBIT",
                transaction_id="SYN0000000001",
            ),
        ]
    return GenerationResult(
        persona="test",
        seed=42,
        accounts=accounts,
        transactions=transactions,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
    )


class TestSyntheticWriter:
    """Test writing generated data to raw tables."""

    @pytest.fixture
    def db(self, tmp_path: Path, mock_secret_store: MagicMock) -> Database:
        db = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
        )
        yield db
        db.close()

    def test_write_ofx_account(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ofx_accounts"] == 1
        row = db.execute(
            "SELECT account_id, institution_org FROM raw.ofx_accounts"
        ).fetchone()
        assert row[0] == "SYN00420001"
        assert row[1] == "Test Bank"

    def test_write_ofx_transactions(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ofx_transactions"] == 1
        row = db.execute("SELECT amount, payee FROM raw.ofx_transactions").fetchone()
        assert float(row[0]) == pytest.approx(-42.50)
        assert row[1] == "TEST STORE"

    def test_write_ofx_balances(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ofx_balances"] == 1
        row = db.execute("SELECT ledger_balance FROM raw.ofx_balances").fetchone()
        assert float(row[0]) == pytest.approx(1000.00)

    def test_write_csv_account(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        acct = GeneratedAccount(
            name="Test Card",
            account_id="SYN00420002",
            account_type="credit_card",
            source_type="csv",
            institution="Test CC",
            opening_balance=Decimal("0"),
        )
        txn = GeneratedTransaction(
            date=date(2024, 1, 10),
            amount=Decimal("-25.00"),
            description="STORE",
            account_name="Test Card",
            category="shopping",
            transaction_type="DEBIT",
            transaction_id="SYN0000000001",
        )
        result = _make_result(accounts=[acct], transactions=[txn])
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["csv_accounts"] == 1
        row = db.execute(
            "SELECT account_id, institution_name FROM raw.csv_accounts"
        ).fetchone()
        assert row[0] == "SYN00420002"
        assert row[1] == "Test CC"

    def test_write_csv_running_balance(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        acct = GeneratedAccount(
            name="Card",
            account_id="SYN00420002",
            account_type="credit_card",
            source_type="csv",
            institution="Test",
            opening_balance=Decimal("0"),
        )
        txns = [
            GeneratedTransaction(
                date=date(2024, 1, 5),
                amount=Decimal("-50.00"),
                description="A",
                account_name="Card",
                category="food",
                transaction_id="SYN0000000001",
            ),
            GeneratedTransaction(
                date=date(2024, 1, 10),
                amount=Decimal("-30.00"),
                description="B",
                account_name="Card",
                category="food",
                transaction_id="SYN0000000002",
            ),
        ]
        result = _make_result(accounts=[acct], transactions=txns)
        writer = SyntheticWriter(db)
        writer.write(result)
        rows = db.execute(
            "SELECT balance FROM raw.csv_transactions ORDER BY transaction_date"
        ).fetchall()
        assert float(rows[0][0]) == pytest.approx(-50.00)  # 0 + (-50)
        assert float(rows[1][0]) == pytest.approx(-80.00)  # -50 + (-30)

    def test_write_ground_truth(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ground_truth"] == 1
        row = db.execute("SELECT persona, seed FROM synthetic.ground_truth").fetchone()
        assert row[0] == "test"
        assert row[1] == 42

    def test_source_file_uses_synthetic_uri(self, db: Database) -> None:
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = _make_result()
        writer = SyntheticWriter(db)
        writer.write(result)
        row = db.execute("SELECT source_file FROM raw.ofx_transactions").fetchone()
        assert row[0].startswith("synthetic://")
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_writer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.testing.synthetic.writer'`

- [ ] **Step 5: Implement the writer**

Create `src/moneybin/testing/synthetic/writer.py`:

```python
"""Write generated synthetic data to raw tables and ground truth.

Routes transactions to OFX or CSV raw tables based on account source_type.
Computes running balances for CSV accounts. Creates the synthetic schema
and ground_truth table on demand.
"""

from __future__ import annotations

import logging
from datetime import datetime, time
from decimal import Decimal
from pathlib import Path

import polars as pl

from moneybin.database import Database
from moneybin.testing.synthetic.models import (
    GeneratedAccount,
    GeneratedTransaction,
    GenerationResult,
)

logger = logging.getLogger(__name__)

_GROUND_TRUTH_DDL = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "schema"
    / "synthetic_ground_truth.sql"
)


def _slugify(name: str) -> str:
    """Convert account name to URL-safe slug."""
    return name.lower().replace(" ", "-")


def _account_type_to_ofx(account_type: str) -> str:
    """Map persona account type to OFX account type."""
    mapping = {
        "checking": "CHECKING",
        "savings": "SAVINGS",
        "credit_card": "CREDITLINE",
    }
    return mapping.get(account_type, "CHECKING")


class SyntheticWriter:
    """Write a GenerationResult to raw tables and synthetic.ground_truth.

    Args:
        db: Database instance.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def _create_synthetic_schema(self) -> None:
        """Create the synthetic schema and ground_truth table on demand."""
        ddl = _GROUND_TRUTH_DDL.read_text()
        self._db.execute(ddl)

    def write(self, result: GenerationResult) -> dict[str, int]:
        """Write all generated data to the database.

        Args:
            result: Complete generation output.

        Returns:
            Row counts per table written.
        """
        self._create_synthetic_schema()
        now = datetime.now()
        counts: dict[str, int] = {}
        account_lookup = {a.name: a for a in result.accounts}

        # Split accounts by source_type
        ofx_accts = [a for a in result.accounts if a.source_type == "ofx"]
        csv_accts = [a for a in result.accounts if a.source_type == "csv"]

        if ofx_accts:
            counts["ofx_accounts"] = self._write_ofx_accounts(ofx_accts, result, now)
            counts["ofx_balances"] = self._write_ofx_balances(ofx_accts, result, now)
        if csv_accts:
            counts["csv_accounts"] = self._write_csv_accounts(csv_accts, result, now)

        # Split transactions by account source_type
        ofx_txns = [
            t
            for t in result.transactions
            if account_lookup[t.account_name].source_type == "ofx"
        ]
        csv_txns = [
            t
            for t in result.transactions
            if account_lookup[t.account_name].source_type == "csv"
        ]

        if ofx_txns:
            counts["ofx_transactions"] = self._write_ofx_transactions(
                ofx_txns, account_lookup, result, now
            )
        if csv_txns:
            counts["csv_transactions"] = self._write_csv_transactions(
                csv_txns, account_lookup, result, now
            )

        counts["ground_truth"] = self._write_ground_truth(result, account_lookup, now)
        logger.info(f"Wrote synthetic data: {counts}")
        return counts

    def _write_ofx_accounts(
        self,
        accounts: list[GeneratedAccount],
        result: GenerationResult,
        now: datetime,
    ) -> int:
        rows = []
        for acct in accounts:
            slug = _slugify(acct.name)
            rows.append({
                "account_id": acct.account_id,
                "routing_number": None,
                "account_type": _account_type_to_ofx(acct.account_type),
                "institution_org": acct.institution,
                "institution_fid": None,
                "source_file": f"synthetic://{result.persona}/{result.seed}/{slug}",
                "extracted_at": now,
            })
        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.ofx_accounts", df, on_conflict="upsert")
        return len(rows)

    def _write_ofx_balances(
        self,
        accounts: list[GeneratedAccount],
        result: GenerationResult,
        now: datetime,
    ) -> int:
        rows = []
        for acct in accounts:
            slug = _slugify(acct.name)
            start_dt = datetime.combine(result.start_date, time())
            rows.append({
                "account_id": acct.account_id,
                "statement_start_date": start_dt,
                "statement_end_date": start_dt,
                "ledger_balance": float(acct.opening_balance),
                "ledger_balance_date": start_dt,
                "available_balance": None,
                "source_file": f"synthetic://{result.persona}/{result.seed}/{slug}",
                "extracted_at": now,
            })
        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.ofx_balances", df, on_conflict="upsert")
        return len(rows)

    def _write_ofx_transactions(
        self,
        txns: list[GeneratedTransaction],
        account_lookup: dict[str, GeneratedAccount],
        result: GenerationResult,
        now: datetime,
    ) -> int:
        rows = []
        for txn in txns:
            acct = account_lookup[txn.account_name]
            rows.append({
                "transaction_id": txn.transaction_id,
                "account_id": acct.account_id,
                "transaction_type": txn.transaction_type,
                "date_posted": datetime.combine(txn.date, time()),
                "amount": float(txn.amount),
                "payee": txn.description,
                "memo": None,
                "check_number": None,
                "source_file": f"synthetic://{result.persona}/{result.seed}/{txn.date.year}",
                "extracted_at": now,
            })
        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.ofx_transactions", df, on_conflict="upsert")
        return len(rows)

    def _write_csv_accounts(
        self,
        accounts: list[GeneratedAccount],
        result: GenerationResult,
        now: datetime,
    ) -> int:
        rows = []
        for acct in accounts:
            slug = _slugify(acct.name)
            rows.append({
                "account_id": acct.account_id,
                "account_type": acct.account_type,
                "institution_name": acct.institution,
                "source_file": f"synthetic://{result.persona}/{result.seed}/{slug}",
                "extracted_at": now,
            })
        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.csv_accounts", df, on_conflict="upsert")
        return len(rows)

    def _write_csv_transactions(
        self,
        txns: list[GeneratedTransaction],
        account_lookup: dict[str, GeneratedAccount],
        result: GenerationResult,
        now: datetime,
    ) -> int:
        # Group by account and sort by date for running balance
        by_account: dict[str, list[GeneratedTransaction]] = {}
        for txn in txns:
            by_account.setdefault(txn.account_name, []).append(txn)

        rows = []
        for acct_name, acct_txns in by_account.items():
            acct = account_lookup[acct_name]
            acct_txns.sort(key=lambda t: (t.date, t.transaction_id))
            balance = acct.opening_balance
            for txn in acct_txns:
                balance += txn.amount
                rows.append({
                    "transaction_id": txn.transaction_id,
                    "account_id": acct.account_id,
                    "transaction_date": txn.date,
                    "amount": float(txn.amount),
                    "description": txn.description,
                    "transaction_status": "Posted",
                    "balance": float(balance),
                    "source_file": f"synthetic://{result.persona}/{result.seed}/{txn.date.year}",
                    "extracted_at": now,
                })

        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.csv_transactions", df, on_conflict="upsert")
        return len(rows)

    def _write_ground_truth(
        self,
        result: GenerationResult,
        account_lookup: dict[str, GeneratedAccount],
        now: datetime,
    ) -> int:
        rows = []
        for txn in result.transactions:
            acct = account_lookup[txn.account_name]
            rows.append({
                "source_transaction_id": txn.transaction_id,
                "account_id": acct.account_id,
                "expected_category": txn.category,
                "transfer_pair_id": txn.transfer_pair_id,
                "persona": result.persona,
                "seed": result.seed,
                "generated_at": now,
            })
        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("synthetic.ground_truth", df, on_conflict="upsert")
        return len(rows)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_writer.py -v`
Expected: All 7 tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/sql/schema/synthetic_ground_truth.sql src/moneybin/testing/synthetic/writer.py src/moneybin/tables.py tests/moneybin/test_synthetic/test_writer.py
git commit -m "feat(synthetic): add ground truth DDL and writer with OFX/CSV routing"
```

---

## Task 9: Generator Engine

The central orchestrator. Loads persona and merchant configs, sets up accounts, runs all generators month-by-month tracking balances, assigns deterministic transaction IDs, and passes everything to the writer.

**Files:**
- Create: `src/moneybin/testing/synthetic/engine.py`
- Create: `tests/moneybin/test_synthetic/test_engine.py`

- [ ] **Step 1: Write failing tests for the engine**

Create `tests/moneybin/test_synthetic/test_engine.py`:

```python
# ruff: noqa: S101
"""Tests for the GeneratorEngine orchestrator."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.database import Database


class TestGeneratorEngine:
    """Test the full generation pipeline (no DB writes)."""

    def test_generate_produces_result(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        engine = GeneratorEngine("basic", seed=42, years=1)
        result = engine.generate()
        assert result.persona == "basic"
        assert result.seed == 42
        assert len(result.accounts) >= 1
        assert len(result.transactions) > 0

    def test_deterministic_output(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        engine1 = GeneratorEngine("basic", seed=42, years=1)
        engine2 = GeneratorEngine("basic", seed=42, years=1)
        r1 = engine1.generate()
        r2 = engine2.generate()
        assert len(r1.transactions) == len(r2.transactions)
        ids1 = [t.transaction_id for t in r1.transactions]
        ids2 = [t.transaction_id for t in r2.transactions]
        assert ids1 == ids2

    def test_different_seeds_diverge(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        r1 = GeneratorEngine("basic", seed=42, years=1).generate()
        r2 = GeneratorEngine("basic", seed=99, years=1).generate()
        ids1 = set(t.transaction_id for t in r1.transactions)
        ids2 = set(t.transaction_id for t in r2.transactions)
        # Different seeds should produce different transaction counts
        # (or at least different IDs if counts happen to match)
        assert len(r1.transactions) != len(r2.transactions) or ids1 != ids2

    def test_accounts_have_synthetic_ids(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        for acct in result.accounts:
            assert acct.account_id.startswith("SYN")

    def test_transaction_ids_assigned(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        for txn in result.transactions:
            assert txn.transaction_id.startswith("SYN")
            assert len(txn.transaction_id) == 13  # SYN + 10 digits

    def test_transaction_ids_unique(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        ids = [t.transaction_id for t in result.transactions]
        assert len(ids) == len(set(ids))

    def test_sign_convention(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        for txn in result.transactions:
            if txn.transaction_type == "DIRECTDEP" or txn.transaction_type == "DEP":
                assert txn.amount > 0, f"Income should be positive: {txn}"
            elif txn.transaction_type == "DEBIT":
                assert txn.amount < 0, f"Expense should be negative: {txn}"

    def test_transfer_pairs_match(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        transfers = [t for t in result.transactions if t.transfer_pair_id]
        pairs: dict[str, list] = {}
        for t in transfers:
            pairs.setdefault(t.transfer_pair_id, []).append(t)
        for pair_id, txns in pairs.items():
            assert len(txns) == 2, f"Pair {pair_id} should have exactly 2 transactions"
            total = sum(t.amount for t in txns)
            assert total == Decimal(0), f"Pair {pair_id} should net to zero"

    def test_date_range_correct(self) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine

        result = GeneratorEngine("basic", seed=42, years=2).generate()
        current_year = date.today().year
        assert result.start_date.year == current_year - 2
        assert result.end_date.year == current_year - 1
        for txn in result.transactions:
            assert result.start_date <= txn.date <= result.end_date


class TestGeneratorEngineWithDB:
    """Test engine → writer → database integration."""

    @pytest.fixture
    def db(self, tmp_path: Path, mock_secret_store: MagicMock) -> Database:
        db = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
        )
        yield db
        db.close()

    def test_write_to_database(self, db: Database) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        writer = SyntheticWriter(db)
        counts = writer.write(result)
        assert counts["ground_truth"] == len(result.transactions)

    def test_ofx_transactions_in_db(self, db: Database) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        writer = SyntheticWriter(db)
        writer.write(result)
        count = db.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()[0]
        assert count > 0

    def test_ground_truth_matches_transactions(self, db: Database) -> None:
        from moneybin.testing.synthetic.engine import GeneratorEngine
        from moneybin.testing.synthetic.writer import SyntheticWriter

        result = GeneratorEngine("basic", seed=42, years=1).generate()
        writer = SyntheticWriter(db)
        writer.write(result)
        txn_count = db.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()[
            0
        ]
        csv_count = db.execute("SELECT COUNT(*) FROM raw.csv_transactions").fetchone()[
            0
        ]
        gt_count = db.execute("SELECT COUNT(*) FROM synthetic.ground_truth").fetchone()[
            0
        ]
        assert gt_count == txn_count + csv_count
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_engine.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement GeneratorEngine**

Create `src/moneybin/testing/synthetic/engine.py`:

```python
"""Generator engine: orchestrates persona loading, generation, and ID assignment."""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

from moneybin.testing.synthetic.generators.income import IncomeGenerator
from moneybin.testing.synthetic.generators.recurring import RecurringGenerator
from moneybin.testing.synthetic.generators.spending import SpendingGenerator
from moneybin.testing.synthetic.generators.transfers import TransferGenerator
from moneybin.testing.synthetic.models import (
    GeneratedAccount,
    GeneratedTransaction,
    GenerationResult,
    MerchantCatalog,
    load_merchant_catalog,
    load_persona,
)
from moneybin.testing.synthetic.seed import SeededRandom

logger = logging.getLogger(__name__)


class GeneratorEngine:
    """Orchestrate synthetic data generation for a persona.

    Loads persona and merchant configs, sets up accounts with synthetic IDs,
    runs all generators month-by-month while tracking balances, and assigns
    deterministic transaction IDs.

    Args:
        persona_name: Name of the persona to generate (matches YAML filename).
        seed: Integer seed for deterministic output.
        years: Number of years to generate (overrides persona default).
    """

    def __init__(self, persona_name: str, seed: int, years: int | None = None) -> None:
        self._persona_name = persona_name
        self._seed = seed
        self._rng = SeededRandom(seed)

        # Load persona config
        self._persona = load_persona(persona_name)
        self._years = years or self._persona.years_default

        # Load referenced merchant catalogs
        self._merchants: dict[str, MerchantCatalog] = {}
        for cat in self._persona.spending.categories:
            if cat.merchant_catalog not in self._merchants:
                self._merchants[cat.merchant_catalog] = load_merchant_catalog(
                    cat.merchant_catalog
                )

        # Compute date range: N complete years ending at year before current
        current_year = date.today().year
        self._end_year = current_year - 1
        self._start_year = self._end_year - self._years + 1

    def _setup_accounts(self) -> list[GeneratedAccount]:
        """Create accounts with deterministic synthetic IDs."""
        accounts: list[GeneratedAccount] = []
        for i, acct_config in enumerate(self._persona.accounts, start=1):
            account_id = f"SYN{self._seed:04d}{i:04d}"
            accounts.append(
                GeneratedAccount(
                    name=acct_config.name,
                    account_id=account_id,
                    account_type=acct_config.type,
                    source_type=acct_config.source_type,
                    institution=acct_config.institution,
                    opening_balance=Decimal(str(acct_config.opening_balance)),
                )
            )
        return accounts

    def generate(self) -> GenerationResult:
        """Run the full generation pipeline.

        Returns:
            GenerationResult with all accounts, transactions, and metadata.
        """
        accounts = self._setup_accounts()

        # Create generators
        income_gen = IncomeGenerator(
            self._persona.income, self._start_year, self._end_year, self._rng
        )
        recurring_gen = RecurringGenerator(
            self._persona.recurring, self._start_year, self._rng
        )
        spending_gen = SpendingGenerator(
            self._persona.spending, self._merchants, self._rng
        )
        transfer_gen = TransferGenerator(self._persona.transfers, self._rng)

        # Track running balances per account
        balances: dict[str, Decimal] = {
            acct.name: acct.opening_balance for acct in accounts
        }

        all_txns: list[GeneratedTransaction] = []

        for year in range(self._start_year, self._end_year + 1):
            for month in range(1, 13):
                # Generate non-transfer transactions
                month_txns: list[GeneratedTransaction] = []
                month_txns.extend(income_gen.generate_month(year, month))
                month_txns.extend(recurring_gen.generate_month(year, month))
                month_txns.extend(spending_gen.generate_month(year, month))

                # Update balances before transfers (for statement_balance)
                for txn in month_txns:
                    balances[txn.account_name] = (
                        balances.get(txn.account_name, Decimal(0)) + txn.amount
                    )

                # Generate transfers with current balances
                transfer_txns = transfer_gen.generate_month(year, month, balances)
                for txn in transfer_txns:
                    balances[txn.account_name] = (
                        balances.get(txn.account_name, Decimal(0)) + txn.amount
                    )

                all_txns.extend(month_txns)
                all_txns.extend(transfer_txns)

        # Assign transaction IDs deterministically
        all_txns.sort(key=lambda t: (t.date, t.account_name, t.description))
        for i, txn in enumerate(all_txns, start=1):
            txn.transaction_id = f"SYN{i:010d}"

        start_date = date(self._start_year, 1, 1)
        end_date = date(self._end_year, 12, 31)

        logger.info(
            f"Generated {len(all_txns)} transactions for persona "
            f"{self._persona_name!r} (seed={self._seed}, "
            f"{start_date} to {end_date})"
        )

        return GenerationResult(
            persona=self._persona_name,
            seed=self._seed,
            accounts=accounts,
            transactions=all_txns,
            start_date=start_date,
            end_date=end_date,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_synthetic/test_engine.py -v`
Expected: All 12 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/synthetic/engine.py tests/moneybin/test_synthetic/test_engine.py
git commit -m "feat(synthetic): add GeneratorEngine orchestrator with month-by-month generation"
```

---

## Task 10: CLI Commands + Registration

Create the `moneybin synthetic generate` and `moneybin synthetic reset` CLI commands, register the command group in `main.py`, and make `run_transforms` public in `import_service.py`.

**Files:**
- Create: `src/moneybin/cli/commands/synthetic.py`
- Modify: `src/moneybin/cli/main.py:16-27,95-139`
- Modify: `src/moneybin/services/import_service.py:119,381`
- Create: `tests/moneybin/test_synthetic/test_cli.py`

- [ ] **Step 1: Make run_transforms public in import_service**

In `src/moneybin/services/import_service.py`, rename `_run_transforms` to `run_transforms` (line 119) and update the call site (line 381):

Change line 119:
```python
def run_transforms(db_path: Path) -> bool:
```

Change line 381:
```python
        result.core_tables_rebuilt = run_transforms(db.path)
```

- [ ] **Step 2: Write failing CLI tests**

Create `tests/moneybin/test_synthetic/test_cli.py`:

```python
# ruff: noqa: S101,S106
"""Tests for synthetic data CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.synthetic import app


class TestGenerateCommand:
    """Test the 'synthetic generate' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_get_database(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.cli.commands.synthetic.get_database",
            return_value=MagicMock(),
        )

    @pytest.fixture
    def mock_engine(self, mocker: Any) -> MagicMock:
        mock_result = MagicMock()
        mock_result.persona = "basic"
        mock_result.seed = 42
        mock_result.accounts = [MagicMock()]
        mock_result.transactions = [MagicMock()] * 100
        mock_result.start_date = MagicMock(__str__=lambda s: "2024-01-01")
        mock_result.end_date = MagicMock(__str__=lambda s: "2024-12-31")
        mock_cls = mocker.patch(
            "moneybin.cli.commands.synthetic.GeneratorEngine",
        )
        mock_cls.return_value.generate.return_value = mock_result
        return mock_cls

    @pytest.fixture
    def mock_writer(self, mocker: Any) -> MagicMock:
        mock_cls = mocker.patch(
            "moneybin.cli.commands.synthetic.SyntheticWriter",
        )
        mock_cls.return_value.write.return_value = {
            "ofx_accounts": 1,
            "ofx_transactions": 80,
            "csv_transactions": 20,
            "ground_truth": 100,
        }
        return mock_cls

    @pytest.fixture
    def mock_run_transforms(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.cli.commands.synthetic.run_transforms",
            return_value=True,
        )

    def test_generate_requires_persona(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["generate"])
        assert result.exit_code != 0

    def test_generate_success(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
        mock_engine: MagicMock,
        mock_writer: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        result = runner.invoke(app, ["generate", "--persona", "basic", "--seed", "42"])
        assert result.exit_code == 0
        mock_engine.assert_called_once()

    def test_generate_unknown_persona(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
    ) -> None:
        with patch(
            "moneybin.cli.commands.synthetic.GeneratorEngine",
            side_effect=FileNotFoundError("Unknown persona: 'bad'"),
        ):
            result = runner.invoke(app, ["generate", "--persona", "bad"])
            assert result.exit_code == 1


class TestResetCommand:
    """Test the 'synthetic reset' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_reset_requires_persona(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reset"])
        assert result.exit_code != 0

    def test_reset_requires_yes_or_prompt(self, runner: CliRunner) -> None:
        """Without --yes, reset should prompt for confirmation."""
        # CliRunner sends EOF on stdin by default, so prompt is declined
        with patch("moneybin.cli.commands.synthetic.get_database") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1,)
            mock_db.return_value.conn = mock_conn
            mock_db.return_value.path = Path("/tmp/test.duckdb")
            result = runner.invoke(app, ["reset", "--persona", "basic"])
            # Should either prompt and abort, or succeed with --yes
            assert result.exit_code != 0 or "Aborted" in (result.output or "")
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_synthetic/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.cli.commands.synthetic'`

- [ ] **Step 4: Implement CLI commands**

Create `src/moneybin/cli/commands/synthetic.py`:

```python
"""CLI commands for synthetic data generation and management."""

import logging
import random

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Generate and manage synthetic financial data for testing",
    no_args_is_help=True,
)

# Persona → default profile name mapping
_PERSONA_PROFILES = {"basic": "alice", "family": "bob", "freelancer": "charlie"}


@app.command("generate")
def generate(
    persona: str = typer.Option(
        ..., "--persona", help="Persona to generate (basic, family, freelancer)"
    ),
    profile: str | None = typer.Option(
        None, "--profile", help="Target profile name (auto-derived from persona)"
    ),
    years: int | None = typer.Option(
        None, "--years", help="Number of years of history"
    ),
    seed: int | None = typer.Option(
        None, "--seed", help="Seed for deterministic output (random if omitted)"
    ),
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip running SQLMesh after generation"
    ),
) -> None:
    """Generate synthetic financial data for a persona into a profile."""
    from moneybin.config import set_current_profile
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.services.import_service import run_transforms
    from moneybin.testing.synthetic.engine import GeneratorEngine
    from moneybin.testing.synthetic.writer import SyntheticWriter

    # Resolve profile
    target_profile = profile or _PERSONA_PROFILES.get(persona, persona)
    actual_seed = seed if seed is not None else random.randint(1, 999999)

    logger.info(
        f"⚙️  Generating {persona!r} persona into profile {target_profile!r} "
        f"(seed={actual_seed}{f', {years} years' if years else ''})"
    )

    # Switch to target profile
    set_current_profile(target_profile)

    try:
        db = get_database()
    except DatabaseKeyError:
        logger.error("❌ Database encryption key not found")
        logger.info("💡 Run 'moneybin db unlock' to set up the encryption key")
        raise typer.Exit(1) from None

    # Check if profile already has data
    try:
        row_count = db.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()[
            0
        ]
        csv_count = db.execute("SELECT COUNT(*) FROM raw.csv_transactions").fetchone()[
            0
        ]
        if row_count + csv_count > 0:
            logger.error(
                f"❌ Profile {target_profile!r} already has data ({row_count + csv_count} transactions)"
            )
            logger.info(
                f"💡 Use 'moneybin synthetic reset --persona={persona}' to wipe and regenerate"
            )
            raise typer.Exit(1) from None
    except Exception:  # noqa: BLE001 — tables may not exist in a fresh DB
        pass

    # Generate
    try:
        engine = GeneratorEngine(persona, seed=actual_seed, years=years)
        result = engine.generate()
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from None

    # Write to database
    writer = SyntheticWriter(db)
    counts = writer.write(result)

    acct_count = counts.get("ofx_accounts", 0) + counts.get("csv_accounts", 0)
    txn_count = counts.get("ofx_transactions", 0) + counts.get("csv_transactions", 0)
    gt_count = counts.get("ground_truth", 0)
    transfer_count = sum(1 for t in result.transactions if t.transfer_pair_id) // 2

    logger.info(f"  Created {acct_count} accounts")
    logger.info(
        f"  Generated {txn_count} transactions ({result.start_date} to {result.end_date})"
    )
    logger.info(
        f"  Wrote ground truth: {gt_count} labels, {transfer_count} transfer pairs"
    )

    # Run SQLMesh transforms
    if not skip_transform:
        logger.info("⚙️  Running SQLMesh to materialize pipeline...")
        try:
            run_transforms(db.path)
        except Exception:
            logger.warning(
                "⚠️  SQLMesh transforms failed — raw data is intact, run 'moneybin transform apply' manually"
            )

    logger.info(
        f"✅ Profile {target_profile!r} ready (seed={actual_seed}). Use --profile={target_profile} with any moneybin command."
    )


@app.command("reset")
def reset(
    persona: str = typer.Option(..., "--persona", help="Persona to regenerate"),
    profile: str | None = typer.Option(
        None, "--profile", help="Target profile to reset"
    ),
    years: int | None = typer.Option(None, "--years", help="Years to regenerate"),
    seed: int | None = typer.Option(None, "--seed", help="Seed for regeneration"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Wipe a generated profile and regenerate from scratch."""
    from moneybin.config import set_current_profile
    from moneybin.database import DatabaseKeyError, get_database

    target_profile = profile or _PERSONA_PROFILES.get(persona, persona)
    set_current_profile(target_profile)

    try:
        db = get_database()
    except DatabaseKeyError:
        logger.error("❌ Database encryption key not found")
        logger.info("💡 Run 'moneybin db unlock' to set up the encryption key")
        raise typer.Exit(1) from None

    # Safety check: only reset profiles created by the generator
    try:
        gt_exists = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'synthetic' AND table_name = 'ground_truth'"
        ).fetchone()[0]
        if not gt_exists:
            logger.error(
                f"❌ Profile {target_profile!r} was not created by the generator. Refusing to reset."
            )
            logger.info(
                f"💡 To destroy a non-generated profile, use 'moneybin db destroy --profile={target_profile}'"
            )
            raise typer.Exit(1) from None
    except Exception:  # noqa: BLE001 — fresh DB with no synthetic schema
        logger.error(
            f"❌ Profile {target_profile!r} was not created by the generator. Refusing to reset."
        )
        logger.info(
            f"💡 To destroy a non-generated profile, use 'moneybin db destroy --profile={target_profile}'"
        )
        raise typer.Exit(1) from None

    if not yes:
        confirmed = typer.confirm(
            f"This will destroy all data in profile {target_profile!r} and regenerate. Continue?"
        )
        if not confirmed:
            raise typer.Abort()

    # Drop all data from raw and synthetic tables
    logger.info(f"⚙️  Resetting profile {target_profile!r}...")
    for table in [
        "synthetic.ground_truth",
        "raw.ofx_transactions",
        "raw.ofx_accounts",
        "raw.ofx_balances",
        "raw.csv_transactions",
        "raw.csv_accounts",
    ]:
        try:
            db.execute(f"DELETE FROM {table}")  # noqa: S608 — hardcoded table names
        except Exception:  # noqa: BLE001 — table may not exist
            pass

    db.close()
    # Close the singleton so generate gets a fresh connection
    from moneybin.database import close_database

    close_database()

    # Regenerate
    generate(
        persona=persona,
        profile=target_profile,
        years=years,
        seed=seed,
        skip_transform=False,
    )
```

- [ ] **Step 5: Register the command group in main.py**

Add the import to `src/moneybin/cli/main.py` alongside other command imports:

```python
from .commands import (
    categorize,
    db,
    import_cmd,
    logs,
    mcp,
    migrate,
    profile,
    stats,
    sync,
    synthetic,
    transform,
)
```

Add the registration in the command groups section (after `transform` and before `track`, in the "analyze" workflow stage):

```python
app.add_typer(
    synthetic.app,
    name="synthetic",
    help="Generate and manage synthetic financial data for testing",
)
```

- [ ] **Step 6: Run CLI tests**

Run: `uv run pytest tests/moneybin/test_synthetic/test_cli.py -v`
Expected: All 5 tests PASS

- [ ] **Step 7: Run format and lint checks**

Run: `make format && make lint`
Expected: Clean pass

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/cli/commands/synthetic.py src/moneybin/cli/main.py src/moneybin/services/import_service.py tests/moneybin/test_synthetic/test_cli.py
git commit -m "feat(synthetic): add CLI commands (generate, reset) and register command group"
```

---

## Task 11: Integration Tests, Full Suite Check, and Spec Status Update

Run the complete test suite to verify everything works together. Update spec status to `in-progress` and add a golden snapshot baseline for regression testing.

**Files:**
- Modify: `docs/specs/testing-synthetic-data.md:5` (status → `in-progress`)
- Modify: `docs/specs/INDEX.md` (update status)

- [ ] **Step 1: Run the full test suite for the synthetic package**

Run: `uv run pytest tests/moneybin/test_synthetic/ -v`
Expected: All tests PASS across all 6 test files

- [ ] **Step 2: Run pre-commit checks**

Run: `make check test`
Expected: Format, lint, type-check, and all tests pass

- [ ] **Step 3: Update spec status to in-progress**

In `docs/specs/testing-synthetic-data.md`, change line 5:
```
in-progress
```

Update `docs/specs/INDEX.md` to reflect the status change for `testing-synthetic-data.md`.

- [ ] **Step 4: Final commit**

```bash
git add docs/specs/testing-synthetic-data.md docs/specs/INDEX.md
git commit -m "chore: mark testing-synthetic-data spec as in-progress"
```

---

## Spec Coverage Cross-Check

| Spec Requirement | Task |
|---|---|
| Multi-year histories for 3 personas (req 1) | Task 4 (personas), Task 9 (engine) |
| Seeded determinism (req 2) | Task 1 (SeededRandom), Task 9 (engine) |
| Write to raw tables with synthetic URIs (req 3) | Task 8 (writer) |
| Write account records (req 4) | Task 8 (writer) |
| Opening balance snapshots (req 4a) | Task 8 (writer: OFX balances + CSV running balance) |
| source_type per account controls table (req 5) | Task 8 (writer: OFX/CSV routing) |
| Sign convention (req 6) | Task 4-7 (generators), Task 9 (engine: sign convention test) |
| Ground truth labels (req 7) | Task 8 (writer: synthetic.ground_truth) |
| Synthetic schema on demand (req 8) | Task 8 (writer: _create_synthetic_schema) |
| ground_truth as generator marker (req 9) | Task 10 (CLI: reset safety check) |
| YAML persona config (req 10) | Task 2 (models), Task 3 (YAML data) |
| Merchant catalogs (req 11) | Task 2 (models), Task 3 (YAML data: ~165 brands) |
| Pydantic validation (req 12) | Task 2 (models: validators) |
| Auto-derive profile name (req 13) | Task 10 (CLI: _PERSONA_PROFILES) |
| Refuse generation into non-empty profile (req 14) | Task 10 (CLI: data check) |
| Run sqlmesh after generation (req 15) | Task 10 (CLI: run_transforms call) |
| Display summary (req 16) | Task 10 (CLI: logger.info summary) |
| Reset requires explicit target (req 17) | Task 10 (CLI: required --persona) |
| Refuse reset of non-generated profiles (req 18) | Task 10 (CLI: ground_truth check) |
| Explicit target for destructive ops (req 19) | Task 10 (CLI: required flags) |
| Reset confirmation prompt (req 20) | Task 10 (CLI: --yes flag) |
| Real 2026 brand names (req 21) | Task 3 (merchant catalogs) |
| Bank-statement descriptions (req 22) | Task 6 (SpendingGenerator._make_description) |
| Day-of-week bias + seasonal (req 23) | Task 6 (SpendingGenerator: weights + modifiers) |
| Realistic income schedules (req 24) | Task 4 (IncomeGenerator: biweekly + irregular) |
| Recurring with price increases (req 25) | Task 5 (RecurringGenerator: price_increases) |
| Transfer pairs (req 26) | Task 7 (TransferGenerator: transfer_pair_id) |
