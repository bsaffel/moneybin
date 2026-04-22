"""Generator engine: orchestrates persona loading, generation, and ID assignment."""

from __future__ import annotations

import logging
import time
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

    Raises:
        FileNotFoundError: If the persona or any referenced merchant catalog
            YAML file does not exist.
    """

    def __init__(self, persona_name: str, seed: int, years: int | None = None) -> None:  # noqa: D107 — args documented in class docstring
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
        from moneybin.metrics.registry import (
            SYNTHETIC_GENERATED_TRANSACTIONS_TOTAL,
            SYNTHETIC_GENERATION_DURATION_SECONDS,
        )

        start_time = time.monotonic()
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

        duration = time.monotonic() - start_time
        SYNTHETIC_GENERATED_TRANSACTIONS_TOTAL.labels(persona=self._persona_name).inc(
            len(all_txns)
        )
        SYNTHETIC_GENERATION_DURATION_SECONDS.labels(
            persona=self._persona_name
        ).observe(duration)

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
