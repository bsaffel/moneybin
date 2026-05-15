"""Transfer generation: account-to-account moves with statement_balance."""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

from moneybin.synthetic.models import GeneratedTransaction, TransferConfig
from moneybin.synthetic.seed import SeededRandom

logger = logging.getLogger(__name__)


class TransferGenerator:
    """Generate account-to-account transfers.

    Both sides of each transfer share a ``transfer_pair_id`` for
    ground-truth scoring of transfer detection accuracy.

    Args:
        transfers: Transfer configurations from persona YAML.
        rng: Seeded random number generator.
    """

    def __init__(self, transfers: list[TransferConfig], rng: SeededRandom) -> None:  # noqa: D107 — args documented in class docstring
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
                logger.warning(
                    f"⚠️  Skipping transfer {config.from_account!r} → "
                    f"{config.to_account!r}: schedule {config.schedule!r} "
                    f"not implemented in v1"
                )
                continue

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
