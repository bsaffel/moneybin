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
from typing import Any

import polars as pl

from moneybin.database import Database
from moneybin.tables import GROUND_TRUTH
from moneybin.testing.synthetic.models import (
    GeneratedAccount,
    GeneratedTransaction,
    GenerationResult,
)

logger = logging.getLogger(__name__)

_GROUND_TRUTH_DDL_PATH = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "schema"
    / "synthetic_ground_truth.sql"
)
_ground_truth_ddl: str | None = None


def _slugify(name: str) -> str:
    """Convert account name to URL-safe slug."""
    return name.lower().replace(" ", "-")


def _account_type_to_ofx(account_type: str) -> str:
    """Map persona account type to OFX account type."""
    mapping = {
        "checking": "CHECKING",
        "savings": "SAVINGS",
        "credit_card": "CREDITCARD",
    }
    return mapping.get(account_type, "CHECKING")


class SyntheticWriter:
    """Write a GenerationResult to raw tables and synthetic.ground_truth.

    Args:
        db: Database instance.
    """

    def __init__(self, db: Database) -> None:  # noqa: D107 — args documented in class docstring
        self._db = db

    def _create_synthetic_schema(self) -> None:
        """Create the synthetic schema and ground_truth table on demand."""
        global _ground_truth_ddl  # noqa: PLW0603 — module-level cache, read once
        if _ground_truth_ddl is None:
            _ground_truth_ddl = _GROUND_TRUTH_DDL_PATH.read_text()
        self._db.execute(_ground_truth_ddl)

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
            counts["tabular_accounts"] = self._write_tabular_accounts(
                csv_accts, result, now
            )

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
            counts["tabular_transactions"] = self._write_tabular_transactions(
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
        rows: list[dict[str, Any]] = []
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
        rows: list[dict[str, Any]] = []
        for acct in accounts:
            slug = _slugify(acct.name)
            start_dt = datetime.combine(result.start_date, time())
            rows.append({
                "account_id": acct.account_id,
                "statement_start_date": start_dt,
                "statement_end_date": start_dt,
                "ledger_balance": Decimal(str(round(acct.opening_balance, 2))),
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
        rows: list[dict[str, Any]] = []
        for txn in txns:
            acct = account_lookup[txn.account_name]
            rows.append({
                "source_transaction_id": txn.transaction_id,
                "account_id": acct.account_id,
                "transaction_type": txn.transaction_type,
                "date_posted": datetime.combine(txn.date, time()),
                "amount": Decimal(str(round(txn.amount, 2))),
                "payee": txn.description,
                "memo": None,
                "check_number": None,
                "source_file": f"synthetic://{result.persona}/{result.seed}/{txn.date.year}",
                "extracted_at": now,
            })
        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.ofx_transactions", df, on_conflict="upsert")
        return len(rows)

    def _write_tabular_accounts(
        self,
        accounts: list[GeneratedAccount],
        result: GenerationResult,
        now: datetime,
    ) -> int:
        rows: list[dict[str, Any]] = []
        for acct in accounts:
            slug = _slugify(acct.name)
            rows.append({
                "account_id": acct.account_id,
                "account_name": acct.name,
                "account_type": acct.account_type,
                "institution_name": acct.institution,
                "source_file": f"synthetic://{result.persona}/{result.seed}/{slug}",
                "source_type": "csv",
                "source_origin": f"synthetic_{result.persona}",
                "import_id": f"synthetic-{result.seed}",
                "extracted_at": now,
            })
        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.tabular_accounts", df, on_conflict="upsert")
        return len(rows)

    def _write_tabular_transactions(
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

        rows: list[dict[str, Any]] = []
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
                    "amount": Decimal(str(round(txn.amount, 2))),
                    "description": txn.description,
                    "status": "Posted",
                    "balance": Decimal(str(round(balance, 2))),
                    "source_file": f"synthetic://{result.persona}/{result.seed}/{txn.date.year}",
                    "source_type": "csv",
                    "source_origin": f"synthetic_{result.persona}",
                    "import_id": f"synthetic-{result.seed}",
                    "extracted_at": now,
                })

        df = pl.DataFrame(rows)
        self._db.ingest_dataframe("raw.tabular_transactions", df, on_conflict="upsert")
        return len(rows)

    def _write_ground_truth(
        self,
        result: GenerationResult,
        account_lookup: dict[str, GeneratedAccount],
        now: datetime,
    ) -> int:
        rows: list[dict[str, Any]] = []
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
        self._db.ingest_dataframe(GROUND_TRUTH.full_name, df, on_conflict="upsert")
        return len(rows)
