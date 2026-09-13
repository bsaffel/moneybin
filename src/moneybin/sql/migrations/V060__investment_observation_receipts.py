"""V060: preserve investment revisions and deterministic delivery receipts."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def _source_value(value: object) -> str:
    # Frozen with V060: later adapter changes must not alter an upgrade's digest.
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported observation value type: {type(value).__name__}")


_CREATE_REVISIONS = """
CREATE TABLE raw.plaid_investment_transactions (
    investment_transaction_id VARCHAR NOT NULL, account_id VARCHAR NOT NULL,
    security_id VARCHAR, transaction_date DATE NOT NULL,
    transaction_datetime TIMESTAMP, transaction_name VARCHAR,
    quantity DECIMAL(28,10), amount DECIMAL(18,2) NOT NULL,
    price DECIMAL(28,10), fees DECIMAL(18,2), iso_currency_code VARCHAR,
    unofficial_currency_code VARCHAR, investment_transaction_type VARCHAR,
    investment_transaction_subtype VARCHAR, observation_version VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL DEFAULT 'plaid', source_origin VARCHAR NOT NULL,
    PRIMARY KEY (investment_transaction_id, source_origin, observation_version)
)
"""


def migrate(conn: Any) -> None:
    """Backfill existing observations and freeze legacy snapshot ordering once."""
    conn.execute("CREATE SEQUENCE IF NOT EXISTS raw.investment_ingestion_sequence")
    legacy = conn.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'plaid_investment_transactions'
          AND column_name = 'source_file'
    """).fetchone()
    if legacy:
        result = conn.execute("""
            SELECT * FROM raw.plaid_investment_transactions
            ORDER BY source_origin, extracted_at, source_file, investment_transaction_id
        """)
        columns = [column[0] for column in result.description]
        rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
        conn.execute("DROP TABLE raw.plaid_investment_transactions")
        conn.execute(_CREATE_REVISIONS)
        for row in rows:
            values = {
                key: value
                for key, value in row.items()
                if key not in {"source_file", "extracted_at", "loaded_at"}
            }
            content = json.dumps(
                values, sort_keys=True, separators=(",", ":"), default=_source_value
            )
            version = "plaid_" + hashlib.sha256(content.encode()).hexdigest()[:16]
            conn.execute(
                """
                INSERT INTO raw.plaid_investment_transactions (
                    investment_transaction_id, account_id, security_id,
                    transaction_date, transaction_datetime, transaction_name,
                    quantity, amount, price, fees, iso_currency_code,
                    unofficial_currency_code, investment_transaction_type,
                    investment_transaction_subtype, observation_version,
                    source_type, source_origin
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    row["investment_transaction_id"],
                    row["account_id"],
                    row["security_id"],
                    row["transaction_date"],
                    row["transaction_datetime"],
                    row["transaction_name"],
                    row["quantity"],
                    row["amount"],
                    row["price"],
                    row["fees"],
                    row["iso_currency_code"],
                    row["unofficial_currency_code"],
                    row["investment_transaction_type"],
                    row["investment_transaction_subtype"],
                    version,
                    row["source_type"],
                    row["source_origin"],
                ],
            )
            conn.execute(
                """
                INSERT INTO raw.plaid_investment_transaction_receipts (
                    investment_transaction_id, source_origin, source_file,
                    observation_version, extracted_at, loaded_at
                ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    row["investment_transaction_id"],
                    row["source_origin"],
                    row["source_file"],
                    version,
                    row["extracted_at"],
                    row["loaded_at"],
                ],
            )

    sequenced = conn.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'plaid_investment_holdings_snapshots'
          AND column_name = 'ingestion_sequence'
    """).fetchone()
    if not sequenced:
        snapshots = conn.execute("""
            SELECT source_origin, source_file, holdings_date, holdings_count,
                   transactions_window_start, source_type, extracted_at, loaded_at
            FROM raw.plaid_investment_holdings_snapshots
            ORDER BY source_origin, extracted_at, source_file
        """).fetchall()
        # Rebuild atomically: DuckDB cannot add NOT NULL after same-transaction updates.
        conn.execute("DROP TABLE raw.plaid_investment_holdings_snapshots")
        conn.execute("""
            CREATE TABLE raw.plaid_investment_holdings_snapshots (
                source_origin VARCHAR NOT NULL, source_file VARCHAR NOT NULL,
                holdings_date DATE, holdings_count INTEGER NOT NULL,
                transactions_window_start DATE NOT NULL,
                source_type VARCHAR NOT NULL DEFAULT 'plaid',
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ingestion_sequence BIGINT NOT NULL
                    DEFAULT nextval('raw.investment_ingestion_sequence'),
                PRIMARY KEY (source_origin, source_file)
            )
        """)
        for snapshot in snapshots:
            conn.execute(
                """
                INSERT INTO raw.plaid_investment_holdings_snapshots (
                    source_origin, source_file, holdings_date, holdings_count,
                    transactions_window_start, source_type, extracted_at, loaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                snapshot,
            )
