"""V030: add Plaid's extended transaction columns to raw.plaid_transactions.

Captures the default-returned Plaid fields the broker previously discarded. Tier-1
fields (original_description, currency, authorized_date, pending_transaction_id,
payment_channel, check_number, location.*) flow on into core.fct_transactions; the
Tier-2 columns (merchant_entity_id, category_detailed, category_confidence) are
captured for later consumption by merchant-resolution / categorization increments.

Pure additive DDL — ``ADD COLUMN IF NOT EXISTS ... NULL`` with no DEFAULT, so no
backfill and the migration is a no-op on replay. Existing rows land NULL until
re-pulled (``moneybin sync pull --force`` re-fetches and upserts the values).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# (column, sql_type, comment) — applied in order; types match raw_plaid_transactions.sql.
_COLUMNS: list[tuple[str, str, str]] = [
    (
        "original_description",
        "VARCHAR",
        "Plaid original_description: the raw, unmodified bank-statement text "
        "(distinct from the cleaned description=name); NULL for non-Plaid sources.",
    ),
    ("iso_currency_code", "VARCHAR", "Plaid iso_currency_code (ISO 4217)."),
    (
        "authorized_date",
        "DATE",
        "Plaid authorized_date: when the transaction was authorized.",
    ),
    (
        "pending_transaction_id",
        "VARCHAR",
        "Plaid pending_transaction_id: the pending txn this posted row resolved.",
    ),
    (
        "payment_channel",
        "VARCHAR",
        "Plaid payment_channel: online, in store, or other.",
    ),
    (
        "check_number",
        "VARCHAR",
        "Plaid check_number for check transactions; NULL otherwise.",
    ),
    (
        "merchant_entity_id",
        "VARCHAR",
        "Plaid merchant_entity_id: stable cross-connection merchant id "
        "(Tier-2a: consumed by merchant resolution, not yet wired to core).",
    ),
    ("location_address", "VARCHAR", "Plaid location.address: merchant street address."),
    ("location_city", "VARCHAR", "Plaid location.city."),
    ("location_region", "VARCHAR", "Plaid location.region (state)."),
    ("location_postal_code", "VARCHAR", "Plaid location.postal_code."),
    ("location_country", "VARCHAR", "Plaid location.country."),
    ("location_latitude", "DOUBLE", "Plaid location.lat."),
    ("location_longitude", "DOUBLE", "Plaid location.lon."),
    (
        "category_detailed",
        "VARCHAR",
        "Plaid personal_finance_category.detailed "
        "(Tier-2b: consumed by categorization, not yet wired to core).",
    ),
    (
        "category_confidence",
        "VARCHAR",
        "Plaid personal_finance_category.confidence_level (Tier-2b).",
    ),
]


def migrate(conn: object) -> None:
    """Add the extended Plaid transaction columns. Idempotent."""
    for name, sql_type, comment in _COLUMNS:
        logger.info(f"V030: ADD COLUMN IF NOT EXISTS raw.plaid_transactions.{name}")
        conn.execute(  # type: ignore[attr-defined]
            f"ALTER TABLE raw.plaid_transactions ADD COLUMN IF NOT EXISTS {name} {sql_type}"
        )
        # DuckDB's COMMENT ON does not accept `?` parameters; use a literal like V029.
        # name + comment come from the hardcoded _COLUMNS list (no user input); comments
        # contain no apostrophes.
        conn.execute(  # type: ignore[attr-defined]  # noqa: S608  # DDL from hardcoded constants, not user input
            f"COMMENT ON COLUMN raw.plaid_transactions.{name} IS '{comment}'"
        )
