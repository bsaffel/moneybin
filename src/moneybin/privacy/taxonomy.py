"""Privacy data classification: tiers, classes, and column registry.

The taxonomy is the single source of truth for which columns in
``core.*`` and ``app.*`` carry which privacy class. Later PRs build
redaction, consent gates, and audit logging on top of this mapping —
mis-classifying a column here propagates to every downstream control,
so the audit recorded in
``docs/specs/privacy-data-classification.md`` (Classification Audit
section) is load-bearing.
"""

from __future__ import annotations

from enum import IntEnum, StrEnum


class Tier(IntEnum):
    """Privacy sensitivity tier. Integer ordering allows ``max(tier)``."""

    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class DataClass(StrEnum):
    """Privacy data class. Every column in core.* / app.* maps to one.

    The ``tier`` property returns the Tier each class belongs to. Add
    new members by extending the enum AND ``_TIER_BY_CLASS`` below.
    """

    ACCOUNT_IDENTIFIER = "account_identifier"
    ROUTING_NUMBER = "routing_number"
    INSTITUTION_ACCOUNT_NUMBER = "institution_account_number"
    BALANCE = "balance"
    TXN_AMOUNT = "txn_amount"
    INCOME_AMOUNT = "income_amount"
    MERCHANT_NAME = "merchant_name"
    DESCRIPTION = "description"
    USER_NOTE = "user_note"
    TXN_DATE = "txn_date"
    CATEGORY = "category"
    INSTITUTION = "institution"
    CURRENCY = "currency"
    TXN_TYPE = "txn_type"
    AGGREGATE = "aggregate"
    RECORD_ID = "record_id"
    TIMESTAMP_OBSERVABILITY = "timestamp_observability"

    @property
    def tier(self) -> Tier:
        """Return the privacy ``Tier`` this class belongs to."""
        return _TIER_BY_CLASS[self]


_TIER_BY_CLASS: dict[DataClass, Tier] = {
    DataClass.ACCOUNT_IDENTIFIER: Tier.CRITICAL,
    DataClass.ROUTING_NUMBER: Tier.CRITICAL,
    DataClass.INSTITUTION_ACCOUNT_NUMBER: Tier.CRITICAL,
    DataClass.BALANCE: Tier.HIGH,
    DataClass.TXN_AMOUNT: Tier.HIGH,
    DataClass.INCOME_AMOUNT: Tier.HIGH,
    DataClass.MERCHANT_NAME: Tier.MEDIUM,
    DataClass.DESCRIPTION: Tier.MEDIUM,
    DataClass.USER_NOTE: Tier.MEDIUM,
    DataClass.TXN_DATE: Tier.MEDIUM,
    DataClass.CATEGORY: Tier.LOW,
    DataClass.INSTITUTION: Tier.LOW,
    DataClass.CURRENCY: Tier.LOW,
    DataClass.TXN_TYPE: Tier.LOW,
    DataClass.AGGREGATE: Tier.LOW,
    DataClass.RECORD_ID: Tier.LOW,
    DataClass.TIMESTAMP_OBSERVABILITY: Tier.LOW,
}

# Populated in Task 3 from the live DuckDB catalog. Keyed by
# (schema, table) -> {column: DataClass}. Every column in core.* and
# app.* must appear here; CI test enforces this.
CLASSIFICATION: dict[tuple[str, str], dict[str, DataClass]] = {}
