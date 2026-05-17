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

# Keyed by (schema, table) -> {column: DataClass}. Every column in
# core.* and app.* must appear here; the completeness test enforces
# this. Judgment calls are documented in
# docs/specs/privacy-data-classification.md ("Classification Audit").
CLASSIFICATION: dict[tuple[str, str], dict[str, DataClass]] = {
    ("app", "account_settings"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "account_subtype": DataClass.TXN_TYPE,
        "archived": DataClass.TXN_TYPE,
        "credit_limit": DataClass.BALANCE,
        "display_name": DataClass.USER_NOTE,
        "holder_category": DataClass.TXN_TYPE,
        "include_in_net_worth": DataClass.TXN_TYPE,
        "iso_currency_code": DataClass.CURRENCY,
        "last_four": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "official_name": DataClass.INSTITUTION,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "audit_log"): {
        "action": DataClass.TXN_TYPE,
        "actor": DataClass.TXN_TYPE,
        "after_value": DataClass.TXN_AMOUNT,
        "audit_id": DataClass.RECORD_ID,
        "before_value": DataClass.TXN_AMOUNT,
        "context_json": DataClass.DESCRIPTION,
        "occurred_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "parent_audit_id": DataClass.RECORD_ID,
        "target_id": DataClass.RECORD_ID,
        "target_schema": DataClass.RECORD_ID,
        "target_table": DataClass.RECORD_ID,
    },
    ("app", "balance_assertions"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "assertion_date": DataClass.TXN_DATE,
        "balance": DataClass.BALANCE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "notes": DataClass.USER_NOTE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "budgets"): {
        "budget_id": DataClass.RECORD_ID,
        "category": DataClass.CATEGORY,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "end_month": DataClass.TXN_DATE,
        "monthly_amount": DataClass.TXN_AMOUNT,
        "start_month": DataClass.TXN_DATE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "categorization_rules"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "category": DataClass.CATEGORY,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "created_by": DataClass.TXN_TYPE,
        "is_active": DataClass.TXN_TYPE,
        "match_type": DataClass.TXN_TYPE,
        "max_amount": DataClass.TXN_AMOUNT,
        "merchant_pattern": DataClass.MERCHANT_NAME,
        "min_amount": DataClass.TXN_AMOUNT,
        "name": DataClass.USER_NOTE,
        "priority": DataClass.AGGREGATE,
        "rule_id": DataClass.RECORD_ID,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "category_overrides"): {
        "category_id": DataClass.CATEGORY,
        "is_active": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "imports"): {
        "import_id": DataClass.RECORD_ID,
        "labels": DataClass.USER_NOTE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "updated_by": DataClass.TXN_TYPE,
    },
    ("app", "match_decisions"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "account_id_b": DataClass.ACCOUNT_IDENTIFIER,
        "confidence_score": DataClass.AGGREGATE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "decided_by": DataClass.TXN_TYPE,
        "match_id": DataClass.RECORD_ID,
        "match_reason": DataClass.USER_NOTE,
        "match_signals": DataClass.AGGREGATE,
        "match_status": DataClass.TXN_TYPE,
        "match_tier": DataClass.TXN_TYPE,
        "match_type": DataClass.TXN_TYPE,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
        "source_origin_a": DataClass.TXN_TYPE,
        "source_origin_b": DataClass.TXN_TYPE,
        "source_transaction_id_a": DataClass.RECORD_ID,
        "source_transaction_id_b": DataClass.RECORD_ID,
        "source_type_a": DataClass.TXN_TYPE,
        "source_type_b": DataClass.TXN_TYPE,
    },
    ("app", "metrics"): {
        "bucket_bounds": DataClass.AGGREGATE,
        "bucket_counts": DataClass.AGGREGATE,
        "labels": DataClass.AGGREGATE,
        "metric_name": DataClass.AGGREGATE,
        "metric_type": DataClass.TXN_TYPE,
        "recorded_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "value": DataClass.AGGREGATE,
    },
    ("app", "proposed_rules"): {
        "category": DataClass.CATEGORY,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "decided_by": DataClass.TXN_TYPE,
        "match_type": DataClass.TXN_TYPE,
        "merchant_pattern": DataClass.MERCHANT_NAME,
        "proposed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "proposed_rule_id": DataClass.RECORD_ID,
        "sample_txn_ids": DataClass.RECORD_ID,
        "source": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
        "subcategory": DataClass.CATEGORY,
        "trigger_count": DataClass.AGGREGATE,
    },
    ("app", "rule_deactivations"): {
        "deactivated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "deactivation_id": DataClass.RECORD_ID,
        "new_category": DataClass.CATEGORY,
        "new_subcategory": DataClass.CATEGORY,
        "override_count": DataClass.AGGREGATE,
        "reason": DataClass.TXN_TYPE,
        "rule_id": DataClass.RECORD_ID,
    },
    ("app", "schema_migrations"): {
        "applied_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "checksum": DataClass.RECORD_ID,
        "content_hash": DataClass.RECORD_ID,
        "execution_ms": DataClass.AGGREGATE,
        "filename": DataClass.RECORD_ID,
        "success": DataClass.TXN_TYPE,
        "version": DataClass.AGGREGATE,
    },
    ("app", "seed_source_priority"): {
        "priority": DataClass.AGGREGATE,
        "source_type": DataClass.TXN_TYPE,
    },
    ("app", "tabular_formats"): {
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "date_format": DataClass.TXN_TYPE,
        "delimiter": DataClass.TXN_TYPE,
        "encoding": DataClass.TXN_TYPE,
        "field_mapping": DataClass.DESCRIPTION,
        "file_type": DataClass.TXN_TYPE,
        "header_signature": DataClass.DESCRIPTION,
        "institution_name": DataClass.INSTITUTION,
        "last_used_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "multi_account": DataClass.TXN_TYPE,
        "name": DataClass.RECORD_ID,
        "number_format": DataClass.TXN_TYPE,
        "sheet": DataClass.TXN_TYPE,
        "sign_convention": DataClass.TXN_TYPE,
        "skip_rows": DataClass.AGGREGATE,
        "skip_trailing_patterns": DataClass.DESCRIPTION,
        "source": DataClass.TXN_TYPE,
        "times_used": DataClass.AGGREGATE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "transaction_categories"): {
        "categorized_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "categorized_by": DataClass.TXN_TYPE,
        "category": DataClass.CATEGORY,
        "confidence": DataClass.AGGREGATE,
        "merchant_id": DataClass.RECORD_ID,
        "rule_id": DataClass.RECORD_ID,
        "subcategory": DataClass.CATEGORY,
        "transaction_id": DataClass.RECORD_ID,
    },
    ("app", "transaction_notes"): {
        "author": DataClass.TXN_TYPE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "note_id": DataClass.RECORD_ID,
        "text": DataClass.USER_NOTE,
        "transaction_id": DataClass.RECORD_ID,
    },
    ("app", "transaction_splits"): {
        "amount": DataClass.TXN_AMOUNT,
        "category": DataClass.CATEGORY,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "created_by": DataClass.TXN_TYPE,
        "note": DataClass.USER_NOTE,
        "ord": DataClass.AGGREGATE,
        "split_id": DataClass.RECORD_ID,
        "subcategory": DataClass.CATEGORY,
        "transaction_id": DataClass.RECORD_ID,
    },
    ("app", "transaction_tags"): {
        "applied_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "applied_by": DataClass.TXN_TYPE,
        "tag": DataClass.USER_NOTE,
        "transaction_id": DataClass.RECORD_ID,
    },
    ("app", "user_categories"): {
        "category": DataClass.CATEGORY,
        "category_id": DataClass.RECORD_ID,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "description": DataClass.CATEGORY,
        "is_active": DataClass.TXN_TYPE,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "user_merchants"): {
        "canonical_name": DataClass.MERCHANT_NAME,
        "category": DataClass.CATEGORY,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "created_by": DataClass.TXN_TYPE,
        "exemplars": DataClass.MERCHANT_NAME,
        "match_type": DataClass.TXN_TYPE,
        "merchant_id": DataClass.RECORD_ID,
        "raw_pattern": DataClass.MERCHANT_NAME,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "versions"): {
        "component": DataClass.TXN_TYPE,
        "installed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "previous_version": DataClass.AGGREGATE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "version": DataClass.AGGREGATE,
    },
    ("core", "bridge_transfers"): {
        "amount": DataClass.TXN_AMOUNT,
        "credit_transaction_id": DataClass.RECORD_ID,
        "date_offset_days": DataClass.AGGREGATE,
        "debit_transaction_id": DataClass.RECORD_ID,
        "transfer_id": DataClass.RECORD_ID,
    },
    ("core", "dim_accounts"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "account_subtype": DataClass.TXN_TYPE,
        "account_type": DataClass.TXN_TYPE,
        "archived": DataClass.TXN_TYPE,
        "credit_limit": DataClass.BALANCE,
        "display_name": DataClass.USER_NOTE,
        "extracted_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "holder_category": DataClass.TXN_TYPE,
        "include_in_net_worth": DataClass.TXN_TYPE,
        "institution_fid": DataClass.INSTITUTION,
        "institution_name": DataClass.INSTITUTION,
        "iso_currency_code": DataClass.CURRENCY,
        "last_four": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "loaded_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "official_name": DataClass.INSTITUTION,
        "routing_number": DataClass.ROUTING_NUMBER,
        "source_file": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "dim_categories"): {
        "category": DataClass.CATEGORY,
        "category_id": DataClass.CATEGORY,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "description": DataClass.CATEGORY,
        "is_active": DataClass.TXN_TYPE,
        "is_default": DataClass.TXN_TYPE,
        "plaid_detailed": DataClass.CATEGORY,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "dim_merchants"): {
        "canonical_name": DataClass.MERCHANT_NAME,
        "category": DataClass.CATEGORY,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "created_by": DataClass.TXN_TYPE,
        "exemplars": DataClass.MERCHANT_NAME,
        "match_type": DataClass.TXN_TYPE,
        "merchant_id": DataClass.RECORD_ID,
        "raw_pattern": DataClass.MERCHANT_NAME,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "fct_balances"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "balance": DataClass.BALANCE,
        "balance_date": DataClass.TXN_DATE,
        "source_ref": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "fct_balances_daily"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "balance": DataClass.BALANCE,
        "balance_date": DataClass.TXN_DATE,
        "is_observed": DataClass.TXN_TYPE,
        "observation_source": DataClass.TXN_TYPE,
        "reconciliation_delta": DataClass.BALANCE,
    },
    ("core", "fct_transaction_lines"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "description": DataClass.DESCRIPTION,
        "is_pending": DataClass.TXN_TYPE,
        "is_transfer": DataClass.TXN_TYPE,
        "line_amount": DataClass.TXN_AMOUNT,
        "line_category": DataClass.CATEGORY,
        "line_id": DataClass.RECORD_ID,
        "line_kind": DataClass.TXN_TYPE,
        "line_note": DataClass.USER_NOTE,
        "line_subcategory": DataClass.CATEGORY,
        "merchant_name": DataClass.MERCHANT_NAME,
        "source_count": DataClass.AGGREGATE,
        "source_type": DataClass.TXN_TYPE,
        "transaction_date": DataClass.TXN_DATE,
        "transaction_id": DataClass.RECORD_ID,
        "transaction_month": DataClass.TXN_DATE,
        "transaction_year": DataClass.TXN_DATE,
        "transaction_year_month": DataClass.TXN_DATE,
        "transaction_year_quarter": DataClass.TXN_DATE,
        "transfer_pair_id": DataClass.RECORD_ID,
    },
    ("core", "fct_transactions"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
        "amount_absolute": DataClass.TXN_AMOUNT,
        "authorized_date": DataClass.TXN_DATE,
        "categorized_by": DataClass.TXN_TYPE,
        "category": DataClass.CATEGORY,
        "check_number": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "currency_code": DataClass.CURRENCY,
        "description": DataClass.DESCRIPTION,
        "has_splits": DataClass.AGGREGATE,
        "is_pending": DataClass.TXN_TYPE,
        "is_transfer": DataClass.TXN_TYPE,
        "loaded_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "location_address": DataClass.MERCHANT_NAME,
        "location_city": DataClass.MERCHANT_NAME,
        "location_country": DataClass.MERCHANT_NAME,
        "location_latitude": DataClass.MERCHANT_NAME,
        "location_longitude": DataClass.MERCHANT_NAME,
        "location_postal_code": DataClass.MERCHANT_NAME,
        "location_region": DataClass.MERCHANT_NAME,
        "match_confidence": DataClass.AGGREGATE,
        "memo": DataClass.DESCRIPTION,
        "merchant_name": DataClass.MERCHANT_NAME,
        "note_count": DataClass.AGGREGATE,
        "notes": DataClass.USER_NOTE,
        "payment_channel": DataClass.TXN_TYPE,
        "pending_transaction_id": DataClass.RECORD_ID,
        "source_count": DataClass.AGGREGATE,
        "source_extracted_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "source_type": DataClass.TXN_TYPE,
        "split_count": DataClass.AGGREGATE,
        "splits": DataClass.TXN_AMOUNT,
        "subcategory": DataClass.CATEGORY,
        "tag_count": DataClass.AGGREGATE,
        "tags": DataClass.USER_NOTE,
        "transaction_date": DataClass.TXN_DATE,
        "transaction_day": DataClass.TXN_DATE,
        "transaction_day_of_week": DataClass.TXN_DATE,
        "transaction_direction": DataClass.TXN_TYPE,
        "transaction_id": DataClass.RECORD_ID,
        "transaction_month": DataClass.TXN_DATE,
        "transaction_type": DataClass.TXN_TYPE,
        "transaction_year": DataClass.TXN_DATE,
        "transaction_year_month": DataClass.TXN_DATE,
        "transaction_year_quarter": DataClass.TXN_DATE,
        "transfer_pair_id": DataClass.RECORD_ID,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
}
