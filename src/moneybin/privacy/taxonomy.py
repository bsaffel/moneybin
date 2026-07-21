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

import re
from enum import IntEnum, StrEnum

# Trailing-anchored `[class: <name>]` sigil. The catalog stores the human
# description as the prefix and the sigil as the suffix; this regex strips
# the suffix so the prefix can be compared / restored. Public so
# `schema._apply_comments` can recognize sigils written by the privacy sync.
SIGIL_RE = re.compile(r"\s*\[class:\s*[a-z0-9_]+\s*\]\s*$")


def strip_sigil(comment: str | None) -> str:
    """Return ``comment`` with any trailing ``[class: ...]`` sigil removed.

    Whitespace before the sigil is consumed. An input of ``None`` becomes
    the empty string.
    """
    return SIGIL_RE.sub("", comment or "").rstrip()


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
    # Not a classification — the absence of one. Assigned by the fail-closed
    # paths in ``sql_lineage`` / ``sql_query`` when a column reaches the user
    # WITHOUT lineage having positively established what it holds (an
    # undeclared deployed column, or a runtime column no projection resolved
    # to). It is CRITICAL and masked WHOLE: a partial mask such as
    # ACCOUNT_IDENTIFIER's ``"****" + value[-4:]`` would surface the last four
    # characters of a value we cannot name, and the whole point of this class
    # is that we do not know what those characters are. Never write it into
    # ``CLASSIFICATION`` or a ``@report(classes=…)`` map — declaring a column
    # "unresolved" defeats the completeness tests that exist to catch gaps.
    UNRESOLVED = "unresolved"

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
    DataClass.UNRESOLVED: Tier.CRITICAL,
}

# Keyed by (schema, table) -> {column: DataClass}. Every column in
# core.* and app.* must appear here; the completeness test enforces
# this. Judgment calls are documented in
# docs/specs/privacy-data-classification.md ("Classification Audit").
CLASSIFICATION: dict[tuple[str, str], dict[str, DataClass]] = {
    ("app", "account_link_decisions"): {
        "candidate_account_id": DataClass.RECORD_ID,
        "confidence_score": DataClass.AGGREGATE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "decided_by": DataClass.TXN_TYPE,
        "decision_id": DataClass.RECORD_ID,
        "match_reason": DataClass.USER_NOTE,
        # Unlike match_decisions.match_signals (scores), this carries weak-signal
        # values that include account digits (institution_last4) — masked, not the
        # LOW-tier AGGREGATE passthrough. JSON masking is coarse here; the typed
        # accounts_links surface (M1S.5) presents signals with structured masking.
        "match_signals": DataClass.ACCOUNT_IDENTIFIER,
        "provisional_account_id": DataClass.RECORD_ID,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
    },
    ("app", "account_links"): {
        # Opaque minted canonical handle (spec D1/D6) — a record id, not PII; it
        # passes through so agents/users can read it back as a parameter.
        "account_id": DataClass.RECORD_ID,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "decided_by": DataClass.TXN_TYPE,
        "link_id": DataClass.RECORD_ID,
        "ref_kind": DataClass.TXN_TYPE,
        # Conservative (M1S.1): ref_value can be a full account number for
        # full_number/source_native, so it is masked by default. Per-ref_kind
        # un-masking of opaque persistent_tokens is an M1S.5 read-surface concern.
        "ref_value": DataClass.ACCOUNT_IDENTIFIER,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
        "source_origin": DataClass.TXN_TYPE,
        "source_type": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
    },
    ("app", "account_settings"): {
        "account_id": DataClass.RECORD_ID,
        "account_subtype": DataClass.TXN_TYPE,
        "archived": DataClass.TXN_TYPE,
        "credit_limit": DataClass.BALANCE,
        "currency_code": DataClass.CURRENCY,
        "display_name": DataClass.USER_NOTE,
        "holder_category": DataClass.TXN_TYPE,
        "default_cost_basis_method": DataClass.TXN_TYPE,
        "include_in_net_worth": DataClass.TXN_TYPE,
        "last_four": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "official_name": DataClass.INSTITUTION,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "ai_consent_grants"): {
        "grant_id": DataClass.RECORD_ID,
        "feature_category": DataClass.CATEGORY,
        "backend": DataClass.INSTITUTION,
        "consent_mode": DataClass.TXN_TYPE,
        "granted_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "revoked_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "grant_prompt": DataClass.DESCRIPTION,
    },
    ("app", "audit_log"): {
        "action": DataClass.TXN_TYPE,
        "actor": DataClass.TXN_TYPE,
        "after_value": DataClass.TXN_AMOUNT,
        "audit_id": DataClass.RECORD_ID,
        "before_value": DataClass.TXN_AMOUNT,
        "context_json": DataClass.DESCRIPTION,
        "is_undo": DataClass.TXN_TYPE,
        "occurred_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "operation_id": DataClass.RECORD_ID,
        "parent_audit_id": DataClass.RECORD_ID,
        "target_id": DataClass.RECORD_ID,
        "target_schema": DataClass.RECORD_ID,
        "target_table": DataClass.RECORD_ID,
        "undoes_operation_id": DataClass.RECORD_ID,
    },
    ("app", "balance_assertions"): {
        "account_id": DataClass.RECORD_ID,
        "assertion_date": DataClass.TXN_DATE,
        "balance": DataClass.BALANCE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "notes": DataClass.USER_NOTE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "budgets"): {
        "budget_id": DataClass.RECORD_ID,
        "category": DataClass.CATEGORY,
        "category_id": DataClass.RECORD_ID,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "end_month": DataClass.TXN_DATE,
        "monthly_amount": DataClass.TXN_AMOUNT,
        "start_month": DataClass.TXN_DATE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "categorization_decisions"): {
        "attempt_number": DataClass.AGGREGATE,
        "categorized_by": DataClass.TXN_TYPE,
        "category": DataClass.CATEGORY,
        "category_id": DataClass.RECORD_ID,
        "category_revision": DataClass.AGGREGATE,
        "confidence": DataClass.AGGREGATE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "decided_by": DataClass.TXN_TYPE,
        "decision_id": DataClass.RECORD_ID,
        "merchant_id": DataClass.RECORD_ID,
        "proposed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
        "rule_id": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
        "subcategory": DataClass.CATEGORY,
        "transaction_id": DataClass.RECORD_ID,
    },
    ("app", "categorization_rules"): {
        "account_id": DataClass.RECORD_ID,
        "category": DataClass.CATEGORY,
        "category_id": DataClass.RECORD_ID,
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
    ("app", "category_source_map"): {
        "category_id": DataClass.RECORD_ID,
        "code_level": DataClass.TXN_TYPE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "source_category_code": DataClass.CATEGORY,
        "source_taxonomy_version": DataClass.AGGREGATE,
        "source_type": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "gsheet_connections"): {
        "account_id": DataClass.RECORD_ID,
        "account_name": DataClass.INSTITUTION,
        "adapter": DataClass.TXN_TYPE,
        "alias": DataClass.RECORD_ID,
        "column_mapping": DataClass.DESCRIPTION,
        "connection_id": DataClass.RECORD_ID,
        "consecutive_failure_count": DataClass.AGGREGATE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "date_format": DataClass.TXN_TYPE,
        "header_signature": DataClass.DESCRIPTION,
        "last_status_reason": DataClass.DESCRIPTION,
        "last_pull_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "last_pull_import_id": DataClass.RECORD_ID,
        "last_success_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "number_format": DataClass.TXN_TYPE,
        "sheet_gid": DataClass.RECORD_ID,
        "sheet_name": DataClass.INSTITUTION,
        "sign_convention": DataClass.TXN_TYPE,
        "skip_rows": DataClass.AGGREGATE,
        "skip_trailing_patterns": DataClass.DESCRIPTION,
        "spreadsheet_id": DataClass.RECORD_ID,
        "status": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "workbook_name": DataClass.INSTITUTION,
    },
    ("app", "export_destinations"): {
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "destination_id": DataClass.RECORD_ID,
        "kind": DataClass.TXN_TYPE,
        "local_path": DataClass.RECORD_ID,
        "managed_tab_prefix": DataClass.USER_NOTE,
        "name": DataClass.USER_NOTE,
        "spreadsheet_id": DataClass.RECORD_ID,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "imports"): {
        "import_id": DataClass.RECORD_ID,
        "labels": DataClass.USER_NOTE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "updated_by": DataClass.TXN_TYPE,
    },
    ("app", "import_previews"): {
        "channel": DataClass.TXN_TYPE,
        "consumed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "expires_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "file_path": DataClass.RECORD_ID,
        "file_sha256": DataClass.RECORD_ID,
        "file_size_bytes": DataClass.AGGREGATE,
        "import_id": DataClass.RECORD_ID,
        "issued_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "preview_id": DataClass.RECORD_ID,
        "snapshot_json": DataClass.TXN_AMOUNT,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "lot_selections"): {
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "investment_transaction_id": DataClass.RECORD_ID,
        "lot_id": DataClass.RECORD_ID,
        # Units drawn from a lot for a disposal: position-size information,
        # masked like transaction amounts.
        "quantity": DataClass.TXN_AMOUNT,
    },
    ("app", "match_decisions"): {
        "account_id": DataClass.RECORD_ID,
        "account_id_b": DataClass.RECORD_ID,
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
    ("app", "merchant_link_decisions"): {
        "decision_id": DataClass.RECORD_ID,
        "ref_kind": DataClass.TXN_TYPE,
        "ref_value": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        # A merchant name — medium tier, not a bare id.
        "provider_merchant_name": DataClass.MERCHANT_NAME,
        "candidate_merchant_id": DataClass.RECORD_ID,
        "confidence_score": DataClass.AGGREGATE,
        # JSON signal payload may echo the provider merchant_name → classify as MERCHANT_NAME.
        "match_signals": DataClass.MERCHANT_NAME,
        "status": DataClass.TXN_TYPE,
        "decided_by": DataClass.TXN_TYPE,
        "match_reason": DataClass.USER_NOTE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
    },
    ("app", "merchant_links"): {
        "link_id": DataClass.RECORD_ID,
        "merchant_id": DataClass.RECORD_ID,
        "ref_kind": DataClass.TXN_TYPE,
        # Opaque provider merchant id — never an account number, so RECORD_ID (LOW),
        # NOT the ACCOUNT_IDENTIFIER exception account_links.ref_value carries.
        "ref_value": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
        "decided_by": DataClass.TXN_TYPE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
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
        "category_id": DataClass.RECORD_ID,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "decided_by": DataClass.TXN_TYPE,
        "match_type": DataClass.TXN_TYPE,
        "merchant_pattern": DataClass.MERCHANT_NAME,
        "proposed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "proposed_rule_id": DataClass.RECORD_ID,
        "rule_id": DataClass.RECORD_ID,
        "sample_txn_ids": DataClass.RECORD_ID,
        "source": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
        "subcategory": DataClass.CATEGORY,
        "trigger_count": DataClass.AGGREGATE,
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
    ("app", "securities"): {
        "coingecko_id": DataClass.TXN_TYPE,
        "cost_basis_method": DataClass.TXN_TYPE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        # Catalog provenance flag (user vs plaid) — closed enum, not PII.
        "created_by": DataClass.TXN_TYPE,
        "currency_code": DataClass.CURRENCY,
        # Public-instrument reference data (what an instrument IS), not user
        # PII — that the user HOLDS it, and how much, lives in the lots and
        # holdings tables where quantities/amounts carry their own classes.
        "cusip": DataClass.TXN_TYPE,
        "exchange": DataClass.TXN_TYPE,
        "figi": DataClass.TXN_TYPE,
        "is_cash_equivalent": DataClass.TXN_TYPE,
        "isin": DataClass.TXN_TYPE,
        "name": DataClass.TXN_TYPE,
        "security_id": DataClass.RECORD_ID,
        "security_type": DataClass.TXN_TYPE,
        "ticker": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "security_link_decisions"): {
        "decision_id": DataClass.RECORD_ID,
        "ref_kind": DataClass.TXN_TYPE,
        # Opaque provider security ref (plaid security_id, or
        # institution_id:institution_security_id) — never an account number.
        "ref_value": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        # Public-instrument reference data, same as securities.ticker/name.
        "provider_ticker": DataClass.TXN_TYPE,
        "provider_name": DataClass.TXN_TYPE,
        "candidate_security_id": DataClass.RECORD_ID,
        "confidence_score": DataClass.AGGREGATE,
        # Match-basis signals echo ticker/name — LOW tier, same as the
        # provider_ticker/provider_name columns they're derived from.
        "match_signals": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
        "decided_by": DataClass.TXN_TYPE,
        "match_reason": DataClass.USER_NOTE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
    },
    ("app", "security_links"): {
        "link_id": DataClass.RECORD_ID,
        "security_id": DataClass.RECORD_ID,
        "ref_kind": DataClass.TXN_TYPE,
        # Opaque provider security ref, never an account number — RECORD_ID
        # (LOW), matching merchant_links.ref_value's rationale, not the
        # ACCOUNT_IDENTIFIER exception account_links.ref_value carries.
        "ref_value": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        "status": DataClass.TXN_TYPE,
        "decided_by": DataClass.TXN_TYPE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "reversed_by": DataClass.TXN_TYPE,
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
    ("app", "pdf_formats"): {
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "date_format": DataClass.TXN_TYPE,
        "document_kind": DataClass.TXN_TYPE,
        "extraction_recipe": DataClass.DESCRIPTION,
        "field_mapping": DataClass.DESCRIPTION,
        "front_end": DataClass.TXN_TYPE,
        "institution_name": DataClass.INSTITUTION,
        "last_used_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "layout_fingerprint": DataClass.DESCRIPTION,
        "name": DataClass.RECORD_ID,
        "number_format": DataClass.TXN_TYPE,
        "routing": DataClass.TXN_TYPE,
        "seed_alias": DataClass.RECORD_ID,
        "sign_convention": DataClass.TXN_TYPE,
        "source": DataClass.TXN_TYPE,
        "times_used": DataClass.AGGREGATE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "version": DataClass.AGGREGATE,
    },
    ("app", "transaction_categories"): {
        "categorized_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "categorized_by": DataClass.TXN_TYPE,
        "category": DataClass.CATEGORY,
        "category_id": DataClass.RECORD_ID,
        "confidence": DataClass.AGGREGATE,
        "merchant_id": DataClass.RECORD_ID,
        "rule_id": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        "subcategory": DataClass.CATEGORY,
        "transaction_id": DataClass.RECORD_ID,
    },
    ("app", "transaction_id_aliases"): {
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "new_transaction_id": DataClass.RECORD_ID,
        "old_transaction_id": DataClass.RECORD_ID,
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
        "category_id": DataClass.RECORD_ID,
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
        "class": DataClass.TXN_TYPE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "description": DataClass.CATEGORY,
        "is_active": DataClass.TXN_TYPE,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("app", "user_merchants"): {
        "canonical_name": DataClass.MERCHANT_NAME,
        "category": DataClass.CATEGORY,
        "category_id": DataClass.RECORD_ID,
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
    ("core", "bridge_category_source_map"): {
        "category_id": DataClass.RECORD_ID,
        "code_level": DataClass.TXN_TYPE,
        "is_default": DataClass.TXN_TYPE,
        "source_category_code": DataClass.CATEGORY,
        "source_taxonomy_version": DataClass.AGGREGATE,
        "source_type": DataClass.TXN_TYPE,
    },
    ("core", "bridge_transfers"): {
        "amount": DataClass.TXN_AMOUNT,
        "credit_transaction_id": DataClass.RECORD_ID,
        "date_offset_days": DataClass.AGGREGATE,
        "debit_transaction_id": DataClass.RECORD_ID,
        "transfer_id": DataClass.RECORD_ID,
    },
    ("core", "dim_accounts"): {
        # Opaque minted canonical surrogate (spec D6) — not PII. PII lives in
        # app.account_links.ref_value (ACCOUNT_IDENTIFIER).
        "account_id": DataClass.RECORD_ID,
        "account_subtype": DataClass.TXN_TYPE,
        "account_type": DataClass.TXN_TYPE,
        "archived": DataClass.TXN_TYPE,
        "credit_limit": DataClass.BALANCE,
        "currency_code": DataClass.CURRENCY,
        "display_name": DataClass.USER_NOTE,
        "extracted_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "holder_category": DataClass.TXN_TYPE,
        "include_in_net_worth": DataClass.TXN_TYPE,
        "institution_fid": DataClass.INSTITUTION,
        "institution_name": DataClass.INSTITUTION,
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
        "class": DataClass.TXN_TYPE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "description": DataClass.CATEGORY,
        "is_active": DataClass.TXN_TYPE,
        "is_default": DataClass.TXN_TYPE,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "dim_holdings"): {
        "account_id": DataClass.RECORD_ID,
        "security_id": DataClass.RECORD_ID,
        # Position size (units held) — masked like app.lot_selections.quantity.
        "quantity": DataClass.TXN_AMOUNT,
        # Aggregate open cost basis: a held "stock" figure, not a single flow —
        # classified like an account balance rather than a transaction amount.
        "cost_basis": DataClass.BALANCE,
        "average_cost": DataClass.BALANCE,
        "currency_code": DataClass.CURRENCY,
        # The broker's non-authoritative claim about the same position. Being a
        # reference rather than MoneyBin's own figure changes nothing about its
        # sensitivity — it discloses the identical holding, so each column
        # carries the same class as the ledger-derived column it mirrors.
        "provider_reported_quantity": DataClass.TXN_AMOUNT,
        "provider_reported_cost_basis": DataClass.BALANCE,
        "provider_reported_value": DataClass.BALANCE,
        "provider_reported_as_of": DataClass.TIMESTAMP_OBSERVABILITY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "dim_merchants"): {
        "canonical_name": DataClass.MERCHANT_NAME,
        "category": DataClass.CATEGORY,
        # FK to core.dim_categories.category_id — missed here until the
        # generalized derivation check (reports-foundation.md) caught it: the
        # completeness test's core.dim_merchants stub (tests/moneybin/
        # db_helpers.py) had independently drifted to omit this column too,
        # so neither guard alone would have surfaced the gap.
        "category_id": DataClass.RECORD_ID,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "created_by": DataClass.TXN_TYPE,
        "exemplars": DataClass.MERCHANT_NAME,
        "match_type": DataClass.TXN_TYPE,
        "merchant_id": DataClass.RECORD_ID,
        "raw_pattern": DataClass.MERCHANT_NAME,
        "subcategory": DataClass.CATEGORY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "dim_securities"): {
        # Mirrors app.securities: public-instrument reference data (what an
        # instrument IS), not user PII. Holdings/lots carry the "user holds it"
        # signal with their own quantity/basis classes.
        "security_id": DataClass.RECORD_ID,
        "name": DataClass.TXN_TYPE,
        "security_type": DataClass.TXN_TYPE,
        "ticker": DataClass.TXN_TYPE,
        "exchange": DataClass.TXN_TYPE,
        "cusip": DataClass.TXN_TYPE,
        "isin": DataClass.TXN_TYPE,
        "figi": DataClass.TXN_TYPE,
        "coingecko_id": DataClass.TXN_TYPE,
        "is_cash_equivalent": DataClass.TXN_TYPE,
        "currency_code": DataClass.CURRENCY,
    },
    ("core", "fct_balances"): {
        "account_id": DataClass.RECORD_ID,
        "balance": DataClass.BALANCE,
        "balance_date": DataClass.TXN_DATE,
        "currency_code": DataClass.CURRENCY,
        "source_ref": DataClass.RECORD_ID,
        "source_type": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "fct_balances_daily"): {
        "account_id": DataClass.RECORD_ID,
        "balance": DataClass.BALANCE,
        "balance_date": DataClass.TXN_DATE,
        "currency_code": DataClass.CURRENCY,
        "is_observed": DataClass.TXN_TYPE,
        "observation_source": DataClass.TXN_TYPE,
        "reconciliation_delta": DataClass.BALANCE,
    },
    ("core", "fct_investment_lots"): {
        "lot_id": DataClass.RECORD_ID,
        "account_id": DataClass.RECORD_ID,
        "security_id": DataClass.RECORD_ID,
        "acquisition_date": DataClass.TXN_DATE,
        "acquisition_type": DataClass.TXN_TYPE,
        # Units — position-size information, masked like
        # app.lot_selections.quantity.
        "original_quantity": DataClass.TXN_AMOUNT,
        "remaining_quantity": DataClass.TXN_AMOUNT,
        # A held "stock" figure (open basis at a point in time), classified
        # like an account balance rather than a single transaction amount.
        "cost_basis_total": DataClass.BALANCE,
        "cost_basis_remaining": DataClass.BALANCE,
        "cost_basis_method": DataClass.TXN_TYPE,
        "currency_code": DataClass.CURRENCY,
        "is_open": DataClass.TXN_TYPE,
        "source_transaction_id": DataClass.RECORD_ID,
        "basis_incomplete": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "fct_investment_transactions"): {
        "investment_transaction_id": DataClass.RECORD_ID,
        "account_id": DataClass.RECORD_ID,
        "security_id": DataClass.RECORD_ID,
        "trade_date": DataClass.TXN_DATE,
        "settlement_date": DataClass.TXN_DATE,
        "original_acquisition_date": DataClass.TXN_DATE,
        "type": DataClass.TXN_TYPE,
        "subtype": DataClass.TXN_TYPE,
        # Links legs of one decomposed economic event — an id, not a category.
        "event_group_id": DataClass.RECORD_ID,
        "quantity": DataClass.TXN_AMOUNT,
        "price": DataClass.TXN_AMOUNT,
        "amount": DataClass.TXN_AMOUNT,
        "fees": DataClass.TXN_AMOUNT,
        "currency_code": DataClass.CURRENCY,
        # The provider's original type/subtype strings, preserved for audit — a
        # closed-vocabulary routing tag from the source, like `type`/`subtype`.
        "provider_type": DataClass.TXN_TYPE,
        "provider_subtype": DataClass.TXN_TYPE,
        "source_type": DataClass.TXN_TYPE,
        "source_origin": DataClass.TXN_TYPE,
        "description": DataClass.DESCRIPTION,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "fct_realized_gains"): {
        "realized_gain_id": DataClass.RECORD_ID,
        "account_id": DataClass.RECORD_ID,
        "security_id": DataClass.RECORD_ID,
        "disposal_txn_id": DataClass.RECORD_ID,
        "lot_id": DataClass.RECORD_ID,
        "quantity": DataClass.TXN_AMOUNT,
        "acquisition_date": DataClass.TXN_DATE,
        "disposal_date": DataClass.TXN_DATE,
        # 1099-B reconciliation figures — held/realized values, classified
        # like an account balance rather than a single transaction amount.
        "proceeds": DataClass.BALANCE,
        "cost_basis": DataClass.BALANCE,
        "gain_loss": DataClass.BALANCE,
        "term": DataClass.TXN_TYPE,
        "cost_basis_method": DataClass.TXN_TYPE,
        "basis_incomplete": DataClass.TXN_TYPE,
        "currency_code": DataClass.CURRENCY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "fct_transaction_lines"): {
        "account_id": DataClass.RECORD_ID,
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
        "account_id": DataClass.RECORD_ID,
        "amount": DataClass.TXN_AMOUNT,
        "amount_absolute": DataClass.TXN_AMOUNT,
        "authorized_date": DataClass.TXN_DATE,
        "categorized_by": DataClass.TXN_TYPE,
        "category": DataClass.CATEGORY,
        "check_number": DataClass.DESCRIPTION,
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
        "merchant_id": DataClass.RECORD_ID,
        "merchant_name": DataClass.MERCHANT_NAME,
        "note_count": DataClass.AGGREGATE,
        "notes": DataClass.USER_NOTE,
        "original_description": DataClass.DESCRIPTION,
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
    ("core", "uncategorized_queue"): {
        # Curator-impact queue for the categorization surface
        # (services/categorization/queries.py, transactions_categorize_pending);
        # moved out of reports.* per reports-foundation.md R5. account_id is
        # RECORD_ID here to match every other account_id in this registry
        # (spec D6) — NOT ACCOUNT_IDENTIFIER, unlike the deleted
        # _bridged_classes.py entry this mirrors.
        "transaction_id": DataClass.RECORD_ID,
        "account_id": DataClass.RECORD_ID,
        "account_name": DataClass.USER_NOTE,
        "txn_date": DataClass.TXN_DATE,
        "amount": DataClass.TXN_AMOUNT,
        "description": DataClass.DESCRIPTION,
        "merchant_id": DataClass.RECORD_ID,
        "merchant_normalized": DataClass.MERCHANT_NAME,
        # CURRENT_DATE is public, so age_days is bijective with txn_date
        # (txn_date = CURRENT_DATE - age_days) — a date, not an aggregate.
        "age_days": DataClass.TXN_DATE,
        # ABS(amount) * age_days: exact once age_days is visible (>= MEDIUM
        # tier), so priority_score recovers ABS(amount) by division at any
        # tier that unmasks age_days but not amount. Must stay HIGH.
        "priority_score": DataClass.TXN_AMOUNT,
        "source_type": DataClass.TXN_TYPE,
        "source_id": DataClass.RECORD_ID,
    },
}
