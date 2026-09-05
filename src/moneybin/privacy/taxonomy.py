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
    # A serialized/composite value (JSON, a packed multi-field string) that may
    # embed identifier-class material at a position the declaration cannot
    # pin down. CRITICAL and masked WHOLE, for the same reason UNRESOLVED is:
    # a DuckDB JSON column reaches the redaction transform as `str`, so
    # ACCOUNT_IDENTIFIER's `"****" + value[-4:]` would surface the tail of the
    # SERIALIZED TEXT — whichever key happens to sort last — not the tail of
    # any value inside it. Unlike UNRESOLVED, this IS a positive declaration:
    # the column's shape and worst-case content are known, just not which
    # bytes land in the last four. Use this instead of UNRESOLVED for a
    # `CLASSIFICATION`/`core`+`app` column — UNRESOLVED there is the
    # fail-closed marker for a column NOBODY classified, and declaring it
    # explicitly would defeat the completeness tests that exist to catch that
    # gap. Origin: match_signals (issue #451).
    COMPOSITE_IDENTIFIER = "composite_identifier"
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
    # Not a classification either — the absence of a DECLARED one, in a schema
    # where declaring every column was never the plan. Assigned by
    # ``_class_of_key`` to an undeclared raw/prep column. Unlike UNRESOLVED it
    # is LOW and passes values through, masking only account/routing/SSN
    # SHAPES: raw/prep exist to be read while debugging an import. Never write
    # it into ``CLASSIFICATION`` or a ``@report(classes=…)`` map.
    FLOORED = "floored"

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
    DataClass.COMPOSITE_IDENTIFIER: Tier.CRITICAL,
    DataClass.UNRESOLVED: Tier.CRITICAL,
    DataClass.FLOORED: Tier.LOW,
}

# Keyed by (schema, table) -> {column: DataClass}. Every column in
# core.* and app.* must appear here; the completeness test enforces
# this. Judgment calls are documented in
# docs/specs/privacy-data-classification.md ("Classification Audit").
CLASSIFICATION: dict[tuple[str, str], dict[str, DataClass]] = {
    ("app", "account_link_decisions"): {
        "candidate_account_id": DataClass.RECORD_ID,
        # Both names as they stood when the decision was made, frozen because an
        # accepted merge removes the provisional from every live lookup. USER_NOTE
        # matches display_name everywhere else — and holds because the freeze
        # reads core.dim_accounts alone (a constructed institution + subtype +
        # last4 label) *and* refuses an 'Account ' || account_id label, which
        # for an unlinked account embeds the source-native key. The model no
        # longer emits one -- its terminal arm is the literal 'Unnamed account'
        # -- so that refusal is now defence in depth. Core alone is not
        # sufficient either way; see
        # fetch_core_display_names. The raw fallback derives its label from
        # INSTITUTION_ACCOUNT_NUMBER columns, so it is deliberately never written
        # here; see AccountLinksService._frozen_names.
        "candidate_display_name": DataClass.USER_NOTE,
        "confidence_score": DataClass.AGGREGATE,
        "decided_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "decided_by": DataClass.TXN_TYPE,
        "decision_id": DataClass.RECORD_ID,
        "match_reason": DataClass.USER_NOTE,
        # Unlike match_decisions.match_signals (scores), this carries weak-signal
        # values that include account digits (institution_last4) — masked, not the
        # LOW-tier AGGREGATE passthrough. COMPOSITE_IDENTIFIER, not
        # ACCOUNT_IDENTIFIER: a DuckDB JSON column reaches the transform as `str`,
        # so ACCOUNT_IDENTIFIER's partial mask would surface the tail of the
        # SERIALIZED JSON TEXT, not of any signal value (issue #451). Whole-masking
        # costs only the rendered value here; the typed accounts_links surface
        # (M1S.5) presents signals with structured masking.
        "match_signals": DataClass.COMPOSITE_IDENTIFIER,
        "provisional_account_id": DataClass.RECORD_ID,
        # Frozen alongside candidate_display_name above; same reasoning.
        "provisional_display_name": DataClass.USER_NOTE,
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
    ("app", "profile_settings"): {
        "scope": DataClass.RECORD_ID,
        "home_currency": DataClass.CURRENCY,
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
    ("app", "exchange_rate_overrides"): {
        "from_currency": DataClass.CURRENCY,
        "to_currency": DataClass.CURRENCY,
        # MEDIUM, unlike security_price_overrides.price_date. That column is
        # LOW because a price mark exists precisely where no execution does;
        # the reason to override an FX rate is the opposite — the bank's rate
        # on a day the user actually converted money — so the date carries the
        # same signal as a transaction date and takes the same class.
        "rate_date": DataClass.TXN_DATE,
        # A published daily reference rate is a market fact, not a personal
        # one: it discloses no balance or amount, and the pair it prices is
        # already CURRENCY/LOW. Requirement 10 also requires showing the exact
        # rate behind any converted figure, which a HIGH class would fight.
        "rate": DataClass.CURRENCY,
        "note": DataClass.USER_NOTE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
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
    ("app", "user_reports"): {
        "class_downgrades": DataClass.DESCRIPTION,
        "class_fingerprint": DataClass.RECORD_ID,
        "classes": DataClass.DESCRIPTION,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "description": DataClass.USER_NOTE,
        "is_active": DataClass.TXN_TYPE,
        "name": DataClass.USER_NOTE,
        "params": DataClass.DESCRIPTION,
        # MEDIUM is a deliberate accepted risk: a user must be able to read
        # their own SQL back to edit it, and pattern-matching string literals
        # to classify them would corrupt legitimate queries. R8/R9 exist so the
        # natural way to write a filter is a parameter, not an inline literal.
        "query_sql": DataClass.USER_NOTE,
        "report_id": DataClass.RECORD_ID,
        "semantics": DataClass.DESCRIPTION,
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
    ("app", "rule_conflicts"): {
        "conflict_id": DataClass.RECORD_ID,
        "detected_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "existing_category": DataClass.CATEGORY,
        "existing_name": DataClass.USER_NOTE,
        "existing_priority": DataClass.AGGREGATE,
        "existing_rule_id": DataClass.RECORD_ID,
        "existing_rule_updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "existing_subcategory": DataClass.CATEGORY,
        # A digest over the matcher, which carries the merchant pattern — an
        # opaque surrogate for joining, never rendered as the pattern itself.
        "matcher_digest": DataClass.RECORD_ID,
        "proposed_account_id": DataClass.RECORD_ID,
        "proposed_category": DataClass.CATEGORY,
        "proposed_created_by": DataClass.TXN_TYPE,
        "proposed_match_type": DataClass.TXN_TYPE,
        "proposed_max_amount": DataClass.TXN_AMOUNT,
        "proposed_merchant_pattern": DataClass.MERCHANT_NAME,
        "proposed_min_amount": DataClass.TXN_AMOUNT,
        "proposed_name": DataClass.USER_NOTE,
        "proposed_priority": DataClass.AGGREGATE,
        "proposed_subcategory": DataClass.CATEGORY,
        "resolution": DataClass.TXN_TYPE,
        "resolved_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "resolved_rule_id": DataClass.RECORD_ID,
        "status": DataClass.TXN_TYPE,
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
    ("app", "security_price_overrides"): {
        "security_id": DataClass.RECORD_ID,
        # price_date names the day the user chose to value the position, not a
        # day they traded: a mark exists precisely where no execution does.
        "price_date": DataClass.TIMESTAMP_OBSERVABILITY,
        "quote_currency": DataClass.CURRENCY,
        # A mark is a number the user wrote — a 409A valuation, a private-company
        # estimate — so it is a personal financial fact, not a market observation
        # that happens to be stored locally. It matches the tier of
        # core.fct_security_prices.close, which this table feeds.
        "close": DataClass.TXN_AMOUNT,
        "note": DataClass.USER_NOTE,
        "created_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
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
    ("core", "bridge_merchant_entities"): {
        # merchant_entity_id is an opaque source-system merchant id, never a
        # financial account number — RECORD_ID (LOW), matching
        # app.merchant_links.ref_value's rationale, not the ACCOUNT_IDENTIFIER
        # exception app.account_links.ref_value carries.
        "merchant_entity_id": DataClass.RECORD_ID,
        "merchant_entity_source_type": DataClass.TXN_TYPE,
        # The name the source stated, unresolved — may embed identifying brand
        # text exactly like core.fct_transactions.merchant_name.
        "source_merchant_name": DataClass.MERCHANT_NAME,
        "transaction_id": DataClass.RECORD_ID,
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
        "display_name_is_user_set": DataClass.TXN_TYPE,
        "extracted_at": DataClass.TIMESTAMP_OBSERVABILITY,
        "holder_category": DataClass.TXN_TYPE,
        "include_in_net_worth": DataClass.TXN_TYPE,
        "institution_fid": DataClass.INSTITUTION,
        "institution_name": DataClass.INSTITUTION,
        "institution_slug": DataClass.INSTITUTION,
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
        # market_value/unrealized_gain (Pillar C): quantity × a resolved close, and
        # that figure less cost_basis — the same "held stock" character as
        # cost_basis/average_cost above, not a single flow. Same class, same tier.
        "market_value": DataClass.BALANCE,
        "unrealized_gain": DataClass.BALANCE,
        # price_date names a market close's date (public reference data, like
        # fct_security_prices.price_date), not a fact about the user — LOW tier,
        # matching that precedent rather than TXN_DATE.
        "price_date": DataClass.TIMESTAMP_OBSERVABILITY,
        # Which provider supplied the close — a routing tag, like
        # fct_security_prices.source_type.
        "price_source": DataClass.TXN_TYPE,
        # CURRENT_DATE - price_date: CURRENT_DATE is public, so this is bijective
        # with price_date (uncategorized_queue.age_days precedent) and inherits its
        # class rather than TXN_DATE.
        "days_since_observed": DataClass.TIMESTAMP_OBSERVABILITY,
        "valuation_status": DataClass.TXN_TYPE,
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
    ("core", "fct_security_prices"): {
        "security_id": DataClass.RECORD_ID,
        # price_date carries the same problem as close, one column over. With a
        # 'trade_implied' row it is literally fct_investment_transactions
        # .trade_date — the day the user traded — so a LOW tier here returns the
        # security and the date of a personal execution as public data, and it
        # does so even for a query that omits close entirely. Raising close
        # protects the amount, not the fact of the trade.
        "price_date": DataClass.TXN_DATE,
        "quote_currency": DataClass.CURRENCY,
        # close does NOT. A provider close alone would be public reference data,
        # but this column is the resolved winner across three sources: with a
        # 'trade_implied' row it is literally fct_investment_transactions.price —
        # the user's own fill — and with an 'override' row it is a valuation the
        # user authored. `sql_query` serves core, so a LOW tier here advertises
        # and returns a personal transaction amount as public data.
        #
        # Classified by the strictest value the column can carry rather than by
        # source, because CLASSIFICATION maps a column to one class and cannot
        # vary per row. That costs the provider closes a tier they do not need;
        # the alternative — splitting user-derived closes into their own column
        # or model so the provider series can return to LOW — is a core schema
        # change, deliberately not made under a review fix. Held by
        # test_a_resolved_close_is_never_less_sensitive_than_what_flows_into_it.
        "close": DataClass.TXN_AMOUNT,
        "source_type": DataClass.TXN_TYPE,
        "price_basis": DataClass.TXN_TYPE,
        "updated_at": DataClass.TIMESTAMP_OBSERVABILITY,
    },
    ("core", "fct_transaction_lines"): {
        "account_id": DataClass.RECORD_ID,
        "currency_code": DataClass.CURRENCY,
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
        "currency_code": DataClass.CURRENCY,
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

# CRITICAL columns of the fixed raw/prep tables. A SIBLING of CLASSIFICATION,
# not a part of it: CLASSIFICATION's completeness test asserts total coverage,
# and raw/prep have hundreds of columns that grow with every new import format
# (raw.gsheet_<alias> views are minted from a user's own spreadsheet headers and
# cannot be enumerated at all). Everything not listed here rides the FLOORED
# content net instead, which passes a value through unless it is SHAPED like an
# SSN or a run of eight or more digits.
#
# `account_id` is NOT one class across these tables — it holds each source's
# NATIVE key, and what that is differs per source. `core`/`app` declare
# `account_id` RECORD_ID because there it is an opaque minted surrogate
# (account-identity-resolution.md Decisions 1 and 6); that precedent stops at
# the `core` boundary and must not be carried down here.
#
# Two classes appear for account keys, following the live core/app precedent:
# INSTITUTION_ACCOUNT_NUMBER where the value provably IS the institution's
# number (as `dim_accounts.last_four` is), and ACCOUNT_IDENTIFIER where the
# column may hold a number or an opaque token and cannot be told apart (as
# `app.account_links.ref_value` is). Both are CRITICAL and share the same
# partial-masking transform, so the choice sets the label's accuracy, not the
# masking. ROUTING_NUMBER masks WHOLE and is not interchangeable with either.
INTERNAL_CRITICAL: dict[tuple[str, str], dict[str, DataClass]] = {
    # --- OFX: `account_id` is the `<ACCTID>` element, i.e. the institution's
    # own account number (raw_ofx_accounts.sql). `source_account_key` is that
    # same column re-aliased by every stg_ofx__* model.
    ("raw", "ofx_accounts"): {
        "account_id": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "routing_number": DataClass.ROUTING_NUMBER,
    },
    ("raw", "ofx_balances"): {
        "account_id": DataClass.INSTITUTION_ACCOUNT_NUMBER,
    },
    ("raw", "ofx_transactions"): {
        "account_id": DataClass.INSTITUTION_ACCOUNT_NUMBER,
    },
    ("prep", "stg_ofx__accounts"): {
        "account_id": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "source_account_key": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "routing_number": DataClass.ROUTING_NUMBER,
    },
    ("prep", "stg_ofx__balances"): {
        "account_id": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "source_account_key": DataClass.INSTITUTION_ACCOUNT_NUMBER,
    },
    ("prep", "stg_ofx__transactions"): {
        "account_id": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "source_account_key": DataClass.INSTITUTION_ACCOUNT_NUMBER,
    },
    # --- Plaid: `account_id` is the provider's own surrogate, globally unique
    # per Plaid and therefore not an institution account number; the
    # account-number material rides `mask` (last four digits) instead. The
    # surrogate stays readable so an agent can correlate a Plaid import across
    # tables while debugging it.
    ("raw", "plaid_accounts"): {
        "mask": DataClass.INSTITUTION_ACCOUNT_NUMBER,
    },
    ("prep", "stg_plaid__accounts"): {
        "mask": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        # `NULL::TEXT AS routing_number` — a schema-uniformity placeholder that
        # is always NULL today (stg_plaid__accounts.sql). Declared anyway: it
        # costs nothing now and is already right the day Plaid populates it.
        "routing_number": DataClass.ROUTING_NUMBER,
    },
    # --- Tabular (CSV/Excel/Parquet): the native key is a slug of whatever the
    # file presents as the account, and the mapped account column can be the
    # account number itself, so the column cannot be told apart from one.
    #
    # `account_name` is that same file value UNSLUGIFIED, and it must carry the
    # same class as the `account_id` slug derived from it — one value, one
    # class. A multi-account import keeps both halves: `account_ids =
    # [slugify(name) for name in raw_names]` while `acct_id_to_name` retains the
    # original, which is written straight to this column (import_service.py).
    # Declaring only the slug masks the derivative and publishes the source.
    # The content net cannot cover the gap: a 7-digit number is under the 8-digit
    # run it looks for, and a separator-formatted one contains no run at all.
    # `core` is no precedent for passing it through — `dim_accounts.display_name`
    # is CONSTRUCTED from institution + subtype + last4, never the raw file value.
    ("raw", "tabular_accounts"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "account_name": DataClass.ACCOUNT_IDENTIFIER,
        # `account_label` beside it is deliberately ABSENT, and that is the
        # difference between the two columns: it is account_name after the
        # importer stripped the last four and masked any embedded number, and it
        # becomes core.dim_accounts.display_name (USER_NOTE). Declaring it in a
        # registry of CRITICAL columns would claim one string is two classes
        # depending on which layer read it. It rides the FLOORED content net,
        # which its masking has already put it on the safe side of.
        "account_number": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "account_number_masked": DataClass.INSTITUTION_ACCOUNT_NUMBER,
    },
    ("raw", "tabular_transactions"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
    },
    ("prep", "stg_tabular__accounts"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "source_account_key": DataClass.ACCOUNT_IDENTIFIER,
        "account_name": DataClass.ACCOUNT_IDENTIFIER,
        "account_number": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        "account_number_masked": DataClass.INSTITUTION_ACCOUNT_NUMBER,
        # `NULL::TEXT AS routing_number` placeholder, as for Plaid above.
        "routing_number": DataClass.ROUTING_NUMBER,
    },
    # --- Import log: `account_names` is the same tabular file value again, and
    # the OFX importer writes raw <ACCTID> values into it verbatim — its call
    # site says so ("institution-assigned account numbers, not display names").
    #
    # WHOLE, not the partial mask the two columns above take. A DuckDB JSON
    # column reaches the transform as `str`, so ACCOUNT_IDENTIFIER's
    # `"****" + value[-4:]` would publish the TAIL of the serialized array,
    # which for a one-element array of a bare number is the tail of an account
    # number. Same reasoning as `source_bytes` below, and the same scoping
    # argument for using UNRESOLVED in this map.
    ("raw", "import_log"): {
        "account_names": DataClass.UNRESOLVED,
    },
    ("prep", "stg_tabular__transactions"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "source_account_key": DataClass.ACCOUNT_IDENTIFIER,
    },
    # --- Cross-source int models: these UNION every source's native key into
    # one column, so the OFX branch alone makes the whole column an account
    # number for some rows. Which source a row came from is not knowable from
    # the column, hence the may-be-a-number class.
    ("prep", "int_transactions__unioned"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "source_account_key": DataClass.ACCOUNT_IDENTIFIER,
    },
    ("prep", "int_transactions__matched"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        # `match_group_id` is `account_id || '|' || MIN(packed member)`
        # (int_transactions__matched.sql), so it carries the account key of
        # every row it groups — a real <ACCTID> whenever the account has no
        # resolver link. The content net cannot reach it: the account sits at
        # the HEAD of a longer string, and the two documented gaps (a 4-to-7
        # digit key, a separator-formatted one) leave no 8-digit run to find.
        # UNRESOLVED rather than the partial mask `account_id` takes, for the
        # same reason as the two payloads below: this is a composite, so
        # ACCOUNT_IDENTIFIER would misname the shape AND `"****" + value[-4:]`
        # would publish the tail of a member id. Whole-masking costs only the
        # rendered label — masking runs on result rows, so `GROUP BY
        # match_group_id` still aggregates correctly.
        "match_group_id": DataClass.UNRESOLVED,
    },
    ("prep", "int_transactions__merged"): {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
    },
    # --- Staged import bytes: a bank file held verbatim (an OFX export carries
    # <ACCTID> and <BANKID> in the clear). `_mask_floored` deliberately passes
    # `bytes` through untouched, so the content net offers this column nothing.
    # UNRESOLVED is the honest class — we cannot name what the bytes hold — and
    # is the only one that masks WHOLE without claiming a shape. The prohibition
    # in UNRESOLVED's own comment is scoped to CLASSIFICATION and
    # `@report(classes=...)`, where declaring it would defeat a completeness
    # test; this map has no completeness test to defeat.
    ("raw", "import_preview_snapshots"): {
        "source_bytes": DataClass.UNRESOLVED,
    },
}
