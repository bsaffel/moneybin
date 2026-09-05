"""Prefix-grouped error code taxonomy.

Every UserError raised from a MoneyBin tool path MUST use one of these
constants for its `code` argument. Agents branch on these strings; they
are part of the public surface contract.

A code's prefix names the domain it came from: either a cross-cutting
concern (`mutation_`, `audit_`, `refresh_`, `undo_`, `recovery_`,
`infra_`) or an MCP tool namespace (`import_`, `sync_`, `gsheet_`,
`sql_`, `account_`, `entity_`, `investment_`, `privacy_`, `report_`,
`review_`, `taxonomy_`, `transaction_`). An agent can branch on the
family without enumerating every member.

Adding a new code:
1. Pick a prefix from VALID_PREFIXES in
   `tests/moneybin/test_errors/test_error_codes.py`. If none fits,
   surface the gap on `docs/specs/data-recovery-contract.md` Req 3 and
   update the spec first — do not invent ad-hoc prefixes.
2. Add the constant ordered alphabetically within its prefix group.
3. The constant name is the value uppercased: IMPORT_PARSE_ERROR =
   "import_parse_error".

Reference these constants; never hardcode the string. ``TestWireCodes``
checks both sides — every ``code=`` literal and every comparison against
a ``.code`` attribute must name a *declared value* — which is what a
literal spelled inline still satisfies, so the convention is on you. It
exists because a literal that never reaches this module is invisible to
every other test here: that is how 104 undeclared codes shipped on the
wire while these tests stayed green.

Codes are stable. Renaming a code is a breaking change for any agent
that branches on it; treat as one-way per .claude/rules/design-principles.md.
"""

# ---------------------------------------------------------------------------
# Import — loading raw data
# ---------------------------------------------------------------------------

IMPORT_ACCOUNT_SIGNAL_UNSUPPORTED = "import_account_signal_unsupported"
IMPORT_CURSOR_INVALID = "import_cursor_invalid"
IMPORT_FILE_NOT_FOUND = "import_file_not_found"
IMPORT_FORMAT_UNKNOWN = "import_format_unknown"
IMPORT_ID_NOT_ALLOWED = "import_id_not_allowed"
IMPORT_INVALID_DATE_FORMAT = "import_invalid_date_format"
IMPORT_INVALID_FILE_PATH = "import_invalid_file_path"
IMPORT_INVALID_NUMBER_FORMAT = "import_invalid_number_format"
IMPORT_INVALID_SIGN_CONVENTION = "import_invalid_sign_convention"
IMPORT_PAGINATION_NOT_ALLOWED = "import_pagination_not_allowed"
IMPORT_PARSE_ERROR = "import_parse_error"
# A scanned / image-only PDF with no selectable text layer: the deterministic
# rung has nothing to structure, nothing to seed, and the text bridge can't read
# a page image — extraction needs a vision-capable backend (Req 5, smart-import-pdf).
IMPORT_PDF_NO_TEXT_LAYER = "import_pdf_no_text_layer"
IMPORT_PREVIEW_BRIDGE_RESPONSE_REQUIRED = "import_preview_bridge_response_required"
IMPORT_PREVIEW_CHANGED = "import_preview_changed"
IMPORT_PREVIEW_CHANNEL_CONFLICT = "import_preview_channel_conflict"
IMPORT_PREVIEW_CONSUMED = "import_preview_consumed"
IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED = "import_preview_direct_import_required"
IMPORT_PREVIEW_ERROR = "import_preview_error"
IMPORT_PREVIEW_EXPIRED = "import_preview_expired"
IMPORT_PREVIEW_MAPPING_INVALID = "import_preview_mapping_invalid"
IMPORT_PREVIEW_NOT_FOUND = "import_preview_not_found"
IMPORT_PREVIEW_PLAN_MISMATCH = "import_preview_plan_mismatch"
IMPORT_PREVIEW_PLAN_MISSING = "import_preview_plan_missing"
IMPORT_PREVIEW_SNAPSHOT_MISSING = "import_preview_snapshot_missing"
IMPORT_REVERT_ALREADY_REVERTED = "import_revert_already_reverted"
IMPORT_REVERT_INVALID_TARGET = "import_revert_invalid_target"
IMPORT_REVERT_NOT_FOUND = "import_revert_not_found"
IMPORT_REVERT_SUPERSEDED = "import_revert_superseded"
IMPORT_REVERT_UNSUPPORTED = "import_revert_unsupported"
IMPORT_SAVED_FORMAT_BUILTIN_IMMUTABLE = "import_saved_format_builtin_immutable"
IMPORT_SAVED_FORMAT_NOT_FOUND = "import_saved_format_not_found"
IMPORT_SECTIONS_DUPLICATE = "import_sections_duplicate"
IMPORT_SECTIONS_REQUIRED = "import_sections_required"
IMPORT_SIGN_PROPOSAL_CHANGED = "import_sign_proposal_changed"
IMPORT_SUPERSEDED = "import_superseded"


# ---------------------------------------------------------------------------
# Mutation — app-state writes (categories, accounts, rules, etc.)
# ---------------------------------------------------------------------------

# Note on MUTATION_INVALID_INPUT and MUTATION_NOT_FOUND vs. INFRA_*:
# The exception classifier (`classify_user_error`) routes bare ValueError →
# INFRA_INVALID_INPUT and bare LookupError → INFRA_NOT_FOUND because the
# classifier sits across all tool paths and cannot distinguish read context
# from write context. Write-side call sites that mean "the entity-shape you
# tried to write is invalid" or "the entity you targeted does not exist"
# should `raise UserError(code=MUTATION_INVALID_INPUT, ...)` (or
# MUTATION_NOT_FOUND) directly at the site rather than rely on the
# classifier. The per-domain retrofits in PRs 9a-N migrate write-site
# raise ValueError/LookupError to explicit UserError(code=MUTATION_*) calls.
MUTATION_AMBIGUOUS = "mutation_ambiguous"
MUTATION_CONFIRMATION_DECLINED = "mutation_confirmation_declined"
MUTATION_CONFIRMATION_EXPIRED = "mutation_confirmation_expired"
MUTATION_CONFIRMATION_MISMATCH = "mutation_confirmation_mismatch"
MUTATION_CONFIRMATION_REPLAYED = "mutation_confirmation_replayed"
# The mutation needs explicit human agreement the caller could not obtain: the
# client cannot elicit, there is no active session to ask, or the human
# declined. Raised INSTEAD of mutating — never alongside a partial write. The
# `hint` names the CLI equivalent so the user always has a way through.
MUTATION_CONFIRMATION_REQUIRED = "mutation_confirmation_required"
MUTATION_CONSTRAINT_VIOLATION = "mutation_constraint_violation"
MUTATION_INVALID_INPUT = "mutation_invalid_input"
MUTATION_NOT_FOUND = "mutation_not_found"
MUTATION_NOTHING_TO_DO = "mutation_nothing_to_do"
MUTATION_REDACTION_CHOICE_REQUIRED = "mutation_redaction_choice_required"


# ---------------------------------------------------------------------------
# Audit — doctor / invariant failures
# ---------------------------------------------------------------------------

AUDIT_CURSOR_NOT_ALLOWED = "audit_cursor_not_allowed"
AUDIT_FK_VIOLATION = "audit_fk_violation"
AUDIT_IDENTIFIER_NOT_ALLOWED = "audit_identifier_not_allowed"
AUDIT_IDENTIFIER_NOT_FOUND = "audit_identifier_not_found"
AUDIT_IDENTIFIER_REQUIRED = "audit_identifier_required"
AUDIT_INVARIANT_FAILURE = "audit_invariant_failure"
AUDIT_ORPHAN_STATE = "audit_orphan_state"
AUDIT_SIGN_VIOLATION = "audit_sign_violation"
AUDIT_UNBALANCED_TRANSFER = "audit_unbalanced_transfer"


# ---------------------------------------------------------------------------
# Refresh — pipeline (matcher / categorizer / SQLMesh)
# ---------------------------------------------------------------------------

REFRESH_CATEGORIZE_FAILED = "refresh_categorize_failed"
REFRESH_MATCH_FAILED = "refresh_match_failed"
REFRESH_MODEL_FAILED = "refresh_model_failed"
REFRESH_UNKNOWN_STEP = "refresh_unknown_step"


# ---------------------------------------------------------------------------
# Undo — audit-log undo consumer (PR 3)
# ---------------------------------------------------------------------------

UNDO_ALREADY_UNDONE = "undo_already_undone"
UNDO_CASCADE_BLOCKED = "undo_cascade_blocked"
UNDO_OPERATION_NOT_FOUND = "undo_operation_not_found"


# ---------------------------------------------------------------------------
# Recovery — recovery tooling itself
# ---------------------------------------------------------------------------

RECOVERY_NO_PATH = "recovery_no_path"


# ---------------------------------------------------------------------------
# Infra — database, migrations, encryption (existing codes retained)
# ---------------------------------------------------------------------------

INFRA_CATALOG_UNAVAILABLE = "infra_catalog_unavailable"
INFRA_CRYPTO_UNAVAILABLE = "infra_crypto_unavailable"
INFRA_DATABASE_LOCKED = "infra_database_locked"
INFRA_DATABASE_NOT_INITIALIZED = "infra_database_not_initialized"
INFRA_FILE_NOT_FOUND = "infra_file_not_found"
INFRA_INVALID_ARGUMENTS = "infra_invalid_arguments"
INFRA_INVALID_INPUT = "infra_invalid_input"
INFRA_IO_ERROR = "infra_io_error"
INFRA_NOT_FOUND = "infra_not_found"
INFRA_NOT_IMPLEMENTED = "infra_not_implemented"
INFRA_PERMISSION_DENIED = "infra_permission_denied"
INFRA_SCHEMA_DRIFT = "infra_schema_drift"
INFRA_SETUP_REQUIRED = "infra_setup_required"
INFRA_TIMED_OUT = "infra_timed_out"
INFRA_TOO_MANY_ITEMS = "infra_too_many_items"
# Terminal fallback: an exception classify_user_error does not recognize. The
# agent still gets a branchable code instead of the bare str(exc) that fastmcp's
# mask_error_details would otherwise leave. Carries the exception *type* only —
# never its message, which can embed file paths, SQL, and financial data.
INFRA_UNCLASSIFIED_ERROR = "infra_unclassified_error"
INFRA_WRONG_KEY = "infra_wrong_key"


# ---------------------------------------------------------------------------
# Sync — external connectors (Plaid, future SimpleFIN, etc.)
# ---------------------------------------------------------------------------

SYNC_AUTH_SESSION_INVALID = "sync_auth_session_invalid"
SYNC_AUTH_SESSION_NOT_FOUND = "sync_auth_session_not_found"
SYNC_CONFIRMATION_NOT_ALLOWED = "sync_confirmation_not_allowed"
SYNC_DISCONNECT_MODE_CONFLICT = "sync_disconnect_mode_conflict"
SYNC_ERROR = "sync_error"
SYNC_INSTITUTION_AMBIGUOUS = "sync_institution_ambiguous"
SYNC_INSTITUTION_REQUIRED = "sync_institution_required"
SYNC_LINK_MODE_CONFLICT = "sync_link_mode_conflict"
SYNC_STATUS_MODE_CONFLICT = "sync_status_mode_conflict"


# ---------------------------------------------------------------------------
# Google Sheets connector — user-controlled storage (direct OAuth)
# ---------------------------------------------------------------------------
# Distinct from sync_* (mediated providers): the gsheet connector is a
# separate domain per the _connect/_link verb split in surface-design.md.
# Like sync_, this is a taxonomy-completeness prefix, not a recovery code.

GSHEET_AUTH_ARGUMENT_CONFLICT = "gsheet_auth_argument_conflict"
GSHEET_CONFIRMATION_NOT_ALLOWED = "gsheet_confirmation_not_allowed"
GSHEET_CONNECTION_ID_NOT_ALLOWED = "gsheet_connection_id_not_allowed"
GSHEET_CONNECT_MODE_CONFLICT = "gsheet_connect_mode_conflict"
GSHEET_ERROR = "gsheet_error"
GSHEET_RECONNECT_ARGUMENT_CONFLICT = "gsheet_reconnect_argument_conflict"


# ---------------------------------------------------------------------------
# External data feeds — the shape shared by every provider
# ---------------------------------------------------------------------------
# Reported only by a feed that has no code of its own. Each concrete feed
# overrides this with its own code, so a caller can still tell a rate failure
# from a price failure.

FEED_ERROR = "feed_error"

# ---------------------------------------------------------------------------
# Price feeds — market data providers (Tiingo, CoinGecko)
# ---------------------------------------------------------------------------
# Distinct from sync_* (mediated account providers): a price feed carries no
# account credential and no PII, only public market data. Like sync_ and
# gsheet_, a taxonomy-completeness prefix rather than a recovery code.

PRICE_FEED_ERROR = "price_feed_error"


# ---------------------------------------------------------------------------
# Exchange-rate feeds — currency reference rates (Frankfurter / ECB)
# ---------------------------------------------------------------------------
# Its own code rather than price_feed_: the two fail independently, and a user
# whose net worth will not convert needs to know it is the rate feed that is
# down, not the market-data one. Like price_feed_, no credential and no PII —
# only a currency pair and a date leave the machine.

RATE_FEED_ERROR = "rate_feed_error"


# ---------------------------------------------------------------------------
# SQL — ad-hoc read-only query surface (sql_query / sql_schema)
# ---------------------------------------------------------------------------

SQL_INVALID_QUERY = "sql_invalid_query"
SQL_QUERY_ERROR = "sql_query_error"
SQL_SCHEMA_NOT_ALLOWED = "sql_schema_not_allowed"
# Distinct from SQL_UNKNOWN_TABLE: the relation exists and `sql_query` reads
# it, but it carries no `audience="interface"` tag so the curated doc has no
# entry. The caller's recovery differs — reach for DESCRIBE, not a name fix.
SQL_TABLE_NOT_CURATED = "sql_table_not_curated"
SQL_UNKNOWN_TABLE = "sql_unknown_table"


# ---------------------------------------------------------------------------
# Account — accounts and balances surface
# ---------------------------------------------------------------------------

ACCOUNT_AMBIGUOUS = "account_ambiguous"
ACCOUNT_BALANCE_AS_OF_NOT_ALLOWED = "account_balance_as_of_not_allowed"
ACCOUNT_BALANCE_CURSOR_INVALID = "account_balance_cursor_invalid"
ACCOUNT_BALANCE_DATE_RANGE_INVALID = "account_balance_date_range_invalid"
ACCOUNT_BALANCE_DATES_NOT_ALLOWED = "account_balance_dates_not_allowed"
ACCOUNT_BALANCE_THRESHOLD_NOT_ALLOWED = "account_balance_threshold_not_allowed"
ACCOUNT_CURSOR_INVALID = "account_cursor_invalid"
ACCOUNT_CURSOR_NOT_ALLOWED = "account_cursor_not_allowed"
ACCOUNT_INCLUDE_CLOSED_NOT_ALLOWED = "account_include_closed_not_allowed"
ACCOUNT_INVALID_FIELD = "account_invalid_field"
ACCOUNT_LIMIT_NOT_ALLOWED = "account_limit_not_allowed"
ACCOUNT_NOT_FOUND = "account_not_found"
ACCOUNT_QUERY_NOT_ALLOWED = "account_query_not_allowed"
ACCOUNT_QUERY_REQUIRED = "account_query_required"
ACCOUNT_REFERENCE_NOT_ALLOWED = "account_reference_not_allowed"
ACCOUNT_REFERENCE_REQUIRED = "account_reference_required"


# ---------------------------------------------------------------------------
# Entity — shared reference resolution (stable ID, alias, normalized)
# ---------------------------------------------------------------------------

ENTITY_REFERENCE_AMBIGUOUS = "entity_reference_ambiguous"
ENTITY_REFERENCE_NOT_FOUND = "entity_reference_not_found"


# ---------------------------------------------------------------------------
# FX — currency conversion (the `moneybin fx` surface)
# ---------------------------------------------------------------------------
# Distinct from rate_feed_: those report that the provider failed, these report
# that no rate could be resolved from any layer — override, cache, or provider.
# The two codes below are deliberately not one. `fetch` answers None for both an
# unsupported currency and a supported pair missing one date, and the remedies
# differ: the first needs a manual override, the second needs a different date.

FX_CURRENCY_INVALID = "fx_currency_invalid"
FX_CURRENCY_UNSUPPORTED = "fx_currency_unsupported"
FX_OVERRIDE_PAIR_INVALID = "fx_override_pair_invalid"
FX_OVERRIDE_RATE_INVALID = "fx_override_rate_invalid"
FX_RATE_UNAVAILABLE = "fx_rate_unavailable"


# ---------------------------------------------------------------------------
# Investment — holdings, lots, securities
# ---------------------------------------------------------------------------

INVESTMENT_ACCOUNT_NOT_ALLOWED = "investment_account_not_allowed"
INVESTMENT_CURSOR_INVALID = "investment_cursor_invalid"
INVESTMENT_DATE_RANGE_INVALID = "investment_date_range_invalid"
INVESTMENT_DATES_NOT_ALLOWED = "investment_dates_not_allowed"
INVESTMENT_METHOD_NOT_SPECIFIC = "investment_method_not_specific"
INVESTMENT_OPEN_ONLY_NOT_ALLOWED = "investment_open_only_not_allowed"
INVESTMENT_PRICE_MARK_CURRENCY_AMBIGUOUS = "investment_price_mark_currency_ambiguous"
INVESTMENT_PRICE_MARK_CURRENCY_INVALID = "investment_price_mark_currency_invalid"
INVESTMENT_PRICE_MARK_UNREPRESENTABLE = "investment_price_mark_unrepresentable"
INVESTMENT_SECURITY_NOT_BOUND = "investment_security_not_bound"
INVESTMENT_SECURITY_NOT_IN_CATALOG = "investment_security_not_in_catalog"


# ---------------------------------------------------------------------------
# Privacy — consent ledger and privacy log
# ---------------------------------------------------------------------------

PRIVACY_CURSOR_INVALID = "privacy_cursor_invalid"
PRIVACY_PAGINATION_NOT_ALLOWED = "privacy_pagination_not_allowed"


# ---------------------------------------------------------------------------
# Report — registered report catalog and execution
# ---------------------------------------------------------------------------

REPORT_CHANGED_DURING_CONFIRMATION = "report_changed_during_confirmation"
REPORT_CLASS_CONFIRM_REQUIRED = "report_class_confirm_required"
REPORT_CLASS_NOT_WEAKER = "report_class_not_weaker"
REPORT_CLASSIFICATION_STALE = "report_classification_stale"
REPORT_COLUMN_UNKNOWN = "report_column_unknown"
REPORT_DOWNGRADE_UNREADABLE = "report_downgrade_unreadable"
REPORT_FIELD_TOO_LONG = "report_field_too_long"
REPORT_ID_AMBIGUOUS = "report_id_ambiguous"
REPORT_ID_NOT_FOUND = "report_id_not_found"
REPORT_ID_REQUIRED = "report_id_required"
REPORT_LIMIT_INVALID = "report_limit_invalid"
REPORT_NAME_ARCHIVED = "report_name_archived"
REPORT_NAME_INVALID = "report_name_invalid"
REPORT_NAME_TAKEN = "report_name_taken"
REPORT_PARAMETER_DEFAULT_NOT_ALLOWED = "report_parameter_default_not_allowed"
REPORT_PARAMETER_DUPLICATE = "report_parameter_duplicate"
REPORT_PARAMETER_INVALID_RANGE = "report_parameter_invalid_range"
REPORT_PARAMETER_INVALID_TYPE = "report_parameter_invalid_type"
REPORT_PARAMETER_INVALID_VALUE = "report_parameter_invalid_value"
REPORT_PARAMETER_MISSING = "report_parameter_missing"
REPORT_PARAMETER_UNKNOWN = "report_parameter_unknown"
REPORT_QUERY_COLUMN_DUPLICATE = "report_query_column_duplicate"
REPORT_QUERY_EXECUTION_FAILED = "report_query_execution_failed"
REPORT_QUERY_INVALID = "report_query_invalid"
REPORT_QUERY_SCHEMA_NOT_ALLOWED = "report_query_schema_not_allowed"
REPORT_QUERY_UNRESOLVABLE = "report_query_unresolvable"
REPORT_REASON_REQUIRED = "report_reason_required"


# ---------------------------------------------------------------------------
# Review — pending decision queues
# ---------------------------------------------------------------------------

REVIEW_CURSOR_INVALID = "review_cursor_invalid"
REVIEW_PAGINATION_NOT_ALLOWED = "review_pagination_not_allowed"
REVIEW_STATUS_NOT_ALLOWED = "review_status_not_allowed"


# ---------------------------------------------------------------------------
# Taxonomy — categories, subcategories, categorization rules
# ---------------------------------------------------------------------------

TAXONOMY_CATEGORY_ALREADY_EXISTS = "taxonomy_category_already_exists"
TAXONOMY_CATEGORY_HAS_REFERENCES = "taxonomy_category_has_references"
TAXONOMY_CATEGORY_IS_DEFAULT = "taxonomy_category_is_default"
TAXONOMY_CATEGORY_NOT_FOUND = "taxonomy_category_not_found"
TAXONOMY_CATEGORY_REFERENCE_NOT_FOUND = "taxonomy_category_reference_not_found"
TAXONOMY_CURSOR_INVALID = "taxonomy_cursor_invalid"
TAXONOMY_INCLUDE_INACTIVE_NOT_ALLOWED = "taxonomy_include_inactive_not_allowed"
TAXONOMY_RULE_CONFLICT = "taxonomy_rule_conflict"
TAXONOMY_RULE_CONFLICT_STALE = "taxonomy_rule_conflict_stale"
TAXONOMY_RULE_NOT_FOUND = "taxonomy_rule_not_found"


# ---------------------------------------------------------------------------
# Transaction — rows and their annotations (notes, tags, splits)
# ---------------------------------------------------------------------------

TRANSACTION_AMOUNT_RANGE_INVALID = "transaction_amount_range_invalid"
TRANSACTION_CATEGORIZATION_ERRORS = "transaction_categorization_errors"
TRANSACTION_CURSOR_INVALID = "transaction_cursor_invalid"
TRANSACTION_DATE_RANGE_INVALID = "transaction_date_range_invalid"
TRANSACTION_INVALID_AMOUNT = "transaction_invalid_amount"
TRANSACTION_INVALID_BATCH_SIZE = "transaction_invalid_batch_size"
TRANSACTION_INVALID_INPUT = "transaction_invalid_input"
TRANSACTION_NOTE_NOT_FOUND = "transaction_note_not_found"
TRANSACTION_REFERENCE_NOT_FOUND = "transaction_reference_not_found"
TRANSACTION_SPLIT_TOTAL_INVALID = "transaction_split_total_invalid"
TRANSACTION_TAG_RENAME_CONFLICT = "transaction_tag_rename_conflict"
