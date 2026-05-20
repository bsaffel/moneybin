"""Prefix-grouped error code taxonomy.

Every UserError raised from a MoneyBin tool path MUST use one of these
constants for its `code` argument. Agents branch on these strings; they
are part of the public surface contract.

Adding a new code:
1. Pick a prefix from VALID_PREFIXES below. If none fits, surface the
   gap on `docs/specs/data-recovery-contract.md` Req 3 and update the
   spec first — do not invent ad-hoc prefixes.
2. Add the constant ordered alphabetically within its prefix group.
3. The constant name is the value uppercased: IMPORT_PARSE_ERROR =
   "import_parse_error".

Codes are stable. Renaming a code is a breaking change for any agent
that branches on it; treat as one-way per .claude/rules/design-principles.md.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Import — loading raw data
# ---------------------------------------------------------------------------

IMPORT_FILE_NOT_FOUND = "import_file_not_found"
IMPORT_FORMAT_UNKNOWN = "import_format_unknown"
IMPORT_INVALID_FILE_PATH = "import_invalid_file_path"
IMPORT_PARSE_ERROR = "import_parse_error"
IMPORT_SUPERSEDED = "import_superseded"


# ---------------------------------------------------------------------------
# Mutation — app-state writes (categories, accounts, rules, etc.)
# ---------------------------------------------------------------------------

MUTATION_AMBIGUOUS = "mutation_ambiguous"
MUTATION_CONSTRAINT_VIOLATION = "mutation_constraint_violation"
MUTATION_INVALID_INPUT = "mutation_invalid_input"
MUTATION_NOT_FOUND = "mutation_not_found"


# ---------------------------------------------------------------------------
# Audit — doctor / invariant failures
# ---------------------------------------------------------------------------

AUDIT_FK_VIOLATION = "audit_fk_violation"
AUDIT_ORPHAN_STATE = "audit_orphan_state"
AUDIT_SIGN_VIOLATION = "audit_sign_violation"
AUDIT_UNBALANCED_TRANSFER = "audit_unbalanced_transfer"


# ---------------------------------------------------------------------------
# Refresh — pipeline (matcher / categorizer / SQLMesh)
# ---------------------------------------------------------------------------

REFRESH_CATEGORIZE_FAILED = "refresh_categorize_failed"
REFRESH_MATCH_FAILED = "refresh_match_failed"
REFRESH_MODEL_FAILED = "refresh_model_failed"


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

INFRA_DATABASE_LOCKED = "infra_database_locked"
INFRA_DATABASE_NOT_INITIALIZED = "infra_database_not_initialized"
INFRA_FILE_NOT_FOUND = "infra_file_not_found"
INFRA_INVALID_INPUT = "infra_invalid_input"
INFRA_IO_ERROR = "infra_io_error"
INFRA_NOT_FOUND = "infra_not_found"
INFRA_SCHEMA_DRIFT = "infra_schema_drift"
INFRA_TIMED_OUT = "infra_timed_out"
INFRA_TOO_MANY_ITEMS = "infra_too_many_items"
INFRA_WRONG_KEY = "infra_wrong_key"


# ---------------------------------------------------------------------------
# Sync — external connectors (Plaid, future SimpleFIN, etc.)
# ---------------------------------------------------------------------------

SYNC_ERROR = "sync_error"
