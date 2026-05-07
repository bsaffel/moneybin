"""Metric definitions for MoneyBin.

All metrics use the ``moneybin_`` prefix. Each metric is a module-level
constant bound to the default prometheus_client registry.

Adding a new metric: define it here, then either use ``@tracked`` at the
call site or record manually (e.g. ``CATEGORIZATION_AUTO_RATE.set(0.78)``).
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Import pipeline ──────────────────────────────────────────────────────────

IMPORT_RECORDS_TOTAL = Counter(
    "moneybin_import_records_total",
    "Total records imported across all sources",
    ["source_type"],
)

IMPORT_DURATION_SECONDS = Histogram(
    "moneybin_import_duration_seconds",
    "Duration of import operations in seconds",
    ["source_type"],
)

IMPORT_ERRORS_TOTAL = Counter(
    "moneybin_import_errors_total",
    "Total import errors by source and error type",
    ["source_type", "error_type"],
)

INBOX_SYNC_TOTAL = Counter(
    "moneybin_inbox_sync_total",
    "Inbox file outcomes per sync",
    ["outcome"],
)

INBOX_SYNC_DURATION_SECONDS = Histogram(
    "moneybin_inbox_sync_duration_seconds",
    "Duration of one inbox drain (seconds)",
)

# ── Tabular import ───────────────────────────────────────────────────────────

TABULAR_FORMAT_MATCHES = Counter(
    "moneybin_tabular_format_matches_total",
    "Tabular format matches by format name and source",
    ["format_name", "format_source"],
)

TABULAR_DETECTION_CONFIDENCE = Counter(
    "moneybin_tabular_detection_confidence_total",
    "Column mapping detection confidence distribution",
    ["confidence"],
)

TABULAR_IMPORT_BATCHES = Counter(
    "moneybin_tabular_import_batches_total",
    "Import batch lifecycle events",
    ["status"],
)

OFX_IMPORT_BATCHES = Counter(
    "moneybin_ofx_import_batches_total",
    "OFX/QFX/QBO import batches by status (complete, partial, failed).",
    labelnames=("status",),
)

# ── SQLMesh transforms ───────────────────────────────────────────────────────

SQLMESH_RUN_DURATION_SECONDS = Histogram(
    "moneybin_sqlmesh_run_duration_seconds",
    "Duration of SQLMesh model runs in seconds",
    ["model"],
)

# ── Deduplication ─────────────────────────────────────────────────────────────

DEDUP_MATCHES_TOTAL = Counter(
    "moneybin_dedup_matches_total",
    "Total duplicate records matched and merged",
    ["match_tier", "decided_by"],
)

DEDUP_PAIRS_SCORED = Counter(
    "moneybin_dedup_pairs_scored_total",
    "Total candidate pairs scored by the matching engine",
)

DEDUP_REVIEW_PENDING = Gauge(
    "moneybin_dedup_review_pending",
    "Number of match proposals awaiting user review",
)

DEDUP_MATCH_CONFIDENCE = Histogram(
    "moneybin_dedup_match_confidence",
    "Distribution of match confidence scores",
)

# ── Transfer detection ───────────────────────────────────────────────────────

TRANSFER_PAIRS_SCORED = Counter(
    "moneybin_transfer_pairs_scored_total",
    "Total transfer candidate pairs scored by the matching engine",
)

TRANSFER_MATCHES_PROPOSED = Counter(
    "moneybin_transfer_matches_proposed_total",
    "Total transfer pairs proposed for review",
)

TRANSFER_MATCH_CONFIDENCE = Histogram(
    "moneybin_transfer_match_confidence",
    "Distribution of transfer match confidence scores",
)

# ── Categorization ────────────────────────────────────────────────────────────

CATEGORIZATION_AUTO_RATE = Gauge(
    "moneybin_categorization_auto_rate",
    "Fraction of transactions auto-categorized (0.0–1.0)",
)

CATEGORIZATION_RULES_FIRED_TOTAL = Counter(
    "moneybin_categorization_rules_fired_total",
    "Total categorization rule firings by rule",
    ["rule_id"],
)

CATEGORIZE_BULK_ITEMS_TOTAL = Counter(
    "moneybin_categorize_bulk_items_total",
    "Number of items processed by bulk_categorize, by outcome",
    ["outcome"],
)

CATEGORIZE_BULK_DURATION_SECONDS = Histogram(
    "moneybin_categorize_bulk_duration_seconds",
    "Wall-clock duration of CategorizationService.bulk_categorize calls",
)

CATEGORIZE_BULK_ERRORS_TOTAL = Counter(
    "moneybin_categorize_bulk_errors_total",
    "Number of bulk_categorize calls that raised before returning a result",
)

CATEGORIZE_ASSIST_CALLS_TOTAL = Counter(
    "moneybin_categorize_assist_calls_total",
    "Number of categorize_assist invocations (MCP + CLI)",
    ["surface"],
)

CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL = Counter(
    "moneybin_categorize_assist_txns_returned_total",
    "Total redacted transactions returned across all categorize_assist calls",
)

CATEGORIZE_ASSIST_DURATION_SECONDS = Histogram(
    "moneybin_categorize_assist_duration_seconds",
    "Duration of categorize_assist server-side processing (excludes LLM time)",
)

# ── Account matching ─────────────────────────────────────────────────────────

ACCOUNT_MATCH_OUTCOMES_TOTAL = Counter(
    "moneybin_account_match_outcomes_total",
    "Outcomes of account-name resolution during tabular import",
    ["result"],
)

# ── MCP server ────────────────────────────────────────────────────────────────

MCP_TOOL_CALLS_TOTAL = Counter(
    "moneybin_mcp_tool_calls_total",
    "Total MCP tool invocations by tool name",
    ["tool_name"],
)

MCP_TOOL_DURATION_SECONDS = Histogram(
    "moneybin_mcp_tool_duration_seconds",
    "Duration of MCP tool calls in seconds",
    ["tool_name"],
)

# ── Synthetic data ───────────────────────────────────────────────────────────

SYNTHETIC_GENERATED_TRANSACTIONS_TOTAL = Counter(
    "moneybin_synthetic_generated_transactions_total",
    "Total synthetic transactions generated",
    ["persona"],
)

SYNTHETIC_GENERATION_DURATION_SECONDS = Histogram(
    "moneybin_synthetic_generation_duration_seconds",
    "Duration of synthetic data generation runs in seconds",
    ["persona"],
)

SYNTHETIC_RESET_TOTAL = Counter(
    "moneybin_synthetic_reset_total",
    "Total synthetic dataset resets performed",
    ["persona"],
)

# ── Database ──────────────────────────────────────────────────────────────────

DB_QUERY_DURATION_SECONDS = Histogram(
    "moneybin_db_query_duration_seconds",
    "Duration of database queries in seconds",
    ["operation"],
)
