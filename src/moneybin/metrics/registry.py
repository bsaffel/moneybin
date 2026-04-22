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
