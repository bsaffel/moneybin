"""Metrics collection and instrumentation for MoneyBin.

Public API:
    - Metric constants (``IMPORT_RECORDS_TOTAL``, etc.) for manual recording
    - ``flush_to_duckdb()`` / ``load_from_duckdb()`` in ``persistence`` submodule

For instrumentation, use ``moneybin.observability.tracked`` and
``moneybin.observability.track_duration`` instead of importing from here.
"""

from .registry import (
    CATEGORIZATION_AUTO_RATE,
    CATEGORIZATION_RULES_FIRED_TOTAL,
    DB_QUERY_DURATION_SECONDS,
    DEDUP_MATCHES_TOTAL,
    IMPORT_DURATION_SECONDS,
    IMPORT_ERRORS_TOTAL,
    IMPORT_RECORDS_TOTAL,
    MCP_TOOL_CALLS_TOTAL,
    MCP_TOOL_DURATION_SECONDS,
    SQLMESH_RUN_DURATION_SECONDS,
)

__all__ = [
    "CATEGORIZATION_AUTO_RATE",
    "CATEGORIZATION_RULES_FIRED_TOTAL",
    "DB_QUERY_DURATION_SECONDS",
    "DEDUP_MATCHES_TOTAL",
    "IMPORT_DURATION_SECONDS",
    "IMPORT_ERRORS_TOTAL",
    "IMPORT_RECORDS_TOTAL",
    "MCP_TOOL_CALLS_TOTAL",
    "MCP_TOOL_DURATION_SECONDS",
    "SQLMESH_RUN_DURATION_SECONDS",
]
