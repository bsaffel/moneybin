"""Tests for metrics registry definitions."""

import pytest
from prometheus_client import Counter, Gauge, Histogram


class TestMetricDefinitions:
    """Tests that all expected metrics are defined with correct types and labels."""

    @pytest.fixture(autouse=True)
    def _fresh_registry(self) -> None:
        """Placeholder for per-test registry isolation if needed later."""

    @pytest.mark.unit
    def test_import_records_total_is_counter(self) -> None:
        from moneybin.metrics.registry import IMPORT_RECORDS_TOTAL

        assert isinstance(IMPORT_RECORDS_TOTAL, Counter)

    @pytest.mark.unit
    def test_import_records_total_has_source_type_label(self) -> None:
        from moneybin.metrics.registry import IMPORT_RECORDS_TOTAL

        assert "source_type" in IMPORT_RECORDS_TOTAL._labelnames  # type: ignore[reportPrivateUsage,reportUnknownMemberType] — testing prometheus internals

    @pytest.mark.unit
    def test_import_duration_is_histogram(self) -> None:
        from moneybin.metrics.registry import IMPORT_DURATION_SECONDS

        assert isinstance(IMPORT_DURATION_SECONDS, Histogram)

    @pytest.mark.unit
    def test_import_errors_total_is_counter(self) -> None:
        from moneybin.metrics.registry import IMPORT_ERRORS_TOTAL

        assert isinstance(IMPORT_ERRORS_TOTAL, Counter)

    @pytest.mark.unit
    def test_categorization_auto_rate_is_gauge(self) -> None:
        from moneybin.metrics.registry import CATEGORIZATION_AUTO_RATE

        assert isinstance(CATEGORIZATION_AUTO_RATE, Gauge)

    @pytest.mark.unit
    def test_mcp_tool_calls_total_is_counter(self) -> None:
        from moneybin.metrics.registry import MCP_TOOL_CALLS_TOTAL

        assert isinstance(MCP_TOOL_CALLS_TOTAL, Counter)

    @pytest.mark.unit
    def test_mcp_tool_duration_is_histogram(self) -> None:
        from moneybin.metrics.registry import MCP_TOOL_DURATION_SECONDS

        assert isinstance(MCP_TOOL_DURATION_SECONDS, Histogram)

    @pytest.mark.unit
    def test_db_query_duration_is_histogram(self) -> None:
        from moneybin.metrics.registry import DB_QUERY_DURATION_SECONDS

        assert isinstance(DB_QUERY_DURATION_SECONDS, Histogram)
