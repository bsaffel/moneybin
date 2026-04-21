"""Tests for instrumentation decorators and context managers."""

import logging
import time

import pytest
from prometheus_client import REGISTRY


class TestTracked:
    """Tests for the @tracked decorator."""

    @pytest.mark.unit
    def test_tracked_function_returns_normally(self) -> None:
        """Decorated function should return its result unchanged."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_op")
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5

    @pytest.mark.unit
    def test_tracked_increments_call_counter(self) -> None:
        """@tracked should increment the operation's call counter."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_calls")
        def noop() -> None:
            pass

        before = REGISTRY.get_sample_value(
            "moneybin_tracked_calls_total",
            {"operation": "test_calls", "source_type": ""},
        )
        noop()
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_calls_total",
            {"operation": "test_calls", "source_type": ""},
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_tracked_observes_duration(self) -> None:
        """@tracked should record duration in histogram."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_duration")
        def slow() -> None:
            time.sleep(0.05)

        before = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_duration", "source_type": ""},
        )
        slow()
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_duration", "source_type": ""},
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_tracked_increments_error_counter_on_exception(self) -> None:
        """@tracked should increment error counter when function raises."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_errors")
        def fail() -> None:
            raise ValueError("boom")

        before = REGISTRY.get_sample_value(
            "moneybin_tracked_errors_total",
            {
                "operation": "test_errors",
                "error_type": "ValueError",
                "source_type": "",
            },
        )
        with pytest.raises(ValueError, match="boom"):
            fail()
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_errors_total",
            {
                "operation": "test_errors",
                "error_type": "ValueError",
                "source_type": "",
            },
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_tracked_emits_debug_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """@tracked should emit a DEBUG log line on completion."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_log")
        def ping() -> str:
            return "pong"

        with caplog.at_level(logging.DEBUG):
            ping()

        assert any("test_log" in r.message for r in caplog.records)

    @pytest.mark.unit
    def test_tracked_with_labels(self) -> None:
        """@tracked should support static labels."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_labels", labels={"source_type": "csv"})
        def import_csv() -> None:
            pass

        import_csv()
        value = REGISTRY.get_sample_value(
            "moneybin_tracked_calls_total",
            {"operation": "test_labels", "source_type": "csv"},
        )
        assert value is not None and value >= 1


class TestTrackDuration:
    """Tests for the track_duration context manager."""

    @pytest.mark.unit
    def test_track_duration_records_histogram(self) -> None:
        """track_duration should record duration in the histogram."""
        from moneybin.metrics.instruments import track_duration

        before = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_ctx", "source_type": ""},
        )
        with track_duration("test_ctx"):
            time.sleep(0.01)
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_ctx", "source_type": ""},
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_track_duration_emits_debug_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """track_duration should emit a DEBUG log on exit."""
        from moneybin.metrics.instruments import track_duration

        with caplog.at_level(logging.DEBUG):
            with track_duration("test_ctx_log"):
                pass

        assert any("test_ctx_log" in r.message for r in caplog.records)
