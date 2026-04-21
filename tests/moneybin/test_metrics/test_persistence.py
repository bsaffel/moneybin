"""Tests for metrics persistence (flush to / load from DuckDB)."""

import json
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


@pytest.fixture()
def fresh_registry() -> CollectorRegistry:
    """Create a fresh prometheus registry for isolation."""
    return CollectorRegistry()


@pytest.fixture()
def mock_db() -> MagicMock:
    """Create a mock Database with an in-memory DuckDB for real SQL."""
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS app")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app.metrics (
            metric_name VARCHAR NOT NULL,
            metric_type VARCHAR NOT NULL,
            labels JSON,
            value DOUBLE NOT NULL,
            bucket_bounds DOUBLE[],
            bucket_counts BIGINT[],
            recorded_at TIMESTAMP NOT NULL
        )
    """)

    db = MagicMock()
    db.conn = conn
    db.execute = conn.execute
    return db


class TestFlushToDuckDB:
    """Tests for flush_to_duckdb."""

    @pytest.mark.unit
    def test_flush_counter(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Counter values should be flushed as metric rows."""
        from moneybin.metrics.persistence import flush_to_duckdb

        counter = Counter(
            "test_counter", "A test counter", ["op"], registry=fresh_registry
        )
        counter.labels(op="read").inc(5)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        rows = mock_db.execute(
            "SELECT metric_name, metric_type, labels, value FROM app.metrics "
            "WHERE metric_name = ?",
            ["test_counter"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "test_counter"
        assert rows[0][1] == "counter"
        labels = json.loads(rows[0][2])
        assert labels["op"] == "read"
        assert rows[0][3] == 5.0

    @pytest.mark.unit
    def test_flush_gauge(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Gauge values should be flushed."""
        from moneybin.metrics.persistence import flush_to_duckdb

        gauge = Gauge("test_gauge", "A test gauge", registry=fresh_registry)
        gauge.set(0.78)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        rows = mock_db.execute(
            "SELECT metric_type, value FROM app.metrics WHERE metric_name = ?",
            ["test_gauge"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "gauge"
        assert abs(rows[0][1] - 0.78) < 0.001

    @pytest.mark.unit
    def test_flush_histogram(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Histogram should flush with bucket bounds and counts."""
        from moneybin.metrics.persistence import flush_to_duckdb

        hist = Histogram(
            "test_hist",
            "A test histogram",
            ["op"],
            buckets=[0.1, 0.5, 1.0],
            registry=fresh_registry,
        )
        hist.labels(op="query").observe(0.3)
        hist.labels(op="query").observe(0.7)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        rows = mock_db.execute(
            "SELECT metric_type, value, bucket_bounds, bucket_counts FROM app.metrics "
            "WHERE metric_name = ?",
            ["test_hist"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "histogram"
        assert rows[0][1] == pytest.approx(1.0, abs=0.01)  # type: ignore[reportUnknownMemberType] — pytest.approx stub incomplete  # sum of 0.3 + 0.7
        assert rows[0][2] is not None  # bucket_bounds
        assert rows[0][3] is not None  # bucket_counts


class TestLoadFromDuckDB:
    """Tests for load_from_duckdb."""

    @pytest.mark.unit
    def test_load_restores_counter(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Counter should be restored from last snapshot."""
        from moneybin.metrics.persistence import flush_to_duckdb, load_from_duckdb

        counter = Counter(
            "test_restore_counter",
            "Restorable counter",
            ["op"],
            registry=fresh_registry,
        )
        counter.labels(op="write").inc(10)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        # Create a new registry to simulate restart
        new_registry = CollectorRegistry()
        Counter(
            "test_restore_counter",
            "Restorable counter",
            ["op"],
            registry=new_registry,
        )

        load_from_duckdb(mock_db, registry=new_registry)

        # Counter should be restored
        value = new_registry.get_sample_value(
            "test_restore_counter_total", {"op": "write"}
        )
        assert value == 10.0

    @pytest.mark.unit
    def test_load_does_not_restore_gauge(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Gauges are point-in-time; should NOT be restored."""
        from moneybin.metrics.persistence import flush_to_duckdb, load_from_duckdb

        gauge = Gauge("test_restore_gauge", "Restorable gauge", registry=fresh_registry)
        gauge.set(42.0)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        new_registry = CollectorRegistry()
        Gauge("test_restore_gauge", "Restorable gauge", registry=new_registry)

        load_from_duckdb(mock_db, registry=new_registry)

        value = new_registry.get_sample_value("test_restore_gauge", {})
        assert value == 0.0  # Not restored — gauge starts at 0

    @pytest.mark.unit
    def test_load_empty_table_is_noop(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Loading from empty metrics table should not raise."""
        from moneybin.metrics.persistence import load_from_duckdb

        load_from_duckdb(mock_db, registry=fresh_registry)
        # No error raised
