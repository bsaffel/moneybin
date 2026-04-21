/* Prometheus metric snapshots flushed periodically and on shutdown; each row is a point-in-time snapshot of one metric */
CREATE TABLE IF NOT EXISTS app.metrics (
    metric_name VARCHAR NOT NULL, -- Prometheus metric name (e.g. 'moneybin_import_records_total')
    metric_type VARCHAR NOT NULL, -- One of: 'counter', 'histogram', 'gauge'
    labels JSON, -- Label key-value pairs as JSON object
    value DOUBLE NOT NULL, -- Counter/gauge current value, or histogram sum
    bucket_bounds DOUBLE[], -- Histogram upper bounds (NULL for counter/gauge)
    bucket_counts BIGINT[], -- Histogram cumulative bucket counts (NULL for counter/gauge)
    recorded_at TIMESTAMP NOT NULL -- When this snapshot was taken
);
