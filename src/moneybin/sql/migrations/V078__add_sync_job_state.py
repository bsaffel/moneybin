"""V078: add operational recovery for durably ingested broker jobs."""

from __future__ import annotations

_DDL = "/* Operational recovery for a durably ingested broker job; not user-editable state. */\nCREATE TABLE IF NOT EXISTS app.sync_job_state (\n    job_id UUID PRIMARY KEY, -- Immutable broker job identity\n    consumer_id VARCHAR NOT NULL, -- Internal owning profile identity\n    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local durable ingestion time\n    ack_state VARCHAR NOT NULL DEFAULT 'pending' CHECK (ack_state IN ('pending', 'confirmed', 'unknown', 'unavailable')), -- Broker acknowledgement confirmation\n    acked_at TIMESTAMPTZ, -- Broker-confirmed acknowledgement time\n    setup_state VARCHAR NOT NULL DEFAULT 'pending' CHECK (setup_state IN ('pending', 'complete', 'failed')), -- Local account and security binding obligation\n    refresh_state VARCHAR NOT NULL DEFAULT 'pending' CHECK (refresh_state IN ('pending', 'deferred', 'complete', 'failed')), -- Independent local derivation obligation\n    job_details JSON NOT NULL, -- Protected typed source references, receipt and post-ack evidence\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last local recovery transition\n);\n"


def migrate(conn: object) -> None:
    """Add recovery bookkeeping without changing immutable source evidence."""
    conn.execute(_DDL)  # type: ignore[attr-defined]
