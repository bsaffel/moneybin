"""V074: add immutable SimpleFIN observations and job/source receipts."""

from __future__ import annotations

_DDL = """
/* Immutable SimpleFIN accounts evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_accounts (
    batch_id UUID NOT NULL, -- Immutable broker batch identity
    source_origin VARCHAR NOT NULL, -- Scoped source identity
    account_id VARCHAR NOT NULL, -- Original native account identity
    native_scope JSON NOT NULL, -- Original protocol scope identity
    identity_strength VARCHAR NOT NULL, -- Strong native ID or weak v1 fallback
    observation_version VARCHAR NOT NULL, -- Full SHA-256 content revision
    original_record JSON NOT NULL, -- Original credential-free source record
    validation_error VARCHAR, -- Safe reason excluding this observation from projection
    fetched_at TIMESTAMPTZ NOT NULL, -- Broker fetch time, never client arrival time
    source_file VARCHAR NOT NULL, -- First delivery job lineage
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local observation time
    name VARCHAR, -- Provider account name
    currency VARCHAR, -- Original provider currency
    PRIMARY KEY (batch_id, source_origin, account_id)
);

/* Immutable SimpleFIN transactions evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_transactions (
    batch_id UUID NOT NULL, -- Immutable broker batch identity
    source_origin VARCHAR NOT NULL, -- Scoped source identity
    account_id VARCHAR NOT NULL, -- Original native account identity
    native_scope JSON NOT NULL, -- Original protocol scope identity
    identity_strength VARCHAR NOT NULL, -- Strong native ID or weak v1 fallback
    observation_version VARCHAR NOT NULL, -- Full SHA-256 content revision
    original_record JSON NOT NULL, -- Original credential-free source record
    validation_error VARCHAR, -- Safe reason excluding this observation from projection
    fetched_at TIMESTAMPTZ NOT NULL, -- Broker fetch time, never client arrival time
    source_file VARCHAR NOT NULL, -- First delivery job lineage
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local observation time
    transaction_id VARCHAR NOT NULL, -- Native transaction identity
    transaction_reference VARCHAR NOT NULL, -- Full framed source event identity
    amount DECIMAL(18,2), -- Exact supported signed monetary amount
    amount_original VARCHAR, -- Provider numeric string without normalization
    posted_at TIMESTAMPTZ, -- Valid provider posting instant
    transacted_at TIMESTAMPTZ, -- Optional provider transaction instant
    pending BOOLEAN, -- Explicit pending marker where supplied
    description VARCHAR, -- Original provider description
    currency VARCHAR, -- Original account currency
    PRIMARY KEY (batch_id, source_origin, account_id, transaction_id)
);

/* Immutable SimpleFIN balances evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_balances (
    batch_id UUID NOT NULL, -- Immutable broker batch identity
    source_origin VARCHAR NOT NULL, -- Scoped source identity
    account_id VARCHAR NOT NULL, -- Original native account identity
    native_scope JSON NOT NULL, -- Original protocol scope identity
    identity_strength VARCHAR NOT NULL, -- Strong native ID or weak v1 fallback
    observation_version VARCHAR NOT NULL, -- Full SHA-256 content revision
    original_record JSON NOT NULL, -- Original credential-free source record
    validation_error VARCHAR, -- Safe reason excluding this observation from projection
    fetched_at TIMESTAMPTZ NOT NULL, -- Broker fetch time, never client arrival time
    source_file VARCHAR NOT NULL, -- First delivery job lineage
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local observation time
    balance DECIMAL(18,2), -- Exact signed provider balance
    balance_original VARCHAR, -- Original balance numeric string
    available_balance DECIMAL(18,2), -- Optional exact available balance
    available_balance_original VARCHAR, -- Original available balance string
    balance_at TIMESTAMPTZ, -- Valid provider balance instant
    currency VARCHAR, -- Original account currency
    PRIMARY KEY (batch_id, source_origin, account_id)
);

/* Immutable SimpleFIN responses evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_responses (
    batch_id UUID PRIMARY KEY, -- Immutable broker batch identity
    connection_id UUID NOT NULL, -- Broker grant identity
    credential_generation INTEGER NOT NULL, -- Broker credential generation
    protocol_version VARCHAR NOT NULL, -- Pinned upstream protocol version
    fetched_at TIMESTAMPTZ NOT NULL, -- Broker fetch time
    window_start TIMESTAMPTZ NOT NULL, -- Inclusive requested UTC boundary
    window_end TIMESTAMPTZ NOT NULL, -- Exclusive requested UTC boundary
    original_payload JSON NOT NULL, -- Original credential-free Account Set
    source_errors JSON NOT NULL, -- Original source error objects or strings
    content_digest VARCHAR NOT NULL, -- Immutable complete batch digest
    source_file VARCHAR NOT NULL, -- First delivery job lineage
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP -- Local observation time
);

/* Immutable SimpleFIN sync receipts evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_sync_receipts (
    job_id UUID NOT NULL, -- Authenticated broker job identity
    connection_id UUID NOT NULL, -- Broker grant identity
    credential_generation INTEGER NOT NULL, -- Observed credential generation
    protocol_version VARCHAR NOT NULL, -- Pinned protocol version
    status VARCHAR NOT NULL, -- Authenticated terminal broker job status
    started_at TIMESTAMPTZ NOT NULL, -- Broker requested-job order
    completed_at TIMESTAMPTZ NOT NULL, -- Authenticated broker completion evidence
    batch_ids JSON NOT NULL, -- Immutable batch identities including an empty list
    source_metadata JSON NOT NULL, -- Complete accepted source metadata excluding payload batches
    local_outcomes JSON NOT NULL, -- Safe local validation outcomes including unscoped failures
    content_digest VARCHAR NOT NULL, -- Immutable job/source receipt digest
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local receipt observation time
    PRIMARY KEY (job_id, connection_id)
);
"""


def migrate(conn: object) -> None:
    """Create provider-owned Raw tables inside the runner transaction."""
    conn.execute(_DDL)  # type: ignore[attr-defined]
