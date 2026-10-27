/* Exact source membership, immutable canonical bindings and Golden identity history. */
CREATE TABLE IF NOT EXISTS app.investment_event_members (
    membership_id VARCHAR PRIMARY KEY, -- Identity of this immutable membership revision
    event_group_id VARCHAR NOT NULL, -- MoneyBin-owned Golden economic event
    investment_transaction_id VARCHAR NOT NULL, -- MoneyBin-owned Golden semantic leg
    member_kind VARCHAR NOT NULL CHECK (member_kind IN ('source_event', 'opening_lot_reconstruction')), -- Source-backed or reconstructed membership
    source_event_key VARCHAR NOT NULL, -- Complete adapter-constructed Source event
    source_type VARCHAR NOT NULL, -- Comparison adapter type
    source_origin VARCHAR NOT NULL, -- Source identity scope
    native_reference VARCHAR NOT NULL, -- Stable source row or reconstruction key
    observation_version VARCHAR NOT NULL, -- Exact immutable observation or reconstruction revision
    semantic_leg_role VARCHAR NOT NULL, -- Correspondence within the complete economic event
    bound_account_id VARCHAR NOT NULL, -- Account selected at publication
    bound_security_id VARCHAR, -- Security selected at publication; NULL for cash-only legs
    bound_effective_currency VARCHAR NOT NULL, -- Exact denomination selected at publication
    currency_source VARCHAR NOT NULL CHECK (currency_source IN ('source', 'account')), -- Explicit source denomination or inherited Account currency
    account_generation VARCHAR NOT NULL, -- Producing Account identity dependency generation
    security_generation VARCHAR NOT NULL, -- Producing Security identity dependency generation
    currency_generation VARCHAR NOT NULL, -- Producing Currency dependency generation
    decision_id VARCHAR, -- Owning accepted Match; NULL for standalone membership
    reconstruction_evidence JSON, -- Exact reconstruction evidence references and algorithm version, never an observation-value snapshot
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Initial activation time
    retired_at TIMESTAMPTZ, -- Retirement time; NULL while active
    projection_changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP -- Committed projection transition watermark
);
