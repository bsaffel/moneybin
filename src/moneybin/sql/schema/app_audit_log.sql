/* Unified audit log for user-driven mutations to in-scope app state plus
   AI-call provenance (subsumes the previously planned app.ai_audit_log).
   Audit emission is synchronous in the same DuckDB transaction as the
   mutation. AI-specific fields (flow_tier, backend, model, data_sent_hash,
   consent_reference, user_initiated) ride context_json — promoted to indexed
   columns only when a real query pattern demands it. */
CREATE TABLE IF NOT EXISTS app.audit_log (
    audit_id        VARCHAR PRIMARY KEY,                          -- Full UUID4 hex (32 chars). Audit log row count grows with every mutation, plus per-row tag.rename_row children — sized for >100K rows per identifiers.md.
    occurred_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the event happened
    actor           VARCHAR NOT NULL,                             -- 'cli', 'mcp', 'auto_rule', 'system', 'ai:<provider>:<model>'
    action          VARCHAR NOT NULL,                             -- e.g. 'manual.create', 'note.add', 'tag.rename', 'split.add', 'category.set', 'ai.external_call'
    target_schema   VARCHAR,                                      -- e.g. 'app', 'core'
    target_table    VARCHAR,                                      -- e.g. 'transaction_categories', 'transaction_tags'
    target_id       VARCHAR,                                      -- gold transaction_id, rule_id, merchant_id, import_id, etc.
    before_value    JSON,                                         -- Full prior row state; NULL on creation (INSERT). Invariant 10: complete pre-mutation row, not a diff.
    after_value     JSON,                                         -- Full resulting row state; NULL on deletion (DELETE). Invariant 10: complete post-mutation row, not a diff.
    parent_audit_id VARCHAR,                                      -- Self-FK; chains AI-call → user-confirm → category-write, or bulk-rename → per-row events
    context_json    JSON,                                         -- Discriminator-shaped extras: AI fields (flow_tier, backend, model, data_sent_hash), source surface, hashes, etc.
    operation_id    VARCHAR NOT NULL,                             -- Per-call group: every row from one MCP/CLI call shares this op_<uuid4_hex>. A flat sibling group (vs parent_audit_id's causal tree); the unit a later system_audit_undo reverses. Set by the service-layer MutationContext. (Placed last + indexes deferred to V023 — see below.)
    is_undo         BOOLEAN NOT NULL DEFAULT FALSE,               -- TRUE for rows produced by system_audit_undo; FALSE for original mutations. (Added by V024.)
    undoes_operation_id VARCHAR                                   -- When is_undo=TRUE, the operation_id this undo reverses; NULL otherwise. (Added by V024.)
);
CREATE INDEX IF NOT EXISTS idx_audit_log_target ON app.audit_log(target_table, target_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_occurred ON app.audit_log(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON app.audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_actor ON app.audit_log(actor);
CREATE INDEX IF NOT EXISTS idx_audit_log_parent ON app.audit_log(parent_audit_id);
-- idx_audit_log_operation_id and idx_audit_log_occurred_at_op live in
-- V023 because operation_id is added by that migration; this schema file
-- runs before migrations (init_schemas → MigrationRunner), so an index DDL
-- here would bind against the pre-V023 table shape on existing databases.
