/* Unified audit log for user-driven mutations to in-scope app state plus
   AI-call provenance (subsumes the previously planned app.ai_audit_log).
   Audit emission is synchronous in the same DuckDB transaction as the
   mutation. AI-specific fields (flow_tier, backend, model, data_sent_hash,
   consent_reference, user_initiated) ride context_json — promoted to indexed
   columns only when a real query pattern demands it. */
CREATE TABLE IF NOT EXISTS app.audit_log (
    audit_id        VARCHAR PRIMARY KEY,                          -- Truncated UUID4 (12 hex)
    occurred_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the event happened
    actor           VARCHAR NOT NULL,                             -- 'cli', 'mcp', 'auto_rule', 'system', 'ai:<provider>:<model>'
    action          VARCHAR NOT NULL,                             -- e.g. 'manual.create', 'note.add', 'tag.rename', 'split.add', 'category.set', 'ai.external_call'
    target_schema   VARCHAR,                                      -- e.g. 'app', 'core'
    target_table    VARCHAR,                                      -- e.g. 'transaction_categories', 'transaction_tags'
    target_id       VARCHAR,                                      -- gold transaction_id, rule_id, merchant_id, import_id, etc.
    before_value    JSON,                                         -- Prior column subset; NULL on creation
    after_value     JSON,                                         -- New column subset; NULL on deletion
    parent_audit_id VARCHAR,                                      -- Self-FK; chains AI-call → user-confirm → category-write, or bulk-rename → per-row events
    context_json    JSON                                          -- Discriminator-shaped extras: AI fields (flow_tier, backend, model, data_sent_hash), source surface, hashes, etc.
);
CREATE INDEX IF NOT EXISTS idx_audit_log_target ON app.audit_log(target_table, target_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_occurred ON app.audit_log(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON app.audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_actor ON app.audit_log(actor);
CREATE INDEX IF NOT EXISTS idx_audit_log_parent ON app.audit_log(parent_audit_id);
