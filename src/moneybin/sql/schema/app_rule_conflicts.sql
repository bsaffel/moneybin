/* Categorization rules that claim the same canonical matcher but disagree about the category, held for an explicit user decision. A row binds to (existing_rule_id, existing_rule_updated_at): editing the rule changes its updated_at, so the recorded conflict no longer describes live state and drops out of the review queue. All mutations route through RuleConflictsRepo to emit paired app.audit_log rows per app-integrity-invariant.md. */
CREATE TABLE IF NOT EXISTS app.rule_conflicts (
    conflict_id VARCHAR PRIMARY KEY, -- Content hash over the conflict's identity ('conf_' + 16 hex of the existing rule, its updated_at, the matcher digest, and the proposed category) so re-detecting the same conflict updates one row
    matcher_digest VARCHAR NOT NULL, -- SHA-256 over the canonical matcher key (pattern, match type, amount bounds, account) shared by both rules; see canonical_matcher_key
    existing_rule_id VARCHAR NOT NULL, -- The active app.categorization_rules row that already owns this matcher
    existing_rule_updated_at TIMESTAMP NOT NULL, -- The existing rule's updated_at when the conflict was detected; a mismatch against live state means the conflict is stale
    existing_name VARCHAR NOT NULL, -- The existing rule's label, so review can name it without a second read
    existing_category VARCHAR NOT NULL, -- Category the existing rule assigns
    existing_subcategory VARCHAR, -- Subcategory the existing rule assigns; NULL when it assigns none
    existing_priority INTEGER NOT NULL, -- The existing rule's evaluation priority; lower wins
    proposed_name VARCHAR NOT NULL, -- Label of the rule that was refused
    proposed_merchant_pattern VARCHAR NOT NULL, -- Pattern of the refused rule, stored verbatim as authored
    proposed_match_type VARCHAR NOT NULL, -- How the refused rule's pattern is applied: contains, exact, or regex
    proposed_min_amount DECIMAL(18, 2), -- Lower bound of the refused rule; NULL means no lower bound
    proposed_max_amount DECIMAL(18, 2), -- Upper bound of the refused rule; NULL means no upper bound
    proposed_account_id VARCHAR, -- Account the refused rule was scoped to; NULL means all accounts
    proposed_category VARCHAR NOT NULL, -- Category the refused rule would have assigned
    proposed_subcategory VARCHAR, -- Subcategory the refused rule would have assigned; NULL when it assigns none
    proposed_priority INTEGER NOT NULL, -- Evaluation priority the refused rule asked for
    proposed_created_by VARCHAR NOT NULL, -- Who authored the refused rule: user or ai
    status VARCHAR NOT NULL DEFAULT 'pending', -- pending until decided; resolved once the user replaced, reprioritized, or cancelled it
    resolution VARCHAR, -- The decision taken: replace, reprioritize, or cancel; NULL while pending
    resolved_rule_id VARCHAR, -- Rule the resolution activated; NULL for cancel and while pending
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When rule creation refused the proposal and recorded this conflict
    resolved_at TIMESTAMP -- When the user decided; NULL while pending
);
