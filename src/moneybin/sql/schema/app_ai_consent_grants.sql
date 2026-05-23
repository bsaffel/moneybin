/* Consent ledger: one row per granted (feature_category, backend) authorization for sending real financial data to an AI backend. Persisted record + user control surface; the data-withholding enforcement gate is deferred (ledger-first, see privacy-and-ai-trust.md). All mutations route through ConsentRepo to emit paired app.audit_log rows per app-integrity-invariant.md. Revoked grants are retained (revoked_at set) for audit history. */
CREATE TABLE IF NOT EXISTS app.ai_consent_grants (
    grant_id VARCHAR PRIMARY KEY, -- Truncated UUID4 (uuid4().hex[:12]) per identifiers.md strategy 3
    feature_category VARCHAR NOT NULL, -- AI flow category: mcp-data-sharing, smart-import-parsing, ml-categorization, matching-overview (free string; per-tool granularity deferred)
    backend VARCHAR NOT NULL, -- AI backend this consent applies to (e.g. anthropic, openai, ollama); consent is per (feature_category, backend)
    consent_mode VARCHAR NOT NULL CHECK (consent_mode IN ('persistent', 'one-time')), -- persistent: survives sessions until revoked; one-time: single authorized use
    granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the user granted this consent
    revoked_at TIMESTAMP, -- When revoked; NULL while active. Revoked rows are retained for audit history.
    grant_prompt TEXT NOT NULL -- Exact consent text the user saw and agreed to (source of truth for "what did I consent to?"); not surfaced in read payloads
);
