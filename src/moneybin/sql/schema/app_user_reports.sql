/* User-created saved reports, evaluated at query time. All mutations route through UserReportsRepo to emit paired app.audit_log rows per app-integrity-invariant.md. */
CREATE TABLE IF NOT EXISTS app.user_reports (
    report_id VARCHAR PRIMARY KEY, -- Namespaced letter-led saved-report identity ('user:r' + uuid4().hex[:12]); the audit target and the identity that survives a rename
    name VARCHAR NOT NULL UNIQUE, -- User-facing report name; the handle every lifecycle operation takes, resolved to report_id at the service boundary
    description VARCHAR, -- Agent-visible summary of what the report answers
    query_sql VARCHAR NOT NULL, -- User-authored read-only SELECT carrying $name placeholders bound at run time
    params JSON NOT NULL DEFAULT '[]', -- Declared ParamSpec list; data_class is derived at save, never declared by the author
    classes JSON NOT NULL, -- Derived output-column privacy map, keyed by DuckDB result column name
    semantics JSON NOT NULL, -- ReportSemantics fields, explicitly unknown for an arbitrary user query
    class_downgrades JSON NOT NULL DEFAULT '{}', -- Approved downgrades as {column: {from, to, reason}}; 'from' is the derived class the approval was granted against
    class_fingerprint VARCHAR NOT NULL, -- Hash over the derivation inputs; a mismatch forces re-resolution before the run rather than trusting the stored map
    is_active BOOLEAN NOT NULL DEFAULT true, -- False archives the report: hidden from the default catalog, still runnable by name
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Saved-report creation timestamp
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last saved-report mutation timestamp
);
