/* Learned PDF layouts. On first contact the bridge proposes a recipe + mapping +
   routing; the user vets it; it is saved here. Future PDFs whose fingerprint
   matches replay the recipe deterministically with no bridge. Parallels
   app.tabular_formats. */
CREATE TABLE IF NOT EXISTS app.pdf_formats (
    name VARCHAR PRIMARY KEY,            -- Machine identifier (e.g. "chase_checking_pdf")
    institution_name VARCHAR NOT NULL,   -- Human-readable issuer (e.g. "Chase")
    document_kind VARCHAR NOT NULL,      -- Free slug for the document type (e.g. "checking_statement", "1099b")
    layout_fingerprint JSON NOT NULL,    -- Text/structural signature used to recognize this layout on future imports
    front_end VARCHAR NOT NULL,          -- IR producer for replay: text (pdfplumber), table (camelot), or vision (re-run via bridge; not cheap-replay)
    extraction_recipe JSON NOT NULL,     -- Declarative rules: metadata-capture anchors (account id, period, balances) + IR->rows rules (region anchors, row delimiters, field order, type/sign)
    routing VARCHAR NOT NULL CHECK (routing IN ('transactions', 'seed')), -- Outcome this format produces
    field_mapping JSON,                  -- Destination field -> extracted field (transactions routing); NULL for seed
    seed_alias VARCHAR,                  -- View alias for routing='seed' (raw.pdf_<seed_alias>); NULL for transactions
    sign_convention VARCHAR,             -- negative_is_expense | negative_is_income (credit cards, confirm-gated) | split_debit_credit (transactions)
    date_format VARCHAR,                 -- strftime format for date parsing
    number_format VARCHAR NOT NULL DEFAULT 'us', -- us | european | swiss_french | zero_decimal
    source VARCHAR NOT NULL DEFAULT 'detected', -- detected (machine auto-derive) | bridge (agent-authored, human-vetted) | manual (hand-authored/edited). Self-heal re-derives only 'detected' rows -- see extractors/pdf/routing.py::_attempt_self_heal
    version INTEGER NOT NULL DEFAULT 1,         -- Bumped on each recipe refresh via PdfFormatsRepo.bump_version (mutation audited per Invariant 10); prior versions recoverable through app.audit_log undo (Invariant 11, data-recovery-contract.md)
    times_used INTEGER NOT NULL DEFAULT 0,      -- Successful imports using this format
    last_used_at TIMESTAMP,                     -- Most recent successful use
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this format was first created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp when this format was last modified
);
