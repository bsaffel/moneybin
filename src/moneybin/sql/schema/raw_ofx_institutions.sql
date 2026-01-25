-- Raw OFX institutions table
-- Stores financial institution information from OFX/QFX files
CREATE TABLE IF NOT EXISTS raw.ofx_institutions (
    organization VARCHAR,
    fid VARCHAR,
    source_file VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization, fid)
);
