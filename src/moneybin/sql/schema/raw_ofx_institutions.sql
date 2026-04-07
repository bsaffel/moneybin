/* Financial institution metadata from OFX/QFX files; maps institution org name (ORG) to numeric routing identifier (FID) */
CREATE TABLE IF NOT EXISTS raw.ofx_institutions (
    organization VARCHAR, -- OFX <ORG> element; human-readable institution name; part of primary key
    fid VARCHAR, -- OFX <FID> element; numeric identifier used in OFX routing; part of primary key
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (organization, fid)
);
