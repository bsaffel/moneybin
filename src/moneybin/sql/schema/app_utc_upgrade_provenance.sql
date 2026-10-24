/* Migration-owned historical clock attribution; never rewrites original audit JSON */
CREATE TABLE IF NOT EXISTS app.utc_upgrade_provenance (
    contract_version INTEGER PRIMARY KEY CHECK (contract_version = 2), -- Temporal interpretation contract version
    rules JSON NOT NULL -- Accepted column writer eras and explicit historical timezone evidence
);
