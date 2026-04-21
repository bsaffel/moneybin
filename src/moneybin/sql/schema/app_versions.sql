/* Component version tracking for upgrade detection */
CREATE TABLE IF NOT EXISTS app.versions (
    component VARCHAR PRIMARY KEY,     -- component identifier: 'moneybin', 'sqlmesh', etc.
    version VARCHAR NOT NULL,          -- current version string (semver)
    previous_version VARCHAR,          -- version before the last update (NULL on first install)
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- when version was last changed
    installed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- when component was first recorded
);
