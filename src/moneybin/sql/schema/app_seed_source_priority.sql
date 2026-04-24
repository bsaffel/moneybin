/* Source-priority ranking for golden-record merge rules; rebuilt from MatchingSettings on every matcher run */
CREATE TABLE IF NOT EXISTS app.seed_source_priority (
    source_type VARCHAR NOT NULL,  -- Source type identifier (e.g. plaid, csv, ofx)
    priority INTEGER NOT NULL,     -- Lower number = higher precedence (1 = best)
    PRIMARY KEY (source_type)
);
