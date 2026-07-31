/* Profile-level user settings (multi-currency.md Requirement 4).
   Exactly one row per profile database — the database *is* the profile — so
   SQLMesh report guards can read `home_currency` with a bare SELECT.
   Absence of a row (or a NULL) means the user has not chosen one yet; it is
   never defaulted to 'USD', which would relabel a EUR-only user's money. */
CREATE TABLE IF NOT EXISTS app.profile_settings (
    scope         VARCHAR NOT NULL PRIMARY KEY DEFAULT 'profile'
                  CHECK (scope = 'profile'),  -- Singleton guard: one settings row per profile database
    home_currency VARCHAR,                    -- ISO 4217 (USD, EUR, ...); NULL means not yet chosen, never an implied USD
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Last settings mutation timestamp
);
