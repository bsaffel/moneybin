/* Freeze both account names onto each merge proposal so an accepted merge stays
   readable after it takes effect.

   Accepting a proposal re-points every accepted link off the provisional account
   onto the candidate, so the next transform drops the provisional from the
   core.dim_accounts grain — and the raw fallback keys on an accepted link too.
   Both live lookups therefore go dark for the one decision class that cannot be
   re-derived, and `accounts links history` rendered every successful merge as a
   pair of opaque ids.

   Existing rows stay NULL and keep resolving live: a merge already accepted has
   no surviving name to backfill from, and a still-pending one reads correctly
   from core. AccountLinksService.set writes both names as it decides. */
ALTER TABLE app.account_link_decisions
    ADD COLUMN IF NOT EXISTS provisional_display_name VARCHAR;

ALTER TABLE app.account_link_decisions
    ADD COLUMN IF NOT EXISTS candidate_display_name VARCHAR;
