/* OFX financial-institution registry, keyed by the <FI><FID> element.

   OFX <ORG> is a routing code, not a name: Chase publishes <ORG>B1</ORG> and
   Wells Fargo publishes <ORG>WF</ORG>. Aliasing it straight to
   core.dim_accounts.institution_name therefore showed users "B1" for a column
   documented as the human-readable institution name. FID is the exact,
   institution-assigned id, so it is the reliable key; core.dim_accounts joins
   this registry to resolve a display name and falls back to the raw <ORG> when
   the FID is unknown.

   This CSV is the single source of truth for the FID mapping and has two
   consumers: this seed model (display_name, for core.dim_accounts) and
   extractors/institution_resolution.py (slug, at import time). Keeping one file
   is what stops the two from drifting as institutions are added.

   The two columns carry very different risk. display_name is presentation —
   edit it freely. slug becomes the import's source_origin, which is an input to
   the transaction_id content hash, so editing an existing slug re-keys every
   transaction imported under it and needs a migration. Adding a new row is safe
   either way.

   Extensible — add a row as PRs identify new institutions in the wild; an
   unknown FID costs a nice display name, never correctness. */
MODEL (
  name seeds.institutions,
  kind SEED (
    path 'institutions.csv'
  ),
  columns (
    fid TEXT,
    slug TEXT,
    display_name TEXT
  ),
  grain fid
)
