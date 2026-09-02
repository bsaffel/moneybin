/* The price-source registry: every source a close can carry, declared once.

   Five sites used to restate this vocabulary by hand — the ref_kind CASE in
   prep.stg_security_prices, the rank CASE and the provider enumeration in
   core.fct_security_prices, PriceService's adapter dispatch, and
   SecurityLinksService's feed-key routing set. Adding a provider meant ~10
   coordinated edits, and missing one dropped or mis-ranked rows in silence:
   C.2 shipped the tiingo and coingecko writers one commit ahead of the ref_kind
   CASE, and every row they wrote in between was discarded permanently. Both SQL
   models now join this table and moneybin.price_sources loads this same CSV, so
   a new provider is one row rather than a coordination problem.

   ref_kind — the kind of app.security_links reference this source's rows resolve
   through. NULL means the source never lands in raw.security_prices at all:
   `override` and `trade_implied` are derived at model build, which is what lets a
   security no feed covers carry a price. That NULL is load-bearing rather than
   cosmetic — core.fct_security_prices scopes its same-pull withhold to the rows
   that HAVE a ref_kind, so the withhold follows the provider set automatically
   instead of naming it.

   source_rank — declared precedence when two sources hold a close for one
   security, date, and currency. APPEND RANKS; NEVER REORDER THEM. Inserting a
   provider ahead of an incumbent changes which close wins on every historical
   date where both hold a row, silently revaluing core.dim_holdings.market_value
   and the C.3 daily series. Rank 99 is the fallback for a source this table does
   not name, and it is reachable only from the two branches core.fct_security_prices
   computes itself: a PROVIDER row absent from this table never reaches a rank at
   all, because prep.stg_security_prices' INNER JOIN has already discarded it.

   ref_role — what accepting a review decision for this ref_kind DOES, and the
   reason this is a column rather than a derivation. 'feed_key' BINDS a price feed;
   'identity' MERGES two catalog rows for one instrument and deletes one. They are
   opposite operations behind one reviewer intent, which is why
   SecurityLinksService.accept routes on it. It is deliberately NOT derived from
   security_types below: that column is operational and empties when a provider is
   retired, and reclassifying a retired provider's ref_kind from 'feed_key' to
   'identity' would route its still-pending decisions into the merge path — the
   precise destruction the routing exists to prevent. A ref_kind's role is a
   permanent property of the ref_kind, so retirement must not be able to change it.

   security_types — pipe-delimited; which app.securities.security_type values
   PriceService routes to this source, and the ONLY column retirement touches.
   Empty means the service fetches nothing for it: `plaid` resolves through links
   but its closes arrive from the Plaid extractor, and the two derived sources have
   no adapter at all. A non-empty value requires a ref_kind and a 'feed_key'
   ref_role, since a routed source with neither writes rows staging discards.

   A RETIRED SOURCE KEEPS ITS ROW FOREVER. raw.security_prices is append-only, so
   a provider's closes outlive the decision to stop fetching from it. Retiring one
   means clearing security_types alone, never deleting the row and never clearing
   ref_kind or ref_role: deleting the row discards every historical close it wrote
   from prep and core, silently, and clearing ref_role re-routes its open reviews.
   Edit the CSV to change entries; SQLMesh detects changes automatically. */
MODEL (
  name seeds.price_source_map,
  kind SEED (
    path 'price_source_map.csv'
  ),
  columns (
    source_type TEXT,
    source_rank INT,
    ref_kind TEXT,
    ref_role TEXT,
    security_types TEXT
  ),
  grain source_type
)
