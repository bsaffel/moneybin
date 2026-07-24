/* Source account-type spelling -> canonical (account_type, account_subtype).

   Every source names the same concepts differently: OFX <ACCTTYPE> emits
   CHECKING / SAVINGS / MONEYMRKT / CD / CREDITLINE, Plaid emits depository /
   credit / loan / investment, and a CSV column mapping emits whatever the file
   said. All three used to land verbatim in one column, so `accounts --type
   credit` (exact-match) silently omitted OFX cards and display_name flipped
   spelling on every re-sync. The three stg_*__accounts views normalize through
   this registry so the vocabulary is decided in exactly one place.

   Canonical set is the Plaid-style one: it is what core.fct_balances already
   keys its liability negation on and what account_subtype is already documented
   in, so adopting it collapses the split rather than adding a fifth spelling.

   Lookup is on UPPER(alias) — free-text sources vary in case. A blank
   account_subtype means the canonical type carries no finer distinction; an
   alias absent from this table resolves to NULL type (honest unknown, and the
   dim's merge skips NULLs so a stronger source can still supply it) with the
   original value preserved as the subtype. Extensible — add rows as new source
   spellings appear; edit the CSV and SQLMesh detects the change. */
MODEL (
  name seeds.account_type_map,
  kind SEED (
    path 'account_type_map.csv'
  ),
  columns (
    alias TEXT,
    account_type TEXT,
    account_subtype TEXT
  ),
  grain alias
)
