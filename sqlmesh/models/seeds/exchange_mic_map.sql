/* MIC <-> common-name registry for exchange normalization (sync-plaid-investments.md).
   Both resolver compare sides normalize through this: alias (uppercased free text, or a
   MIC itself via the identity rows) -> canonical ISO-10383 MIC. Extensible — add rows
   for exchanges a portfolio touches; an alias missing here is treated as ABSENT by the
   resolver (never a contradiction), so an incomplete registry costs recall, not
   correctness. Edit the CSV to change entries; SQLMesh detects changes automatically.

   Note: Nasdaq listing tiers (GS, GLOBAL SELECT, GLOBAL MARKET, CAPITAL MARKET) are
   normalized to the operating MIC XNAS rather than their segment MICs, since all tiers
   are listings on the same legal exchange, and the resolver's question is "same venue?". */
MODEL (
  name seeds.exchange_mic_map,
  kind SEED (
    path 'exchange_mic_map.csv'
  ),
  columns (
    alias TEXT,
    mic TEXT
  ),
  grain alias
)
