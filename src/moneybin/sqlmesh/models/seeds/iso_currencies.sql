/* ISO 4217 List One, published 2026-09-17 by maintenance agency SIX.
   Source: https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml
   Source SHA-256: 33139b438657d1cee116ba737807ea71d19d6de4b90f799a09c56f0cc6a1b0ff
   Only factual code/minor-unit pairs are retained, without names/descriptions.
   Update from that source by reviewing the membership delta, including removals.
   Current-list membership does not claim exhaustive historical support or FX availability.
   NULL unit means N.A.: testing/no-currency, metals and bond units stay held in
   SimpleFIN's cash/credit path. Numeric unit is membership evidence, never a
   rounding rule: actual amounts must already be exact DECIMAL(18,2) values. */
MODEL (
  name seeds.iso_currencies,
  kind SEED (
    path 'iso_currencies.csv'
  ),
  columns (
    currency_code TEXT,
    minor_unit INT
  ),
  grain currency_code
)
