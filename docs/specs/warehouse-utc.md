# Warehouse UTC timestamp contract

- **Type:** Architecture
- **Status:** in-progress
- **Delivery:** M1G.5 prerequisite; coordinated pre-v1 breaking migration.

The coordinated implementation is present; integration verification and delivery
remain in progress. Fresh profiles are expected after the upcoming SQL baseline.
Any retained-profile conversion is a separately prepared, explicitly authorized
one-time offline operation; it is not an installed application feature.

## Contract

All maintained instants use DuckDB `TIMESTAMPTZ`, aware UTC Python datetimes,
and UTC database sessions. Machine output uses
`YYYY-MM-DDTHH:MM:SS.ffffffZ`. Business dates retain `DATE` and ISO
`YYYY-MM-DD`; posting, statement and valuation dates do not move when a
profile's reporting timezone changes.

Schemas, writers, transformations and consumers migrate together. Changing
only the connection timezone cannot repair historical values or naive Python
writers. Normalize an explicit offset with `astimezone(UTC)`. Interpret an
offset-free value only with an established source timezone or protocol default.
Unknown wall times cannot become maintained instants. Preserve original provider
payloads and historical audit JSON.

Ad-hoc SQL can compute timezone-free `TIMESTAMP` values, such as
`date_trunc('month', DATE '2026-10-21')`. After privacy redaction, the shared
SQL boundary formats these as offset-free ISO text with six fractional digits
(`2026-10-01T00:00:00.000000`), including nested lists and structs. CLI human
output and CLI/MCP JSON preserve that clock value without timezone conversion.
Aware instants retain the strict UTC contract and business dates remain dates;
generic instant validators do not accept unknown wall times.
Driver infinity sentinels retain their existing output refusal.

## Source meaning

| Input | Treatment |
|---|---|
| SimpleFIN epoch seconds | Convert directly to UTC instants. |
| Plaid posting and authorization dates | Preserve business DATEs. |
| Plaid optional date-times | Require an explicit offset. Midnight placeholders do not establish actual event precision. |
| OFX date-times | Apply the protocol numeric offset (inclusive −12 to +12 hours, including fractions) or GMT default; reject offsets outside that range and preserve date precision and original evidence. |
| Tabular, PDF and Sheets transaction dates | Preserve DATE semantics; arbitrary wall-clock cells establish no timezone. |
| Market observations | Preserve the adapter's explicit valuation-date rule. |
| Application clocks | Generate aware UTC at creation. |

OFX posting, statement-boundary and ledger-balance source dates are stored
separately from their UTC instants. Matching, staging and import summaries use
those source dates; UTC conversion cannot shift their business day. Fresh
profiles include the source-date columns. Profiles missing them fail the
startup contract and require fresh creation or separately arranged offline
maintenance; the application does not infer historical source dates from UTC.

Provider details remain behind the existing sync API and source adapters;
consumers use canonical tables.

## Reporting calendar

Persist an IANA `reporting_timezone` in the existing audited profile settings
row. Interactive creation suggests the machine's zone and records the chosen
value. Headless creation accepts an explicit value or persists and reports
UTC. Moving computers never changes the saved policy.

Resolve local default periods and day grouping before aggregation. Convert
calendar bounds to UTC and filter instants with `[start, end)`. DST days may
have 23 or 25 hours. Business-DATE reports filter the source date directly.
CLI, MCP and SQL-backed reports share the policy.

Current holdings valuation uses the saved reporting date for price eligibility,
`valued` versus `carried_forward`, and `days_since_observed`. The holdings view
reads the saved timezone at query time, so local midnight and timezone changes
take effect without rebuilding dated price observations. Provider price dates,
user mark dates and trade dates remain unchanged. Price-feed retrieval still
uses completed UTC days; that extraction cutoff does not define reporting
“today”.

Security-price key retirement and handover days also use the saved reporting
timezone; source price dates remain unchanged.

The `calendar_bounds` helper requires an unambiguous, existing local midnight
at both endpoints. It rejects midnight gaps, repeated midnights and skipped
calendar dates; it does not choose an offset or shift a boundary. Current
reporting paths do not call this helper. A caller requiring those boundaries
must first define the calendar-selection policy separately from strict legacy
timestamp interpretation.

## Incompatible profiles and offline maintenance

Normal opens inspect stored types and the temporal contract before schema
initialization, automatic migrations or transformations. Read-only opens and
initialization shortcuts cannot bypass this gate. Managed connections,
including SQLMesh cursors, select UTC before configuration sealing.
Standalone `db shell`, `db ui` and `db query` sessions also initialize UTC.

The installed application has no UTC inspection, preparation, conversion or
conversion-recovery commands, no conversion migration and no opener bypass.
Old incompatible profiles are refused. Create fresh profiles after the planned
SQL baseline, or arrange separate offline maintenance when retaining history
is necessary. The SQL baseline and profile resets require separate approval.

Completed converter source is preserved as a local reference, not a supported
standalone utility. If needed, reconstruct a compatible isolated maintenance
checkout from that source and obtain explicit approval for the data operation.
Historical timezone provenance must be proven: the current host timezone,
a date interval or a guessed writer era cannot supply missing evidence.
Unknown zones, ambiguous DST folds and nonexistent wall times block conversion.
Keep original encrypted backups and original provider/audit evidence.

`app.versions` records `utc_contract = 2` only for a complete UTC schema and
valid saved reporting policy. A marker alone does not excuse an incompatible
schema. Retain `app.utc_upgrade_provenance` and strict field-specific audit
replay for any history already converted by separately arranged maintenance.
Fresh profiles record an empty provenance rule set and canonical UTC evidence.
Never broaden legacy acceptance or infer absent provenance.

Encrypted database restore remains an installed operation. Before validation,
stop processes and close connections using either source or destination,
including MoneyBin, MCP, CLI, notebooks and external database clients. Keep
both databases closed through replacement and any recovery. Offline means no
other database users. Locks, fingerprints and encrypted snapshots are additional
safeguards. Restore validates the source key and temporal contract before
replacing the destination and retains an encrypted original backup. Use
`moneybin db restore --recover` to reconcile a completed interrupted promotion
against its journal and matching original backup. Recovery cannot select a
backup through `--from` or `--latest`.

Restore writes and syncs a private temporary journal beside the destination,
then publishes the complete journal atomically without overwriting an existing
journal. It syncs the directory before promotion. An abrupt exit before
publication can leave an unjournaled candidate and temporary journal requiring
deliberate manual cleanup while both databases remain offline. Recovery refuses
a missing or unverifiable final journal and never promotes an unpromoted candidate.

## Consumers

- Audit replay decodes known temporal fields before mutation with persisted,
  field-specific provenance. Preserve original JSON, nulls and omitted fields;
  alias-forwarded undo uses the same decoder.
- Timestamp cursors use canonical UTC keys and stable tie-breakers. Scopes
  carry temporal contract version 2; old tokens request a pagination restart.
  DATE/id and other non-temporal cursor scopes retain their contracts.
- Export artifact version 2 uses canonical timestamps in dictionaries, JSON,
  CSV and checksums. Encrypted database restore validates source contract and
  key before destination replacement. Incompatible old backups require fresh profiles or separately arranged offline maintenance.
- Sync recovery retains checkpoints, acknowledgments and retry phases.
  Amounts, signs, currencies, identities and source dates are invariant.
- Consent, current rules, review history, notes/splits, schema resources, report
  explanations and Sheets connections use the same canonical machine timestamps.
  Human instant output uses the saved reporting calendar, offset and IANA zone.
- FX freshness derived from historical profile audit images uses the same
  persisted field-specific decoder as replay. Missing or ambiguous provenance
  refuses interpretation while preserving original JSON.

## Observability and verification

Restore retains writer-lock and checkpoint observability through existing
operation classes. Converter-only metrics are absent. Logs contain no payloads,
timestamp samples, timezone maps or credentials.

Verify fresh and synthetic incompatible databases under UTC, America/New_York
and Asia/Tokyo, including defaults, bulk loading, read-only startup, DST
folds/gaps, audit undo/redo, equal-instant pagination and reporting boundaries.
Archive and restore must change eligibility and advance freshness. Verify
preservation of financial values and source evidence, source-key/type refusal
before replacement, reader-lock protection and interrupted-restore recovery.
Subprocess tests cover installed restore/recover and absent converter commands;
package tests exclude converter modules and the unpublished conversion migration.

Targeted local checks precede broad CI. Implementation, publication, retained
profile conversion and live acceptance remain distinct completion boundaries.

## Related contracts

- [Core freshness](core-updated-at-convention.md)
- [Shared primitives](architecture-shared-primitives.md)
- [Data protection](privacy-data-protection.md)
- [Observability](observability.md)


### Current-main classification and freshness integration

`app.account_settings.account_type_changed_at` and
`account_subtype_changed_at` are aware UTC instants. Actual classification edits,
clears, merge moves and undo advance these stamps; no-op edits preserve them.
`prep.int_accounts__merged` folds them into accounting freshness because
SimpleFIN financial eligibility depends on effective type and subtype. Human
renames remain read-through. Reporting-timezone writes use the ordinary
post-commit profile-settings restatement, while business dates and source
landing evidence retain their original meaning.

Published migrations V001–V075 remain unchanged. Pending SimpleFIN and override
migrations occupy V076–V080; V077 adds the canonical type override and the two
classification stamps. Historical add/backfill behavior and fresh UTC bootstrap
are separate contracts: a historical `TIMESTAMP` append is not a legacy-profile
conversion, and no converter ships in the maintained runtime.
