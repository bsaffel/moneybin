# MoneyBin MCP

> **Status:** in-progress — the 50-tool registry operates today; promotion is
> pending observed context-budget and host-native-deferral evidence.
>
> Companions: [`mcp-tool-surface-scaling.md`](mcp-tool-surface-scaling.md)
> (registry, budgets, evidence, and admission),
> [`mcp-architecture.md`](mcp-architecture.md) (layers and transport), and
> [`moneybin-capabilities.md`](moneybin-capabilities.md) (CLI/MCP outcomes).

## Purpose

This is the concrete current MCP contract. Generic clients receive the complete
50-tool standard registry. Capable hosts may optionally defer schemas from that
same registry without reconnect, packs, or profiles; names, annotations,
approvals, allowlists, and audit identity do not change. The previous per-tool
catalog is archived at
[`archived/moneybin-mcp-pre-cutover.md`](archived/moneybin-mcp-pre-cutover.md).
Future MCP capabilities remain unnamed until admission through the bounded
registry.

> **M1J.7 staged amendment:** investment-event matching reuses this registry;
> it adds no tool. Delivery slice 2 adds `investment_match` to the
> `refresh_run.steps` enum and `investment_matches` to the `reviews.kind` enum.
> Delivery slice 3 adds the `investment_match` discriminator to the
> `reviews_decide.decisions` item union and requires prompt-only human
> Elicitation bound to the exact complete batch for both accept and reject. It
> accepts and issues no fallback token, refuses non-eliciting clients with
> `mutation_confirmation_required` naming the CLI, and declares the prompt's
> dynamic disclosure up to Tier HIGH. A mixed batch containing any
> investment-Match decision ratifies or rejects the entire atomic batch. Slice
> 4 enables acceptance through that same gate. Slice 3 also adds the six stable
> investment-planner result fields and recovery behavior defined in the owning
> matching spec to direct and embedded refresh payloads. Until those slices
> land, these are planned extensions, not claims about the live 50-tool schema.

## Standard registry

The 13 user-facing domains below group 17 literal tool-name prefixes. A prefix
is the portion before the first underscore: `identity_*` belongs to Reviews,
`gsheet_*` belongs to Sync, and `refresh_*` plus `sql_*` belong to Platform.

| Domain | Tools |
|---|---|
| System | `system_status`, `system_audit`, `system_audit_undo` |
| Profile | `profile`, `profile_set` |
| Reports | `reports` |
| Accounts | `accounts`, `accounts_set`, `accounts_balances`, `accounts_balance_assert`, `accounts_links_run` |
| Investments | `investments`, `investments_record`, `investments_securities_set`, `investments_lots_select` |
| Transactions | `transactions`, `transactions_create`, `transactions_annotate`, `transactions_categorize_assist`, `transactions_categorize_commit`, `transactions_categorize_run`, `transactions_categorize_rules`, `transactions_categorize_rules_set` |
| Reviews | `reviews`, `reviews_decide`, `identity_links_decide` |
| Taxonomy | `taxonomy`, `taxonomy_set` |
| Import | `import_files`, `import_preview`, `import_confirm`, `import_status`, `import_revert`, `import_inbox_sync`, `import_labels_set` |
| Sync | `sync_link`, `sync_status`, `sync_pull`, `sync_disconnect`, `gsheet`, `gsheet_connect`, `gsheet_pull`, `gsheet_disconnect` |
| Privacy | `privacy`, `privacy_consent_set` |
| Exports | `export_run`, `exports_set` |
| Platform | `refresh_run`, `sql_query`, `sql_schema` |

## Contract matrix

The rendered **standard snapshot** from `tools/list` is the canonical exact
input schema for every row below. This matrix is the stable selection guide: it
names each tool, its primary selector or discriminator, its intent, and its
safety family without duplicating FastMCP's drifting JSON schema.

| Tool | Selector or discriminator | Intent | Safety family |
|---|---|---|---|
| `system_status` | `detail`, `sections` | Orientation and pending-work inventory | Read / dynamic / up to medium / status-derived |
| `system_audit` | `audit_id`, `cursor`, `limit`, `operation_id`, `view` | Audited mutation history | Read / dynamic / up to high / audit-derived |
| `system_audit_undo` | `operation_id` | Reverse one undoable operation | Audited recovery / at least low |
| `profile` |  | Active profile metadata and managed settings | Read / at least low |
| `profile_set` | `home_currency` | Set the profile's home currency | Audited write / at least low |
| `reports` | `display_currency`, `limit`, `parameters`, `report_id` | Catalog or execute a registered report | Read / dynamic / up to critical / report-derived |
| `accounts` | `cursor`, `include_closed`, `limit`, `query`, `reference`, `view` | Account collection | Read / dynamic / up to critical / view-derived |
| `accounts_set` | `account_id`, `account_subtype`, `clear_fields`, `credit_limit`, `currency_code`, `default_cost_basis_method`, `display_name`, `holder_category`, `include_in_net_worth`, `is_archived`, `last_four`, `official_name` | Account target state | Audited write / at least critical |
| `accounts_balances` | `as_of`, `cursor`, `end`, `limit`, `reference`, `start`, `threshold`, `view` | Balance projection and reconciliation | Read / dynamic / up to high / balance-derived |
| `accounts_balance_assert` | `account`, `amount`, `as_of`, `confirmation_token`, `state` | Record a balance assertion | Audited write / at least medium |
| `accounts_links_run` | `account_id`, `candidate_account_id` | Propose account merges — sweep, or one named pair | Audited write / at least low |
| `investments` | `account`, `cursor`, `end`, `limit`, `open_only`, `security`, `start`, `view` | Holdings and ledger projection | Read / dynamic / up to high / view-derived |
| `investments_record` | `events` | Record an investment event | Audited write / at least low |
| `investments_securities_set` | `coingecko_id`, `cost_basis_method`, `currency_code`, `cusip`, `exchange`, `figi`, `is_cash_equivalent`, `isin`, `name`, `security_id`, `security_type`, `ticker` | Securities-catalog target state | Audited write / at least low |
| `investments_lots_select` | `disposal_txn_id`, `selections` | Full lot-selection target state | Audited write / at least high |
| `transactions` | `account`, `category`, `cursor`, `end`, `limit`, `max_amount`, `merchant`, `min_amount`, `start`, `text` | Transaction projection | Read / at least high |
| `transactions_create` | `transactions` | Create a manual transaction | Audited write / at least low |
| `transactions_annotate` | `confirmation_token`, `requests` | Batch stable-ID note lifecycle, tag/split target states, and tag rename | Audited write / non-idempotent / dynamically destructive / at least low |
| `transactions_categorize_assist` | `account_filter`, `date_range`, `limit` | Scrubbed categorization candidates | Read / scrubbed / at least medium |
| `transactions_categorize_commit` | `items` | Commit reviewed categorizations | Confirmed write / at least low |
| `transactions_categorize_run` | `methods`, `operation` | Run categorization engines | Audited workflow / at least low |
| `transactions_categorize_rules` | `view` | Current categorization rules | Read / at least high |
| `transactions_categorize_rules_set` | `confirmation_token`, `rules` | Rule target state | Confirmed write / at least low |
| `reviews` | `cursor`, `kind`, `limit`, `status` | Pending/history queues, including current blast-radius evidence for pending `kind='auto_rules'` rows | Read / dynamic / up to high / queue-derived |
| `reviews_decide` | `decisions` | Resolve ordinary or auto-rule review items; `kind='auto_rule'` carries proposal-scoped `allow_broad` | Confirmed write / at least low |
| `identity_links_decide` | `confirmation_token`, `decisions` | Resolve identity links | Confirmed write / at least medium (prompt-disclosed) |
| `taxonomy` | `cursor`, `include_inactive`, `limit`, `query`, `view` | Read taxonomy projections | Read / dynamic / up to medium / view-derived |
| `taxonomy_set` | `confirmation_token`, `items` | Taxonomy target state | Audited write / at least low |
| `import_files` | `account_bindings`, `force`, `paths`, `refresh` | Import files | Audited workflow / at least critical / file-derived |
| `import_preview` | `file_path`, `mapping` | Stage and inspect an import proposal | Staged write (`readOnlyHint=false`, `idempotentHint=false`) / dynamic / up to critical / file-derived |
| `import_confirm` | `account_bindings`, `account_id`, `account_metadata`, `account_name`, `bridge_response`, `confirmation_token`, `preview_id`, `save_format` | Ratify an import proposal | Confirmed write / dynamic / up to critical / preview-derived |
| `import_status` | `cursor`, `import_id`, `limit`, `sections` | Import lifecycle status | Read / dynamic / up to medium / import-derived |
| `import_revert` | `confirmation_token`, `format_name`, `import_id`, `operation` | Revert an import batch or delete a saved format | Confirmed destructive / at least low — rollback is permanent with no undo; format deletion is audited and `system_audit_undo`-recoverable |
| `import_inbox_sync` | `refresh` | Drain the import inbox | Audited workflow / at least critical |
| `import_labels_set` | `import_id`, `labels` | Import-label target state | Audited write / at least medium |
| `sync_link` | `institution`, `mode` | Start mediated provider linking | Credential flow / at least medium |
| `sync_status` | `auth_session_id`, `session_id` | Provider connection status | Read / dynamic / up to medium / session-derived |
| `sync_pull` | `institution` | Pull linked-provider data | External mutation / at least medium |
| `sync_disconnect` | `confirmation_token`, `institution`, `mode` | Disconnect provider or credentials | Institution disconnect is a confirmed destructive write; logout is recoverable / at least low |
| `gsheet` | `connection_id`, `view` | Google Sheets connections | Read / dynamic / up to medium / connection-derived |
| `gsheet_connect` | `accept_seed_fallback`, `account_id`, `account_name`, `adapter`, `alias`, `column_mapping`, `confirm_mapping`, `connection_id`, `force_reauth`, `no_initial_pull`, `url` | Bind user-controlled storage | Credential flow / dynamic / up to medium / connection-derived |
| `gsheet_pull` | `connection_id` | Pull sheet data | External mutation / at least medium |
| `gsheet_disconnect` | `confirmation_token`, `connection_id`, `state` | Disconnect or purge a sheet binding | Destructive write / dynamic / up to medium / connection-derived |
| `privacy` | `cursor`, `limit`, `view` | Privacy and consent projection | Read / dynamic / up to low / privacy-derived |
| `privacy_consent_set` | `backend`, `categories`, `confirmation_token`, `mode`, `state` | Set consent state | Audited write / at least low |
| `export_run` | `destination`, `redaction_mode`, `subject` | Publish a bundle or registered report to a named destination | External delivery / dynamic / non-idempotent / at least medium |
| `exports_set` | `confirmation_token`, `target` | Export-destination target state | Audited write / at least medium |
| `refresh_run` | `steps` | Refresh derived state | Audited workflow / at least medium |
| `sql_query` | `query` | Operator SQL escape hatch | Read / dynamic / up to critical / query-derived |
| `sql_schema` | `table` | Curated SQL schema, plus a live relation listing per queryable schema | Read / dynamic / up to critical / schema-derived |

### Transaction annotation requests

`transactions_annotate` is one atomic workflow umbrella, not a collection
replacement for every annotation type. Its discriminated `requests` union is:

- `note_add(transaction_id, text)` — append a note and return the generated
  `note_id` in that outcome's `target_ids`;
- `note_edit(note_id, text)` — change only the addressed note while retaining
  its identity and audit chain;
- `note_delete(note_id)` — delete only the addressed note;
- `tags_set(transaction_id, tags)` and
  `splits_set(transaction_id, splits)` — declare complete collection state;
- `tag_rename(old_name, new_name)` — rename one tag globally.

Every request is preflighted before the first write and the batch shares one
`operation_id`. Note add and edit do not dynamically request confirmation;
note delete and other changed removals do. Because note append is an event, the
umbrella honestly advertises `idempotentHint=false` even though its target-state
variants remain individually idempotent.

### Ranked account resolution

`accounts(view="resolve", query=..., limit=...)` is a bounded ranked search.
It returns candidates in confidence-descending, stable-account-ID order and
reports an exact total plus `has_more` when `limit` truncates the result. It
does not issue or accept a cursor for this view: confidence is derived from
mutable account names and metadata, so it is not a safe stateless keyset.
Callers refine the query or rerun with a larger limit. The `list` view and
resumable `accounts_balances` views retain immutable-key cursors.

## Response contract

Every tool returns canonical JSON text and equivalent structured content with a
`summary`, `data`, and `actions` envelope. Amounts use the accounting
convention (negative expense, positive income) unless the tool explicitly
states a presentation override; currency-bearing responses name their currency
in `summary.display_currency`. Current registry tools advertise zero output
schemas. A future schema needs the consumer-driven admission record in the
governing spec.

Sensitivity classification and critical-field masking are wired today. The
consent ledger exists, but **global consent enforcement is deferred**: tools
must not rely on an automatic consent gate or degraded response yet. Read tools
and writes still declare their sensitivity and mutable-state, audit, recovery,
and confirmation contracts.

## Coarse contracts and workflow boundaries

- `reports(report_id=..., parameters=..., limit=..., display_currency=...)` first
  returns the catalog without a
  report ID, then executes a selected report. New reports are catalog entries,
  never new tool slots. The catalog listing carries **active** reports only, each
  entry reporting `archived: false`; the CLI's `reports list --include-archived`
  has no MCP counterpart yet, because the parameter would change the serialized
  tool metadata that ADR-016's carrying-weight evidence pins. An archived report
  still runs, exports, and explains by id, so nothing is unreachable — only
  unlisted.

  `display_currency` prices a report's amounts into one currency at read time
  and defaults to the profile's home currency; `summary.display_currency` names
  the result. It is the agent-facing surface for exchange rates — the rate is
  applied and reported rather than requested, which is why the `fx.*`
  capabilities need no tool of their own. A report that aggregates with
  `currency_code` in its grouping key cannot be priced without putting two
  currencies behind one figure, and a pair with no stored rate cannot be priced
  at all; both fall back to per-currency segmentation and name the obstacle in
  `summary.degraded_reason`. `summary.display_currency` then reverts to what the
  rows themselves say — `null` when they span currencies, and the one currency
  they share when they agree, which is still the true answer for those rows.
  Read `summary.degraded_reason`, not a non-null `display_currency`, to learn
  whether the requested pricing happened. That reason is stated only when the
  caller named `display_currency`: the home-currency default falls back
  silently, or every unconverted report on a profile with a home currency would
  carry a warning. Each report states its own rule in
  `semantics.fx_basis`, and the ones that price exactly name the column they
  price on in `semantics.fx_date`. A `display_currency` naming no currency is
  refused before the query runs, so an empty result cannot claim a currency
  that does not exist. Reads never fetch a rate: `refresh_run` gathers them,
  because a read holds no writer lock.

  `summary.applied_rates` carries the provenance behind those figures
  (`multi-currency.md` Requirement 10): one entry per distinct pair and date,
  each with `from_currency`, `to_currency`, `rate`, `source`, `requested_date`,
  and `rate_date`. The two dates differ whenever a weekend or holiday priced
  against the previous published day, which is the case the field exists to make
  visible rather than smooth over. `source` names a provider or the `override` /
  `identity` sentinel, so an agent can tell a user-set rate from a fetched one.
  The key is present only when a conversion actually happened — absent means
  nothing was converted, which is a different claim from "converted at an
  unrecorded rate" — and it is deduplicated, so a thousand rows priced on one
  date report the single rate that priced them.

  A value **derived** from a converted amount is restated so it cannot describe
  the old currency: `core:balance_drift` re-buckets its clean/warning/drift
  verdict against the converted drift, and `core:networth` recomputes
  `net_worth` from its own converted parts so per-column rounding cannot leave
  the total disagreeing with assets plus liabilities. `no-data` and
  `currency-mismatch` are left alone — neither states a magnitude. The
  `balance_drift` `status` *parameter* still filters in the account's own
  currency, before any rate is known: filter on `all` and read the returned
  `status` when converting.
- `accounts`, `investments`, `transactions`, `reviews`, `taxonomy`, `privacy`,
  and `gsheet` expose typed views or filters under one domain identity. Their
  paired `_set`, `_decide`, or domain verb tools retain material write and
  confirmation boundaries.
- **An account merge is confirmed by prompt only.** `identity_links_decide`
  accepting an `account_link`, and `accounts_links_set` with `action="accept"`,
  are the one exception to the opaque-token fallback: they refuse a supplied
  `confirmation_token` with `mutation_invalid_input`, and refuse a client that
  cannot elicit with `mutation_confirmation_required` naming the CLI, rather
  than issuing a token. The fallback token is returned to the *calling agent*,
  so honoring it would let that agent satisfy the merge confirmation itself —
  and a merge re-keys a whole account's transaction history, the case
  `design-principles.md` places at the top of the confirmation bar. Merchant-
  and security-link accepts keep the token path: neither re-keys a transaction.
- `import_files` and `import_preview` establish an import; `import_confirm`
  ratifies system proposals, including an elicited human decision for a PDF
  sign inversion. Clients without elicitation receive an opaque,
  payload-bound `confirmation_token` and retry the same operation; both paths
  recompute and compare the live proposal inside the write transaction before
  importing. `import_status` and `import_revert` provide recovery.

- `import_preview` persists encrypted metadata in `app.import_previews` and
  staged bytes in `raw.import_preview_snapshots`. Preview issue, consume, and
  expiry transitions are audit events; unused previews expire after the
  configured TTL. Its `readOnlyHint=false` annotation reflects that retained
  state even though it does not commit ledger rows.
- `refresh_run` owns the bounded derived-state workflow. Its `steps` vocabulary
  is currently `gsheet`, `match`, `transform`, `categorize`, `identity`,
  `rates`, executed in that canonical order. M1J.7 slice 2 inserts
  `investment_match` after `match` and before `transform`; selecting only that
  value in `refresh_run.steps` plans pending investment reviews, while selecting
  `transform` runs that planner transitively and then invokes non-selectable
  membership reconciliation before rebuilding the Golden ledger. Slice 3 adds
  `investment_matches_pending_unique`,
  `investment_matches_pending_competing`, `investment_matches_suppressed`,
  `investment_matches_stale`, `investment_matching_skipped`, and
  `investment_matching_error` to `RefreshRunPayload` and every shared
  embedded-refresh payload. The four counts are stable integer keys; the first
  two plus an action directing `reviews` to status `pending` and the planned
  M1J.7 kind value `investment_matches` are the pending summary. Callers read
  the skipped flag and nullable sanitized error before interpreting zeros. If
  the expanded requested set contains `transform`, the failure retries
  `refresh_run` scoped to `transform`; otherwise it retries `refresh_run`
  scoped to the M1J.7 `investment_match` value, including when another
  non-transform step was requested alongside it. A failed transitive planner
  prerequisite prevents SQLMesh apply without overloading its `error` field or
  cash matching's `matching_error`. `rates`
  caches the reference rates the profile's own
  transactions, balances and holdings imply; it runs last because nothing
  downstream consumes it, and it reports `rates_written` plus any
  `rate_pairs_failed` (retried next run), `rate_pairs_unsupported` (never
  retried; needs `moneybin fx set`), and `rate_pairs_discarded` (the provider
  answered and part of the answer was unusable, so coverage may be short on some
  dates) rather than failing the call. A crash in the step itself reports
  `rate_backfill_error`, the same `DESCRIPTION`-classified shape
  `matching_error` and `categorization_error` use: `rates_written` is `null`
  both when the step declined to run and when it ran and died, so the error is
  the only field that separates them. `rate_pairs_failed` and
  `rate_backfill_error` each earn a `recovery_actions` entry
  (`refresh_run(steps=["rates"])`, emitted once even when both are set),
  matching the match and categorize steps; the other two pair lists name
  conditions a retry cannot change, so offering one would be a loop with no
  terminating condition.
- The tools that *embed* a refresh — `sync_pull`, `import_files`,
  `import_inbox_sync` — carry that same step outcome on their own payloads,
  under the same field names, because each runs the full cascade on the user's
  behalf. `transforms_error` on those payloads reports only the SQLMesh apply;
  `matching_error`, `categorization_error`, `identity_errors` and the
  exchange-rate group report the four best-effort steps it cannot speak for.
  The names are deliberately identical to `refresh_run`'s so an agent reading
  two surfaces learns one vocabulary for one outcome. They carry its
  `recovery_actions` too, built by the same function: a step that crashed inside
  an import is no less retryable for having crashed there, and these are the
  surfaces where the CLI's stderr warning has no counterpart, so a payload field
  with nothing executable beside it is the whole of what the agent gets. That
  function takes the apply outcome as a required argument, because these callers
  hold it in their own `transforms_error` field rather than in the step outcome:
  when the apply failed it is the blocker, and every step retry is withheld so
  the agent fixes it before chasing a secondary crash that would recur anyway.
- `sql_query` is the read-only escape hatch and `sql_schema` explains the
  interface schema. They do not replace domain validation for writes.

### Export delivery

The 50-tool standard registry contains exactly two export-specific tools and
sits at the 50-tool hard limit exactly — admitting another means retiring one:

- `export_run` publishes either the closed 13-table canonical bundle or one
  catalog report to a named local or Sheets destination. Every call supplies
  `redaction_mode`; omission elicits a choice where supported and otherwise
  returns a structured refusal. `redacted` is the safe default, never a saved
  destination preference. Each successful run records its receipt — export id,
  destination id and name, format, artifact file name, subject kind, report id,
  row counts, and checksums — to `app.audit_log` under action `export.run`, so
  a later turn or session reads it with `system_audit()` instead of relying on
  the one-time return value. The row names the artifact, never its full path,
  because `export.md` R9 keeps local paths out of persisted state. It records
  what a run produced rather than where the file is now — a destination root
  can be repointed or removed after publication — and the checksums are what
  confirm a candidate file is that artifact. It carries no parameter binding
  and no derivative of one: `export_id` is unique per run and the checksums
  differ whenever the content does, so neither the values nor a hash of them
  is needed to tell two runs apart. Recording is best-effort, so the tool
  description tells the caller to treat the returned receipt as the only
  certain copy. That write opens its own connection after publication returns,
  so no writer lock is held across filesystem or Sheets I/O. Because it opens
  after publication rather than at tool start, it runs inside the request's
  publication barrier: an already-ended request skips the write, and a started
  one holds the timeout handler until it finishes, so the tool's response and
  the receipt can never diverge. A receipt write that cannot open is logged and
  counted in `moneybin_export_receipt_failures_total`, and never converts a
  published export into an error, because the artifact already exists and a
  reported failure would invite a re-run that publishes a second one. The
  published artifact is permanent: `system_audit_undo` refuses the row because
  its target lies outside the repository-owned `app.*` surface. That target is
  a readable `export.run` pair rather than nulls, so the refusal names what it
  declines instead of rendering an empty schema and table.
- `exports_set` asserts one named local or Sheets destination's typed target
  state. It shares the same service/repository owners as
  `moneybin export destination ...`; removing configuration never deletes
  artifacts, workbooks, or tabs, but still requires payload-bound confirmation.
  artifacts, workbooks, or tabs.
- `system_status(sections=["exports"])` reports destination readiness through
  the existing orientation tool, so status does not consume a third export
  slot.

Sheets destinations are output-only and cannot overlap an inbound `gsheet`
connection. Publication replaces only MoneyBin-managed tabs after staging and
validation; a failure preserves the latest known-good visible tabs. Local
delivery publishes immutable CSV, Parquet, or XLSX artifacts, with ZIP limited
to completed CSV and Parquet bundles.

## Prompts and resources

Prompts are workflow guidance, not an alternate registry. They use only the
standard names above and lead with `system_status` when orientation is needed.

### Registered prompts

`monthly_review`, `categorization_organize`, `review_auto_rules`, `onboarding`,
`curate_recent_transactions`, `review_curation_history`, and `sync_review`.

### Resources

`moneybin://schema` is the one registered, client-requested resource for
privacy-safe read-only SQL; it does not create a discovery, pack, or profile
mode.

## Capability parity and exemptions

MCP/CLI parity is capability and observable-outcome parity, not name equality.
The executable map and isolated-state tests live in
[`tests/fixtures/mcp_capabilities/outcome-map.json`](../../tests/fixtures/mcp_capabilities/outcome-map.json)
and `test_capability_parity.py`. Explicit CLI-only exemptions are limited to
secret material and hands-on operator territory; see
[`mcp-architecture.md`](mcp-architecture.md) and the outcome map.

## Registration and verification

`register_core_tools()` must exactly equal `STANDARD_TOOL_NAMES`; no hidden
FastMCP aliases are allowed. Tests inventory the actual `tools/list` response,
render coarse schemas, enforce description and metadata budgets, prove parity,
and compare the deterministic 50-tool capture against the frozen baseline.

The deterministic comparison passed, but `promotion_ready: false`: the context
budget and host-native deferral are not observed. The governing spec and
ADR-016 therefore remain in-progress and Proposed respectively.
