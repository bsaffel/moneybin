# MoneyBin Capabilities — Executable Cross-Surface Outcomes

> **Status:** implemented

This spec defines parity between MoneyBin's two active user surfaces. Parity
means that the CLI and the 50-tool standard registry can produce the same
durable user outcome. It does not require similar command or tool names.
Generic clients receive the full registry; a capable host may optionally defer
schemas from that same registry. Observed host-native deferral evidence remains
absent, and deferral never creates a second capability surface.

The checked source of truth is
[`tests/fixtures/mcp_capabilities/outcome-map.json`](../../tests/fixtures/mcp_capabilities/outcome-map.json).
Its tests resolve the live Typer tree, the standard MCP registry, and every
named service method. A prose table cannot provide those guarantees and is not
duplicated here.

## Contract

Every map row contains:

| Field | Meaning |
|---|---|
| `capability_id` | Stable domain-oriented identifier; unique across the map |
| `mcp_tools` | Exact names in the standard 50-tool MCP registry |
| `cli_commands` | Exact space-delimited executable Typer paths |
| `service_methods` | Importable callables that own the behavior |
| `observable_outcomes` | Stable rows, states, counts, audit operations, or results used to judge equivalence |
| `exemption` | `null`, or one narrow single-surface policy exception with a written reason |

A non-exempt row must name both active surfaces, at least one service method,
and at least one observable outcome. Multiple commands may map to one MCP tool
and one command may participate in more than one outcome. Consolidated
boundaries are intentional: for example, CLI annotation verbs converge on
`transactions_annotate`, while all registered reports converge on `reports`.

The future REST surface is out of scope until it has an executable registry.
When it becomes active, it must join this contract rather than create a second
coverage catalog.

## Coverage

Counted from the map on 2026-08-30, it contains:

- 50 non-exempt capability rows covering all 50 standard MCP tools. `reports`
  serves two capabilities — the catalog read and report execution — under one
  tool identity.
- 194 implemented Typer paths, including hidden compatibility aliases, with
  exact equality against the live command tree after explicit unimplemented
  stubs are removed.
- 17 policy-exempt rows.
- 9 reserved Typer paths that are still explicit `_not_implemented` stubs.

The stub list is executable, not documentary: every excluded path is invoked
with valid minimal arguments and must return the not-implemented outcome.
Implementing a reserved command therefore fails parity until its outcome row
is added.

## Consolidated families

| Family | Standard MCP boundary | Representative CLI paths | Outcome |
|---|---|---|---|
| System and audit | `system_status`, `system_audit`, `system_audit_undo` | `system status`, `system audit *`, `transactions matches undo` | Same health state, audit history, and reversible operation |
| Reports | `reports` | `reports list`, `reports run`, `reports networth`, `reports spending`, and other registered reports | Same catalog runner, rows, period, provenance, and truncation across the built-in, extension, and user tiers |
| Saved-report lifecycle | none — `admission-pending` | `reports create`, `reports set`, `reports delete`, `reports reclassify` | Same audited `app.user_reports` row, derived class map, and human-confirmed downgrade |
| Report verification | none — `admission-pending` | `reports explain` | Same query in both provenance forms, per-column class provenance, lineage, drift freshness, and graduation eligibility for every tier |
| Export delivery | `export_run` | `export bundle`, `export report` | Same `ExportService.run` subject, named destination, redaction mode, format, row counts, checksums, receipt identity, and safe failures. Both surfaces record the run's receipt to `app.audit_log` under action `export.run`, readable afterwards through `system_audit` (MCP) or `moneybin system audit` (CLI) |
| Export destination target state | `exports_set`; readiness through `system_status(sections=["exports"])` | `export destination list`, `export destination add local`, `export destination add sheets`, `export destination remove` | Same `ExportService`/repository-owned named destination readiness and typed local or Sheets state |
| Accounts | `accounts`, `accounts_set`, `accounts_balances`, `accounts_balance_assert`, `accounts_links_run` | `accounts list/get/summary/set`, `accounts balance *`, `accounts links run` | Same account projections, settings, observations, assertions, and merge proposals |
| Investments | `investments`, `investments_record`, `investments_securities_set`, `investments_lots_select` | `investments *` | Same ledger, holdings, lots, securities, and gains |
| Investment prices | `investments(view="holdings")` carries the resolved valuation; no observation-grain tool is named yet | `investments prices pull/set/delete/list` | Same `PriceService` refresh, audited user marks, and resolved series. An observation-grain MCP capability stays unnamed until it passes tool admission; the valuation it produces is already surfaced on holdings |
| Tiingo price credential | exempt — `secret-material` | `investments prices token` | Same profile-scoped `SecretStore` entry; an API token must never enter an LLM context |
| Exchange rates | `reports(display_currency=...)` | `fx rate`, `fx list`, `fx set`, `fx delete` | Same `CurrencyService` resolution — the rate for a pair and date, the business day it was published for, and the layer that answered — plus the audited user correction and the stored series. Multi-currency is a crosscutting service concern rather than a tool namespace ([`mcp-architecture.md`](mcp-architecture.md)), so the agent-facing outcome is display conversion: `display_currency` applies the rate and reports the result in `summary.display_currency` rather than being asked for a rate, and names the obstacle in `summary.degraded_reason` when a report cannot be priced. Recording a correction (`fx set` / `fx delete`) has no MCP counterpart yet and claims no exemption category — an override outranks every provider rate for its date, so its shape is a design question rather than a wrapper |
| Transactions | `transactions`, `transactions_create`, `transactions_annotate` | `transactions list/create`, notes, tags, and splits | Same transaction rows, stable-ID note lifecycle, and complete tag/split target state |
| Categorization | `transactions_categorize_*`, `reviews*`, `identity_links_decide` | `transactions categorize *`, match and identity review commands | Same engine results, rules, queue state, and decisions. A rule claiming an active rule's canonical matcher under a different category is refused on both surfaces, queued in `app.rule_conflicts`, and decided with `replace` / `reprioritize` / `cancel` — `reviews_decide(kind='rule_conflict')` on MCP, `transactions categorize rules resolve` on the CLI |
| Taxonomy | `taxonomy`, `taxonomy_set` | `categories *`, `merchants *` | Same category and merchant target state through `CategorizationService` |
| Import | `import_*` | `import files/preview/confirm/status/revert/inbox/labels`, `import formats *` | Same import log, raw rows, confirmation state, labels, and audited saved-format lifecycle |
| Sync | `sync_link`, `sync_status`, `sync_pull`, `sync_disconnect` | `sync login/link/status/pull/disconnect/logout` | Same authenticated, linked, pulled, disconnected, or logged-out state |
| Google Sheets | `gsheet`, `gsheet_connect`, `gsheet_pull`, `gsheet_disconnect` | `gsheet *` | Same connection and pulled source state |
| Privacy | `privacy`, `privacy_consent_set` | `privacy status/log/grant/revoke/revoke-all` | Same effective grants and privacy log |
| Refresh | `refresh_run` | `refresh`, match/identity commands, `transform apply` | Same selected step outcomes and proposal state |
| Exchange-rate coverage | `refresh_run(steps=["rates"])` | `refresh --step rates` | Same cached span in `raw.exchange_rates`; both report rates written and the pairs the provider could not answer |
| SQL | `sql_query`, `sql_schema` | `sql query` | Same rows, classification, cap, and CRITICAL masking. Schema discovery is MCP-only: `sql_schema`'s curated catalog, per-table detail, and `'<schema>.*'` relation listing have no CLI counterpart, and this is an unclosed gap rather than an exemption — an operator driving `moneybin sql query` composes against `DESCRIBE` and the docs |

Five parity gaps discovered by executing this model were closed as part of
the implementation:

1. `accounts(view="summary")` now has `moneybin accounts summary`.
2. `transactions_categorize_run(operation="improve_ai")` now reaches the same
   provider-native upgrade owned by
   `moneybin transactions categorize improve-ai`.
3. The existing sync quartet now exposes device authorization without adding
   tools: `sync_link(mode="login")` begins, `sync_status(auth_session_id=...)`
   polls, and `sync_disconnect(mode="logout")` clears credentials and pending
   profile-scoped sessions. Institution disconnects use payload-bound
   confirmation and re-resolve the live connection immediately before the
   remote delete. The CLI login remains a blocking wrapper over the same
   begin/poll client primitives.
4. `transactions categorize rules apply` now invokes only the rules engine,
   matching its command intent and
   `transactions_categorize_run(methods=["rules"])`; it no longer applies
   merchant or provider-native categorizations.
5. The existing destructive `import_revert` boundary now uses a strict
   discriminated operation for either import rollback or audited user-saved
   format deletion. Both branches require exact payload-bound confirmation,
   verified against live state inside the write transaction. They differ in
   recovery: format deletion is audited and restorable with
   `system_audit_undo`; import rollback permanently deletes the batch's raw
   rows and has no undo. Built-in formats remain immutable, and no read tool
   mutates format state.

The audit also found that category and merchant CLI names were placeholders.
`categories list/create/set` and `merchants list/create` now execute the same
`CategorizationService` behavior as `taxonomy` and `taxonomy_set`.

## Exemptions

Only these categories are allowed:

| Category | Allowed use |
|---|---|
| `secret-material` | Database keys, passphrases, and key derivation that must never enter an LLM context |
| `operator-territory` | Local database/process/server/profile/bootstrap or physical filesystem control |
| `granular-operator-debug` | Surgical pipeline, metrics, log, synthetic-data, or local redaction inspection |
| `protocol-only` | Machine-to-machine payload mechanics with no useful human command |
| `admission-pending` | The capability is a legitimate MCP candidate whose bounded-registry admission record is not yet complete, so it has no public tool name |

The first four are permanent policy exceptions: nothing about them is expected
to change. `admission-pending` is the one temporary category, and it says so —
the capability ships on the CLI while the registry-budget record in
[`mcp-tool-surface-scaling.md`](mcp-tool-surface-scaling.md) is outstanding. It
exists because forcing such a row into `operator-territory` would record a
reason that is not the real one, in an artifact CI checks. A row carrying it
must name the spec requirement that owes the admission record.

Every exempt row still names its owning service callable and observable
outcome. OAuth or browser interaction alone is not an exemption: an MCP agent
can safely present a verification URL and user code while secret device codes
remain in the profile-scoped `SecretStore`. The newly created or currently
addressed flow is always retained; beyond it, the newest sessions are kept up
to 16 pending flows and 16 terminal results per profile. Expired flows lose
their device codes before the bounded collection is persisted.

## Enforcement

[`tests/moneybin/test_mcp/test_capability_parity.py`](../../tests/moneybin/test_mcp/test_capability_parity.py)
enforces:

- unique, well-formed rows;
- exact coverage of `STANDARD_TOOL_NAMES`;
- exact coverage of every implemented Typer path;
- executable not-implemented outcomes for reserved paths;
- explicit coverage and delegation guards for hidden compatibility aliases;
- importable callable service methods;
- executable outcome parity on isolated copies of the same initialized
  database for refresh match/identity, reports, annotations, taxonomy,
  consent, import, and SQL; and
- equivalent persisted secret-session logout behavior for sync.

The old canonical-name symmetric-difference test is retired. Similar names can
still improve discoverability, but they are neither necessary nor sufficient
for capability parity.
