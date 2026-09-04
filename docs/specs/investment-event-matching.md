# Investment Event Matching

> Last updated: 2026-09-04
> Status: ready
> Address: M1J.7 (Investments — cross-source event matching)
> Type: Feature
> Owns: provider-neutral matching of whole investment events, review decisions,
> Golden event materialization, and investment-transaction identity continuity
> Refines: [`investments-overview.md`](investments-overview.md) and
> [`matching-overview.md`](matching-overview.md)
> Constrained by:
> [`investments-data-model.md`](investments-data-model.md),
> [`account-identity-resolution.md`](account-identity-resolution.md),
> [`matching-consistency-boundaries.md`](matching-consistency-boundaries.md),
> [`app-integrity-invariant.md`](app-integrity-invariant.md), and
> [`observability.md`](observability.md)

## One-line goal

Recognize manual and provider observations of the same whole investment event,
let a person ratify the result, and rebuild one stable Golden event without
losing source fidelity or prior curation.

## Decision summary

MoneyBin adds a dedicated, provider-neutral investment-event matcher with a
review-first launch posture:

1. A comparison layer adapts every source into event headers and typed legs.
2. SQL performs normalization, candidate blocking, evidence calculation, and
   Golden projections; Python performs whole-event assignment and applies
   reviewed decisions.
3. `event_group_id` becomes the stable MoneyBin-owned identity of a Golden
   investment event. A singleton transaction is a one-leg event.
4. Matching is atomic at the event boundary. MoneyBin never accepts only part
   of a reinvestment, transfer, or other multi-leg event.
5. Every candidate remains pending until a person accepts or rejects it in the
   first production release. The engine may label a candidate `auto_eligible`
   for measurement, but that label has no mutation authority.
6. Accepted membership, rejected candidate fingerprints, explicit field
   resolutions, provenance, and transaction-id aliases are durable and
   reversible.
7. Broad mixed-history containment remains in force until a separate promotion
   proves that matched history is safe enough to narrow it.

The contract starts with manual and Plaid adapters, but no table, decision key,
or service contract names Plaid as the generic provider.

## Why this exists

Manual entry and provider sync currently contribute independent rows to
`core.fct_investment_transactions`. When both histories describe the same
economic activity, downstream lots, holdings, realized gains, income, and fees
can all be counted twice. Suppressing one source wholesale is safer than a
wrong ledger, but it also hides legitimate non-overlapping history.

Cash matching cannot simply absorb this problem. Cash candidates are primarily
row or graph relationships; an investment event may contain several
semantically linked legs whose quantities, cash effects, fees, dates, and
accounts must agree as one unit. Treating those legs independently could keep
the dividend from one source and the reinvestment purchase from another, or
accept one side of a transfer without the other. Either result creates a
plausible but incorrect tax ledger.

## Goals

- Match manual and Plaid observations of the same economic investment event.
- Make the matching contract provider-neutral so another provider adds an
  adapter rather than a parallel matching system.
- Preserve raw source observations unchanged.
- Give each accepted event and leg a stable MoneyBin-owned identity.
- Preserve explicit user curation when a better source observation joins an
  existing event.
- Show the evidence, field differences, alternatives, and downstream effect
  before a person decides.
- Make acceptance, rejection, staleness, and undo atomic and auditable.
- Keep lots, holdings, gains, and reports deterministic rebuilds of the Golden
  ledger.

## Non-goals

- Automatically accepting investment matches in the first production release.
- Replacing cash matching or storing investment state in
  `app.match_decisions`.
- Inferring account or security identity. Matching consumes already-ratified
  `account_id` and `security_id` values.
- Reconstructing a split ratio that a source did not provide. Plaid split
  matching remains disabled until M1J.5 supplies a trustworthy adapter.
- Repairing incomplete provider history, inventing missing event legs, or
  synthesizing a transfer counterpart.
- Changing tax-lot policy, cost-basis elections, or wash-sale handling.
- Narrowing mixed-history containment as part of the matcher launch.
- Providing a partial-acceptance escape hatch for a multi-leg event.

## Vocabulary and grain

| Term | Meaning |
|---|---|
| Source observation | One raw provider or manual record preserved unchanged. |
| Comparison leg | One normalized investment-transaction row used as matching evidence. |
| Source event | One or more comparison legs that one source says form one economic event. |
| Proposal | One inferred set of source events that may represent the same real event. |
| Golden event | The ratified, canonical event exposed through the core ledger. |
| Golden leg | One canonical investment transaction inside a Golden event. |
| Event fingerprint | A versioned digest of normalized member identities and match-relevant values. |

In the Golden core ledger, `event_group_id` identifies both a multi-leg event
and a singleton event. It is therefore no longer merely an optional hint
connecting decomposed rows. Raw and source-staging values remain nullable and
source-shaped; they are inputs to source-event construction, not canonical
identities. `investment_transaction_id` identifies a Golden leg within the
Golden event.

## Architecture

```mermaid
flowchart LR
    R["Immutable source observations"] --> A["Provider-neutral adapters"]
    A --> H["Comparison event headers"]
    A --> L["Comparison event legs"]
    H --> C["SQL candidate evidence"]
    L --> C
    C --> P["Python whole-event assignment"]
    P --> Q["Pending investment-match reviews"]
    Q --> D["Audited decision application"]
    D --> M["Golden event membership"]
    D --> F["Explicit field resolutions"]
    M --> G["SQL Golden ledger projection"]
    F --> G
    G --> T["Lots, holdings, gains, and reports"]
    M --> V["Field and row provenance"]
```

### Ownership boundaries

The investment matcher is a dedicated module. It may reuse generic graph data
structures and the existing review workflow, but it does not extend the cash
match decision schema or make cash services understand investment legs.

SQL owns operations that are naturally relational and inspectable:

- provider-neutral normalization;
- event and leg comparison views;
- candidate blocking and evidence columns;
- deterministic Golden-field defaults;
- Golden-ledger materialization; and
- field and source-row provenance projections.

Python owns operations whose correctness depends on graph-wide state or an
audited mutation:

- global candidate assignment;
- competing-Proposal detection;
- freshness validation;
- atomic accept, reject, and undo; and
- orchestration of the downstream rebuild.

This split keeps the evidence queryable without expressing global assignment as
opaque procedural SQL or moving deterministic warehouse projections into
application code.

## Comparison contract

### Source event construction

Each adapter emits a source event key and one or more typed legs. When the
source already provides a trustworthy group reference, the adapter uses it.
Manual grouped rows use their authored group. An ungrouped transaction becomes
a singleton source event.

Each event header carries:

- `source_type` and `source_origin`;
- source event key;
- resolved account set and security set;
- normalized event type and date interval;
- member count and event fingerprint; and
- adapter capability flags, including whether split semantics are supported.

Each comparison leg carries the source-row identity plus normalized type,
subtype, semantic leg role, account, security, trade and settlement dates,
quantity, price, amount, fees, currency, native references, and the original
Golden identity when one exists.

Normalization may remove representational differences such as sign convention,
decimal scale, case, and trade-date-versus-settlement-date placement. It must
not erase a material disagreement.

### Eligibility gates

A candidate is ineligible unless all applicable legs agree on:

- ratified canonical account identity;
- ratified canonical security identity;
- currency code for every compared monetary field; and
- an event shape supported by both adapters.

A transfer compares the entire account pair, direction, security, and quantity.
A missing or unresolved identity routes to its existing account- or
security-identity review; the event matcher does not guess it.

Splits require an exact normalized ratio and an adapter that declares split
support. The manual adapter may support that contract. The Plaid adapter must
declare splits unsupported until M1J.5 resolves provider split semantics, so a
Plaid split cannot enter a matching Proposal in this increment.

### Candidate bands

Candidates are evaluated in descending confidence:

1. **Native identity.** A source-native relationship or an already-ratified
   membership identifies the same event.
2. **Exact economic identity.** Shape, identities, dates, quantities, and cash
   values agree after harmless normalization.
3. **Constrained fuzzy identity.** Required identities and shape agree, while
   bounded date or numeric differences remain within the type-specific matrix.

Descriptions explain a candidate but never establish it. A fuzzy trade must
pass both quantity and cash evidence. A correction or reversal requires a
native relationship or remembered ratified relationship; similarity alone is
insufficient.

### Initial tolerance matrix

These thresholds admit review candidates; they do not authorize acceptance.

| Evidence | Candidate threshold |
|---|---|
| Buy, sell, or reinvest date | Same date or within 5 calendar days across trade and settlement dates |
| Dividend, interest, or fee date | Same date or within 3 calendar days |
| Transfer date | Same date or within 7 calendar days |
| Quantity | Exact at 10 decimal places, or difference no greater than `max(0.000001, abs(quantity) * 0.00000001)` |
| Amount | Difference no greater than `0.01` after sign normalization |
| Fees | Gross/net reconciliation differs by no more than `0.01` |
| Price | Difference no greater than `max(0.01, abs(price) * 0.0001)`, or the quantity/cash equation reconciles within `0.01` |
| Correction or reversal | Native or remembered relationship only |
| Split | Exact normalized ratio and supported adapters only |

The scenario suite owns the boundary examples for every threshold. A threshold
change is a behavior change and must update those examples.

## Whole-event assignment

The planner assigns source events, not individual legs. An accepted Proposal
must satisfy all of these invariants:

- each source event belongs to at most one active Golden event;
- every leg of every source event moves together;
- the proposed leg correspondence is total for the supported event shape;
- competing assignments remain visible rather than being broken by arbitrary
  row order; and
- repeated identical events are solved globally, not greedily.

An unambiguous two-by-two set of same-day trades may produce two Proposals when
the total assignment is unique. A one-to-two or otherwise equally valid
assignment remains competing and cannot be accepted until the ambiguity is
resolved.

Every planned Proposal records a versioned fingerprint over its normalized
members and match-relevant fields. Acceptance rereads the source observations,
recomputes the fingerprint, and refuses a stale Proposal. Re-running the
planner must not create another pending review for an unchanged pending,
accepted, or rejected fingerprint.

## Durable state

The exact migration DDL may follow repository conventions, but the following
semantic homes are fixed.

### `app.investment_match_decisions`

One audited Proposal decision with status `pending`, `accepted`, `rejected`,
`stale`, or `reversed`. It stores the Proposal identity, algorithm version,
source-event keys, normalized fingerprint, confidence band, evidence summary,
timestamps, and actor. Rejected fingerprints suppress unchanged Proposals; a
materially different fingerprint is a new Proposal.

### `app.investment_event_members`

The active and historical mapping from each source event and source leg to its
Golden `event_group_id`, Golden `investment_transaction_id`, and semantic leg
role. The refresh step registers a standalone membership the first time it sees
a new source event, so even an unmatched singleton has a stable Golden event
identity. Acceptance rewrites the entire affected membership set atomically.
Reversal retires that accepted membership rather than deleting its history.
Historical membership retains prior Golden ids and legacy source-group ids as
the event-id forwarding path.

There is no separate mutable `app.investment_events` registry. An active Golden
event exists because it has active membership. This avoids two mutable sources
of truth for event existence.

### `app.investment_match_field_resolutions`

Only explicit user choices for material field conflicts. Deterministic defaults
remain derived in SQL and do not become mutable copies. A resolution identifies
the Golden event or leg, field, chosen source observation, decision, and audit
metadata.

### `app.investment_transaction_id_aliases`

Append-only mapping from a previously published investment transaction id to
the active Golden leg id. Existing `app.lot_selections` and other curated
references resolve through this alias path so acceptance does not silently
orphan them. Decision application re-points both disposal references and
derived acquisition `lot_id` references to their new canonical ids inside the
same transaction. An accepted match is blocked when a selection cannot be
mapped without ambiguity.

All mutations use repositories under `src/moneybin/repositories/`. Services do
not issue raw writes against these protected tables.

## Golden identity and field selection

### Identity

On first observation, MoneyBin mints a truncated UUID `event_group_id` that is
independent of provider row ids and values, then persists it with the standalone
membership. Existing populated group ids are retained as legacy aliases during
the migration; they do not become source-dependent Golden identities.

When acceptance joins existing Golden events, the oldest established Golden
event identity remains canonical, with the opaque id as the deterministic
tie-breaker. Historical membership forwards every retired event id to the
result. Golden leg ids follow the corresponding established semantic leg where
one exists; other previously published transaction ids become append-only
aliases. Adding a third observation to an existing event therefore changes
neither the event id nor its leg ids.

A material change to leg structure stales the existing decision. The system
does not silently repurpose a leg id for a different semantic role. When an
accepted match replaces previously published source-derived transaction ids,
the alias table preserves continuity.

### Field fidelity

Golden fields follow this order:

1. preserve an explicit user resolution or curation;
2. normalize harmless representational differences;
3. prefer the provider observation for objective financial fields; and
4. require an explicit field choice for a material conflict.

Objective fields include dates, quantities, prices, amounts, fees, currencies,
and provider-native references. Provider preference is a default, not a claim
that provider data is infallible. A manually curated field that was explicitly
chosen remains authoritative when another observation joins the event.

Every Golden field exposes provenance to its chosen source observation or
explicit resolution. Event membership provenance separately retains every
contributing source row, including observations whose value was not selected.

## Review and operation surfaces

### Planning and inspection

```text
moneybin investments matches run
moneybin investments matches pending
moneybin investments matches history
```

`run` refreshes pending Proposals but changes no Golden membership. `pending`
shows the whole event, corresponding legs, confidence band, exact evidence,
material field differences, competing candidates, and the expected downstream
effect. `history` shows accepted, rejected, stale, and reversed decisions.

### Decision and undo

Investment matching joins the existing cross-domain review surface:

```text
moneybin review --type investment-matches --confirm <review-id>
moneybin review --type investment-matches --reject <review-id>
moneybin system audit undo <operation-id>
```

MCP reuses `reviews`, `reviews_decide`, and `system_audit_undo` with an explicit
`investment_matches` kind. It does not add one tool per command.

Accept and reject operate on a whole Proposal. The confirmation copy states
that acceptance rebuilds the investment ledger and its dependent lots,
holdings, gains, and reports. A stale Proposal cannot be confirmed. Undo
restores the prior active membership and rebuilds the same dependency set.

### Refresh and failure semantics

The refresh registry gains an `investment_match` step after source staging and
identity resolution but before the Golden investment ledger and its dependent
models. Planning is safe to repeat.

Decision state commits before the rebuild it requires. If the decision commits
and the subsequent rebuild fails, the operation reports both facts: the
decision is durable, derived surfaces are stale, and broad containment remains
active. It must not claim rollback or expose the partially refreshed result as
current. A later refresh retries the rebuild from the same durable decision.

## Containment and promotion

The existing mixed manual/provider history containment remains the safety
boundary throughout initial rollout. An account stays contained when it has a
pending, competing, stale, unsupported, or otherwise ambiguous event risk.
Accepted matches do not by themselves prove that the rest of the account is
safe.

Narrowing containment is a separate promotion decision after this matcher has
real-data evidence. That change must define which unmatched rows become trusted
and prove that no relevant event shape can still double-count the ledger.

Automatic acceptance is also a separate promotion. The first release may
record which Proposals would have been auto-eligible so precision can be
measured without granting them mutation authority.

## Observability

Metrics use bounded, non-sensitive labels and are added to
`src/moneybin/metrics/registry.py`:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `investment_match_proposals_total` | Counter | `band`, `outcome` | Planned, pending, suppressed, competing, or stale Proposals |
| `investment_match_decisions_total` | Counter | `decision` | Accepted, rejected, or reversed decisions after commit |
| `investment_match_events_total` | Counter | `event_type`, `outcome` | Whole events materialized or withheld |
| `investment_match_rebuild_total` | Counter | `outcome` | Successful or failed dependent rebuilds |
| `investment_match_duration_seconds` | Histogram | `operation` | Planning, decision, and rebuild latency |

Logs may include counts, opaque event ids, status codes, event types, and
operation names. They must not include descriptions, security names, monetary
values, quantities, account labels, or source payloads.

## Scenario matrix

The implementation is not accepted until the following cases have explicit
fixtures and expected Golden-ledger outcomes.

| Area | Required scenarios |
|---|---|
| Simple events | Exact and fuzzy buy, sell, dividend, interest, and fee matches; legitimate unmatched neighbors remain separate |
| Dates | Same date and both sides of every type-specific boundary; trade date matched to settlement date |
| Precision | Exact decimal normalization plus inside/outside quantity, amount, fee, and price thresholds |
| Reinvestment | Manual and provider multi-leg shapes; income and acquisition move atomically; a missing leg is not accepted |
| Transfers | Both account directions and quantities agree; one-sided or mismatched transfers remain ineligible |
| Repetition | Unique two-to-two assignment of identical same-day trades; ambiguous one-to-two assignment remains competing |
| Partial history | Non-overlapping manual and provider periods remain present once containment is eventually narrowed |
| Corrections | Native or remembered correction/reversal accepted; fuzzy-only similarity rejected |
| Identity | Unresolved or contradictory account, security, or currency identities remain ineligible |
| Splits | Normalized contract fixtures pass for supported adapters; Plaid split candidates stay disabled |
| Stability | Repeated sync, input reordering, and an additional source observation preserve Golden ids and avoid duplicate reviews |
| Extensibility | A third-provider fixture joins an accepted event without changing public Golden identities |
| Curation | Explicit field and lot-selection curation survives acceptance, added observations, rebuild, and undo |
| Downstream | Exact lots, holdings, realized gains, income, and fee results before acceptance, after acceptance, and after undo |
| Recovery | Stale Proposal refusal; committed decision plus failed rebuild reports durable decision and stale derived state |

## Verification

- Pure normalization and tolerance tests for every supported event type.
- Pure global-assignment tests, including competing and repeated-event graphs.
- DuckDB repository tests for atomic membership, rejection suppression, aliases,
  field resolutions, audit records, and reversal.
- SQLMesh tests for comparison views, Golden projection, provenance, and stable
  identities.
- Scenario tests for every row in the matrix, including exact downstream tax-lot
  outputs.
- CLI and MCP parity tests for plan, inspect, accept, reject, stale, failure, and
  undo outcomes.
- Property or invariant tests proving a source event has at most one active
  Golden membership and every accepted multi-leg event is complete.
- Real mixed-history validation before any containment or auto-accept promotion.

No auto-accept threshold may ship from synthetic precision alone. Promotion
requires zero false consolidations in the labeled scenario corpus and the
approved real-data validation set, with ambiguous cases remaining visible.

## Delivery slices

Each slice is independently reviewable. Public implementation issues are opened
only after this contract is accepted.

1. **Comparison foundation.** Add manual and Plaid adapters, event/leg comparison
   views, normalization, tolerances, and explicit inactive split capability.
2. **Review-only planner.** Add whole-event assignment, versioned fingerprints,
   competing detection, and pending Proposals without changing the core ledger.
3. **Decision workflow.** Persist decisions and rejection suppression; add the
   review CLI/MCP paths, audit integration, and metrics.
4. **Golden materialization.** Add stable event and leg identities, membership,
   aliases, field resolution, provenance, and the dependent rebuild.
5. **Lifecycle proof.** Complete the scenario matrix, repeated-sync and failure
   recovery tests, labeled real-data validation, and evidence for a later
   containment decision.

## Deferred decisions

- The precision threshold, eligible bands, and safeguards for a future
  auto-accept promotion.
- The exact containment rule that can replace broad account-level withholding.
- Plaid split matching, owned by M1J.5.
- Provider-specific adapters beyond manual and Plaid.
- Event shapes not represented by the current ledger taxonomy.
