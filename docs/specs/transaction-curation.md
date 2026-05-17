# Feature: Transaction Curation

## Status

implemented

## Goal

Establish the user-state layer on top of MoneyBin's canonical transaction store: manual transaction entry (CLI single, MCP bulk), free-text multi-note threads, multi-tag annotations, split-via-annotation as an interim before first-class splits, import-batch labels, and a unified edit-history audit log. All on a new and growing `app.*` user-state schema. The existing `raw → prep → core` ingestion contract — its matching, dedup, merge, and categorization behaviors — is unchanged: manual transactions enter via one new raw source. `core.fct_transactions` is extended *additively* with curation-presentation columns (LIST/STRUCT joins from `app.*`), and a sibling `core.fct_transaction_lines` view is added; see §Data Model.

This spec is the curator's surface — the row-by-row grooming that turns "data MoneyBin has" into "data the user trusts and uses."

## Background

M0–M1 shipped a defensible engine: encrypted DuckDB, smart import + dedup + transfer detection, rule + auto-rule categorization, accounts and net worth, ~33 MCP tools. The product today is a credible "ETL + canonical store + MCP surface for personal finance."

The **Curator segment** is MoneyBin's most defensible position: "I want my financial history to be a clean, verifiable, queryable record I trust — not a passive feed from a black-box service. I'll spend an hour a month grooming it because the result is mine." Lunch Money, Tiller, Fina, Beancount/Fava users self-select into curating; MoneyBin's encrypted-local-DuckDB + MCP-native posture serves them better than any competitor — *if* the curation surface exists.

It currently doesn't. `app.transaction_notes` exists with single-note semantics; nothing else (tags, splits, import labels, audit history, manual entry) is reachable from CLI or MCP. Manual entry is in-scope: a curator can't trust a record they can't repair.

This spec ships the bundle as a single coherent surface. It is the lead M2A spec, gating other M2/M3 work because Plaid, investments, and reporting all benefit from knowing the user-state shape.

### Related specs

- [`matching-same-record-dedup.md`](matching-same-record-dedup.md) — `app.match_decisions`, gold-key `transaction_id`. This spec extends the matcher's candidate-blocking rule.
- [`categorization-overview.md`](categorization-overview.md) / [`categorization-auto-rules.md`](categorization-auto-rules.md) — `app.transaction_categories`, priority hierarchy, auto-rule training. This spec extends the auto-rule training query.
- [`smart-import-tabular.md`](smart-import-tabular.md) — `raw.tabular_transactions` shape, `Database.ingest_dataframe()`, `raw.import_log` lifecycle. Manual entry mirrors this flow.
- [`smart-import-financial.md`](smart-import-financial.md) — reversible imports via `import_id`. Manual entries are reversible by the same mechanism.
- [`mcp-architecture.md`](mcp-architecture.md) / [`moneybin-mcp.md`](moneybin-mcp.md) — MCP v2 conventions, sensitivity tiers, response envelopes.
- [`mcp-sql-discoverability.md`](mcp-sql-discoverability.md) — `moneybin://schema` resource. New curation columns and tables register here.
- [`moneybin-cli.md`](moneybin-cli.md) — CLI v2 entity-group taxonomy. New commands land under `transactions`, `import`, `system`.
- [`net-worth.md`](net-worth.md) — balance-level reconciliation. This spec defers per-transaction reconciliation (the "verified" concept) to a future transaction-reconciliation spec; coordinates an Out-of-Scope addition there.
- [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) — `app.ai_audit_log` schema. This spec subsumes that table into the unified `app.audit_log` and updates the privacy spec accordingly.
- [`testing-synthetic-data.md`](testing-synthetic-data.md) — persona YAML format. This spec ships a new "curator" persona.
- [`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md) — five-tier assertion taxonomy and scenario YAML schema.

### Decisions made during design (cross-references for reviewers)

| Decision | Rationale | Documented in |
|---|---|---|
| Verified curator flag dropped | Primary reason: conflicts with the brand's "integrity by construction" claim — a per-row `verified` flag creates an implicit "unverified" category that the user must worry about, and per-row grooming is a poor curator workflow at any meaningful volume. Trophy metrics belong on system-asserted invariants (`moneybin doctor` in M2C), not user assertions. Secondary reason: overlap with future transaction-level reconciliation. | §Out of Scope; cross-spec edit to `net-worth.md` |
| `app.audit_log` is unified (subsumes `app.ai_audit_log` table) | One audit surface, one retention story; AI-specific fields ride `context_json` | §Data Model; cross-spec edit to `privacy-and-ai-trust.md`, `mcp-architecture.md` |
| Manual transactions live in `raw.manual_transactions` (mirror tabular shape) | Existing pipeline runs unchanged; `source_type='manual'` discriminator drives matcher and auto-rule exemptions | §Data Model, §Pipeline Integration |
| Curation tables stored relationally in `app.*`, presented as DuckDB nested types in `core.fct_transactions` | Write ergonomics from flat tables, consumer ergonomics from `LIST`/`LIST(STRUCT)` | §Architectural Pattern |
| Notes are multi-note (extending existing `app.transaction_notes`) | Curator journaling needs history; audit log is wrong UX for that | §Data Model |
| CLI imperative (`add`/`remove`/`edit`), MCP declarative (`*_set`) | Humans procedural, LLMs target-state — same service layer, asymmetric vocabulary | §Architectural Pattern, §CLI Interface, §MCP Interface |
| `transactions_create` MCP tool is bulk (1–100 per call); CLI is single-txn | LLMs batch naturally; humans type interactively | §MCP Interface |

## Architectural Pattern

This spec establishes two cross-cutting patterns that future specs reference. Both are written here for `architecture-shared-primitives.md` (the sibling M2 entry spec) and `mcp-ux-standards.md` (a future spec) to lift verbatim.

### Pattern 1 — Curation storage and presentation

**User state lives in flat relational tables in `app.*` for write ergonomics. Presentation in `core.fct_*` / `core.dim_*` is via DuckDB nested types — `LIST(VARCHAR)` for tag-shaped sets, `LIST(STRUCT(...))` for ordered child records — aggregated in the SQLMesh JOIN.**

- Storage stays flat so adding/removing a child is a parameterized INSERT/DELETE per row, not a read-modify-write on a JSON column.
- Presentation aggregates with `LIST(STRUCT_PACK(...) ORDER BY ...)` inside a CTE, joined into the fact view.
- Consumers read one row per real-world entity with all curation context inline; **they never join `app.*` directly**.
- Convenience views (`core.vw_*`) flatten via `UNNEST` when consumers want a per-child grain — but they read from `core.fct_*`, not from `app.*`. *If it's not resolved into the fact, it didn't happen.*

This extends the existing "Core Dimensions Are the Single Source of Truth" rule (`.claude/rules/database.md`) from scalar overrides (`app.account_settings.display_name`) to nested-type aggregates (`app.transaction_tags` → `tags LIST(VARCHAR)` on `core.fct_transactions`).

When write volume is low (e.g., import labels — once per batch, rarely edited), the LIST column may live directly on the `app.*` row instead of a sibling M:N table — read-modify-write is acceptable at low volume, and consolidating user-state into one row per entity prevents `app.*` table sprawl.

### Pattern 2 — Same service layer, asymmetric surface vocabularies

**The service layer is the source of truth for capability. CLI and MCP call into the same methods but expose different verb vocabularies to match how each surface is used.**

- **CLI: imperative verbs** (`add`/`remove`/`edit`/`delete`/`clear`). Humans think procedurally; fine-grained ops compose into curator workflows. Example: `transactions tags add txn_x foo`, then `transactions tags remove txn_x bar`.
- **MCP: declarative state-setters** (`*_set`) when the operation's semantic is "make state look like this." LLMs reason about target states more reliably than diffs. Example: `transactions_tags_set(transaction_id=x, tags=["foo","baz"])` — the service computes the diff and applies it.
- **Symmetry contract**: any capability reachable from one surface is reachable from the other. The translation lives in the service layer, not in surface-specific business logic.
- **When NOT to declarative-set**: when individual ops have distinct semantics that don't collapse into a target-state representation. Notes (`add`/`edit`/`delete` operate on individual `note_id`s with separate audit semantics) keep imperative verbs in both CLI and MCP.

A follow-up audit pass on the existing MCP surface (Out-of-Scope §Follow-ups) is recommended for `moneybin-mcp.md` v2 — several existing toggles (`accounts_include`, `accounts_archive`, `categories_set (done — vocabulary sweep, 2026-05-17)`) are candidates for declarative-set consolidation.

## Requirements

### Manual entry

1. Users can create one or more manual transactions via CLI (`transactions create`, single-txn) or MCP (`transactions_create`, bulk 1–100 per call).
2. Manual transactions land in a new `raw.manual_transactions` table mirroring the `raw.tabular_transactions` column shape.
3. A new `prep.stg_manual__transactions` staging view is added to the existing `prep.int_transactions__unioned` model. No prep or core models *on the manual-ingestion path* change shape beyond adding this staging view. (Curation-presentation columns added to `core.fct_transactions` and the new `core.fct_transaction_lines` view are described in §Data Model — additive joins from `app.*`, not changes to the ingestion contract.)
4. Each manual-entry CLI invocation or MCP bulk call writes exactly one row to `raw.import_log` with `source_type='manual'`, `format_name='manual_entry'`, and the resulting `import_id` is reusable for batch labeling and reversal.
5. Manual transactions enter the standard pipeline: transform → match → categorize. They are reversible via the existing `import revert <import_id>` flow.
6. Manual transactions are excluded from cross-source dedup (Tier 3) candidate selection. They are never proposed as matches against imported rows in either direction. Explicit user merge via `transactions matches confirm` is the only path that pairs them.
7. Manual transactions are excluded from the auto-rule generator's training set. Auto-rules continue to learn from user category edits made *to imported* rows.
8. Manual transactions written with `--category` get an `app.transaction_categories` row with `categorized_by='user'` (same priority tier as any user override).

### Notes

9. Each transaction can carry zero or more notes. Notes are append-only chronological entries authored by the user.
10. The existing `app.transaction_notes` table is migrated from single-note to multi-note (`note_id` PK; existing rows backfilled with generated `note_id`, `author='legacy'`).
11. Notes have a maximum text length of 2000 characters (service-layer enforced).
12. `core.fct_transactions` exposes notes as `LIST(STRUCT(note_id, text, author, created_at))` ordered by `created_at`. NULL when no notes exist; `note_count` scalar is also exposed.

### Tags

13. Each transaction can carry zero or more tags. Tags are flat strings in `app.transaction_tags(transaction_id, tag, applied_at, applied_by)` with `(transaction_id, tag)` as primary key.
14. Tag pattern: `^[a-z0-9_-]+(:[a-z0-9_-]+)?$` — slug-flavored, optional namespace prefix separated by a colon (e.g., `tax:business-expense`, `vacation:hawaii-2026`, `recurring`). Service-layer validated.
15. Bulk rename across rows is supported: `transactions tags rename old_tag new_tag` updates every row in `app.transaction_tags` and emits a parent audit event plus per-row child events with `parent_audit_id` chaining.
16. `core.fct_transactions` exposes tags as `LIST(VARCHAR)` (sorted). NULL when no tags exist; `tag_count` scalar is also exposed.

### Splits (via annotation)

17. Each transaction can be split into 1+ child rows recorded in `app.transaction_splits(split_id, transaction_id, amount, category, subcategory, note, ord, created_at, created_by)`.
18. The sum of child amounts is **not** strictly enforced equal to parent amount on every write. Real curator workflow is iterative ("split off the $60 supplies portion now, come back tomorrow when I remember the gas"). The CLI prints a warning when sum is unbalanced after each `add`/`remove`. Strict reconciliation is deferred to the future transaction-reconciliation spec.
19. `core.fct_transactions` exposes splits as `LIST(STRUCT(split_id, amount, category, subcategory, note))` ordered by `ord`, plus `split_count` and `has_splits` scalars. The parent row stays at full amount — splits never replace the parent in this view.
20. A new convenience view `core.fct_transaction_lines` flattens splits via `UNNEST(t.splits)` from `core.fct_transactions`, producing 1 row per non-split transaction and N rows per split transaction. Spending-by-category reports use this view; consumers wanting transaction-level grain stay on `core.fct_transactions`.
21. The view reads through `core.fct_transactions` (not directly from `app.transaction_splits`) — preserving the rule that consumers don't touch `app.*` directly.

### Import labels

22. Each `raw.import_log` row may carry zero or more user-applied labels stored in `app.imports(import_id, labels VARCHAR[], updated_at, updated_by)`. One row per labeled import. Absence = no user-state on that batch.
23. Labels follow the same pattern as tags (`^[a-z0-9_-]+(:[a-z0-9_-]+)?$`) but live in their own column because import labels and transaction tags are queried independently.
24. The `app.imports` table is intentionally consolidated (single row per import with a `LIST(VARCHAR)` labels column) rather than an M:N sibling table, because import labels have low write volume and consolidating prevents `app.*` table sprawl.

### Audit log (unified)

25. A single `app.audit_log` table records all user-driven mutations to in-scope app state, plus AI-call provenance (subsuming the previously planned `app.ai_audit_log` table).
26. Audit emission is synchronous, in the same DuckDB transaction as the mutation. Implementation: `AuditService.record_audit_event(action, target, before, after, *, actor, parent_audit_id=None, context=None)`.
27. In-scope services emit events: `TransactionService` (manual entry, notes, tags, splits), `ImportService` (labels), `CategorizationService` (category set/clear), merchant service (create/set), rule service (create/update/delete), AI provider boundary (`ai.external_call`).
28. Out-of-scope services do not emit; retroactive coverage is the post-launch `audit-log.md` spec's responsibility.
29. `before_value` and `after_value` capture the relevant column subset of the affected row, not the entire table row. Bulk operations (`tag.rename`) emit one parent event capturing the operation intent and per-row child events with `parent_audit_id` chaining.
30. AI-specific fields (flow_tier, backend, model, data_sent_hash, consent_reference, user_initiated) ride `context_json` — promoted to indexed columns only when a real query pattern demands it.
31. The existing `get_ai_audit_log` MCP/CLI surface continues to work — internally rewritten to query `app.audit_log` with `action LIKE 'ai.%'`. No compatibility view; `privacy-and-ai-trust.md` is updated to reference the unified table directly.

### Cross-cutting

32. All in-scope tables live in the `app.*` schema. This spec is the first to populate it heavily; `architecture-shared-primitives.md` (sibling M2 entry spec) formalizes the layer in `AGENTS.md`.
33. CLI is single-txn / per-row imperative. MCP is declarative-set or bulk where the semantic naturally collapses (tags, splits, labels, manual entry).
34. CLI/MCP capabilities are symmetric — anything one can do, the other can express.
35. Two manual transactions with identical `(account_id, transaction_date, amount, description)` coexist as distinct rows. Manual entries are user-authoritative; deduplication (if desired) is the user's explicit action via `transactions delete` or splits.

## Data Model

### New table: `raw.manual_transactions`

Mirrors `raw.tabular_transactions` column shape. Source-type discriminator is `'manual'`; source-origin is `'user'`.

```sql
CREATE TABLE IF NOT EXISTS raw.manual_transactions (
    source_transaction_id   VARCHAR PRIMARY KEY, -- 'manual_' + truncated UUID4 (12 hex)
    source_type             VARCHAR NOT NULL DEFAULT 'manual', -- Discriminator; matches matcher and auto-rule exemption predicates
    source_origin           VARCHAR NOT NULL DEFAULT 'user', -- Origin tag; always 'user' for manual entries
    import_id               VARCHAR NOT NULL, -- FK to raw.import_log.import_id; one batch per CLI call or MCP bulk call
    account_id              VARCHAR NOT NULL, -- FK to core.dim_accounts
    transaction_date        DATE NOT NULL, -- Date of the transaction as the user reports it
    amount                  DECIMAL(18,2) NOT NULL, -- Signed; negative = expense, positive = income
    description             VARCHAR NOT NULL, -- User-supplied description (free text)
    merchant_name           VARCHAR, -- Optional user-supplied merchant; resolved against app.merchants on next pipeline pass
    memo                    VARCHAR, -- Additional free-text memo
    category                VARCHAR, -- Optional user-supplied category at entry time
    subcategory             VARCHAR, -- Optional user-supplied subcategory
    payment_channel         VARCHAR, -- Optional: in_store, online, other
    transaction_type        VARCHAR, -- Optional source-style type code
    check_number            VARCHAR, -- Optional check number
    currency_code           VARCHAR DEFAULT 'USD', -- ISO 4217 currency code
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the row was inserted
    created_by              VARCHAR NOT NULL -- 'cli' or 'mcp'; future-extensible for multi-user identity
);
```

### Extended table: `app.transaction_notes` (single-note → multi-note)

```sql
CREATE TABLE IF NOT EXISTS app.transaction_notes (
    note_id         VARCHAR PRIMARY KEY, -- Truncated UUID4 (12 hex); unique per note
    transaction_id  VARCHAR NOT NULL, -- FK to core.fct_transactions
    text            VARCHAR NOT NULL, -- Note body; max 2000 chars (service-layer enforced)
    author          VARCHAR NOT NULL, -- 'cli', 'mcp', 'legacy' (migrated rows), or future user identity
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- When the note was added
);
CREATE INDEX IF NOT EXISTS idx_transaction_notes_txn ON app.transaction_notes(transaction_id);
```

**Migration:** existing rows (single-note shape: PK was `transaction_id`, columns `note`, `created_at`) get a new `note_id = uuid4().hex[:12]`, `author = 'legacy'`, preserving `created_at`. The text column is renamed `note → text`.

### New table: `app.transaction_tags`

```sql
CREATE TABLE IF NOT EXISTS app.transaction_tags (
    transaction_id  VARCHAR NOT NULL, -- FK to core.fct_transactions
    tag             VARCHAR NOT NULL, -- 'namespace:value' or bare 'value'; pattern enforced at service layer
    applied_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When this tag was applied
    applied_by      VARCHAR NOT NULL, -- 'cli', 'mcp', or future user identity
    PRIMARY KEY (transaction_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_transaction_tags_tag ON app.transaction_tags(tag);
```

### New table: `app.transaction_splits`

```sql
CREATE TABLE IF NOT EXISTS app.transaction_splits (
    split_id        VARCHAR PRIMARY KEY, -- Truncated UUID4 (12 hex)
    transaction_id  VARCHAR NOT NULL, -- FK to core.fct_transactions; the parent
    amount          DECIMAL(18,2) NOT NULL, -- Signed; sum across children should equal parent.amount but is not strictly enforced
    category        VARCHAR, -- References category taxonomy (string for now; migrates with category_id introduction)
    subcategory     VARCHAR, -- References subcategory taxonomy
    note            VARCHAR, -- Optional per-split note
    ord             INTEGER NOT NULL DEFAULT 0, -- Display order; ties broken by split_id
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR NOT NULL -- 'cli' or 'mcp'
);
CREATE INDEX IF NOT EXISTS idx_transaction_splits_txn ON app.transaction_splits(transaction_id);
```

### New table: `app.imports`

```sql
CREATE TABLE IF NOT EXISTS app.imports (
    import_id   VARCHAR PRIMARY KEY, -- FK to raw.import_log.import_id; one row per labeled import
    labels      VARCHAR[], -- LIST(VARCHAR); NULL when no labels; same slug pattern as tags
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by  VARCHAR NOT NULL
);
```

### New table: `app.audit_log`

```sql
CREATE TABLE IF NOT EXISTS app.audit_log (
    audit_id          VARCHAR PRIMARY KEY, -- Truncated UUID4 (12 hex)
    occurred_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the event happened
    actor             VARCHAR NOT NULL, -- 'cli', 'mcp', 'auto_rule', 'system', 'ai:<provider>:<model>'
    action            VARCHAR NOT NULL, -- e.g. 'manual.create', 'note.add', 'tag.rename', 'split.add', 'category.set', 'ai.external_call'
    target_schema     VARCHAR, -- e.g. 'app', 'core'
    target_table      VARCHAR, -- e.g. 'transaction_categories', 'transaction_tags'
    target_id         VARCHAR, -- gold transaction_id, rule_id, merchant_id, import_id, etc.
    before_value      JSON, -- Prior column subset; NULL on creation
    after_value       JSON, -- New column subset; NULL on deletion
    parent_audit_id   VARCHAR, -- Self-FK; chains AI-call → user-confirm → category-write, or bulk-rename → per-row events
    context_json      JSON -- Discriminator-shaped extras: AI fields (flow_tier, backend, model, data_sent_hash), source surface, hashes, etc.
);
CREATE INDEX IF NOT EXISTS idx_audit_log_target ON app.audit_log(target_table, target_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_occurred ON app.audit_log(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON app.audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_actor ON app.audit_log(actor);
CREATE INDEX IF NOT EXISTS idx_audit_log_parent ON app.audit_log(parent_audit_id);
```

### Retired table: `app.ai_audit_log`

This spec replaces the previously planned `app.ai_audit_log` table (designed in `privacy-and-ai-trust.md` but unimplemented as of 2026-05-06) with the unified `app.audit_log`. AI-specific columns become conventions on `context_json`:

- `flow_tier`: integer 1–3
- `feature`: e.g. `smart_import_parse`, `mcp_transaction_query`
- `backend`: provider name
- `model`: specific model
- `data_sent_summary`: human-readable redaction-level summary
- `data_sent_hash`: SHA-256 of the actual payload
- `response_summary`: shape description
- `consent_reference`: FK to `app.ai_consent_grants`
- `user_initiated`: boolean

Existing `get_ai_audit_log` CLI/MCP surface continues to work, internally querying `app.audit_log` with `action LIKE 'ai.%'`. No compatibility view is created — clean cut, single table.

### Modified SQLMesh model: `core.fct_transactions`

Adds curation columns by joining and aggregating from the `app.*` curation tables. The model stays one row per gold transaction.

```sql
WITH
notes_agg AS (
  SELECT
    transaction_id,
    LIST(STRUCT_PACK(
      note_id := note_id,
      text := text,
      author := author,
      created_at := created_at
    ) ORDER BY created_at) AS notes,
    COUNT(*) AS note_count
  FROM app.transaction_notes
  GROUP BY transaction_id
),
tags_agg AS (
  SELECT
    transaction_id,
    LIST(tag ORDER BY tag) AS tags,
    COUNT(*) AS tag_count
  FROM app.transaction_tags
  GROUP BY transaction_id
),
splits_agg AS (
  SELECT
    transaction_id,
    LIST(STRUCT_PACK(
      split_id := split_id,
      amount := amount,
      category := category,
      subcategory := subcategory,
      note := note
    ) ORDER BY ord, split_id) AS splits,
    COUNT(*) AS split_count
  FROM app.transaction_splits
  GROUP BY transaction_id
),
enriched AS (
  SELECT
    t.*,
    /* (existing categorization, merchant, transfer joins from current model) */
    n.notes,
    n.note_count,
    tg.tags,
    tg.tag_count,
    s.splits,
    s.split_count,
    COALESCE(s.split_count, 0) > 0 AS has_splits
  FROM prep.int_transactions__merged AS t
  LEFT JOIN app.transaction_categories AS c ON t.transaction_id = c.transaction_id
  LEFT JOIN app.merchants              AS m ON c.merchant_id    = m.merchant_id
  LEFT JOIN core.bridge_transfers      AS bt_debit  ON t.transaction_id = bt_debit.debit_transaction_id
  LEFT JOIN core.bridge_transfers      AS bt_credit ON t.transaction_id = bt_credit.credit_transaction_id
  LEFT JOIN notes_agg  AS n  ON t.transaction_id = n.transaction_id
  LEFT JOIN tags_agg   AS tg ON t.transaction_id = tg.transaction_id
  LEFT JOIN splits_agg AS s  ON t.transaction_id = s.transaction_id
)
SELECT
  /* (all existing columns) */
  notes,        /* LIST(STRUCT(note_id, text, author, created_at)); chronological. NULL when no notes. */
  note_count,   /* INTEGER; NULL when no notes. Use note_count > 0 rather than len(notes) > 0 to avoid 3-valued logic. */
  tags,         /* LIST(VARCHAR); sorted; 'namespace:value' or bare 'value'. NULL when no tags. */
  tag_count,    /* INTEGER; NULL when no tags. */
  splits,       /* LIST(STRUCT(split_id, amount, category, subcategory, note)); ordered by ord. NULL when no splits. */
  split_count,  /* INTEGER; NULL when no splits. */
  has_splits    /* BOOLEAN; FALSE when split_count IS NULL OR split_count = 0. */
FROM enriched;
```

**NULL semantics.** When no curation rows exist, the LIST columns are NULL (not empty list). DuckDB's `UNNEST(NULL)` produces zero rows, so iterating consumers work without ceremony. `'x' = ANY(NULL)` is NULL, which filters out untagged rows correctly in WHERE clauses. Use the `_count` scalars (`note_count > 0`, `tag_count > 0`, `split_count > 0`) for "is anything there?" predicates to avoid 3-valued logic on `len(...)`.

### New SQLMesh model: `core.fct_transaction_lines`

Split-expanded grain. 1 row per unsplit transaction, N rows per split transaction.

```sql
MODEL (
  name core.fct_transaction_lines,
  kind VIEW,
  grain (transaction_id, line_id)
);

SELECT
  t.transaction_id,
  COALESCE(s.split_id, 'whole') AS line_id, /* 'whole' for unsplit transactions; split_id for split children */
  COALESCE(s.amount, t.amount) AS line_amount, /* Per-line amount; equals parent.amount for unsplit rows */
  COALESCE(s.category, t.category) AS line_category, /* Per-line category */
  COALESCE(s.subcategory, t.subcategory) AS line_subcategory,
  s.note AS line_note, /* NULL on unsplit rows; per-split note when present */
  CASE WHEN s.split_id IS NULL THEN 'whole' ELSE 'split' END AS line_kind, /* 'whole' or 'split' */
  /* All non-split-varying columns from fct_transactions */
  t.account_id,
  t.transaction_date,
  t.merchant_name,
  t.description,
  t.is_pending,
  t.transfer_pair_id,
  t.is_transfer,
  t.source_type,
  t.source_count,
  t.transaction_year,
  t.transaction_month,
  t.transaction_year_month,
  t.transaction_year_quarter
FROM core.fct_transactions AS t
LEFT JOIN UNNEST(t.splits) AS s
WHERE NOT t.has_splits OR s.split_id IS NOT NULL;
```

The view reads from `core.fct_transactions` (not directly from `app.transaction_splits`) — preserving the rule that consumers don't touch `app.*` directly. *If it's not resolved in the fact, it didn't happen.*

### New staging view: `prep.stg_manual__transactions`

Trivial column-pass-through with light typing, joined into `prep.int_transactions__unioned` alongside the tabular and OFX sources. No new union columns; `source_type='manual'` is the discriminator.

### Source-priority list

`MatchingSettings.source_priority` (config) gets `manual` prepended at priority 0 (highest). Practically irrelevant since manual rows are exempt from candidate selection (see Pipeline Integration), but documented for completeness — if a user explicitly merges a manual row with an import via `transactions matches confirm`, manual fields win the merge.

## Pipeline Integration

### Matching engine — manual exemption

Per [`matching-same-record-dedup.md`](matching-same-record-dedup.md), Tier 3 cross-source dedup blocks candidates by `(account_id, transaction_date_window, amount)`. Without intervention, a manual "$4.50 coffee on March 14" would match a Plaid `STARBUCKS #4783 $4.50` on March 14.

This spec adds one predicate to the candidate-selection step: rows with `source_type='manual'` are excluded from candidate selection in either direction. They are not written into the candidate-pair scoring table.

The escape hatch for explicit user merges: `transactions matches confirm <manual_id> <import_id>` writes `app.match_decisions` with `match_tier='user_manual_merge'` and emits an audit event. This is rare-by-design.

**Cross-spec edit (follow-up, lands with implementation):** `matching-same-record-dedup.md` gets a paragraph under "Matching Engine § Candidate blocking" documenting the exemption. Not included in the spec PR.

### Auto-rule generator — manual exemption

Per [`categorization-auto-rules.md`](categorization-auto-rules.md), the auto-rule generator trains on `app.transaction_categories` rows with `categorized_by='user'`. This spec adds one predicate to the training query:

```sql
JOIN core.fct_transactions ft ON ft.transaction_id = tc.transaction_id
WHERE tc.categorized_by = 'user'
  AND ft.source_type != 'manual'   -- new
```

Manual descriptions are user-authored and idiosyncratic ("Coffee at Joe's"). They don't generalize as patterns for matching imported descriptions ("STARBUCKS #4783 SEATTLE WA"). Training on them produces brittle rules that match nothing or, worse, match unrelated rows.

The auto-rule generator continues to learn from user category edits made *to imported* rows — that's unchanged. Manual rows themselves are simply not training data.

**Cross-spec edit (follow-up, lands with implementation):** `categorization-auto-rules.md` gets a paragraph under the training-data section documenting the exemption. Not included in the spec PR.

### Categorization priority hierarchy — unchanged

Existing hierarchy (`user > rule > auto_rule > ml > plaid > ai`) is unchanged. Manual transactions enter the categorization pipeline like any other source — if the user supplied a category at entry time, it's written to `app.transaction_categories` with `categorized_by='user'` (top of the hierarchy). If they didn't, downstream auto-rule / ML / etc. operate normally.

Verification (the deferred curator badge — see §Out of Scope) would have promoted `categorized_by='auto_rule'` rows to user-priority for protection and training. With verify dropped, this spec adds nothing to the hierarchy.

## CLI Interface

All commands follow `moneybin-cli.md` v2 conventions: path-prefix-verb-suffix, plural top-level groups, singular sub-resource nouns, imperative leaf verbs, universal flags (`--profile`, `--verbose`, `--output`, `--yes`).

### `transactions create` — manual entry (single)

```
transactions create <amount> <description>
    --account ID                Required: account to attach the transaction to
    --date YYYY-MM-DD           Default: today
    [--category NAME]           Sets categorized_by='user' on creation
    [--subcategory NAME]
    [--merchant NAME]           Free-text; merchant resolution runs on next pipeline pass
    [--memo TEXT]
    [--note TEXT]               Adds an initial entry to app.transaction_notes
    [--tag TAG]                 Repeatable; e.g. --tag tax:business --tag vacation:hawaii-2026
    [--check-number N]
    [--payment-channel CH]
    [--currency CODE]           Default: USD
    [--yes]                     Skip confirmation
```

Single transaction per invocation. Returns the new `transaction_id` (gold key) and a one-line confirmation with next-step hints. Triggers the full pipeline (transform → match → categorize) for the new row.

### `transactions notes` — multi-note thread

```
transactions notes add <txn_id> <text>           [--yes]
transactions notes list <txn_id>                 [--output table|json]
transactions notes edit <note_id> <text>         [--yes]
transactions notes delete <note_id>              [--yes]
```

Imperative verbs operating on individual `note_id`s. Each emits its own audit event with before/after.

### `transactions tags` — M:N tags

```
transactions tags add <txn_id> <tag> [<tag>...]      Idempotent; tag pattern validated
transactions tags remove <txn_id> <tag> [<tag>...]
transactions tags list [--txn-id ID] [--account ID] [--from DATE] [--to DATE]
                                                      Without flags: distinct tags with usage counts.
                                                      With --txn-id: tags on one transaction.
transactions tags rename <old_tag> <new_tag>          Bulk rename across all rows; emits parent + per-row child audit events
```

`tags rename` is the single bulk-write operation in the CLI surface; every other write is per-row.

### `transactions splits` — split-via-annotation

```
transactions splits add <txn_id> <amount> --category NAME [--subcategory NAME] [--note TEXT]
                                                      Warns if SUM(children) ≠ parent.amount but does not block.
transactions splits list <txn_id>                     [--output table|json]
transactions splits remove <split_id>                 [--yes]
transactions splits clear <txn_id>                    [--yes]   Removes all splits on a transaction
```

The sum invariant is warn-not-block by design — real curator workflow is iterative. Reports using `core.fct_transaction_lines` tolerate partial splits gracefully (the unsplit remainder shows under the parent's category).

### `transactions audit` — entity-scoped audit query

```
transactions audit <txn_id>          Shows all audit_log entries for this transaction_id
                                     (across target_tables: transaction_notes, transaction_tags,
                                     transaction_splits, transaction_categories, etc.)
                                     [--output table|json] [--limit N]
```

Convenience wrapper. Equivalent to `system audit list --target-id <txn_id>` but pre-scoped to the transaction. Curators reach for "what did I do to this row?" much more often than the cross-cutting query.

### `system audit` — cross-cutting audit log

```
system audit list [--actor PATTERN] [--action PATTERN]
                  [--target-table NAME] [--target-id ID]
                  [--from TIMESTAMP] [--to TIMESTAMP]
                  [--limit N] [--output table|json]
system audit show <audit_id>         Full record incl. before/after JSON
```

`system` is the existing v2 group for cross-cutting meta operations. Audit fits there because it's not entity-scoped — rule edits, merchant edits, AI calls, and transaction edits all flow through it.

### `import labels` — batch labeling

```
import labels add <import_id> <label> [<label>...]
import labels remove <import_id> <label> [<label>...]
import labels list [--import-id ID]       With --import-id: labels on one batch
                                          Without: distinct labels across all batches with counts
```

Three-verb surface. Labels on `app.imports.labels` (LIST(VARCHAR)). Browsing past imports is a future surface (see Out of Scope §Follow-ups).

### Function naming

Per `feedback_cli_function_naming.md` and `.claude/rules/cli.md`: Typer subgroup commands follow `<group>_<verb>`. Examples: `transactions_create`, `transactions_notes_add`, `transactions_notes_edit`, `transactions_tags_add`, `transactions_tags_rename`, `transactions_splits_add`, `transactions_audit`, `system_audit_list`, `system_audit_show`, `import_labels_add`, `import_labels_list`.

## MCP Interface

Per `mcp-architecture.md` and `moneybin-mcp.md` v2: path-prefix-verb-suffix names, sensitivity tiers, response envelopes, write-tool confirmation conventions.

Nine new tools, two prompts, one new resource and two extended. Catalog grows from ~33 to ~42 — comfortably under the 50-tool friction line.

### Tools

| Tool | Sensitivity | Shape |
|---|---|---|
| `transactions_create` | write | `(transactions: list[ManualEntryInput], 1 ≤ len ≤ 100) → list[ManualEntryResult]` — bulk, atomic, single `import_id` per call |
| `transactions_notes_add` | write | `(transaction_id, text) → Note` |
| `transactions_notes_edit` | write | `(note_id, text) → Note` |
| `transactions_notes_delete` | write | `(note_id) → {note_id}` |
| `transactions_tags_set` | write | `(transaction_id, tags: list[str]) → list[Tag]` — declarative; service computes diff |
| `transactions_tags_rename` | write | `(old_tag, new_tag) → {row_count, parent_audit_id}` — bulk rename |
| `transactions_splits_set` | write | `(transaction_id, splits: list[SplitInput]) → list[Split]` — declarative |
| `import_labels_set` | write | `(import_id, labels: list[str]) → list[str]` — declarative |
| `system_audit_list` | medium | `(filters, limit) → list[AuditEvent]` — supports `audit_id` filter for show-equivalent (returns single-element list with full payload) |
| (read tools) | — | Dropped — curation data is on `core.fct_transactions` LIST/STRUCT columns; LLM uses SQL via `moneybin://schema` |

**Why the read tools were cut:** after this spec ships, `notes`, `tags`, and `splits` are columns on `core.fct_transactions`. The LLM writes `SELECT notes, tags, splits FROM core.fct_transactions WHERE transaction_id = ?` via the schema catalog. Adding dedicated read tools would duplicate token surface without adding capability.

**Why declarative-set:** tools that take "the new state" instead of "add this / remove that" are easier for LLMs to use correctly and reduce the tool count by half. The CLI keeps imperative add/remove because humans think procedurally.

**Why notes stay imperative in MCP:** add/edit/delete on `note_id` have distinct semantics that don't collapse into "set all notes" — you can't replace one note with another by passing a list, because the audit chain depends on which note_id is which.

### `transactions_create` — bulk shape and constraints

- **Atomicity**: all-or-nothing per call. Validation failures reject the whole batch with per-item error reporting; no partial commit.
- **Single import_id**: all transactions in one call land under one `raw.import_log` row with `source_type='manual'`. Users can label that batch as a unit.
- **Pipeline runs once**: transform, match, and categorize execute over the batch together — far cheaper than N invocations. The matcher's `source_type='manual'` exemption applies per-row.
- **Bounds**: 1 ≤ N ≤ 100. The MCP 30s tool timeout (`mcp-tool-timeouts.md`) is the binding constraint; 100 is a comfortable headroom.
- **Response envelope**: `{batch_id: import_id, results: [{transaction_id, status, pipeline_summary, ...}]}` — one entry per input transaction in same order.

### Resources

| URI | Content | Sensitivity |
|---|---|---|
| `moneybin://recent-curation` | Last 50 audit events across any target. For "what did I touch this week?" ambient awareness. | medium |
| `moneybin://uncategorized-queue` (extended) | Existing planned resource. This spec adds `notes`, `tags` to the per-row payload so the LLM can see prior curator context when proposing categorizations. | medium |
| `moneybin://schema` (extended) | Existing resource. This spec registers new curation columns and tables: `core.fct_transactions.{notes, note_count, tags, tag_count, splits, split_count, has_splits}`, `core.fct_transaction_lines`, `app.transaction_notes`, `app.transaction_tags`, `app.transaction_splits`, `app.imports`, `app.audit_log`. Includes example queries demonstrating LIST/STRUCT use (`'tax:business' = ANY(tags)`, `UNNEST(notes)`, `note_count > 0` rather than `len(notes) > 0`). | low |

Per `feedback_mcp_resources_not_universal.md`, resources are enhancement-only. Critical reads have a tool path: `system_audit_list` is the tool equivalent of `moneybin://recent-curation`. The schema catalog has its `sql_schema` tool mirror per `mcp-sql-discoverability.md`.

### Prompts

| Prompt | Purpose |
|---|---|
| `prompts/curate_recent_transactions` | Walks the user through last-N-days transactions, surfacing untagged/unnoted rows and offering to add curator state. Calls `transactions list` → `transactions_tags_set` / `transactions_notes_add` in a loop. |
| `prompts/review_curation_history` | Summarizes recent audit events ("In the last week, you re-tagged 12 transactions, added 4 splits, and labeled the Q1 batch."). Read-only. Calls `system_audit_list`. |

Pure prompt content; no schema work. These two prompts wrap the curation-specific tools and remain in scope on their own merit (curator-segment ritual surfaces); the broader "monthly-ritual prompt set" idea was retired 2026-05-16 in favor of a tool-first `reports-anomaly-detection.md` (internal roadmap review).

### Privacy and sensitivity

- All write tools (`transactions_*` and `import_labels_set`) follow the existing write-tool confirmation convention per `mcp-architecture.md`.
- Read tools surfacing row-level data (`system_audit_list` returning before/after JSON) inherit medium-sensitivity behavior: aggregate-only response without `mcp-data-sharing` consent, full data with consent.
- Critical-tier fields (account numbers, full descriptions, amounts) embedded in audit `before_value` / `after_value` JSON follow the existing redaction rules — `SanitizedLogFormatter` handles log emission; the response itself returns full data when consent is granted, summary-only without.

## Audit Emission Contract

`AuditService.record_audit_event(action, target, before, after, *, actor, parent_audit_id=None, context=None)` is called from service methods, not from CLI/MCP layers. The surface knows the *actor* (`'cli'`, `'mcp'`); the service knows the *action* and target.

Mutating service methods write the audit event in the same DuckDB transaction as the mutation. Failure to record the audit aborts the mutation.

| Action | Emitted from |
|---|---|
| `manual.create`, `manual.delete` | `TransactionService.create_manual_batch`, `delete_manual` |
| `note.add`, `note.edit`, `note.delete` | `TransactionService.add_note`, `edit_note`, `delete_note` |
| `tag.add`, `tag.remove`, `tag.rename` (parent), `tag.rename_row` (child) | `TransactionService.add_tags`, `remove_tags`, `rename_tag` |
| `split.add`, `split.remove`, `split.clear` | `TransactionService.add_split`, `remove_split`, `clear_splits` |
| `import_label.add`, `import_label.remove` | `ImportService.add_labels`, `remove_labels` |
| `category.set`, `category.clear` | `CategorizationService` (extension; existing service emits) |
| `merchant.create`, `merchant.set` | merchant service (extension) |
| `rule.create`, `rule.update`, `rule.delete` | rule service (extension) |
| `ai.external_call` | AI provider boundary (replaces direct write to retired `app.ai_audit_log` table) |

In-scope services emit; out-of-scope services are silent until the post-launch `audit-log.md` spec retrofits coverage. The README and `INDEX.md` will not claim "complete audit log" until that lands.

### Before/after capture rules

- For row-level mutations: `before_value` and `after_value` are JSON snapshots of the relevant *column subset* of the affected row, not full table rows. This keeps the audit table from becoming a row-history copy of every table.
- For bulk operations (`tag.rename`): the parent event captures the operation intent (`{old_tag, new_tag, row_count}`); per-row child events capture each row's before/after with `parent_audit_id` chaining.
- For idempotent operations (re-applying an existing tag): the event is still recorded with `before == after`, marked via a `context_json.noop = true` flag. Useful for forensic "did this run?" questions.

## Cross-Spec Edits (Follow-up — Land with Implementation)

These edits to sibling specs are required follow-ups. They are **NOT** included in the spec PR — they land alongside the implementation PR for this spec, when the code-side commitments make them concrete:

1. **[`matching-same-record-dedup.md`](matching-same-record-dedup.md)** — paragraph addition under "Matching Engine § Candidate blocking" documenting the `source_type='manual'` exemption.
2. **[`categorization-auto-rules.md`](categorization-auto-rules.md)** — paragraph addition under the training-data section documenting the `source_type='manual'` exemption.
3. **[`privacy-and-ai-trust.md`](privacy-and-ai-trust.md)** — replace "Schema: `app.ai_audit_log`" section with a pointer to the unified `app.audit_log` and convention list for AI fields on `context_json`. The `get_ai_audit_log` consumer surface is preserved.
4. **[`mcp-architecture.md`](mcp-architecture.md)** — update Future Specs table entry for "Audit Log" to point at the unified `app.audit_log` and reference this spec's schema section.
5. **[`net-worth.md`](net-worth.md)** — append to "Out of Scope": transaction-level reconciliation (per-transaction cleared/reconciled markers) is not yet specced; when designed, it should subsume what `transaction-curation.md` deferred as the "verified" concept. Cross-link to this spec's §Out of Scope.

## Implementation Plan

### Files to Create

- `src/moneybin/sql/migrations/V006__transaction_curation.sql` — creates `raw.manual_transactions`, `app.transaction_tags`, `app.transaction_splits`, `app.imports`, `app.audit_log`; alters `app.transaction_notes` to multi-note shape; backfills existing notes with `note_id`/`author='legacy'`; drops planned `app.ai_audit_log` (if present) and re-routes any pre-existing rows into `app.audit_log` with `action='ai.external_call'` and `context_json` populated.
- `src/moneybin/sql/schema/raw_manual_transactions.sql`
- `src/moneybin/sql/schema/app_transaction_notes.sql` (replacement — new shape)
- `src/moneybin/sql/schema/app_transaction_tags.sql`
- `src/moneybin/sql/schema/app_transaction_splits.sql`
- `src/moneybin/sql/schema/app_imports.sql`
- `src/moneybin/sql/schema/app_audit_log.sql`
- `sqlmesh/models/prep/stg_manual__transactions.sql` — staging view feeding `int_transactions__unioned`
- `sqlmesh/models/core/fct_transaction_lines.sql` — split-expanded grain
- `src/moneybin/services/audit_service.py` — `AuditService.record_audit_event`, query methods
- `src/moneybin/cli/transactions.py` (new commands) — `transactions_create`, `transactions_notes_*`, `transactions_tags_*`, `transactions_splits_*`, `transactions_audit`
- `src/moneybin/cli/system.py` (new commands) — `system_audit_list`, `system_audit_show`
- `src/moneybin/cli/import_.py` (new commands) — `import_labels_add`, `import_labels_remove`, `import_labels_list`
- `src/moneybin/mcp/tools/curation.py` — the 9 new MCP write/read tools
- `src/moneybin/mcp/prompts/curate_recent_transactions.py`
- `src/moneybin/mcp/prompts/review_curation_history.py`
- `src/moneybin/mcp/resources/recent_curation.py`
- `tests/scenarios/personas/curator.yaml` — new synthetic persona that adds notes, tags, and splits to ~30% of generated transactions; ground-truth fields document expected curation state
- `tests/scenarios/scenario_manual_entry_dedup.yaml`
- `tests/scenarios/scenario_manual_entry_auto_rule_training.yaml`
- `tests/scenarios/scenario_audit_log_idempotency.yaml`
- `tests/scenarios/scenario_split_via_annotation.yaml`
- Unit tests under `tests/moneybin/test_services/test_audit_service.py`, `test_transaction_service.py` (extended)
- CLI tests under `tests/moneybin/test_cli/test_transactions_create.py`, `test_transactions_notes.py`, `test_transactions_tags.py`, `test_transactions_splits.py`, `test_system_audit.py`, `test_import_labels.py`
- E2E test `tests/e2e/test_e2e_transaction_curation.py`

### Files to Modify

- `src/moneybin/services/transaction_service.py` — extend with manual-entry, notes, tags, splits methods; emit audit events
- `src/moneybin/services/import_service.py` — extend with label management; emit audit events
- `src/moneybin/services/categorization_service.py` — emit audit events on category writes (extension only; not a redesign)
- `src/moneybin/services/auto_rule_service.py` — add `source_type != 'manual'` predicate to training query
- `src/moneybin/matching/engine.py` (or wherever candidate selection lives) — add `source_type != 'manual'` predicate to candidate blocking
- `src/moneybin/services/schema_catalog.py` — register new tables and columns; add example queries for LIST/STRUCT use
- `src/moneybin/tables.py` — add `TableRef` constants for the new app tables
- `src/moneybin/schema.py` — register new schema files
- `sqlmesh/models/core/fct_transactions.sql` — add the three CTE joins and seven new output columns
- `sqlmesh/models/prep/int_transactions__unioned.sql` — add manual staging branch
- `src/moneybin/config.py` — `MatchingSettings.source_priority` adds `manual` at priority 0
- `src/moneybin/mcp/tools/__init__.py` — register new tools
- `tests/e2e/test_e2e_mcp.py` — extend with new tool coverage
- `docs/specs/INDEX.md` — add this spec
- Cross-spec edits per §Cross-Spec Edits in This PR (5 specs)

### Key Decisions

| Decision | Resolution |
|---|---|
| Verified flag location | Dropped; deferred to future transaction-level reconciliation spec |
| Audit log architecture | Unified `app.audit_log`; retire `app.ai_audit_log` table |
| Manual storage location | `raw.manual_transactions` (mirrors tabular shape, runs through standard pipeline) |
| Curation presentation | DuckDB nested types (`LIST`, `LIST(STRUCT)`) on `core.fct_transactions` |
| NULL vs empty list | NULL when no curation data; consumers use `_count > 0` predicates |
| Splits view source | `core.fct_transaction_lines` reads from `core.fct_transactions` (not `app.*` directly) |
| Notes shape | Multi-note (extending existing single-note table) |
| Tag table shape | Flat M:N with slug-pattern VARCHAR (`namespace:value` optional) |
| Import labels shape | Single consolidated `app.imports` row with `LIST(VARCHAR)` labels column |
| MCP bulk vs single | Bulk `transactions_create` (1–100); CLI stays single-txn |
| MCP declarative-set | Tags, splits, import labels use `*_set`; notes stay imperative |
| Service organization | Extend existing `TransactionService` and `ImportService`; new cross-cutting `AuditService` |

## Synthetic Data Requirements

This spec ships a new "curator" persona (`tests/scenarios/personas/curator.yaml`) used by `testing-synthetic-data.md`'s generator. The persona produces:

- A baseline of imported transactions (CSV/OFX) over a 6-month window.
- Manual transactions inserted at ~10% of total volume (cash purchases, Venmo settlements, check entries).
- Notes added to ~15% of transactions, with realistic curator phrasing ("checked statement, this is the fence repair", "split between groceries and household").
- Tags added to ~25% of transactions, drawn from a fixture vocabulary (`tax:business-expense`, `tax:medical`, `vacation:hawaii-2026`, `recurring`, `review-later`).
- Splits added to ~5% of transactions (with ground-truth sums equal to parent amount).
- Import labels on a subset of import batches (`tax-year-2024`, `q1-reupload`).

Ground-truth fields in `synthetic.ground_truth` document expected curation state per transaction so the scenario tests can assert against deterministic expectations.

The curator persona is a deliverable of *this* spec, not a follow-up to `testing-synthetic-data.md`. The scenario tests in §Testing Strategy depend on it.

## Testing Strategy

Five-tier coverage per `.claude/rules/testing.md` and `testing-scenario-comprehensive.md`.

### Tier 1 — Unit (`tests/moneybin/test_services/`)

- `test_transaction_service.py` (extended) — `create_manual_batch` (bulk validation, atomic rejection, single import_id, pipeline trigger), notes (add/edit/delete with audit), tags (set semantics — diff computed correctly, rename with parent/child chaining, pattern validation), splits (sum-warning behavior, ord-based ordering, clear-all)
- `test_import_service.py` (extended) — `set_labels` (LIST mutation, idempotency, distinct-tag query)
- `test_audit_service.py` (new) — `record_audit_event` JSON shape, parent_audit_id chains, idempotent-noop marking, query helpers (`list_events` filters, `chain_for(audit_id)`)

### Tier 2 — CLI (`tests/moneybin/test_cli/`)

One file per command group. Cover argument parsing, `--output json` shape, `--yes` confirmation skipping, error messages on validation failure, `--profile` isolation. Specifically:

- `test_transactions_create.py` — single-txn shape, validates account_id exists, pipeline runs, transaction_id returned
- `test_transactions_notes.py` — add/list/edit/delete; max-length error
- `test_transactions_tags.py` — pattern validation, idempotent add, bulk rename echoes row count
- `test_transactions_splits.py` — add (warns on imbalance, doesn't block), list, remove, clear
- `test_transactions_audit.py` — entity-scoped query
- `test_system_audit.py` — list with filters, show single record
- `test_import_labels.py` — add/remove/list

### Tier 3 — E2E (`tests/e2e/test_e2e_transaction_curation.py`)

Subprocess-based golden paths:

1. Create profile → manual transaction → verify it appears in `transactions list` and `core.fct_transactions`.
2. Add notes → tags → splits to a transaction → verify all visible in `transactions show` and via SQL on `core.fct_transactions` LIST columns.
3. Tag rename across 10 transactions → verify all rows updated; audit log has 1 parent + 10 child events with chain.
4. Import a CSV → label the resulting batch via `import labels add` → verify label visible in `import labels list`.
5. Edit a category → verify audit event with before/after.
6. MCP bulk `transactions_create` with 5 items → verify single import_id, all 5 rows in `core.fct_transactions`, batch labelable.

### Tier 4 — Scenario (`tests/scenarios/`)

Four new scenario YAMLs:

- **`scenario_manual_entry_dedup.yaml`** — manual transaction created on same date/amount as a Plaid import row. Assertion: `core.fct_transactions` shows two distinct rows (no auto-merge); `app.match_decisions` has no candidate pair for them.
- **`scenario_manual_entry_auto_rule_training.yaml`** — manual transaction with user-supplied category. Assertion: auto-rule generator does NOT propose a rule based on it. Contrast scenario where same category edit on an imported row DOES generate a proposal.
- **`scenario_audit_log_idempotency.yaml`** — sequence of curation operations (add note, edit note, add tag, rename tag, add split). Assertion: re-running the exact sequence produces the same final state but doubles the audit log row count (audit captures every event, with idempotent re-applications marked `context_json.noop=true`).
- **`scenario_split_via_annotation.yaml`** — import known-amount transaction → add 3 splits → query `core.fct_transaction_lines` → assert 3 rows with correct amounts and categories. Also verify parent row in `core.fct_transactions` keeps full amount and `has_splits=true`.

All four scenarios use the new `curator` persona where applicable.

### Tier 5 — MCP integration (`tests/e2e/test_e2e_mcp.py`)

Existing test file extended to cover the 9 new MCP tools, 2 prompts, 1 new resource, and 2 extended resources: tool registration, response envelope shape, sensitivity-tier behavior (medium tools degrade without consent), `transactions_create`'s `pipeline_summary` in response, declarative-set diff correctness for `transactions_tags_set`.

## Dependencies

- DuckDB native types: `LIST`, `STRUCT`, `STRUCT_PACK`, `UNNEST` (existing capability)
- SQLMesh: existing model infrastructure; no new SQLMesh primitives needed
- Existing services: `TransactionService`, `ImportService`, `CategorizationService`, `AutoRuleService`, matching engine
- Existing infrastructure: `Database`, `TableRef`, `MoneyBinSettings`, `SanitizedLogFormatter`, `@mcp_tool` decorator + privacy middleware, `@tracked` / `track_duration`
- Cross-spec dependencies: `matching-same-record-dedup.md` (gold-key contract), `categorization-overview.md` (priority hierarchy contract), `smart-import-tabular.md` (raw column shape, `Database.ingest_dataframe`), `moneybin-cli.md` v2 (CLI/MCP taxonomy), `mcp-architecture.md` (sensitivity tiers, response envelopes), `moneybin-mcp.md` v2 (in-progress; this spec contributes the declarative-set pattern)

## Out of Scope

- **Verified curator flag.** Per-transaction trusted-data markers (cleared / reconciled / curator-attested) are dropped from this spec. The primary reason is brand-narrative: a per-row `verified` flag creates an implicit "unverified" category that the user must worry about, undermining MoneyBin's "integrity by construction" claim. Manually grooming each row to assert correctness is also a poor workflow at any meaningful volume. The trust-narrative work belongs on system-asserted invariants surfaced through `moneybin doctor` (M2C) — continuous integrity checks (FK, sign convention, balanced transfers, reconciliation deltas) producing a "✅ N invariants passing across M transactions" artifact. The secondary reason for dropping the flag is overlap with future transaction-level reconciliation: when *that* spec is designed, decisions about "does cleared count as trusted?" / "do auto-rule training and re-cat protection ride on cleared, reconciled, or a separate curator badge?" should be made together with a different vocabulary (`reconciled` / `matched_to_statement`) that doesn't collide with the integrity-by-construction framing. Cross-link from `net-worth.md` Out of Scope. **Re-confirmed 2026-05-16** (internal roadmap review): the doctor pivot is the correct trust artifact; a verified flag would create a soft-gating UX problem that doctor sidesteps. Do not reverse.
- **First-class splits.** `app.transaction_splits` and the `core.fct_transactions.splits LIST(STRUCT)` shape are designed to evolve into first-class splits without a schema fork. The evolution itself is its own spec — owned by whichever future work surfaces split-aware imports (e.g., Plaid line items) or split-aware budgeting.
- **Transaction attachments / receipts.** Deferred until a UI surface exists. Receipt blob storage outside the encrypted DuckDB file is its own design problem.
- **Category-id migration.** `app.transaction_splits.category` stays VARCHAR, matching how `app.transaction_categories` stores categories today. A future spec migrates both to `category_id` references when categories become first-class entities.
- **Tag normalization.** Current shape is single VARCHAR with `namespace:value` convention enforced at the service layer. Promotion to a normalized two-table model (`app.tags(tag_id, namespace, value)` + `app.transaction_tags(transaction_id, tag_id)`) is future work — triggered when tag rename/merge/autocomplete UX needs richer semantics.
- **Bulk manual entry CLI.** CLI stays single-txn-per-call. Bulk lives only in MCP, where LLMs batch naturally. The `import file` flow remains the path for tabular bulk loads.
- **Import history browser.** The bundle scope is *labeling* batches, not building a batch-history browser. Listing past imports is a real gap; `import status` (existing) covers health and SQL covers exotic queries. A future `import history` spec can land if the demand materializes.
- **Audit log policy.** Retention, redaction tiers, MCP exposure rules, retroactive emission from out-of-scope services — owned by post-launch `audit-log.md`. This spec ships the table; that spec ships the policy.
- **Multi-user identity for `created_by` / `applied_by` / `author`.** Currently `'cli'` or `'mcp'` (plus `'legacy'` for migrated notes). When a hosted multi-user surface lands, this column gets richer. Schema is forward-compatible.
- **Strict split balance enforcement.** The sum-of-children = parent.amount invariant is warn-not-block. Strict reconciliation lands with the future transaction-reconciliation spec.

### Follow-ups

- This spec establishes the curation storage/presentation pattern + CLI-imperative/MCP-declarative vocabulary contract for `architecture-shared-primitives.md` to lift. An MCP-vocabulary audit pass is added to `moneybin-mcp.md` v2's remaining work (candidates: `accounts_include`, `accounts_archive`, `categories_set (done — vocabulary sweep, 2026-05-17)`).
- Future `mcp-ux-standards.md` (in `moneybin-cli.md`'s "Future Specs to Add") lifts the declarative-set principle from this spec's §Architectural Pattern.
- Future `architecture-shared-primitives.md` formalizes the `app.*` schema layer in `AGENTS.md` and the LIST/STRUCT presentation pattern in `.claude/rules/database.md`.
