---
description: "Identifier generation AND propagation: content hashes, truncated UUIDs, source-provided IDs, semantic slugs; carrying the canonical ID through views, filters, and cross-table relationships"
paths: ["src/moneybin/**/*.py", "sqlmesh/models/**"]
---

# Identifiers

## Decision Tree

Use the first strategy that applies:

| Priority | Strategy | When | Example |
|---|---|---|---|
| 1 | **Source-provided ID** | Upstream system supplies a stable identifier | OFX `<FITID>`, Plaid `transaction_id` |
| 2 | **Content hash** | No source ID, but identity is determined by content | CSV transactions (no FITID) |
| 3 | **UUID4 (truncated)** | User-created entity with no natural key | Merchants, rules, budgets, user-created categories |
| 4 | **Semantic slug** | Human-authored reference data needing readable IDs | Seed data codes (`INC-SAL`), format names (`chase_credit`) |

## Content Hashes

For records whose identity *is* their content — reimporting the same file must produce the same IDs.

- **Algorithm**: SHA-256, truncated to 16 hex chars (64 bits). 64 bits gives ~1-in-a-billion collision probability at 100k records — sufficient for per-source transaction dedup, and short enough to be readable in logs and debugging. Use full SHA-256 if a single table could exceed ~1M records.
- **Prefix**: Source-specific prefix to prevent cross-source collisions (`csv_`, `pdf_`, etc.).
- **Input**: Pipe-delimited concatenation of the fields that define uniqueness, **plus an occurrence suffix on repeats** (below).

```python
content_key = f"{date}|{amount}|{description}|{account_id}"
# 2nd and later rows of identical content get a suffix; the first does not.
raw = content_key if occurrence == 0 else f"{content_key}|{occurrence}"
digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
return f"csv_{digest}"
```

### The occurrence suffix is mandatory — and never touches the first row

A content hash alone cannot represent *two genuinely distinct transactions with
identical content* — two $5.00 coffees at the same shop on the same day. Both
rows hash the same, and the staging `ROW_NUMBER()` dedup drops one. That is
silent financial data loss, and it is the single sharpest edge of strategy #2.

**Rule:** the 2nd and later rows of identical content append `|{occurrence}`,
where `occurrence` is the **0-based ordinal among rows with identical content**,
counted in file order. The **first occurrence keeps the bare content hash.**

Three properties, all load-bearing — a scheme that breaks any one of them
silently corrupts the ledger:

- **Never re-keys an existing row.** Suffixing the first occurrence too (`|0` on
  every row) would rotate the `transaction_id` of every row already imported.
  Old and new ids both survive the `(transaction_id, account_id)` dedup in
  staging, so `core.fct_transactions` **double-counts every pre-existing
  transaction** the next time its file is re-imported. This is why the first row
  stays bare — it is a correctness requirement, not a stylistic one.
- **Position-independent.** `occurrence` counts repeats of the same *content*,
  never the row's position in the file. Inserting an unrelated row above a
  transaction must not re-key it (`test_transaction_id_ignores_source_row_number`).
- **Stable when a duplicate appears later.** A re-export that gains a second
  identical row must not disturb the first: it keeps the bare hash and dedups
  normally, while the new twin arrives as `|1`. (A scheme that suffixes *all*
  members of a colliding group — `base|0`, `base|1` — violates this: the lone
  row's id would change the moment a twin showed up, orphaning it so two
  transactions import as three. Don't do that either.)
- **Order-invariant across a colliding group.** `occurrence` is assigned in file
  order, but that does not make the *result* order-dependent: N rows sharing a
  content key always produce exactly the id set `{base, base|1, … base|N-1}`,
  determined by N alone. A re-export that lists a new identical twin *above* the
  original therefore still yields both ids — it only swaps which physical row
  carries which, and rows identical in every hashed field are interchangeable by
  construction. Reordering can neither drop a transaction nor double one.

Both content-hash transaction-id sites implement exactly this — keep them
identical:

- `moneybin/extractors/tabular/transforms.py::_generate_transaction_ids` (CSV/Excel)
- `moneybin/services/import_service.py` (`_import_pdf_transactions`, `pdf_` ids)

**Not the same thing:** `moneybin/extractors/pdf/seed_store.py` keys seed rows by
`(alias, document, page, row_index, content)`. That is deliberate and different —
a seed row is a raw cell dump whose identity *is* its position in one specific
document, not a transaction identity. Don't "unify" it with the rule above.

## UUID4 (Truncated)

For app-layer entities created by the user or system with no natural key.

- **Length**: 12 hex chars (48 bits) — collision probability ~0.00002% at 10,000 items.
- **Format**: `uuid.uuid4().hex[:12]` (hex only, no hyphens).

```python
merchant_id = uuid.uuid4().hex[:12]
rule_id = uuid.uuid4().hex[:12]
```

Do not truncate shorter than 12 chars. If an entity could plausibly exceed 100,000 records, use the full UUID4 hex (32 chars).

## Source-Provided IDs

Always prefer the upstream system's identifier. Store as-is — do not hash, truncate, or transform. These are stable across re-imports by definition.

**Exception — collision disambiguation.** When the upstream system violates its own uniqueness guarantee (e.g. an OFX institution reusing one `<FITID>` for two distinct same-day transactions), storing both as-is silently drops one to the raw primary key / dedup window. In that narrow case the extractor appends a deterministic content-hash suffix (`<FITID>#<hash>`) to *every* colliding member so both survive; the suffix is a pure function of the row's content, so re-imports reproduce the same ids and dedup stays idempotent. This is disambiguation of a broken source id, not a switch to strategy #2 — non-colliding ids are still stored verbatim. See `moneybin/extractors/ofx/extractor.py::_disambiguate_colliding_fitids`.

## Semantic Slugs

For hand-crafted reference data where readability matters more than uniqueness guarantees. Use short, mnemonic codes (`INC-SAL`, `TRN-INT`) or descriptive names (`chase_credit`, `tiller`). Uniqueness is enforced by the database (PRIMARY KEY or UNIQUE constraint), not by the generation strategy.

## Propagation

Choosing the right ID strategy is half the job — the other half is keeping the ID intact across layers. Three guards close the boundaries where it leaks.

### Guard 1 — Carry the ID through views

When a `core.dim_*` entity has a stable ID, every view that exposes that entity MUST project the ID alongside any display column. Dropping the ID at a view boundary forces downstream consumers to bucket on text, which silently re-buckets when the display value changes.

Concretely: if a SQLMesh model joins to `core.dim_merchants` and selects `canonical_name`, it must also select `merchant_id`. Same for `dim_accounts`, `dim_categories`, and any future `dim_*`. The display column is for rendering; the ID is for joining and aggregating.

Worked example: `core.fct_transactions` joins `core.dim_merchants` to resolve `merchant_name` and projects both `merchant_id` and `merchant_name`. Downstream reports GROUP/PARTITION BY `merchant_id` with `merchant_name` carried as display — a rename in `app.user_merchants.canonical_name` no longer re-buckets historical aggregations.

### Guard 2 — Bind filters to the ID; resolve free-text at the boundary

User-facing filters (CLI flags, MCP parameters) that reference a canonical entity SHOULD accept free-text (`display_name` or `account_id`) for agent ergonomics — but the SQL `WHERE` clause MUST bind to the ID column. Resolve the free-text to the canonical ID at the service boundary; raise an explicit error on ambiguity instead of silently returning doubled or empty results.

A strict-resolver lives alongside fuzzy resolvers when both exist. The two precedents serve different contracts:

| Resolver shape | Use case | Returns on no match | Returns on ambiguity |
|---|---|---|---|
| Strict (filter contract) | `AccountService.resolve_strict` | Raises `AccountNotFoundError` with candidate list | Raises `AmbiguousAccountError` with id+name pairs |
| Permissive (dual-write writer) | `resolve_category_id` in `services/categorization/_shared.py` | `None` (caller handles orphan) | N/A — single result expected by design |

If a filter accepts free-text, ALSO update the MCP tool description and CLI `--help` text to document the resolution behavior — the agent never reads this rules file. The resolver errors should subclass `UserError` (per `src/moneybin/errors.py`) so CLI and MCP surface them cleanly without leaking stack traces.

### Guard 3 — FK across `app.*` tables; never text-key cross-table relationships

When an `app.*` table references another table's entity, the reference column MUST be the FK (`*_id`), not the text equivalent. The pattern: if a SQL statement uses `WHERE text_col = ?` (or worse, `WHERE LOWER(text_col) = LOWER(?)`) to identify a row that has a stable ID, the FK column is missing — add it, dual-write through a migration, then drop the text predicate.

Three smells that signal the bug:

1. `IS NOT DISTINCT FROM` predicates on a text column (papering over NULL-distinct gotchas from the SQL spec).
2. `LOWER(text_col) = LOWER(?)` (papering over case sensitivity in what should be an opaque-id comparison).
3. A `UNIQUE (text_col_a, text_col_b)` constraint that's load-bearing for a JOIN contract (because DuckDB/standard SQL treats NULL as distinct, so the contract leaks).

If you see any of those in a cross-table predicate, the column should be an FK. Open a follow-up to add it.

### Out of scope

These columns are intentionally text and NOT subject to the guards above:

- Source-system upstream text in `raw.*` / `prep.*` — inputs to the resolution pipeline, not references to canonical entities.
- Audit-log payloads in `app.audit_log` (`before_value`, `after_value`, `context_json`) — evidence, not references.
- Closed-vocabulary discriminators: `source_type`, `match_type`, `categorized_by`, `status` — routing tags, not entity references.
- Match-engine inputs: `merchant_pattern`, `description_pattern` — these ARE patterns (regex/substring/exact), not references to other entities.
- Semantic-slug PKs (strategy #4 above) — `app.tabular_formats.name` IS the PK by design; no shadow numeric ID exists to drop.

### Application

When designing a new schema column or surface parameter that names another entity, walk through:

1. Does the referenced entity have a canonical `*_id` somewhere?
2. If yes — is your column the `*_id`, or text? (Should be `*_id`.)
3. If the column must accept free-text input — is it a filter / surface parameter? (Then accept text, resolve to id at the service boundary, raise on ambiguity.)
4. If the cross-table predicate is text-keyed — is there a smell from the three above? (Then surface the FK.)

When in doubt, follow the pattern: additive FK column → dual-write window → text-predicate retirement.
