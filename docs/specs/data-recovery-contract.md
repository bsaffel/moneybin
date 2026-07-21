# Feature: Recoverable State Contract — Agent-Driven Recovery from Data Failures

## Status

in-progress

## Goal

Make every data failure in MoneyBin recoverable through MCP and CLI tools — never through SQL surgery. The system holds the contract; the agent reads structured `recovery_actions` from any failure and executes the named tool calls; nothing the system does is irreversible. This spec lands the cross-cutting infrastructure that closes the recovery gaps surfaced during the 2026-05-19 agent-experience review: error envelopes that don't tell the agent what to do, mutations with no undo, silent refresh crashes, an audit log without a consumer, and the matches domain only reachable from CLI.

The work has eight pieces, all interlocking:

1. A uniform `RecoveryAction` shape on every error and audit failure (push discovery).
2. `operation_id` grouping on `app.audit_log` so a tool call's mutations are one undoable unit.
3. An audit-log undo consumer — Phase 2 of Invariant 10 — exposed via
   `system_audit_undo`, `system_audit(view="history", ...)`, and
   `system_audit(view="detail", operation_id=...)`.
4. A doctor recipe registry: eligible invariant audits ship with a Python
   recipe producing `recovery_actions` from the failure's affected IDs.
5. A self-heal safelist run at refresh time — five active recipes, all reversible through the same audit-log undo, with five strict criteria that gate any future addition. A small "deferred" subsection captures known-shape future recipes that don't yet have a concrete trigger.
6. The matches MCP workflow (`refresh_run(steps=["match"])`,
   `reviews(kind="matches", status="pending")`,
   `reviews(kind="matches", status="history")`, and
   `reviews_decide(decisions=[{"kind":"match","decision_id":"<id>","decision":"accept"}])`)
   closing the CLI-only gap. Reject uses the same decision object with
   `"decision":"reject"`.
7. `RefreshResult` extensions surfacing matching and categorization crashes that today log at DEBUG and accumulate dupes silently.
8. A new always-loaded project rule (`.claude/rules/data-recovery.md`) codifying the contract for future tools, audits, and refresh stages.

The umbrella property is the **trust contract**: the system *recomputes* but never *decides*. Self-heal removes only records whose existence depends entirely on something the user already deleted; it never alters anything the user authored. Every action — including self-heal — is logged and reversible. If the system would have to choose between two reasonable outcomes, it surfaces the choice with structured `recovery_actions` instead of guessing.

## Background

### Current state

Recoverability today is partial and inconsistent across domains:

- **Imports** — `import_revert(import_id=...)` is a clean reverse, with cascade detection (`status="superseded"` if a newer import shadowed the batch). Per-file error isolation on multi-file imports works.
- **App-state mutations** — Invariant 10 Phase 1 (spec `app-integrity-invariant.md`, status `ready`) routes every mutation to `app.*` through a `*Repo` with full pre-image capture in `app.audit_log.before_value` and cascade threading via `parent_audit_id`. The undo *consumer* is deferred to Phase 2.
- **Pipeline audits** — `system_status(sections=["doctor"], detail="full")` runs three SQLMesh named audits (FK integrity, sign convention, transfer balance) plus a categorization-coverage check, returns pass/fail/warn per audit, optionally with affected IDs.
- **Error envelopes** — `UserError(message, code, hint, details)` carries machine-readable codes today, but the code taxonomy is undocumented and varies by domain. Success responses have an `actions: list[str]` array of navigational hints; error responses have nothing equivalent.
- **Matches domain** — CLI commands remain under `moneybin transactions matches`; MCP discovery and decisions use `reviews`, `reviews_decide`, and `refresh_run`.

### Gaps that motivate this spec

Surfaced during the 2026-05-19 brainstorm and prior agent-experience reports:

1. **Errors don't carry recovery actions.** A parse failure says "OFX parse failed at line 47" but doesn't tell the agent which tool fixes it. The agent has to know the recovery path from prior context.
2. **No general undo for `app.*` mutations.** Invariant 10 Phase 1 captures the data; Phase 2 builds the consumer. Without it, "I miscategorized 30 transactions, undo that" requires manual re-edits or reaching for `sql_query` — exactly the SQL surgery this spec rules out.
3. **Doctor failures don't point at recoveries.** "FK integrity failed on these 5 transaction IDs" leaves the agent guessing whether to revert an import, edit the account_id, or treat the orphan as intentional.
4. **Self-healing is absent.** When `import_revert` cascades and leaves orphan `app.transaction_categories` rows, those accumulate forever. No refresh-time cleanup.
5. **Refresh crashes silently.** Matcher and categorizer failures log at DEBUG (`followups.md:71`); the import looks healthy while dupes silently accumulate. No surface to detect partial-pipeline failure.
6. **Matching unreachable from MCP.** This pre-cutover gap is closed by
   `reviews(kind="matches", status="pending")` and
   `reviews_decide(decisions=[...])`.
7. **No "regret" surface.** Even with Phase 1 audit_log data, there's no agent-callable enumeration of recent mutations. The agent can't surface "you changed these 5 things in the last hour, want to undo any?" without raw SQL access.
8. **No project rule.** Each future spec re-invents how errors should hint at recovery, what self-heal is allowed, what counts as an audit-with-recipe vs untyped failure.

### Why this design

**Push + pull discovery.** Every failure carries structured `recovery_actions` (push), and `system_audit(view="history")` enumerates recent operations regardless of error (pull). Push covers the "something broke" case; pull covers the "I changed my mind" case. Together they make `sql_query` unnecessary for any recovery path.

**Audit-log-driven undo over per-domain inverse tools.** Invariant 10 Phase 1 already captures the data; Phase 2 is one consumer, not a dozen `un*` tools. The verb vocabulary in `.claude/rules/surface-design.md` doesn't have a `_undo` verb on purpose — reversibility lives in the audit log, not in paired tools. Explicit named tools exist only where the inverse is structurally a different operation (matches, splits — where the inverse is a state change, not a row restore).

**Safelist + report over aggressive auto-heal.** Refresh-time self-healing is gated on five strict criteria (derivable, idempotent, no information loss, auditable, reversible). Five active recipes pass all five; everything else surfaces as an audit failure with a recovery recipe. The system never destroys user-authored content automatically. The trust contract is concrete: **recompute, don't decide.**

**Block-don't-cascade undo.** `system_audit_undo` blocks when a later operation modified the same row and returns the blocker operations in `recovery_actions`. The agent walks the chain explicitly. Auto-cascade is exactly the magic that loses trust ("I undid one thing and it deleted my categorizations from last week"); blocking forces the agent to communicate the cascade to the user before acting.

**Uniform envelope across MCP and CLI.** CLI commands emit the same `recovery_actions` array in their JSON output. Per `feedback_cli_agent_surface.md`, CLI is a first-class agent surface — same JSON, same redaction, same audit. Human-readable CLI output renders the actions as a numbered list with `moneybin <cmd>` syntax.

### Related specs

- [`app-integrity-invariant.md`](app-integrity-invariant.md) — Phase 1 (audit_log pre-image capture, repository routing, lint rule, doctor invariants). **Prerequisite.** This spec implements Phase 2 (the undo consumer) and supersedes the Phase 2 description in that spec's [Out of Scope](app-integrity-invariant.md#out-of-scope) section.
- [`moneybin-doctor.md`](moneybin-doctor.md) — invariant audits surfaced by `system_status(sections=["doctor"], detail="full")`. This spec adds the recipe registry that yields `recovery_actions` for each audit.
- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — Invariant 10 ("`app.*` mutation routing"), to which this spec adds a sister Invariant 11 ("Recoverability of mutations").
- [`moneybin-mcp.md`](moneybin-mcp.md) / [`moneybin-cli.md`](moneybin-cli.md) — surface specs the new tools and CLI commands extend.
- [`matching-same-record-dedup.md`](matching-same-record-dedup.md) — owns `app.match_decisions`; the matches MCP surface in this spec wraps the existing matching service.
- [`smart-import-financial.md`](smart-import-financial.md) / [`smart-import-tabular.md`](smart-import-tabular.md) — import error sites this spec retrofits with `recovery_actions`.
- [`transaction-curation.md`](transaction-curation.md) — owns curation mutations (notes/tags/splits); error envelopes from these tools get retrofitted.

## Requirements

1. **`RecoveryAction` type.** A structured value with the following shape, used uniformly on every error and audit failure:

    ```python
    class RecoveryAction:
        tool: str  # MCP tool name, e.g. "system_audit_undo"
        arguments: dict[str, Any]  # prefilled args — agent can execute directly
        rationale: str  # short prose: WHY this fixes the failure
        confidence: Literal["certain", "suggested"]
        # certain = this will fix it
        # suggested = agent should weigh other context
        idempotent: bool  # safe to retry on transient failure?
    ```

    Lists are ordered: most-likely-correct first. Empty list = nothing actionable; the agent MUST escalate to the user — never silently treat as auto-recovered.

2. **Universal envelope extension.** `UserError` and the success-path response envelope both gain an optional `recovery_actions: list[RecoveryAction] | None` field. The existing `actions: list[str]` on success responses stays — that field is navigational ("what to do next"), not recovery ("how to fix what broke"). The two coexist with distinct semantics. CLI JSON output carries the same field.

3. **Error code taxonomy.** Prefix-grouped, stable, agent-branchable:

    | Prefix | Domain |
    |--------|--------|
    | `import_*` | Loading raw data |
    | `mutation_*` | App-state writes |
    | `audit_*` | Doctor / invariant failures |
    | `refresh_*` | Pipeline (matcher / categorizer / SQLMesh) |
    | `undo_*` | Audit-log undo |
    | `recovery_*` | Recovery tooling itself (e.g. `recovery_no_path`) |

    Every existing `UserError` code is audited and migrated to this taxonomy in the rollout PR sequence. The `code` field becomes load-bearing — agents may branch on it. CHANGELOG entry under `Changed` for any pre-existing code that changes shape.

    **Implementation note (PR 2):** the taxonomy module (`src/moneybin/error_codes.py`) also declares `infra_*`, `sync_*`, and `gsheet_*` prefixes to absorb existing non-recovery error codes (`infra_database_locked`, `infra_io_error`, `sync_error`, `gsheet_error`, etc.) without leaving them unprefixed. `sync_*` (mediated providers) and `gsheet_*` (user-controlled storage) are distinct connector domains per the `_connect`/`_link` verb split in `surface-design.md`. These prefixes are *not* part of the recovery contract — they exist purely for taxonomy completeness so `test_error_codes::test_every_code_uses_valid_prefix` can be enforced repo-wide. New recovery codes must use one of the six prefixes in the table above.

4. **`operation_id` schema addition.** `app.audit_log` gains three columns:

    | Column | Type | Purpose |
    |--------|------|---------|
    | `operation_id` | `TEXT NOT NULL` | `op_<uuid4_hex>` (32-char hex prefixed with `op_`); all audit rows from one MCP/CLI call share this value. Pre-spec rows use the synthetic backfill form `op_legacy_<audit_id>` so they're queryable but not grouped. |
    | `is_undo` | `BOOLEAN NOT NULL DEFAULT FALSE` | True for rows produced by `system_audit_undo` |
    | `undoes_operation_id` | `TEXT NULL` | If `is_undo=True`, points at the original operation |

    Plus indexes on `(operation_id)` and `(occurred_at DESC, operation_id)`. The service-layer mutation context manager (introduced in this spec) sets `operation_id` once at the start of a tool call; every audit row written during that call inherits it. Self-heal recipes use the same mechanism with `operation_id='op_self_heal_<recipe>_<uuid4_hex>'` and `actor='system:self_heal'`.

5. **Audit-log undo consumer.** The standard MCP audit contract plus its
   dedicated undo operation (with CLI parity):

    - **`system_audit_undo(operation_id=...)`** — push consumer. Reads all audit rows for the operation, computes per-row inverse (insert→delete, update→update-to-before_value, delete→insert), wraps in a transaction, writes new audit rows with `is_undo=True` and `undoes_operation_id`, returns summary with the new operation_id (so the undo itself is undoable). Errors:
        - `undo_operation_not_found` — bad operation_id. `recovery_actions` lists `system_audit(view="history")` to enumerate valid ids.
        - `undo_already_undone` — an undo already reversed this op. `recovery_actions` suggests undoing the undo, `confidence=suggested`.
        - `undo_cascade_blocked` — a later operation modified the same `(target_table, target_id)`. `recovery_actions` lists blocker operation_ids with `system_audit_undo` calls in reverse chronological order.
    - **`system_audit(view="history", limit=..., cursor=...)`** — pull surface.
      Returns recent operations grouped by `operation_id`. Each entry includes the
      tool, arguments, actor, timestamp, tables touched, row count, `can_undo`,
      `undo_blocked_by`, and a `recovery_actions` list (always one
      `system_audit_undo` call).
    - **`system_audit(view="detail", operation_id=...)`** — single-operation
      detail. It returns full `before_value` / `after_value` for every audit row
      in the operation, letting agents pre-check what an undo would change
      without executing. `audit_id` is the alternate detail selector for one
      event chain; it cannot be combined with `operation_id`.

6. **Block-don't-cascade undo.** `system_audit_undo` does NOT auto-cascade. When later operations touch the same rows, it returns `undo_cascade_blocked` with blockers in `recovery_actions`. The agent walks the chain explicitly. No cascading undo operation is admitted in Phase 1; any future capability requires bounded-registry admission.

7. **Doctor recipe registry.** Eligible invariant audits register a Python recipe that produces `recovery_actions` from `affected_ids` and context:

    ```python
    def recovery_recipe(
        affected_ids: list[str],
        context: AuditContext,  # DB handle, settings, current state
    ) -> list[RecoveryAction]: ...
    ```

    Layout: `src/moneybin/audits/recipes/<audit_name>.py` and `src/moneybin/audits/registry.py` (`{audit_name: recipe_fn}` lookup). DoctorService loads the recipe by audit_name when constructing each `AuditResult`. `AuditResult` gains `recovery_actions: list[RecoveryAction]` (per-audit carrier, not bundled at the doctor-response level).

    Registered recipes for existing audits:

    | Audit | Recipe output |
    |-------|---------------|
    | `categorization_coverage` (warn) | (1) `transactions_categorize_run(methods=["rules","merchants"])`. `confidence=certain` |
    | `dedup_reconciliation` (fail) | (1) `refresh_run()` to run the full cascade and rebuild `core.fct_transactions`; (2) `system_status(sections=["doctor"], detail="full")` to inspect the raw/core count delta. `confidence=suggested` |
    | `orphan_app_state` (new) | (1) `transactions_annotate(requests=[{"kind":"note_delete","note_id":...}, ...])` per orphan note and/or empty `tags_set` requests per orphan transaction. `confidence=certain` |

    The three SQLMesh audits `fct_transactions_fk_integrity`,
    `fct_transactions_sign_convention`, and `bridge_transfers_balanced` do not
    register recipes yet. Their affected IDs are transaction IDs, while the
    available recovery operations require account, import, or match-decision
    IDs; `transactions` has no exact transaction-ID selector. A future bridge
    retrofit that can supply a decision ID must inspect
    `reviews(kind="matches", status="history")` and reject the exact proposal
    with
    `reviews_decide(decisions=[{"kind":"match","decision_id":"<id>","decision":"reject"}])`.
    Until then, the audit returns no fabricated action.

8. **Self-heal safelist.** Five active recipes run at refresh time. Each MUST satisfy all five criteria:

    1. **Derivable** — corrected state fully computable from inputs the user controls.
    2. **Idempotent** — running twice = running once.
    3. **No information loss** — never destroys user-authored content.
    4. **Auditable** — writes one `app.audit_log` row per affected entity with `reason='self_heal:<recipe_id>'` and full pre-image.
    5. **Reversible** — undoable through `system_audit_undo` like any other operation.

    Failing criterion 3 or 5 is disqualifying. The safelist:

    | # | Recipe | Trigger | Action |
    |---|--------|---------|--------|
    | 1 | `orphan_categorizations_cleanup` | After `import_revert` or hard data deletion | DELETE `app.transaction_categories` rows whose `transaction_id` no longer resolves in `core.fct_transactions` |
    | 2 | `orphan_splits_cleanup` | Same | DELETE `app.transaction_splits` rows whose parent transaction is gone |
    | 3 | `derived_table_rebuild` | Every refresh (already happens) | Rebuild `core.*` and `reports.*` from raw + app |
    | 4 | `match_index_recompute` | Account aliases change, or after revert | Rebuild matching index from current account+txn state |
    | 5 | `rule_apply_to_uncategorized` | After `transactions_categorize_rules_set` | Apply new rule to transactions where `app.transaction_categories` has no row — never to manually-categorized rows |

    Adding a recipe after Phase 1 requires explicit reference to all five criteria in the PR description; no recipe lands without that justification. Orphan `app.transaction_notes` / `app.transaction_tags` rows after revert explicitly stay OFF the safelist (fail criterion 3) — they surface as `orphan_app_state` audit failures instead.

    **Deferred safelist recipes** — known-shape recipes whose trigger doesn't yet warrant a concrete implementation. Documented so future contributors recognize the shape and don't re-derive the analysis; added to the active safelist (with the five-criteria justification) only when a real driver appears.

    | Recipe | Why deferred | Trigger that would promote it |
    |---|---|---|
    | `account_displayname_reresolve` | Refresh derived display references in `core.dim_accounts` and `reports.*` is already covered by `derived_table_rebuild` (recipe #3) in Phase 1. No non-derived cache of account display names exists today. Promoting now would allocate audit/undo surface for a no-op. | A non-derived projection that caches account display names appears (e.g., a saved-view system, a report that materializes display strings outside SQLMesh). At that point: add the recipe to the active list with explicit five-criteria justification. |

9. **`RefreshResult` error surfacing.** The response from `refresh_run` gains:

    ```python
    class RefreshResult:
        # existing
        transforms_applied: bool
        transforms_count: int
        matched_count: int
        categorized_count: int
        # new
        matching_error: str | None
        categorization_error: str | None
        self_heal_actions: list[SelfHealRecord]


    class SelfHealRecord:
        recipe_id: str  # one of the 6 safelist recipes
        rows_affected: int
        operation_id: str  # for undo via system_audit_undo
        timestamp: str
    ```

    Behavior change: a *real* crash in the matcher/categorizer moves from `logger.debug(...)` to `logger.error(...)` and populates the `*_error` field. A missing-view precondition (`duckdb.CatalogException` / `BinderException` — e.g. first load before SQLMesh apply built the views) is NOT a crash: it stays a quiet `logger.debug(...)` and leaves `*_error` `None`, so a fresh database's first refresh never reports a false failure. (This precondition discrimination is what genuinely closes `followups.md:71`; a blind DEBUG→ERROR would trade silent failure for false-positive noise.) Refresh continues — one stage's failure doesn't abort the pipeline (same partial-failure-isolation pattern import already uses). If any `*_error` is set, the response envelope's `recovery_actions` includes:

    - `refresh_run(steps=["match"])` for a matching-only retry, or `refresh_run(steps=["categorize"])` for a categorization-only retry, `confidence=suggested`.
    - `system_status(sections=["doctor"], detail="full")` for diagnosis, `confidence=suggested`.

10. **Matches MCP workflow.** Four workflow operations over the pair-decision
    model (`app.match_decisions`, one row per proposed pair keyed by `match_id`)
    use three existing standard tools: `refresh_run`, `reviews`, and
    `reviews_decide`.

    | MCP tool | Shape | CLI equivalent |
    |----------|-------|----------------|
    | `refresh_run(steps=["match"])` | 3 (discrete-verb batch) | `moneybin transactions matches run` |
    | `reviews(kind="matches", status="pending")` | 5 (collection projection) | `moneybin transactions review --type matches` |
    | `reviews_decide(decisions=[...])` | 1b (accept/reject one decision) | `moneybin transactions matches set` |
    | `reviews(kind="matches", status="history")` | 5 (time-series) | `moneybin transactions matches history` |

    `refresh_run` and the history projection mirror the existing CLI.
    `reviews(kind="matches", status="pending")` lists pending match proposals
    (pair IDs + confidence, no amounts or descriptions).
    `reviews_decide(decisions=[...])` is the non-interactive decision surface.
    Each item is
    `{kind: "match", decision_id, decision}`, where `decision` is `"accept"`
    or `"reject"`. Rejecting an already-accepted match errors with a recovery
    action pointing at `system_audit_undo`. The CLI
    `moneybin transactions matches undo` calls the same audited recovery path.
    There is no
    `match_group_id`/`primary` write surface — `match_group_id` is a derived
    prep-layer column (the connected-component group key in
    `int_transactions__matched`), and dedup collapses each group by field-level
    source-priority merge (`int_transactions__merged`), so no single physical
    row is "primary."

    No dedicated match-undo MCP tool. `app.match_decisions` is protected by
    Invariant 10 → audit_log → `system_audit_undo`. The existing CLI
    `moneybin transactions matches undo` migrates to call `system_audit_undo`
    internally.

11. **Project rule.** A new always-loaded workflow rule lands at `.claude/rules/data-recovery.md`. It codifies:

    1. The trust contract (system recomputes, never decides; everything reversible; empty `recovery_actions` = escalate).
    2. The five safelist criteria for any new self-heal recipe.
    3. The six recovery paths — every failure must map to exactly one.
    4. The `RecoveryAction` shape and the `error_code` prefix taxonomy.
    5. When to add a doctor recipe vs leave as generic audit failure.
    6. When to use audit-log undo (any `app.*` mutation) vs explicit named tool.
    7. The hard rule: no `recovery_action` may name `sql_query` or any other DDL/write tool.
    8. The "many" convention — recovery citations use the canonical batch-capable tool, never a `_many` variant. Existing tools that only accept a single id must accept a list before being cited in a recipe.

12. **Existing-tool retrofit.** Every existing MCP tool's error path gains either a populated `recovery_actions` field or an explicit empty list with `error_code="recovery_no_path"` (forcing escalation rather than silence). Retrofit lands per-domain in small PRs after the envelope contract ships (PR 3 in the rollout). Domains: import, matching, categorize, accounts, balance, budget, transaction-curation, transform.

13. **`transactions_annotate` repairs orphan state through its natural
    shape.** Each orphan note is deleted by stable `note_id`; tags remain a
    closed collection cleared with empty `tags_set` when the transaction is
    absent from core. Unknown note IDs and empty tag requests with no existing
    state fail resolution. Multiple repairs share the existing atomic batch; no
    recovery-only tool or lossy `notes_clear` variant is added.

14. **Invariant 11 — Recoverability of mutations.** Appended to `architecture-shared-primitives.md` §Architecture Invariants:

    > **Invariant 11 — Recoverability of mutations.** Every mutation that reaches a user-observable surface (MCP, CLI, REST) MUST be recoverable through one of six paths: (a) an existing inverse tool, (b) `system_audit_undo` against the operation's `operation_id`, (c) `import_revert` plus re-import, (d) a self-heal recipe at refresh time, (e) a doctor recipe with structured `recovery_actions`, or (f) a domain-specific MCP tool for state changes whose inverse is structurally distinct (matches, splits). No recovery path may name `sql_query` or any DDL/write tool; reaching for SQL is an indication that a recovery tool is missing and must be added.

## Data Model

`app.audit_log` schema additions (Migration `V0NN_audit_log_operation_id.sql`):

```sql
-- Step 1: add the new columns. operation_id starts NULLABLE so the
-- backfill UPDATE in Step 2 can populate it. DuckDB (like standard SQL)
-- does not permit column references in DEFAULT clauses, so a
-- `DEFAULT 'op_legacy_' || audit_id` is invalid — the backfill must be
-- a separate UPDATE.
ALTER TABLE app.audit_log ADD COLUMN operation_id TEXT;
ALTER TABLE app.audit_log ADD COLUMN is_undo BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE app.audit_log ADD COLUMN undoes_operation_id TEXT NULL;

-- Step 2: backfill existing rows with a deterministic synthetic id so
-- each pre-spec row is independently undoable but not grouped.
UPDATE app.audit_log
   SET operation_id = 'op_legacy_' || audit_id
 WHERE operation_id IS NULL;

-- Step 3: enforce NOT NULL going forward. From here on, the
-- service-layer MutationContext owns assignment at the AuditService
-- write boundary; new rows always have a value.
ALTER TABLE app.audit_log ALTER COLUMN operation_id SET NOT NULL;

-- Step 4: indexes for undo lookups.
CREATE INDEX idx_audit_log_operation_id ON app.audit_log (operation_id);
CREATE INDEX idx_audit_log_occurred_at_op ON app.audit_log (occurred_at DESC, operation_id);
```

Existing rows get a synthetic `operation_id` (`op_legacy_<audit_id>`) so each pre-spec row is independently undoable but not grouped. The column carries no SQL-level DEFAULT after the migration; new rows are populated explicitly by the service-layer MutationContext at the AuditService write boundary.

No other schema changes. The pre-image in `before_value` (full row, per Phase 1 Req 4) is what the undo consumer restores.

## Architectural Pattern

```mermaid
flowchart TB
    subgraph Surfaces[Failure Surfaces]
        Err["MCP/CLI tool error"]
        Aud["doctor-section audit failure"]
        Ref["refresh_run partial crash"]
    end

    subgraph Envelope[Universal Envelope]
        RA["recovery_actions: list[RecoveryAction]"]
    end

    subgraph Discovery[Discovery Mechanisms]
        Push["Push: structured actions on failures"]
        Pull["Pull: system_audit(view=history)"]
    end

    subgraph Mechanisms[Recovery Mechanisms]
        ExTool["Existing target-state tool<br/>(taxonomy_set, accounts_set, ...)"]
        Undo["system_audit_undo(operation_id=...)"]
        Revert["import_revert + re-import"]
        Heal["Self-heal at refresh<br/>(5-recipe safelist)"]
        DomTool["Matches MCP / curation tools"]
    end

    subgraph DataPrim[Data Primitive]
        OpId["app.audit_log.operation_id<br/>(grouping)"]
        Pre["before_value (full pre-image)<br/>from Invariant 10 Phase 1"]
    end

    Err --> RA
    Aud --> RA
    Ref --> RA

    RA --> Push
    Pull -.- RA

    Push --> ExTool
    Push --> Undo
    Push --> Revert
    Push --> DomTool
    Heal --> OpId

    Undo --> OpId
    Undo --> Pre
    Pull --> OpId

    classDef prim fill:#e8f4f8,stroke:#1f7a8c;
    classDef rec fill:#fef3e2,stroke:#cd6c00;
    class OpId,Pre prim;
    class ExTool,Undo,Revert,Heal,DomTool rec;
```

Every failure on the left funnels into the universal envelope; the agent reads `recovery_actions` and dispatches into one of the six recovery paths in Invariant 11. The right side of the diagram shows the five *executing* mechanisms; the sixth path — doctor recipes (Invariant 11 path (e)) — produces `recovery_actions` whose `tool` fields name one of those same five mechanisms, so it routes through the Push discovery layer rather than executing recovery itself. All mechanisms that touch `app.*` land in the audit log via Invariant 10 Phase 1's pre-image capture, so every recovery is itself recoverable.

## Implementation Plan

Phase 2 of Invariant 10 + sister contract work + matches MCP + refresh surfacing + project rule. Lands as a sequence of small reviewable PRs. PR 1 assumes Phase 1 (`app-integrity-invariant.md`) has shipped — Phase 1 is the hard prerequisite.

### PR 1 — `operation_id` schema + service-layer context manager

- Migration `V0NN_audit_log_operation_id.sql` (per Data Model).
- `src/moneybin/services/mutation_context.py` — context manager that mints an `operation_id` (`op_<uuid4_hex>`) at the start of every MCP/CLI tool call and threads it through the repository write path. Repositories pass `operation_id` to `AuditService.record_audit_event()`. The decorator that wraps MCP tool entry points and the CLI command framework both push the operation_id onto a contextvars frame; repositories read it from there.
- `AuditService.record_audit_event()` accepts `operation_id` (required after PR 1) and writes it.
- Backfill test: existing pre-spec audit rows get the legacy synthetic id; new rows from any tool share the contextvars value.
- No behavior changes visible at the MCP/CLI surface yet.

#### PR 1 — as implemented (REC-PR1, deviations from the plan above)

Shipped as `feat/audit-operation-id`. Status stays `in-progress` (PRs 2–N
remain). Four deliberate deviations from the bullets above, each verified
against the code:

1. **Column split.** REC-PR1 adds **only** `operation_id`. The two undo-marker
   columns (`is_undo`, `undoes_operation_id`) in §4.4 are written solely by the
   undo consumer, are additive (not one-way), and land with **PR 3**. Migration
   is `V023__add_operation_id_to_audit_log.py`.
2. **Contextvar-read, not a parameter.** `AuditService.record_audit_event()`
   reads the current id from `mutation_context.current_operation_id()` directly
   — it gains **no** `operation_id` parameter, and repositories are **not**
   touched (every `BaseRepo._emit_audit` call inherits the context). A parameter
   layered on top of a contextvar would be two ways to express one value; the
   service signature is an internal two-way door, so Simplicity First applies.
   The getter mints a fresh `op_<uuid4_hex>` when no context is active, so a bare
   repo write outside any tool call is still its own valid NOT-NULL operation.
3. **`SET NOT NULL` drops/restores indexes.** DuckDB (1.5) refuses
   `ALTER COLUMN … SET NOT NULL` while any non-constraint index exists on the
   table (`DependencyException`), and `audit_log` ships five base indexes — so
   the spec's literal 4-step SQL would fail on every existing database. The
   migration snapshots the table's explicit indexes from `duckdb_indexes()` (the
   PRIMARY KEY is excluded and does not block the ALTER), drops them, tightens,
   then restores them from their stored DDL before adding the two new indexes.
4. **Column placed last; indexes deferred to the migration.**
   `operation_id` is the final column in `app_audit_log.sql`'s `CREATE TABLE`
   (matching `ADD COLUMN`'s append position, so column order is identical
   fresh-vs-upgraded). The two new indexes live **only** in V023, not the schema
   file — `init_schemas` runs before migrations, so an index DDL in the schema
   file would bind against the pre-V023 table shape on existing DBs (same
   pattern as `app_proposed_rules.sql` ↔ V016).

`AuditEvent` (and `to_dict`, `list_events`, `chain_for`) carry `operation_id`
so the row view stays faithful; no undo logic, `UndoService`, or `system_audit_*`
tools ship here — those are PR 3.

### PR 2 — `RecoveryAction` type + error_code taxonomy + envelope plumbing

- Define `RecoveryAction` (Pydantic model) and the `ErrorEnvelope` shape with the new optional field.
- `UserError` gains `recovery_actions: list[RecoveryAction] | None = None`. `build_error_envelope()` wires it through.
- Add the error_code prefix taxonomy as a documented enum/constants module (`src/moneybin/errors/codes.py`).
- Audit existing `UserError(...)` raises in-tree and migrate any codes that don't fit the taxonomy. CHANGELOG entry under `Changed`.
- AuditResult (from `system_status(sections=["doctor"], detail="full")`) gains the same field.
- No tool yet populates `recovery_actions`; that's PRs 5 and 9a-N.

### PR 3 — Audit-log undo consumer (`system_audit_undo` + `_history` + `_get`)

- `src/moneybin/services/undo_service.py` — `UndoService.undo(operation_id)`, `.history(...)`, `.get(operation_id)`.
- MCP tools: `system_audit_undo`, `system_audit(view="history", ...)`, and
  `system_audit(view="detail", operation_id=...)` in
  `src/moneybin/mcp/tools/system.py`.
- CLI parity: `moneybin system audit undo`, `moneybin system audit history`, `moneybin system audit get`.
- Cascade detection: query for any later audit row in `(target_table, target_id, operation_id != self)`. If found, return `undo_cascade_blocked` with the blockers in `recovery_actions` (newest first, since blockers must undo in reverse order).
- Undo emission: each undo writes a new audit row per affected entity with `is_undo=TRUE`, `undoes_operation_id=<original>`, and a fresh `operation_id` of its own. The returned summary includes that new operation_id so the undo itself is queryable and undoable.
- Tests: round-trip per Invariant 10 protected table; cascade-blocked scenario; double-undo (`undo_already_undone`); `undo_operation_not_found`.

#### PR 3 — as implemented (deviations from the design above)

Shipped on `feat/data-recovery-undo`. Status stays `in-progress` (PR 4+ remain).
Deviations from the design as written, with rationale:

- **notes/tags/splits repo-ified first.** The inverse is synthesized generically
  from each audit row's full before/after image, which requires every audited
  `app.*` table to have a repo. `transaction_notes`, `transaction_tags`, and
  `transaction_splits` previously mutated via raw SQL in `TransactionService`;
  they now flow through `TransactionNotesRepo` / `TransactionTagsRepo` /
  `TransactionSplitsRepo`. Audit shape changes that fell out of this: full-row
  capture (not partial diffs); no `noop` audit rows for idempotent tag re-adds;
  `split.clear` emits one `split.remove` per row so each split is individually
  reversible.
- **One generic reverser, no per-repo overrides.** `BaseRepo.undo_event` keys its
  WHERE clause on `pk_columns` and binds JSON/array columns natively (DuckDB
  accepts `dict`/`list`), so `match_decisions` (JSON `match_signals`) needs no
  override. The earlier sketch of a `MatchDecisionsRepo.undo_event` delegating to
  the domain `reverse()` was dropped — it would mis-handle undo-of-insert and
  undo-of-status-change and re-trigger the double-reverse timestamp bug; the
  generic row-restore is strictly more correct.
- **Cascade excludes currently-reversed work (net liveness).** A later operation
  blocks only if it is a *live forward* mutation: undo rows (`is_undo=TRUE`) never
  block, and a forward op blocks only while its effect is *currently* live.
  "Currently" is net parity over the undo chain — an op that was undone and then
  had that undo itself reversed (a round-trip) is live again and blocks once more;
  likewise an op stays undoable after such a round-trip. Correctness is the
  chain's net liveness, not whether an undo was *ever* recorded — a one-shot
  "ever undone" check both traps the user out of re-undoing a round-tripped op and
  lets undo silently clobber a round-tripped blocker. Without this, the documented
  walk (undo the blocker, then the original) could never resolve. The blocker join
  keys on `(target_schema, target_table, target_id)` (matching the row-target
  query), so a same-named table in another schema is never a false-positive
  blocker.
- **Row-grain cascade: `target_id` is the mutated row's PK, not its parent.** The
  notes/tags/splits repos emit `target_id` = the entity's own key (`note_id`,
  `split_id`, `transaction_id:tag`), not the parent `transaction_id`. Cascade
  blocking is therefore scoped to the specific row: two independent annotations on
  the same transaction (e.g. two tags, or a note plus a split) no longer
  false-positive-block each other's undo — only a later mutation of the *same* row
  blocks. (Consequence: filtering the audit log by `target_id = <transaction_id>`
  returns only transaction-level mutations, not its child notes/tags/splits, which
  now carry their own row ids.) An undo's own audit row keys on the row the inverse
  *leaves behind* (`BaseRepo._row_target_id` of the restored/affected row), not the
  reversed event's `target_id` — so a PK-changing undo (tag-rename restore lands on
  the old key) is cascade-scoped to the actually-present row, not the pre-undo key.
- **Read-only audit reads degrade on a pre-V024 schema.** `get_database(read_only=True)`
  skips migrations, so a V023 `audit_log` lacks `is_undo`/`undoes_operation_id`.
  `AuditService` probes for the columns once and substitutes
  `FALSE AS is_undo, NULL AS undoes_operation_id` when absent, so the existing
  read tools (`system audit list/show`, `transactions audit`) keep working on an
  upgraded-but-not-yet-write-opened profile instead of erroring on a missing column.
- **Deterministic, reversible replay order + partial-capture guard.** Rows replay
  in the exact reverse of write order (`events_for_operation` tiebreaks on the
  monotonic `rowid`, never the random `audit_id`), so a future parent-then-child
  insert undoes child-first. An audit row predating full-row capture (Req 4) is
  refused with `recovery_no_path` rather than mis-applying: the re-INSERT path
  rejects a `before` missing NOT NULL columns, and the UPDATE-restore path rejects
  a `before` less complete than `after` (which would silently restore only the
  captured columns) — covering legacy partial `note.delete` / `note.edit` rows.
- **Operations with nothing to reverse are refused.** Two filters guard the same
  "no phantom undo id" invariant: an operation whose audit rows are *all* markers
  (`target_id IS NULL`, e.g. a `tag.rename` matching zero transactions, whose
  parent marker is emitted unconditionally), and an operation whose every row is a
  net no-op (`before == after`, e.g. a legacy idempotent `tag.add`). Either way
  `undo` raises `recovery_no_path` rather than minting an `undo_operation_id` with
  no audit rows (which wouldn't be queryable or undoable). For both cases
  `_undoability` reports `can_undo=False` (it gates on "has at least one row with
  `target_id` set and `before_value` distinct from `after_value`"), so
  `system_audit(view="detail", ...)` / `system_audit(view="history")` agree with `undo`'s refusal instead of
  advertising an undo that would immediately fail.
- **`recovery_no_path` for raw-targeted operations.** An operation that touched a
  table outside the undoable `app.*` surface (e.g. `manual.create` →
  `raw.manual_transactions`) is refused with `recovery_no_path` rather than
  crashing; the recovery is re-import, per the "Undoing `import_revert`" deferred
  note below.
- **`history` summarizes by action verb.** The operation context records
  `operation_id` and `actor` but not the originating tool name/arguments, so
  history entries carry `actions[]` (distinct verbs) rather than the spec's
  `tool`/`arguments` fields, plus structured `can_undo` / `undo_blocked_by` and
  the pre-built `recovery_actions[]` (the `system_audit_undo` call for the entry's
  state — the same structured shape the error envelope carries). The current
  history projection accepts only `view`, `limit`, and `cursor`.
- **`transactions matches undo` not migrated.** Re-pointing that command at
  `system_audit_undo` stays PR 5 work (it keys on `match_id`, not `operation_id`).
- **Service-level integration coverage** lives in `test_undo_service.py` (real DB,
  real repos, no mocks) plus the surface E2E in `test_e2e_transaction_curation.py`,
  rather than separate `tests/integration/test_audit_undo*.py` files.

### PR 4 — Doctor recipe registry + recipes for existing audits

- `src/moneybin/audits/recipes/__init__.py`, `registry.py`, plus one Python module per existing audit (per Req 7).
- `DoctorService` constructs each `AuditResult` with `recovery_actions` populated from `registry.get(audit_name)(affected_ids, context)`.
- New audit + recipe: `orphan_app_state` — scans
  `app.transaction_notes` and `app.transaction_tags` for orphan transaction
  IDs absent from `fct_transactions`. The recipe produces one targeted
  `note_delete` per orphan note and one clear-to-empty tag request per orphan
  transaction.
- Tests per-recipe: seed failing state → audit fires → recipe yields expected actions → tool names + arguments are valid (round-trip-executable).

#### PR 4 — as implemented (deviations from the design above)

Shipped on `feat/data-recovery-doctor-recipes`. Status stays `in-progress` (PR 5+ remain). Deviations from the design as written, with rationale:

- **It's `InvariantResult`, not `AuditResult`.** The doctor's per-check dataclass in `src/moneybin/services/doctor_service.py` is named `InvariantResult`; the spec's `AuditResult` was reconciled to the real name rather than introducing a parallel type. `InvariantResult` already carried `recovery_actions: list[RecoveryAction] | None = None` (REC-PR2); PR 4 populates it.
- **Single wiring seam.** Rather than threading the registry into every `_run_*` method (~17 of them), `DoctorService.run_all` walks each computed `InvariantResult` through one `_apply_recipe` step that looks up the recipe and materializes a new `InvariantResult` with `recovery_actions` set. Pass/skipped invariants short-circuit (nothing to fix). Per-`_run_*` methods stay unaware of the registry.
- **`RecoveryAction` field names match the REC-PR2 contract: `rationale`
  (not `description`) plus `idempotent: bool`.** The spec text used
  `description`; recipes emit the real field name. Declarative note/tag clears,
  categorize-run, refresh-run, and system-doctor are idempotent.
- **Prefixed `affected_ids` for `orphan_app_state`.** The audit emits
  `note:<note_id>` and `tag:<transaction_id>` so the recipe can dispatch by
  entity type without re-querying the DB. Every orphan note retains its own
  entry; multiple tag rows on the same orphan transaction collapse to one. The
  recipe uses targeted `note_delete` and empty `tags_set` requests through
  `transactions_annotate`. Unknown prefixes and empty-id edge cases log a
  warning rather than silently producing zero actions.
- **Orphan note/tag actions are `confidence="certain"`.** Tag clears are
  idempotent. Note deletion is intentionally marked non-idempotent because a
  second call finds no note; the containing MCP umbrella also honestly carries
  `idempotentHint=false`.
- **`dedup_reconciliation` emits `refresh_run()` (full cascade), not `steps=["match"]`.** The spec wording was incomplete: re-running match writes `app.match_decisions` and `prep.*` views but does NOT rebuild `core.fct_transactions`, so the audit's symptom (raw-vs-core count drift) persists across a match-only refresh. The full default cascade — `gsheet → match → transform → categorize → identity` — is what actually addresses the symptom. Recipe rationale text records the why.
- **Recipe scope: 3 recipes (orphan_app_state + categorization_coverage + dedup_reconciliation).** PR 4 ships recipes only for audits where the action is genuinely executable AND adds information the agent doesn't already have. The three SQLMesh audits (`fct_transactions_fk_integrity`, `fct_transactions_sign_convention`, `bridge_transfers_balanced`) deliberately ship WITHOUT recipes: the only executable action available pre-PR9 would be `system_status(sections=["doctor"], detail="full")`, but its full detail only changes behavior for `_run_app_audit_coverage` (sampled-vs-full), not SQLMesh audits — re-running surfaces the identical failure. Emitting it would be circular noise. A per-domain retrofit must augment each audit with the account, source-import, or match-decision ID its recovery operation requires. For bridge transfers, that means `reviews(kind="matches", status="history")` followed by `reviews_decide(decisions=[{"kind":"match","decision_id":"<id>","decision":"reject"}])`. Today's affected IDs are transaction IDs, so no recipe is emitted yet.
- **Audits without a registered recipe leave `recovery_actions=None`.** Per the prompt's "don't fabricate certain recipes" guidance: the 3 SQLMesh audits noted above, the per-table `app_audit_coverage_*` checks (13 of them), the FK-style app audits, and the `app_user_categories_uniqueness` / `app_user_merchants_orphans` warnings have no registered recipe yet — their cleanest recovery requires the per-domain retrofit landing in PR 9. They stay surfaced as failures with `recovery_actions=None` (rather than spurious "investigate manually" hints) so an agent can route them to the operator rather than burning tokens on a useless action.
- **`InvariantResultPayload` gained `recovery_actions: list[RecoveryActionPayload]`** (mirrors the `SystemAuditHistoryEntryPayload` precedent — required list, possibly empty). The MCP doctor section of `system_status` and the CLI `system doctor` text + JSON renderers carry the new field. Privacy classification: `RecoveryActionPayload` is already Tier.LOW (RECORD_ID + DESCRIPTION + AGGREGATE + TXN_TYPE), so `InvariantResultPayload`'s derived tier is unchanged (still Tier.MEDIUM via `detail` = DESCRIPTION).
- **CLI text format: one `💡 [confidence] tool(arguments) — rationale` line per action,** rendered indented under each failing/warning invariant. JSON format: a `recovery_actions` array per invariant carrying the full executable shape (tool, arguments, rationale, confidence, idempotent) so scripted / agent consumers can dispatch directly.
- **`orphan_app_state` audit suppresses pending manual transactions** (migration V026). `transactions_create` returns the predicted gold-key `transaction_id` to the caller immediately, before the next `refresh_run` materializes the row into `core.fct_transactions`. Notes/tags written against that id in the window between create and refresh are legitimate state, not orphans — without suppression the recipe would prescribe clearing them and destroy valid user curation. V026 adds a `transaction_id` column to `raw.manual_transactions` (populated at INSERT in `create_manual_batch`, backfilled from existing `(source_transaction_id, account_id)` pairs via the same SHA256 hash `_predict_manual_gold_key` computes). The audit's `NOT EXISTS(core.fct_transactions)` arm pairs with `AND NOT EXISTS(raw.manual_transactions WHERE transaction_id = ...)` to skip those rows. **Known limitation:** the suppression is broader than ideal — a manual that joins a dedup group during refresh has its predicted id replaced in core by the group's canonical id, but the raw row keeps the predicted id forever, so notes/tags written against the original predicted id stay suppressed even after they become genuinely orphaned. Closing this needs a real materialization signal (the obvious one, `prep.int_transactions__matched`, is a live VIEW that reflects raw rows immediately and can't discriminate pending-vs-processed); deferred to PR9. The trade-off is accepted because (a) the deduped-away case is rare in practice and (b) the primary protection — against destroying notes on freshly-created manuals — is the data-loss path that actually mattered. Surfaced by reviewers on PR #231 across three rounds.

### Matching workflow

The matching workflow is implemented through the three standard tools described
above. The CLI keeps its existing commands and reaches the same service outcomes;
`moneybin transactions matches undo` uses `system_audit_undo`. Surface and
cross-surface tests assert equivalent JSON outcomes.

### PR 6 — `RefreshResult` error surfacing

- Extend `RefreshResult` per Req 9. Update `refresh_run` to populate the new fields.
- Move matcher/categorizer crash logging from DEBUG to ERROR; populate `*_error` fields.
- Update `refresh_run`'s response envelope to include `recovery_actions` when `*_error` is non-None.
- Tests: simulate matcher crash → `RefreshResult.matching_error` populated → envelope `recovery_actions` includes `refresh_run(steps=["match"])` and `system_status(sections=["doctor"], detail="full")`.

### PR 7 — Self-heal safelist recipes

- `src/moneybin/services/self_heal/` — one module per recipe in the active safelist (recipes 1, 2, 4, 5; recipe 3 — derived table rebuild — already exists in refresh and just gains audit_log emission). Deferred recipes (e.g., `account_displayname_reresolve`) are not implemented in Phase 1; the spec's deferred subsection documents their shape for future promotion.
- `refresh_run` invokes the safelist after `derived_table_rebuild`. Each recipe writes per-entity audit rows with `actor='system:self_heal'` and `operation_id='op_self_heal_<recipe_id>_<uuid4_hex>'`. The triggered operation_ids accumulate into `RefreshResult.self_heal_actions`.
- Tests per recipe: seed drift → refresh runs → drift gone → audit rows present → `system_audit_undo(operation_id=...)` reverses the heal.
- Cross-cutting test: chain of revert → refresh → self-heal → audit_history shows the self-heal as undoable; user can `system_audit_undo` it to restore the orphan.

### PR 8 — coarse orphan annotation cleanup

- Route orphan note cleanup through the stable-ID `note_delete` lifecycle
  variant and orphan tag cleanup through declarative empty `tags_set`.
- Update `orphan_app_state` to emit schema-valid
  `transactions_annotate` requests keyed by `note_id` for notes and
  `transaction_id` for tags.

### PRs 9a-N — Per-domain retrofit of `recovery_actions` on existing tools

One small PR per domain. Each retrofits the tool's `UserError` raises with `recovery_actions`. Order suggested by failure frequency from agent-experience reports:

- 9a — Import (`import_files`, `import_preview`, `import_revert`).
- 9b — Categorize (`transactions_categorize_run`,
  `transactions_categorize_rules_set`, `transactions_categorize_commit`).
- 9c — Accounts (`accounts_set`, `accounts_balance_assert` with its `state` selector).
- 9d — Curation (`transactions_annotate` requests for notes, tags, and splits).
- 9e — Budget recovery remains unnamed until it passes bounded-registry
  admission.
- 9f — Transform (`refresh_run(steps=["transform"])` for execution and
  `system_status(sections=["doctor"], detail="full")` for validation).

Each PR includes a property test asserting that every error path in the domain either populates `recovery_actions` or explicitly raises with `error_code="recovery_no_path"`.

### PR 10 — Project rule + Invariant 11 + roadmap + CHANGELOG

- `.claude/rules/data-recovery.md` per Req 11.
- Append Invariant 11 (Req 14) to `architecture-shared-primitives.md`.
- Update `docs/roadmap.md` — close M1L row with ✅ shipped.
- CHANGELOG entry under M1L dated section: added recoverable-state contract, audit-log undo, doctor recipes, matches MCP, refresh error surfacing.
- New guide: `docs/guides/agent-recovery.md` — how agents discover and execute recovery (audience: agent integrators / power users).
- Update `docs/specs/app-integrity-invariant.md` — mark Phase 2 as superseded by this spec; cross-link.
- Update `docs/specs/INDEX.md` — promote this spec to `implemented`.

## Test Coverage

Per `.claude/rules/testing.md` test layers.

| Layer | Test file(s) | Verifies |
|-------|--------------|----------|
| Unit (envelope) | `tests/moneybin/test_errors/test_envelope.py` | `RecoveryAction` validates; error-code constants stable; `UserError` round-trips through `build_error_envelope` |
| Unit (per recipe) | `tests/moneybin/test_audits/test_recipes/test_<audit>.py` | Recipe yields expected `RecoveryAction` list for seed inputs; tool names + arguments are agent-executable |
| Unit (self-heal) | `tests/moneybin/test_self_heal/test_<recipe>.py` | Each safelist recipe: seed drift → run → drift gone → audit rows correct → undo reverses |
| Integration (undo) | `tests/integration/test_audit_undo.py` | Mutate → `system_audit_undo` → verify pre-mutation state; `is_undo` and `undoes_operation_id` set; undo's own row is undoable |
| Integration (cascade) | `tests/integration/test_audit_undo_cascade.py` | op1 → op2 on same row → `system_audit_undo(operation_id="op1")` fails with blocker list = [op2]; undo op2 then op1 succeeds |
| Integration (doctor recipe) | `tests/integration/test_doctor_recipes.py` | Seed audit-failing state → `system_status(sections=["doctor"], detail="full")` → `recovery_actions` non-empty; tools named exist in registry |
| Integration (refresh) | `tests/integration/test_refresh_error_surfacing.py` | Inject matcher crash → `RefreshResult.matching_error` populated; envelope `recovery_actions` correct |
| Integration (matches MCP) | `tests/integration/test_matches_mcp.py` | Four matching workflow operations work through the three standard tools; parity with CLI JSON |
| Property | `tests/moneybin/test_envelope_property.py` | For every registered MCP tool, every code path that raises `UserError` either populates `recovery_actions` or sets `error_code="recovery_no_path"`. Fails CI if a new error site forgets. |
| Scenario | `tests/scenarios/test_scenario_recoverable_state.py` | End-to-end: import → categorize → split → tag → revert → verify orphans cleaned by self-heal; bad rule → undo via `system_audit_undo`; agent never reaches for `sql_query` |
| Cross-surface | `tests/integration/test_cli_mcp_recovery_parity.py` | CLI JSON output of recovery_actions = MCP envelope contents for matched failure shapes |

## Out of Scope

- **Sub-batch row-level `import_revert`.** Existing batch revert + re-import path stays the answer for "50 of 100 rows are bad." Row-level revert adds complexity for an unclear demand signal. Tracked as follow-up if agent-experience reports surface it.
- **Atomic time-range undo.** Sequencing via `system_audit(view="history")` + per-op `system_audit_undo` covers it. An atomic capability is a sharp edge — it could undo across user intent boundaries — and remains unnamed unless bounded-registry admission is justified by real agent UX.
- **Encryption-key recovery.** Out of layer; covered by `privacy-data-protection.md` and external backups.
- **Schema migration rollback.** Covered by `database-migration.md`. The Phase 2 schema additions in this spec are forward-only.
- **External-state side-effect undo (M1G Plaid sync).** No external mutations in the current sync model; sync server is opaque per AGENTS.md. M1G spec decides if needed.
- **Undoing `import_revert` via `system_audit_undo`.** `import_revert` mutates `raw.*`, which is outside Invariant 10 / audit_log scope by design (the schema boundary is load-bearing — `raw.*` is bytes-from-source). The cascade self-heal that `import_revert` triggers (orphan cleanup in `app.transaction_categories`, `app.transaction_splits`) IS audit-logged and individually undoable, but undoing those rows without re-importing would only restore orphans pointing at deleted raw rows. The correct recovery for an unwanted revert is to re-import the source file; `import_revert`'s error envelope on a "no, I want it back" agent prompt MUST escalate to the user with `error_code="recovery_no_path"` rather than silently chain audit undos.
- **`dedup_reconciliation` invariant (formerly `staging_coverage`).** Unblocked separately against the real pair-decision model — no `is_primary`/group column needed; the expected absorbed count is simply the accepted-dedup decision count. Now active; see `moneybin-doctor.md`. Not part of this milestone.
- **Cascading undo.** Block-don't-cascade is the Phase 1 default. A cascading capability remains unnamed unless the walk-and-retry pattern proves too verbose and passes bounded-registry admission.
- **Aggressive auto-heal beyond the safelist.** The five criteria are the gate. Adding a recipe requires explicit justification.
- **Retroactive recovery_actions backfill on pre-spec error logs.** Errors before this spec don't get retroactive actions; the contract starts at deploy time.
- **Recovery analytics / agent-success metrics.** Whether `recovery_actions` actually got executed by agents is interesting but a separate measurement spec.

## Resolved Design Decisions

Resolved during the 2026-05-19/2026-05-20 brainstorm. Captured so future readers can see the path taken and the alternatives weighed against it.

1. **Push + pull discovery, not push-only.** Push (`recovery_actions` on failures) covers the reactive case. Pull (`system_audit(view="history")`) covers regret — the user changed their mind, no error preceded the bad state. Push-only would force agents to reach for `sql_query` for regret cases; that's exactly the surgery this spec rules out. Cost: one extra projection, worth it.

2. **Safelist + report posture for self-heal, not aggressive or detect-only.** "I don't want friction, but I don't want the kind of magic that loses trust." The five criteria are the line: derivable + idempotent + no information loss + auditable + reversible. Five active recipes pass all five (`account_displayname_reresolve` was drafted as a sixth but moved to the deferred subsection per the PR #188 review — it's "largely subsumed by `derived_table_rebuild`" in Phase 1, so reserving an active slot for it would allocate audit/undo surface for a no-op). Everything else — orphan notes, recategorization conflicts, budget references — surfaces as `orphan_app_state` audit failures with structured recovery_actions. Recipe #5 (`rule_apply_to_uncategorized`) stays auto-on; it only creates rows where `app.transaction_categories` has no entry for the transaction, so manual categorizations are protected.

3. **Audit-log-driven undo, not per-domain inverse tools.** Approach C (hybrid) over Approach B (per-domain). Phase 1 already captures the data; Phase 2 is one consumer rather than 6-8 `un*` tools. The verb vocabulary in `.claude/rules/surface-design.md` doesn't have a `_undo` verb on purpose — reversibility lives in the audit log. Explicit named tools exist only where the inverse is structurally a different operation (matches, splits).

4. **Block-don't-cascade undo.** When a later operation modified the same rows, `system_audit_undo` returns `undo_cascade_blocked` with blockers; the agent walks the chain explicitly. Auto-cascade is exactly the magic that loses trust ("I undid one thing and it deleted my categorizations from last week"). A cascading capability remains unnamed unless real agent UX justifies bounded-registry admission.

5. **Mutation tools handle the "many" case natively; no `_many` variants.**
   Confirmed 2026-05-20 and preserved by the coarse surface. Declarative
   collection state lives in `transactions_annotate`, whose `requests` batch
   handles one or many note/tag/split targets atomically. Verb vocabulary stays
   clean; the agent never disambiguates singular and `_many` tools. Codified in
   the new project rule (Req 11.8).

6. **Empty `recovery_actions` = escalate, never silent no-op.** A failure with no actionable recovery MUST set `error_code="recovery_no_path"` explicitly; agents read this and escalate to the user. The contract is that the system never silently treats an unrecoverable error as auto-recovered.

7. **No `recovery_action` may name `sql_query` or DDL tools.** Hard rule. If a failure can only be recovered via SQL surgery, that's a missing tool — escalate to spec and add the tool. Codified in the project rule (Req 11.7).

8. **CLI parity from day one.** CLI JSON output carries the same `recovery_actions`. Per `feedback_cli_agent_surface.md`, CLI is a first-class agent surface — same JSON, same redaction, same audit. Human-readable CLI output renders recovery_actions as a numbered list with `moneybin <cmd>` invocation syntax.

9. **New milestone slot (M1L), not bundled into M1C.** The envelope is a one-way door — lock once, every future spec inherits — so it deserves its own scope. Bundling into M1C dilutes both. Cross-cutting into M1G–M1K fragments the contract.

10. **Invariant 11 (Recoverability of mutations), not just a documentation update.** Codified at the same level as Invariant 10 in `architecture-shared-primitives.md`. Forces future specs to declare their recovery path during design, not after.

11. **UUID4 over ULID for `operation_id`.** Drafted with "ULID" for chronological-sort properties; the spec was updated during review (2026-05-20, PR #188 review feedback) to use `op_<uuid4_hex>` — 32-char UUID4 hex prefixed with `op_`. Reasons: the project already uses UUID4 throughout (`identifiers.md` decision tree; `audit_id` itself is full UUID4 hex), adding a ULID dependency crosses the "fewer dependencies on the critical path" line in `.claude/rules/design-principles.md`, and chronological sort is available without ULID via the existing `app.audit_log.occurred_at` column (the `idx_audit_log_occurred_at_op` index serves the same use case). The `op_` prefix provides the visual discriminability ULID would have given.

12. **Two-step migration for `operation_id` backfill, not a column-reference DEFAULT.** Drafted with `ADD COLUMN operation_id TEXT NOT NULL DEFAULT 'op_legacy_' || audit_id`; the spec was updated during review (2026-05-20, PR #188 — Codex P1 + Claude review) to use the standard add-then-UPDATE-then-SET-NOT-NULL pattern. Reason: DuckDB (and standard SQL) does not allow column references inside `DEFAULT` clauses — that is a `GENERATED ALWAYS AS` feature, not a default. The original SQL would error at migration time and block adoption on every existing database. The four-step migration (ADD nullable → UPDATE backfill → ALTER NOT NULL → CREATE INDEX) is the deterministic safe path.

## Related Work

- Origin: 2026-05-19 brainstorm initiated by Brandon's question on data-quality failure modes and recovery paths.
- Prerequisite: `app-integrity-invariant.md` (Phase 1 — audit_log pre-image capture, repository routing, lint rule). This spec supersedes the Phase 2 description in that spec's Out of Scope section.
- Adjacent: `data-reconciliation.md` (draft) — broader ETL invariant work; this spec lands the agent-recovery contract that draft references.
- Companion rule: `.claude/rules/data-recovery.md` (new, lands in PR 10) — codifies the contract for future specs and tools.
- Followup items rolled into this spec: `followups.md:71` (silent refresh crashes) — covered by Req 9.
