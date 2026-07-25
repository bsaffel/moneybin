# Feature: CLI Output Coherence

## Status
<!-- draft | ready | in-progress | implemented -->
draft

## Milestone

M3K.3 — the third increment under M3K (CLI / MCP UX standards), after M3K.1
([`agent-visualization.md`](agent-visualization.md)) and M3K.2
([`mcp-tool-surface-scaling.md`](mcp-tool-surface-scaling.md)). Those two govern
what the MCP surface returns; this one governs what the CLI's text branch prints.

## Goal

Give the CLI one way to render each kind of result, so every command looks like it
was written by the same hand and every line printed is worth reading. Today three
commands returning rows use three different idioms, four commands print internal
function or MCP tool names at the user, and `reports spending` is illegible at
default terminal width.

## Background

Origin: a 2026-07-25 audit of the CLI's text output, conducted by running every
representative command against a populated profile and reading what came back. It
produced twelve reproducible defects, referenced throughout this spec as F1–F12
and each named at the requirement that closes it. Four of them are worth stating
up front, because they set the scope:

- **F1** — `reports spending` renders eleven columns into an 80-column terminal.
  Every header and nearly every value is elided; not one figure is legible.
- **F2** — `accounts list`, `transactions list`, and `reports networth` are all
  read-projections returning rows, and each renders in a different idiom.
- **F3** — four commands print MCP tool names, Python function names, `repr`
  fragments, or `key=value` debug output at the user.
- **F11** — every invocation prints a profile banner whose parenthetical names two
  possible sources rather than the one that resolved, costing a line per command
  and telling the reader nothing actionable.
- **F12** — `moneybin refresh` narrates one pipeline stage of three, so a run that
  changed nothing and a run that recategorized 400 transactions are nearly
  indistinguishable from its output.

The remaining findings — F4 through F10 — are each named at the requirement that
closes them, in the Requirements section below.

Governing rules, none of which this spec supersedes:

- [`.claude/rules/cli.md`](../../.claude/rules/cli.md) — the `-o/--output`,
  `-q/--quiet`, `--json-fields` contract; exit codes; stdout/stderr split; the
  icon table. This spec adds *how the text branch renders*, which `cli.md` has
  never specified.
- [`.claude/rules/design-principles.md`](../../.claude/rules/design-principles.md)
  → Coherence — "when you add new X, does it look like existing X?" F2 is a
  standing violation of that rule.
- [`.claude/rules/surface-design.md`](../../.claude/rules/surface-design.md) —
  operation shapes. Every command in scope is Shape 5 (read-projection) or Shape 3
  (discrete-verb) and renders accordingly.
- [`.claude/rules/identifiers.md`](../../.claude/rules/identifiers.md) → Guard 1 —
  "the display column is for rendering; the ID is for joining." F7 violates it in
  both directions at once.
- [`design-system/readme.md`](../../design-system/readme.md) → Content
  fundamentals + Visual foundations — the CLI is named there as a surface sharing
  the product's language. Money is always mono with redundant sign; balances
  unsigned; numbers first.
- [`moneybin-cli.md`](moneybin-cli.md) — command taxonomy. **Unchanged by this
  spec.** No command is added, removed, renamed, or re-parented.

The same audit surfaced one defect that is **not** a rendering problem, tracked
separately and referenced here as F0: `reports networth` sums an account once per
balance source, so an account carrying rows from two sources is counted twice.
Liabilities dedup correctly and assets do not, which is the diagnostic lead. It is
named in this spec for one reason — a renderer that silently collapsed the
duplicate rows would make the output *look* correct while the total stayed wrong.
Requirement 35 exists to prevent exactly that.

## Requirements

Numbered, each independently testable.

**Renderers**

1. A module `moneybin.cli.render` exposes exactly three text renderers —
   `render_rows`, `render_summary`, `render_note` — and no command in
   `src/moneybin/cli/` constructs a `rich.Table` or hand-formats an indented list
   directly.
2. `render_rows` is the only way a command prints a collection of records. It
   emits a Rich table to **stdout**.
3. `render_summary` is the only way a command prints a labelled scalar block
   (`Net worth: …`, `Assets: …`). It emits aligned label/value pairs to
   **stdout**.
4. `render_note` is the only way a command prints an informational status line. It
   emits to **stderr** and is suppressed by `-q/--quiet` — except for notes marked
   as **result framing** (requirement 10), which describe the completeness of the
   data itself rather than the progress of the operation.
5. Result data is never suppressed by `-q` (restates `cli.md`; asserted here
   because the renderers are now the enforcement point). Result *framing* — a
   statement about what the result omits — is covered by the same guarantee: `-q`
   suppresses chatter, never the boundaries of the data.

**Column policy (F1)**

6. Every `reports` command declares a `DEFAULT_COLUMNS` tuple naming the columns
   its text branch renders. `render_rows` renders only those.
7. Every command with a `DEFAULT_COLUMNS` narrower than its full projection
   accepts `--wide`, which renders all columns.
8. `--output json` is unaffected by `DEFAULT_COLUMNS` and by `--wide`; it returns
   the full projection, filtered only by `--json-fields`.
9. `DEFAULT_COLUMNS` for every report fits in **80 columns** with no elision of any
    header, and no elision of any value that is not free text. 80 is the width F1
    was reproduced at, so a wider bar would let an implementation satisfy this
    spec while the reported defect persists.
10. When columns are omitted, `render_rows` emits one **result-framing** note
    naming the count and the flag: `12 of 23 columns shown — --wide for all`.
    Silent truncation is prohibited, and per requirements 4–5 this note survives
    `-q` — otherwise `reports spending --quiet` would reintroduce exactly the
    silent truncation this requirement forbids.

**Money and numbers (F9)**

11. A single `format_money` is the only place amounts are stringified for text
    output. Thousands separators always; two decimal places always.
12. Every money column declares a **money kind**, and the renderer never infers
    meaning from the raw number. Three kinds:
    - `flow` — signed under the AGENTS.md accounting convention (negative =
      expense, positive = income). Renders an explicit `+` / `−` (U+2212).
    - `magnitude` — a positive absolute quantity whose polarity is carried by the
      column, not the value (`spending_trend.total_spend` is a positive outflow).
      Renders unsigned; never colored as income.
    - `balance` — a position, not a movement. Renders unsigned.
13. Amounts are right-aligned in `render_rows` columns.
14. Color is driven by the money kind plus the value, never by the value alone.
    A `flow` colors `--pos-income` when positive and `--neg-expense` when
    negative; a `magnitude` in a spend-oriented column is never green; a `balance`
    is uncolored. The sign glyph — where the kind has one — is present regardless
    of color, so the encoding survives a pipe, a non-TTY, and `NO_COLOR`.
    **Rationale:** `spending_trend.py` declares `total_spend` as a positive
    absolute outflow and `mom_delta` as current-minus-previous spend. Coloring on
    raw sign would render spending green as income and invert the meaning of a
    rising `mom_delta`, contradicting AGENTS.md's sign convention.
15. Color is emitted only when stdout is a TTY and `NO_COLOR` is unset.

**Message hygiene (F3, F11, F12)**

16. No user-facing message *leaks* an internal identifier. The audit matches
    **compound internal identifiers and tool-call syntax**, not bare words:
    - a `snake_case` identifier of two or more segments (`transactions_get`,
      `accounts_links_run`), whether bare or as a `name:` prefix;
    - `key=value` debug fragments outside a recovery action.

    It deliberately does **not** match single-word registered tool names. The
    registry contains tools literally named `accounts` and `reports`
    (`src/moneybin/mcp/surface.py:28-29`), which are also unavoidable product
    vocabulary — a test rejecting every registered name would fail on
    `Accounts: 5` and force unnatural prose for no privacy or clarity gain.

    **Explicit exception — `RecoveryAction` rendering.** `system doctor` and the
    error path render `action.tool(key=value, …)` deliberately
    (`src/moneybin/cli/commands/system/doctor.py:138`), because
    `RecoveryAction.tool` *is* an MCP tool name by contract
    (`src/moneybin/errors.py:43`) and the rendered call is meant to be pasted
    directly by an agent. This is a designed AX affordance, not a leak, and
    requirement 22 preserves it. The audit skips recovery-action lines.
17. No user-facing message names an internal dependency. `SQLMesh` is not a user
    concept; the stage is "transforms".
18. `refresh` emits one `render_note` per pipeline stage naming the stage and its
    observable outcome, including stages whose outcome is zero. A run that changed
    nothing and a run that recategorized 400 transactions are distinguishable from
    stderr alone.
19. The profile banner states the resolved source or omits the parenthetical. The
    string `(from config.yaml or first-run wizard)` does not survive.

**Quiet on success (F6)**

20. `system doctor` prints per-invariant lines only for invariants that fail. On a
    fully-passing run it prints its summary line and nothing else.
21. `--verbose` restores the full per-invariant roll.
22. The failure path is unchanged in content: a failing invariant prints its name
    and its detail.

**Stats (F5)**

23. `stats` renders each metric with its distinguishing dimension, such that no
    two rendered lines share a label.
24. Histogram metrics render the unit declared in the metric registry. A metric
    that is not a duration does not render `s`.
25. `stats` groups metrics by domain with a header per group, rather than one
    alphabetical list.

**Identity display (F7)**

26. `accounts list` renders the account ID alongside the display name, so rows
    that share a display name are distinguishable. **Balance is deliberately not
    in scope:** `AccountSummary`
    (`src/moneybin/privacy/payloads/accounts.py`) carries no current-balance
    field and `AccountService.list_accounts()` reads only `dim_accounts`, so
    adding one would change the JSON/MCP contract requirement 8 keeps untouched
    — and F0 shows balance data is currently summed once per source, which a new
    surface would reproduce rather than reveal. Tracked as a follow-up.
27. `transactions list` renders the account display name alongside (or instead of)
    the raw account ID.
28. The two outputs share at least one column whose values are equal for the same
    account, so they can be joined by eye. This is what closes F7 — an ID that
    disambiguates and a name that reads, on both sides.

**Categories (F8)**

29. Category values are resolved to the display taxonomy at one boundary. No
    single rendered column contains both `Food & Drink` and `FOOD_AND_DRINK`.
30. A category with no display mapping renders its raw value **and** is counted in
    a `render_note`, rather than silently mixing into the column.

**Stubs (F4)**

31. Unimplemented commands are hidden from `--help` output at every level
    (`hidden=True`), while remaining invocable so the namespace stays reserved and
    any existing script keeps its exit code.
32. The not-implemented message names a user-facing next action, not a repo path.
     `docs/specs/*.md` does not appear in any message reachable by an installed
     user.
33. The exit-code policy in `stubs.py` (stubs exit `0`) is unchanged.

**Pagination (F10)**

34. The text branch renders a human paging line — shown, total, and the flag to
    continue. The raw cursor token appears only in `--output json`.

**Non-interference with data correctness (F0)**

35. No renderer deduplicates, merges, or suppresses rows. `render_rows` emits one
    line per record it is given. A regression test asserts that a payload
    containing two rows identical in every rendered column still renders two
    lines — so a future "tidier output" change cannot mask a data defect behind a
    presentation fix.

## Data Model

No schema changes. No migration. This spec touches presentation only.

The one adjacent read: requirement 24 needs each histogram metric to declare a
unit. If `src/moneybin/metrics/registry.py` does not already carry one, this adds
a `unit` field to the metric declaration — a registry change, not a database one.

## Implementation Plan

### Files to Create

| Path | Purpose |
|---|---|
| `src/moneybin/cli/render.py` | `render_rows`, `render_summary`, `render_note`, `format_money`, color/TTY gating |
| `tests/moneybin/test_cli/test_render.py` | Renderer unit tests — alignment, sign, separators, TTY gating, quiet |
| `tests/moneybin/test_cli/test_message_hygiene.py` | Requirements 16, 17, 32 — the grep-shaped audits |
| `tests/moneybin/test_cli/test_column_policy.py` | Requirements 6–10 across every `reports` command |

### Files to Modify

| Path | Change |
|---|---|
| `src/moneybin/cli/output.py` | `render_or_json` delegates its text branch to the new renderers; `--wide` joins the shared option set |
| `src/moneybin/reports/_framework/registry.py` (`ReportSpec`) | Carry `DEFAULT_COLUMNS` as spec metadata — **this is where the column policy lives** |
| `src/moneybin/reports/_framework/cli_register.py` | `register_reports_cli` generates the `--wide` option and applies `DEFAULT_COLUMNS` to the generated signature |
| `src/moneybin/reports/definitions/*.py` | Declare each report's `DEFAULT_COLUMNS`, `spending_trend.py` first (F1) |
| `src/moneybin/cli/commands/reports/networth.py` | The two hand-written NetworthService-backed commands; adopt `render_summary` / `render_rows` |
| `src/moneybin/cli/commands/accounts/__init__.py` | Account ID column (26); adopt `render_rows` |
| `src/moneybin/cli/commands/transactions/list_.py` | Account name (27); drop the `transactions_get …` line (16); human paging (34) |
| `src/moneybin/cli/commands/transactions/categorize/__init__.py` | Uncategorized queue is a Shape-5 read-projection — migrate off `render_rich_table` (1) |
| `src/moneybin/cli/commands/refresh.py` | Per-stage notes (18); drop function-name prefixes and `SQLMesh` (16, 17) |
| `src/moneybin/cli/commands/system/doctor.py` | Quiet on success (20–22); recovery-action rendering unchanged per req 16's exception |
| `src/moneybin/cli/commands/stats.py` | Dimensions, units, grouping (23–25) |
| `src/moneybin/cli/utils.py` | Profile banner (19); **retire `render_rich_table`** into `render_rows` — it is the shared `rich.Table` builder req 1 supersedes |
| `src/moneybin/cli/commands/stubs.py` | Message copy (32) |
| `src/moneybin/cli/main.py` + group modules | `hidden=True` on stub registrations (31) |
| `src/moneybin/metrics/registry.py` | Metric `unit` declaration, if absent (24) |
| `.claude/rules/cli.md` | Add a "Text rendering" section pointing at this spec — the rule file is where a future contributor looks first |

### Key Decisions

**Three renderers, not a framework.** `render_rows` / `render_summary` /
`render_note` cover every text branch in the CLI today. This is deliberately not a
pluggable formatter registry: per Simplicity First, the abstraction exists to
remove ambiguity about which shape to use, not to add flexibility. If a fourth
shape is genuinely needed later, it is added then.

**Column policy lives with the command, not in the renderer.** `DEFAULT_COLUMNS`
is declared per report because only the report's author knows which columns answer
the question. A width-based auto-selector would be magic — it would silently pick
different columns on different terminals, which
`design-principles.md` → "Magic stays visible" rules out.

**Truncation is always disclosed (10).** Reporting fewer columns than exist is a
bounded view, and per the review's own standard a bounded view that does not say
so reads as complete when it is not.

**Stubs are hidden, not removed (31).** Removing them frees the namespace for a
future collision and breaks any script that tolerates the `0` exit. Hiding them
fixes the trust problem — root `--help` stops promising what it cannot deliver —
at no compatibility cost. This is a pre-launch surface per
`design-principles.md`, so the change is cheap now and expensive later.

**Sign encoding is redundant by construction (14).** Color is an enhancement, never
the encoding. This matches the design system's colorblind rule and survives the
CLI's non-TTY consumers — scripts and agents — which `cli.md` treats as peers.

**The renderers do not dedup (35).** Stated as a requirement, not left to
judgement, because F0 makes it tempting: an account appearing once per balance
source produces visibly repeated rows, and collapsing them in the renderer would
make the net-worth output *look* correct while the total stayed wrong. Rendering
is not the place to repair data — a presentation fix that hides a model defect is
worse than the defect, because it removes the symptom that would have found it.

## CLI Interface

No command is added, removed, or renamed. One new flag:

| Flag | Attaches to | Purpose |
|---|---|---|
| `--wide` | Read-only commands whose `DEFAULT_COLUMNS` is narrower than their projection | Render all columns |

`--wide` is ignored with `--output json` (which is always full), matching how
`--json-fields` is ignored with `--output text`.

Before and after for the worst case (F1). The "before" is real output at 80
columns; the "after" is a **mock** against the `demo` persona, showing the shape
of the change rather than the final column choices:

```
# before — 11 columns at 80 cols, nothing legible
│ 202… │ LOA… │ 117… │ 4    │ 0.00 │ 117… │      │ 0.00  │ 117… │       │ 391… │

# after — DEFAULT_COLUMNS (mock)
  Month     Category            Total      Txns      MoM      YoY
  2026-07   Groceries         −412.55        18    −0.04x   +0.11x
  2026-07   Utilities         −208.00         3    +0.02x   −0.06x

  6 of 11 columns shown — --wide for all
```

## MCP Interface

No change. MCP tools return the envelope; this spec governs the CLI's **text**
branch only. `render_or_json`'s JSON path — redaction, `derive_tier` stamping,
the `privacy.log` audit event — is untouched, and requirement 8 exists to keep it
that way.

## Testing Strategy

Per `.claude/rules/testing.md` and the project's TDD requirement, each numbered
requirement gets a failing test before its implementation.

**Unit** — `render_money` sign/separator/alignment matrix; TTY and `NO_COLOR`
gating; `-q` suppressing notes but never rows.

**Contract, parameterized across every command** — this is where coherence is
actually enforced, and the tests must enumerate commands from the live registry
rather than a hand-maintained list, so a new command cannot silently skip the
contract:

- Every read-only command's text branch renders through one of the three
  renderers (1).
- Every `reports` command's `DEFAULT_COLUMNS` renders within 100 columns with no
  header elided (9).
- Rendered output contains no registered MCP tool name and no `_run:`-shaped
  prefix (16).
- No reachable message contains `docs/specs/` (32).
- No stub command appears in any `--help` output (31).

**Regression, F-numbered** — one test per finding, each written to fail against
today's code. Two need specific shapes:

- **F6** — a doctor run with one failing invariant must print that invariant and
  not the 48 passing ones. A fixture where everything passes cannot distinguish
  quiet-on-success from a renderer that prints nothing at all.
- **F8** — a fixture containing both a mapped and an unmapped category, asserting
  the column is homogeneous and the unmapped one is counted in a note. A
  fully-mapped fixture passes trivially.

**Not covered by the default gate:** these are CLI-surface tests in
`tests/moneybin/test_cli/`, so `make check test` is the correct gate. No
scenario-suite run is required — no data shape changes.

## Synthetic Data Requirements

None new. The existing demo persona exercises every renderer. The F8 test needs a
category with no display mapping, which is a fixture concern rather than a
generator one.

## Dependencies

None added. Rich is already a dependency and already renders
`transactions list`.

## Out of Scope

- **F0**, `reports networth` summing an account once per balance source. A
  correctness defect in the model, tracked separately and fixed on its own branch
  with its own test. Requirement 35 keeps this spec from masking it.
- **A TUI or any second front-end.** Considered and declined: a persistent
  full-screen app serves humans only, while `cli.md` treats scripts and agents as
  peer consumers of the same surface. Nothing here forecloses one later.
- **New reports or report capabilities** — charts, sparklines, financial-health
  projections, spending-flexibility tagging, regex search, local backups.
  Considered and declined for this increment; each is additive to a coherent
  render layer rather than blocked by its absence.
- **Command taxonomy.** `moneybin-cli.md` owns it and is unchanged.
- **Implementing any stubbed command.** F4 is about visibility only.
