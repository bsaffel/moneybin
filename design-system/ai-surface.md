# MoneyBin AI surface — binding rules

The contract for the ask surface: what it looks like, what leaves the machine,
and who decides. This is a trust contract before it is chrome — where any other
doc's guidance on the ask surface differs, this one wins. `patterns.md` owns the
chrome the surface sits in (the palette, the floating layer); `charts.md` owns
anything the answer draws.

The surface is app-owned in v1. These are the rules it ships against, not a
component API.

## The ask surface is the caret

`▸_` — mono (`--font-data`), brass. Never an icon, never a sparkle, never ✨.
This is a system non-negotiable and it holds in every surface that offers an ask:
the utility strip, the palette's ask row, an empty state.

### Anatomy

- **Ask bar.** Global `⌘⏎`. The console editor wins the chord while focused —
  chords resolve room-first (`patterns.md` → Keyboard chords).
- **Thinking trace.** Mono `--text-faint` status lines that name what is
  happening in the system's own vocabulary — `schema read` · `running sql_query`
  · the consent tier in play. It is a provenance surface, not a personality: no
  prose narration of reasoning, no filler while waiting.
- **Answers are `WidgetCard`s.** Every answer renders as a widget with its `sql`
  chip **mandatory**, and its `audit` line present when the global deep-audit
  toggle is on. An answer that cannot show its query is not shippable.

### The model writes the query; the vault says the number

Numbers reach the user from the local database, never from generated prose. The
model's output is the *query*; the value printed is what the vault returned when
that query ran. **No streamed prose numbers, ever** — a number that arrives as
generated text has no provenance and cannot carry a SQL chip.

## Consent degrades, never begs

Sensitivity is a ladder, and refusing a rung still answers what it can. The gate
asks once, states what it covers, and gets out of the way.

| Tier | Covers | Behavior |
|---|---|---|
| 0 / 1 | Aggregates, schema, structure | Flows. No gate. |
| 2 | Rows — descriptions, amounts, dates | Stops at a consent gate that **still answers with the aggregate**. |
| Critical | Account numbers, routing numbers | **Always masked.** Never sent unmasked, and no consent tier unlocks the real value. |

- **Critical fields are masked, not withheld.** State this precisely in UI copy:
  they leave as deterministic placeholders preserving the last four digits
  (`****1234`), so an answer can still name an account without exposing it. Copy
  that promises critical data "never leaves your machine" is **wrong** — the
  guarantee is *never unmasked*, which is a claim about mechanism rather than
  about completeness, and it is the one the code actually keeps. The binding
  source is the repo's privacy-and-AI-trust spec and the redaction module; this
  doc restates them and never widens them.
- **The gate is one modal** — the floating layer of `patterns.md` §07, same
  anatomy as every other. It is not a banner, an interstitial, or a nag.
- **The grant is revocable in one stated command.** The revocation is named at
  the moment of granting, not buried in settings.
- **Degrading is the default posture.** A declined tier-2 request returns the
  tier-1 answer and says so. It does not re-ask, and it does not return nothing.

## No provider is the default

- **`default_backend: None` ships.** There is no preselected model vendor. The
  ask bar states this plainly and lists the providers **as a table** — names,
  what each sees, where it runs. Never logos; a logo is an endorsement.
- **Every external call writes an `ai.external_call` audit row.** The audit trail
  is the product, not a compliance afterthought.
- **Console assist drafts, never runs.** Generated SQL lands in the editor for a
  human to read and execute. The assist has no execute path.

## Voice

Inherits the system voice (`readme.md` → Content fundamentals) with two additions
that matter more here than elsewhere:

- **No hedging theater.** State the confidence once, in the trace, in the
  system's vocabulary. Do not apologize, and do not perform uncertainty.
- **An error is a fact plus the next action** — the same rule the rest of the
  product follows. "No provider configured. Choose one in Settings, or ask
  against local aggregates." Never an apology, never an exclamation point.
