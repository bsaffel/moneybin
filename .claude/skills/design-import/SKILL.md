---
name: design-import
description: Use when bringing design work created in claude.ai/design — a component, a guideline/specimen card, a token change, or a full screen — back into the repo's design-system/. The inbound direction, opposite of the outbound /design-sync. Triggers: a designer built something in the design tool and handed you a zip and/or a project link to get into the repo.
---

# Design Import (claude.ai/design → design-system/)

## Overview

Inbound counterpart to `/design-sync`. Two facts drive everything:

- **The claude.ai/design project is the source of truth; the downloaded zip is a lossy handoff** — usually docs + a value cross-check, often with NO authorable component/card source. Pull the real assets from the project with the `DesignSync` tool (`get_file`), not the zip.
- **This is a judgment call, not a copy.** Most of the value is classifying each asset, routing it, and NOT importing what doesn't belong. Two decision points are the human's (see Checkpoints).

Build / stage / verify / upload / sandbox mechanics are already documented — read `design-system/.design-sync/NOTES.md`. This skill is the inbound *judgment* layer on top of it; it does not repeat those mechanics.

## Two human checkpoints (magic stays visible)

1. **Classification/routing** — after classifying, confirm the routing with the human before writing into `design-system/`. A wrong silent classification (importing a screen as a component) is costly to undo.
2. **Sync + commit** — confirm before uploading to the shared project and committing.

## Step 1 — Fetch + classify (CHECKPOINT 1)

`DesignSync list_files` the project (assert its id matches `.design-sync/config.json`), diff against `design-system/`, and route each NEW asset:

| Asset | Route | Note |
|---|---|---|
| **A screen / flow / dashboard** — composes existing components; often a `.dc.html` | **App code — PARK it.** Do NOT import into design-system. | design-system holds tokens/components/specimens, not screens. Flag it; there may be no frontend app yet. Its reusable *algorithms* are future-build reference, not a design-system asset. |
| **A component** (`<Name>.jsx` + `.d.ts` + `.prompt.md`) | `components/<group>/` | needs the full triad + `.design-sync/previews/<Name>.tsx` + a `config.json` `docsMap` entry, or it won't bundle |
| **A specimen card** — HTML whose first line is `@dsCard` | `guidelines/` | the common case |
| **A token change** | DIFF against `tokens/*.css` first | **repo tokens are canonical; the zip's are usually stale** (primitive names vs the repo's semantic names). Import only genuinely-new values, into the repo's names. |
| **Prose** (a grammar doc, etc.) | design-system **root** | NOT `guidelines/` — the converter's `.md` glob would grab it. |

The single most important call: **is this a screen or a primitive?** A screen is app code; *parking it is the correct import.*

## Step 2 — Normalize (per type)

Claude Design output needs fixing before it is repo-native:

- **Inject `<meta charset="utf-8">`** into every card `<head>` — it is always omitted, and `−·–▲▼` render as mojibake without it.
- **Freeze any runtime `<script>` to static SVG.** Some cards generate their content with an inline script (e.g. a heatmap). Run its logic once, bake the resulting elements inline, delete the `<script>`. Specimens must render static — coherence with the other cards, and inspectable source.
- **Tokens only, no hardcoded hex.** Card contract: `@dsCard` first line, `../styles.css` link, `var(--*)` throughout.
- Audit the brand non-negotiables on every asset: brass-only accent (never blue), money in mono via `Amount` with explicit +/− signs, hairline borders / no resting shadow, SQL provenance chip on data widgets, no emoji, no exclamation points.

## Step 3 — Verify

Stage into the bundle and verify EVERY card renders on-brand via the Playwright MCP browser — mechanics in `NOTES.md` (serve unsandboxed; `file://` is blocked). Fast path: write one page that iframes all the cards, take a single full-page screenshot.

## Step 4 — Sync + commit (CHECKPOINT 2)

- **Guideline-only import (common case):** targeted upload, no full rebuild — `cp guidelines/*.html ds-bundle/guidelines/`, then `DesignSync finalize_plan` (writes `guidelines/**` + `_ds_needs_recompile`), then fence → write the cards → re-arm the sentinel.
- **Component or token change:** run the full outbound `/design-sync` (it rebuilds the bundle).
- Commit with explicit `git add <paths>` (design-system may show as untracked); never `-A`. Do not commit the zip.

## Common mistakes

| Mistake | Reality |
|---|---|
| Importing a screen into design-system | A screen is app code — park it. design-system holds primitives, not screens. |
| Reconstructing components from the zip | The zip is lossy docs. Real source is the project (`get_file`). |
| Dropping in the zip's `tokens.css` | Stale primitive names; breaks every `var(--*)`. Repo tokens are canonical — diff, don't replace. |
| Leaving a card's runtime `<script>` | Specimens must be static SVG. Freeze it. |
| Forgetting `<meta charset>` | Mojibake on signs and glyphs. Every card needs it. |
| Overwriting a richer repo doc with the zip's shorter one | Reconcile, don't overwrite (e.g. the repo's 12 specimens vs the zip's "8 rules"). |

## See also

- `design-system/.design-sync/NOTES.md` — build / stage / verify / upload mechanics + sandbox gotchas.
- `/design-sync` — the outbound direction (repo → project). `/moneybin-design` — the design-time helper.
