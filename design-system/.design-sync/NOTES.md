# design-sync notes — moneybin-design-system

Repo-specific gotchas for future syncs. This kit is **not a buildable npm
package** — it's a pre-authored "Design Composer" kit (hand-written `.jsx`
components + hand-authored `.d.ts`/`.prompt.md` + token CSS). It runs the
package shape in **synth-entry mode**.

## How it builds

Run from `design-system/` (the config home):

```sh
node .ds-sync/package-build.mjs --config .design-sync/config.json \
  --node-modules .ds-sync/node_modules --entry ./.synth-entry.js --out ./ds-bundle
node .ds-sync/package-validate.mjs ./ds-bundle --no-render-check
```

- `--entry ./.synth-entry.js` is a **deliberately nonexistent path**: it forces
  `PKG_DIR` to walk up to `design-system/package.json` (a minimal file we added),
  and `resolveDistEntry(soft)` returns null → the converter synthesizes the bundle
  entry from `components/**/*.jsx`. There is no real dist to build.
- `.ds-sync/node_modules` holds both the converter deps (esbuild, ts-morph,
  @types/react) **and** react/react-dom@18 (pinned to 18 for the UMD builds
  `vendorReact` copies into `_vendor/`; react 19 dropped UMD).

## CSS / tokens (the non-obvious part)

The DS has **no component CSS** — components use inline styles reading
`var(--*)`. So:

- `cfg.cssEntry` → `.design-sync/component-css.css`, which is **generated from
  `tokens/*.css` by `cfg.buildCmd`** (`cat colors+typography+shape > component-css.css`).
  This makes `_ds_bundle.css` carry the real `:root` token rules. An `@import`-only
  stub (what the raw `styles.css` produces) trips `[CSS_PLACEHOLDER]`.
- `tokens/` in the bundle is **intentionally empty**; tokens live in `_ds_bundle.css`
  and reach designs via the `styles.css` @import closure. The app registers the
  `:root`/`[data-theme]` scopes as its token list.

## Fonts

Self-hosted as **variable TTFs** from `google/fonts` (`raw.githubusercontent.com`),
fetched by `.ds-sync/fetch-fonts.mjs` into `.design-sync/fonts/`, wired via
`cfg.extraFonts`. TTF (not woff2) because github raw is sandbox-allowlisted while
the Google Fonts hosts weren't at first (Brandon later allowlisted `fonts.google*`
— a re-sync could switch to smaller woff2 via those hosts).

## docs / dts

- `cfg.docsMap` points each component at its hand-authored `<Name>.prompt.md`
  (discovery only matches `.md`/`.mdx`, not `.prompt.md`).
- `[DTS_REACT]` warning is **cosmetic** — the hand-authored `.d.ts` are
  self-contained (no React utility-type extension), so emitted `.d.ts` are correct.

## Verification

Done via the **Playwright MCP browser**, not the npm `playwright` render check
(no npm playwright installed; the MCP server has its own chromium). `file://` is
blocked by the MCP browser, so serve locally first:
`node .ds-sync/storybook/http-serve.mjs ./ds-bundle` — but the in-sandbox socket
bind fails `listen EPERM`, so the server must run **unsandboxed**
(`dangerouslyDisableSandbox`). Then navigate the MCP browser to
`http://127.0.0.1:<port>/components/<group>/<Name>/<Name>.html` and screenshot.
`package-validate` is therefore run with `--no-render-check`; the resulting
`[RENDER_SKIPPED]` warn is **expected, not a regression**.

## Sandbox workarounds

- `npm i` needs `--cache "$TMPDIR/npm-cache-ds"` (default `~/.npm/_cacache` is
  write-denied in-sandbox).
- Network fetch (`fetch-fonts.mjs`) and the http server need
  `dangerouslyDisableSandbox` (DNS + socket bind blocked in-sandbox).
- `ps`/`pkill` are sandbox-blocked (can't manage processes from Bash).

## Guidelines & other original content (NOT synced by the converter)

The original Claude Design project had 26 cards; the converter carries only the
**7 components**. The rest are `@dsCard`-marked HTML specimen cards outside the
converter's component scope:

- **28 `guidelines/*.html`** (Colors ×5, Type ×3, Shape ×3, Brand ×2, Charts ×12,
  Iconography, Voice ×2). The converter's `guidelinesGlob` is **`.md`-only**
  (it skips `.html`), so these must be staged + uploaded by hand:
  1. Post-build: `cp guidelines/*.html ds-bundle/guidelines/` (they reference
     `../styles.css` — resolves correctly at `guidelines/` depth).
  2. Upload `guidelines/**` under the plan; re-arm the sentinel. The app's
     self-check registers them from their `@dsCard` first-line markers.
  - **Charset fix applied to source:** the hand-authored guideline HTML lacked
    `<meta charset="utf-8">`, so `−`/`·`/`▲▼` rendered as mojibake. A
    `<meta charset>` was injected into each `guidelines/*.html`. Keep it.
- **`ui_kits/web_app/index.html`** — the "Dashboard home" 1440×900 card
  (`@dsCard` + `@startingPoint`). References `../../styles.css` (resolves at
  `ui_kits/web_app/` depth). Synced via the same hand-upload path as guidelines
  (`ui_kits/**` added to the plan writes; `cp ui_kits/web_app/index.html
  ds-bundle/ui_kits/web_app/` post-build, then upload).
- **`MoneyBin Brand Kit.dc.html`** — the master authoring doc in Design Composer
  (`<x-dc>` + `support.js`) format, not a standard `@dsCard` card. Source of
  truth the rest was extracted from; not a sync target.

## Re-sync risks

- **`component-css.css` is generated** from `tokens/*.css` by `buildCmd`. If tokens
  change and `buildCmd` isn't run, tokens go stale. (The driver/buildCmd handles it.)
- **Fonts** are a point-in-time TTF fetch; re-fetch via `fetch-fonts.mjs`, or move
  to woff2 now that Google Fonts hosts are allowlisted.
- **Render verification is manual (MCP browser)**, not the automated check — a
  re-sync must re-verify via MCP or install npm playwright + chromium.
- **Not shipped (enhancement candidates):** the Newsreader *italic* face.

## Known render warns

- `[RENDER_SKIPPED]` on every validate run — by design (MCP verification). Not new.
- `.d.ts parse check skipped — typescript not in node_modules` — non-blocking;
  add `typescript` to `.ds-sync` deps to enable it.
