# ADR-011: Documentation Site Framework

## Status

accepted

## Context

MoneyBin's public documentation currently lives as raw markdown files in `docs/`. A
built site is required before the `user-facing-doc-polish.md` M2C batch ships — that
spec produces a landing page, user guides, and a reference section that cannot be
consumed as GitHub-rendered markdown by a general audience.

### Existing docs surface

Audited as of 2026-05-15:

| Category | Files | Notes |
|---|---|---|
| `docs/` root | 7 | `README.md`, `roadmap.md`, `features.md`, `comparison.md`, `architecture.md`, `audience.md`, `licensing.md` |
| `docs/guides/` | 14 | CLI reference, data import, MCP server, threat model, security, observability, etc. |
| `docs/reference/` | 13 | Data model, data sources, system overview, server API contract, prompt library |
| `docs/decisions/` | 11 | ADR-000 through ADR-010 |
| `docs/specs/` | 53 | Internal specs including archived — not surfaced in public site nav |
| Other (`tech/`, `architecture/`) | 3 | Implementation-internal |

**Total: ~95 markdown files, ~31 K lines of content.**

Content is plain CommonMark throughout. Checked all files for:
- **YAML front-matter**: none. Files starting with `---` use it as a horizontal rule
  mid-document, not as a YAML block.
- **Non-standard markdown**: no admonitions (`!!!`), no tab fences (`:::`), no raw HTML.
  `Mermaid` diagrams appear in some specs (not yet in guides or reference docs).

The zero-rewrite constraint is therefore simple to satisfy: any framework that ingests
plain CommonMark `.md` files without required front-matter or syntax additions passes.

### Hard constraints

1. Ingest existing `.md` files unchanged — no forced front-matter, no MDX conversion,
   no heading-level changes.
2. Deployable to GitHub Pages or Netlify from CI without significant new infrastructure.
3. Search that works without a paid plan.

## Candidates Evaluated

| Framework | Markdown compat | Node.js | Search | Versioning | Peer adoption |
|---|---|---|---|---|---|
| **MkDocs Material** | Plain `.md`, no front-matter required | No — pure Python | Built-in lunr (offline, zero-config); Algolia DocSearch free for qualifying OSS | `mike`: deploys each version to its own subdirectory; alias `latest` is a redirect | FastAPI, Pydantic, SQLMesh |
| **Docusaurus** | `.md` via `format: detect` (MDX default; CommonMark mode experimental) | Yes — Node 20+ | Algolia DocSearch (free OSS); no official local-search plugin | Built-in: `docs:version` snapshots the entire `docs/` tree per release | Bitwarden, React docs, Meta projects |
| **Astro Starlight** | `.md` files accepted, but **`title:` front-matter required on every page** | Yes — Node 22+ | Pagefind (built-in, offline, high quality) | No built-in versioning mechanism | JavaScript/Cloudflare ecosystem |
| **Sphinx + MyST** | Plain CommonMark `.md`, no front-matter required (confirmed in MyST docs) | No — pure Python | ReadTheDocs search; `sphinx-search` plugin | `sphinx-multiversion` (community) | Python library ecosystem (NumPy, Requests, pip) |
| **VitePress** | Plain `.md`, no front-matter required | Yes — Node 20+ | `minisearch` built-in local search (VitePress 1.x) | No built-in versioning | Vue/JavaScript ecosystem only |

### Findings worth noting

**Starlight disqualified.** Its page-rendering pipeline requires a `title:` field in
YAML front-matter on every `.md` file. All 95 existing docs files would need a header
block added — a mechanical but non-trivial rewrite that violates the hard constraint and
creates ongoing authoring friction.

**DuckDB outlier.** DuckDB uses a custom Jekyll site (`duckdb/duckdb-web`) with bespoke
layouts and a Ruby toolchain. That reflects dedicated web-team resources; it is not a
replicable pattern for a small OSS project and was not evaluated further.

**Sphinx is viable but heavy for this use case.** MyST parser correctly ingests plain
`.md` with no front-matter. However, Sphinx's configuration surface (`conf.py`, RST
extension model, autodoc-first mindset) adds friction for a CLI application project
with no API reference requirements. It is the appropriate choice if MoneyBin later needs
tight API-doc generation from Python docstrings; it is overconfigured for a guides +
reference site today.

**Docusaurus CommonMark mode works in practice** but is documented as experimental.
The Node.js CI dependency is the larger issue: MoneyBin's CI runs under `uv` with no
Node.js today, and adding a Node setup step for the docs build is a friction tax on
every CI run.

**Peer set convergence:**

| Project | Framework | Config confirmed |
|---|---|---|
| FastAPI | MkDocs Material | `mkdocs.yml` in `fastapi/fastapi` |
| Pydantic | MkDocs Material | `mkdocs.yml` — uses `mike`, `social`, `mkdocstrings`, `search` plugins |
| SQLMesh | MkDocs Material | `mkdocs.yml` — uses `include-markdown`, `search`, `glightbox`; hosted on ReadTheDocs |
| DuckDB | Custom Jekyll | `duckdb/duckdb-web` — outlier, not replicable |

## Decision

**MkDocs Material.**

Two decisive factors:

1. **Python-native, zero CI friction.** `mkdocs-material` is a PyPI package; `uv add
   --group docs mkdocs-material` adds it alongside other dev dependencies. The CI docs
   build is one `uv run mkdocs build` invocation — no Node.js, no version management,
   no second toolchain. This matters for a project where CI already has a pinned Python
   environment.

2. **Peer precedent across FastAPI, Pydantic, and SQLMesh.** All three use MkDocs
   Material with essentially the same plugin stack. This means there is a large body of
   real-world `mkdocs.yml` configs to draw from, known patterns for the ReadTheDocs and
   GitHub Pages deploy targets, and a stable plugin ecosystem.

The built-in lunr search covers the launch period without any external service. Algolia
DocSearch (free for qualifying OSS) is available as an upgrade once the site has enough
content and traffic to qualify.

## Consequences

### What ships to close this ADR

- `mkdocs.yml` in the repo root with:
  - Theme: `material` with a color palette and GitHub repo link.
  - Plugins: `search` (built-in), `include-markdown` (for shared content fragments).
  - Nav: three top-level sections — Guides, Reference, Decisions (ADRs). Specs omitted
    from the public nav (internal planning material).
  - `docs_dir: docs` pointing at the existing `docs/` tree.
- CI step: `uv run mkdocs gh-deploy` (or `mkdocs build` + Netlify artifact upload).
  One workflow step; no new services.
- `docs` dependency group in `pyproject.toml`: `mkdocs-material`, `mkdocs-include-markdown-plugin`.

### What is deferred

- **Algolia DocSearch.** Launch with the built-in lunr search. Apply for DocSearch once
  the site is live and indexed. Built-in search is sufficient for a site of this size.
- **`mike` versioning.** MoneyBin will want to pin docs to milestones (M2C, M3A, etc.).
  `mike` is the right tool but requires a decision on the version URL scheme and a CI
  step for branch-scoped deploys. Defer until M3A is close to shipping and there is a
  concrete "current stable vs. development" distinction.
- **`mkdocstrings`.** Auto-generated API docs from Python docstrings. Not needed for a
  CLI/MCP application today; add when there is a developer SDK surface to document.
- **Social cards and other Insiders-tier features.** The free community edition covers
  everything needed at launch.

### Known limitations

- **No first-class MDX / interactive components.** MkDocs Material's tab and admonition
  extensions are Markdown-extension syntax, not React components. This is fine for the
  current docs surface but would be a constraint if the site later needed embedded
  interactive demos or live code sandboxes.
- **`mike` versioning requires a separate `gh-pages` branch strategy.** Each versioned
  build is a separate deploy to a subdirectory; this is not the default `mkdocs gh-deploy`
  behavior and requires workflow changes when versioning is introduced.
- **Mermaid diagrams need the `pymdownx.superfences` extension** and a custom fence
  configured for `mermaid`. This is a one-time config addition, not per-file; existing
  Mermaid blocks in specs are not affected since specs are excluded from the public nav.
  When Mermaid is needed in guides, the extension covers it.

## References

- [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
- [mike (versioning)](https://github.com/jimporter/mike)
- [`user-facing-doc-polish.md`](../specs/user-facing-doc-polish.md) — the M2C doc work this ADR unblocks
- Pydantic `mkdocs.yml`: `pydantic/pydantic` on GitHub — reference for Material plugin stack
- SQLMesh `mkdocs.yml`: `TobikoData/sqlmesh` on GitHub — reference for ReadTheDocs + Material
