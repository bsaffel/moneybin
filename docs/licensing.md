<!-- Last reviewed: 2026-05-17 -->
# Licensing

MoneyBin is licensed under [AGPL-3.0-or-later](../LICENSE). This page explains
in plain language what you can do with the code, what obligations the license
carries, where the derivative-work line falls, and the project's stance on
hosting, forking, and contribution. Descriptive only; for legal advice,
consult a lawyer.

## License at a glance

The exact license string declared in `pyproject.toml` is
`AGPL-3.0-or-later`, and the full license text lives in [`LICENSE`](../LICENSE)
at the repo root. AGPL is a **strong copyleft** license with an additional
**network-server clause** that GPL does not have.

**You can:**

- Use MoneyBin personally for any purpose, commercial or not.
- Modify the code however you like.
- Redistribute the code (modified or not).
- Run MoneyBin as a service for other people.
- Build and commercialize derivative works.

**You must:**

- Keep derivatives under AGPL-3.0-or-later (or a compatible later AGPL).
- Publish the corresponding source code of any modified version you expose
  to other users over a network.
- Preserve copyright and license notices.

**You cannot:**

- Relicense MoneyBin's code under a permissive license (MIT, BSD, Apache)
  without permission from every contributor.
- Ship a closed-source SaaS built on MoneyBin's code — the network-server
  clause requires the source of the running version to be available to
  every user of the service.

## What the network-server clause means

AGPL closes the "SaaS loophole" that plain GPL leaves open. The trigger is
**other people using your modified version over a network**, not modification
or distribution by itself.

Concretely:

- **Local CLI or MCP use on your own machine.** You run `moneybin` against
  your own data, on your own laptop. No public obligation, even if you've
  modified the code. AGPL doesn't trigger because no one else is a "user"
  of your running instance.
- **Self-hosted moneybin-sync, only you as user.** Same as above. You
  are both the operator and the only user; nobody else is connecting to
  the service.
- **Self-hosted moneybin-sync, multiple users.** AGPL triggers. The
  other users have the right to receive the corresponding source code of
  the version you're running, including any modifications you made.
- **Hosted MoneyBin as a service to customers.** AGPL triggers. The same
  obligation applies whether you charge for it or not.

## Who counts as a "user"?

AGPL §13 hinges on "interacting with the program through a computer
network" but doesn't define "user." The safe reading: if a human other
than the operator can cause the running program to act on their behalf
over a network, treat them as a user.

- **Only you, on your own machine.** Not multi-user, even if MoneyBin
  is bound to a `localhost` port.
- **Family members on the same Tailscale / LAN instance.** Each is a
  separate user. You owe them the offer of corresponding source.
- **An accountant or advisor with a read-only dashboard.** A user.
  Read-only access still counts as "interacting with the program."
- **MCP client running as a different OS user on the same machine.** If
  it's your IDE process, you're still the only user. If it runs on
  behalf of someone else, that someone-else is a user.
- **Agents.** An agent you operate is an extension of you. An agent
  operated by someone else, connecting to your instance, is a separate
  user.

## Operational meaning of "corresponding source"

§13 requires the operator to offer corresponding source to users. In
practice:

**Publish:** all modified MoneyBin source, any custom SQLMesh models or
seeds, any custom CLI commands, MCP tools, or extractors that extend
MoneyBin's surface, and the lockfile so users can reproduce the build.

**You typically don't publish:** runtime configuration holding secrets
(API keys, passphrases, `.env`); user-specific runtime data (the
encrypted DuckDB file belongs to the user it stores, not the program);
third-party code under terms that prohibit republication (if you have
this, the dep may not be AGPL-compatible — see [AGPL-incompatible
dependencies](#agpl-incompatible-dependencies)).

**Cadence.** AGPL does not mandate per-commit publication.
**Tag-on-deploy** — every version your service actually serves has a
matching public commit — is the conservative practice.

**"Matches the running build"** means a Git SHA the user can
`git clone` and `uv sync --frozen` to reproduce. Pinned dependencies
(`uv.lock`) are part of corresponding source in practice — without
them, what's published does not reproducibly match what's running.

## Derivative-work boundary

The most common builder question: *if I build something on top of
MoneyBin, does my code have to be AGPL?* It depends on how your code is
combined with MoneyBin's. Cases below reflect the mainstream FSF
position; some lawyers take a less strict view, and case law on dynamic
linking is thin. If your business depends on the distinction, get
specific legal advice.

- **`import moneybin` (linking).** Importing MoneyBin modules into your
  own Python process creates a combined work. Your service becomes
  AGPL. This is the FSF's stated position on dynamic linking with
  (A)GPL software.
- **CLI / MCP consumption.** Shelling out to the CLI or talking to the
  MCP server over stdio/HTTP makes your code a *user* of MoneyBin, not
  a derivative. Your service does **not** become AGPL. The network-
  server clause still binds the MoneyBin instance, but not the caller.
- **Custom MCP tools, CLI commands, SQLMesh models.** These extend
  MoneyBin's surface; they are derivative works and inherit AGPL.
- **Configuration: env vars, runtime settings, agent prompts.** Not
  derivative. Configuration is data the program consumes, not code
  combined with it.
- **Forked `pyproject.toml` with different deps, no source changes.**
  Not a derivative if you ship only the diff. If you ship MoneyBin
  alongside, the network-server clause still binds the instance.

Safe pattern for a closed-source product on top of MoneyBin: keep
MoneyBin in its own process, talk to it over CLI or MCP, treat it as a
service your code consumes. Your code stays under whatever license you
choose; MoneyBin (and modifications) stay under AGPL.

## Commercial SaaS on top of MoneyBin

Two pieces, two licenses:

- **The MoneyBin instance your service runs.** AGPL §13 applies; you
  publish the source of the running version. A public fork linked from
  your service's UI is sufficient.
- **Your proprietary frontend, auth layer, agent prompts.** Can stay
  proprietary **if** they are separate works — separate process,
  communicating with MoneyBin over its public CLI/MCP surface. If you
  import MoneyBin modules into the same Python process as your
  proprietary code, they become a combined work and your code inherits
  AGPL.

There is **no dual-license or commercial license offering today**. If
you need a commercial license to build closed-source derivative work
(i.e., link MoneyBin into a proprietary codebase), open an issue or
contact the maintainer; the project does not currently offer one, but
the conversation is open.

## Output licensing

Output MoneyBin produces from your data — CSV exports, MCP tool
responses, query results, charts — is **not a derivative work**. This
follows the FSF's standard position that the output of running a
(A)GPL program is not covered by the program's license. Your data and
the analyses MoneyBin produces from it are yours to license as you
choose.

## Walk-away guarantee

If MoneyBin is archived tomorrow, the source remains on GitHub. AGPL
guarantees the right to fork and keep running it. The encrypted DuckDB
file is openable by any DuckDB client with the encryption key — no
MoneyBin process required. See the
[threat model's project-sustainability section](guides/threat-model.md#project-sustainability)
and the [SQL access guide](guides/sql-access.md) for direct-access
patterns.

## Contribution licensing

The project's contribution licensing is intentionally lightweight.

- **No CLA.** There is no Contributor License Agreement to sign.
- **No DCO.** No `Signed-off-by` trailer required.
- **License by submission.** By opening a pull request, you license your
  contribution under AGPL-3.0-or-later. See
  [`CONTRIBUTING.md`](../CONTRIBUTING.md) for the contribution workflow.
- **You retain copyright** on your contributions. The project as a whole
  is the union of contributions, and that union inherits AGPL.

Because there is no CLA, the project cannot unilaterally relicense
itself to a permissive license later — see
[What if the license changes](#what-if-moneybins-license-changes). The
maintainer holds the same position as every other contributor:
copyright in their own commits, no special grant, no separate
trademark right beyond what's described under [Forking](#forking).

## Forking

AGPL guarantees the right to fork. The constraints are minimal:

- **You can fork at any time, for any reason.** Personal preference,
  divergent direction, the project goes inactive — all valid.
- **Your fork must keep the AGPL license.** You can choose any version
  compatible with `AGPL-3.0-or-later` (which includes AGPL-3.0 itself
  and any later FSF-published version, at your option).
- **You can rename and rebrand.** A fork is a new project; you do not
  have to call it MoneyBin. The MoneyBin name itself is not formally
  trademarked today, but the project asks fork maintainers to choose a
  distinct name to avoid user confusion.
- **You can ship your fork.** Privately or publicly, free or for pay.
  Same network-server clause applies if you run it as a service.

## Hosting MoneyBin as a service

You can run a hosted MoneyBin instance and charge people money for it.
The license does not prohibit commercial hosting.

What the license requires:

1. **Maintain a fork** (or simply run an unmodified upstream build) that
   matches what your service is running.
2. **Publish the source** of that running version somewhere users of your
   service can reach it. A public GitHub repo linked from your service's
   UI satisfies this.
3. **Pass the license along** to anyone who receives the code (which
   AGPL does automatically — there is nothing extra to do).

In practice this is a small operational cost: tag the commit you deploy,
make sure the repo is public, and link to it. It does not require
contributing changes back upstream — your fork is yours to run, and
upstream is free to ignore it.

## Third-party dependencies

MoneyBin depends on many packages from PyPI, each governed by its own
license. The most prominent on the critical path:

| Dependency | License | Role |
|---|---|---|
| DuckDB | MIT | Embedded analytical database |
| SQLMesh | Apache-2.0 | Data-transform pipeline |
| Typer | MIT | CLI framework |
| FastMCP | Apache-2.0 | MCP server framework |
| Pydantic | MIT | Settings and data validation |
| Polars | MIT | DataFrame engine |

These licenses apply to those packages' code; AGPL applies to MoneyBin
itself. MIT and Apache-2.0 are both compatible with AGPL — MoneyBin can
depend on them without conflict, and the combined work is governed by
AGPL.

The complete dependency tree, including transitive dependencies and pinned
versions, lives in [`uv.lock`](../uv.lock); the direct dependencies are
declared in [`pyproject.toml`](../pyproject.toml). License information for
each installed dependency is in its `*.dist-info/METADATA` after
`uv sync`.

## AGPL-incompatible dependencies

The project will not knowingly accept dependencies whose licenses are
incompatible with AGPL-3.0-or-later — in practice, source-available-
but-not-open licenses (SSPL, BSL, FSL, ELv2, Commons Clause additions)
and any license whose terms cannot be satisfied alongside AGPL's
source-publication obligation. If a current dependency relicenses to
an incompatible license, the response is to replace it or fork the
last compatible version. No ADR codifies this; it follows from the
AGPL choice itself.

## Supply-chain provenance

Honest current state — what exists today, what doesn't.

- **Source of truth: GitHub.** The repository is the canonical artifact;
  tags and commit hashes are the durable references.
- **Dependency pinning: `uv.lock`** pins every direct and transitive
  dep to a specific version and hash. `uv sync --frozen` installs
  exactly the locked versions.
- **No PyPI release** today; no PyPI publish workflow. Installation is
  from source.
- **No container image** today; no GHCR or Docker Hub publish workflow.
- **No release signing** today — no sigstore/cosign, no GPG-signed
  release tags. (Individual maintainer commits may be signed; that is
  separate from release signing.)
- **No SBOM** published today; `uv.lock` can be transformed into
  CycloneDX or SPDX with standard tooling.

To verify what you're running matches upstream today: clone from
GitHub, check out a specific commit, `uv sync --frozen`. There is no
cryptographic chain from a maintainer-signed artifact to your running
build until release signing ships.

## License compatibility

Common cases for someone considering using MoneyBin's code or running
MoneyBin:

| You want to... | OK? |
|---|---|
| Use MoneyBin's code in your AGPL project | Yes |
| Use MoneyBin's code in your GPL-3 project | Yes (AGPL is compatible with GPL-3) |
| Use MoneyBin's code in your MIT / BSD / Apache project | No (would have to ship the combined work as AGPL) |
| Run MoneyBin locally for personal finance | Yes, no source-publication obligation |
| Run MoneyBin behind a personal reverse proxy, only you using it | Yes, no source-publication obligation |
| Run MoneyBin as a hosted service for family or friends | Yes, if you publish your source |
| Run MoneyBin as a commercial hosted service | Yes, if you publish your source |
| Build a closed-source product that calls the MoneyBin CLI or MCP server | Yes (API consumption — see [Derivative-work boundary](#derivative-work-boundary)) |
| Build a closed-source product that imports MoneyBin modules in-process | No (linking creates a combined work) |

The "use in a permissive project" cases say "No" because shipping the
combined work would require the whole work to be AGPL — incompatible
with a project that wants to ship under MIT, BSD, or Apache-2.0 terms.
The code is still readable, forkable, and usable on its own AGPL terms.

## What if MoneyBin's license changes?

The `-or-later` clause means newer FSF-published versions of the GNU
Affero General Public License can be used at the receiver's option.
Relicensing to a more permissive license (MIT, Apache) or a
source-available license (BSL, FSL, ELv2) would require agreement from
every contributor — there is no CLA, copyright is held by contributors
individually, and the project has no plan to pursue it. The deliberate
consequence: MoneyBin cannot quietly become proprietary.
