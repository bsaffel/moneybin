<!-- Last reviewed: 2026-09-03 -->
# MoneyBin

Personal financial data platform: it ingests a person's financial records from
files and connected services, resolves them into one canonical warehouse, and
answers questions about them through a CLI, an MCP server, and direct SQL.

This is the project's shared vocabulary. Use these terms exactly in new and
edited prose, code, commit messages, issues, and conversation. A word under
`_Avoid_` is banned for that entry's sense only: some are plain synonyms, some
are near neighbors that must not blur, and some are bare forms the glossary
always qualifies elsewhere. Prose
and internal names written before this glossary migrate as they are touched, so
the glossary states what the repository is converging on rather than claiming it
already complies. Shipped `core` and `app` columns, MCP tool names, and CLI
command names are public contracts: renaming one is a contract change, not a
migration this glossary authorizes.

MoneyBin is a single context. The same words mean the same things in ingestion,
analysis, and serving; the groupings below are for reading, not boundaries.

## Language

### Ingestion

**Provider**:
The in-tree component behind the `Provider` protocol, which ingests one
external source into the Raw layer. A price adapter also writes to Raw but
implements its own protocol and keeps its own name. Always qualified when the
aggregator sense is meant — see **"Provider"** under Flagged ambiguities.
_Avoid_: extractor, loader, importer

**Import**:
Ingestion driven by a file the user supplies, whether handed over directly or
dropped in the Inbox.
_Avoid_: upload, ingest, load, drop

**Sync**:
Ingestion of a third-party aggregator's data, mediated on the user's behalf by
the MoneyBin sync server. The client never speaks to the aggregator.
_Avoid_: fetch, pull, download, refresh

**Connect**:
Ingestion the client performs directly against a storage service the user
themselves controls, with no server in between.
_Avoid_: integration, hookup, sync, live link

**Institution**:
The bank, broker, or issuer that holds an Account.
_Avoid_: bank, provider, issuer, source, org

**Format**:
A reusable description of one source layout, so a file of that shape can be
read without asking. Some ship with MoneyBin; the rest are saved from a file
read once. For a PDF, the instructions it stores are a **parse recipe**.
_Avoid_: profile, template, mapping, layout, schema

**Inbox**:
The watched folder where a user drops files for unattended Import.
_Avoid_: watch folder, dropbox, queue, staging folder

**Refresh**:
The pass that brings the warehouse up to date. A full one covers the
connected-sheet step, matching, transformation, categorization, identity, and
rates; a caller may select a subset.
_Avoid_: rebuild, update, sync, reprocess

**Source type**:
The discriminator naming what produced one typed row. An ingested type rides
along from Raw; a derived one is minted in Core. Several types share one
ingestion pathway.
_Avoid_: source, format, provider, channel

**Source origin**:
What produced a row — an institution, a connection, an exporter, or MoneyBin
itself for rows the user entered and rows it backfilled. It scopes native
identifiers, so two institutions using the same account number stay distinct.
_Avoid_: origin, institution, connection, item

### The warehouse

**Raw**:
The layer each ingestion lands in, before any cross-source reconciliation. It
records what a source supplied rather than archiving it: a loader may normalize
or repair a value on the way in, and a later ingestion may revise or withdraw
what an earlier one wrote. A few tables hold rows that merely live here rather
than arriving from a source; those are editable.
_Avoid_: bronze, landing, source layer, staging

**Staging**:
The layer that cleans and types each source into a shape Core can combine. It
unions sources only where one pipeline needs it early; the canonical
multi-source union is Core's. Its intermediate models belong to it rather than
forming a layer of their own.
_Avoid_: silver, cleansing layer, transform layer

**Core**:
The canonical, deduplicated, multi-source layer. One model names each
real-world entity at its primary grain, alongside the relationships, derived
facts, and review projections built on them.
_Avoid_: gold, marts, analytics layer, warehouse

**App state**:
Mutable state no derivation can reproduce from Raw — the user's notes, tags,
rules, settings, and decisions, alongside the application's own operational
metadata.
_Avoid_: user data, overrides, config, cache

**Grain**:
The one thing a single row of a model stands for.
_Avoid_: granularity, level, key, cardinality

**Golden record**:
The single canonical row its contributing source rows collapse to, whether one
row contributed or many.
_Avoid_: gold record, master record, survivor, winner

**Provenance**:
The recorded trail from a Golden record back to every source row that
contributed to it. Not lineage — see **"Provenance"** under Flagged
ambiguities.
_Avoid_: history, audit trail, source tracking

### Money

**Transaction**:
One movement of money against an Account, posted or still pending. Investment
activity is a separate ledger — see **"Transaction"** under Flagged ambiguities.
_Avoid_: entry, posting, record, txn

**Split**:
A user's division of one Transaction into parts that may each carry their own
Category, inheriting the Transaction's where they do not. The Transaction keeps
its total.
_Avoid_: allocation, breakdown, itemization, sub-transaction

**Transaction line**:
One row of the split-expanded grain: the whole Transaction when it carries no
Split, or one Split of it when it does.
_Avoid_: line item, split line, child transaction

**Sign convention**:
MoneyBin's fixed reading of a signed amount: negative is money leaving,
positive is money arriving.
_Avoid_: polarity, direction, debit/credit convention

**Home currency**:
The profile's standing default for the currency money is priced in, used when
a request names no Display currency. It is optional and it is a target, not a
guarantee: with none chosen, or with no rate to apply, amounts stay in the
currencies they are already in.
_Avoid_: base currency, default currency, local currency, primary currency

**Display currency**:
The currency a request asks one response to be priced in, overriding Home
currency for that response alone. Stored originals never change, and a
response with no rate to apply comes back split by currency rather than
converted.
_Avoid_: target currency, converted currency, view currency

**Category**:
One node of MoneyBin's spending taxonomy, referenced by id. The taxonomy ships
with a default set and the user may add to it.
_Avoid_: label, classification, bucket, tag

**Tag**:
A user-authored slug label on a Transaction. Unlike a Category it belongs to no
taxonomy at all, and a Transaction may carry many.
_Avoid_: label, category, keyword, flag

**Rule**:
A user-owned pattern that assigns a Category deterministically, with neither a
model nor a person in the loop.
_Avoid_: filter, mapping, matcher, automation

**Merchant**:
The counterparty a Transaction was with, resolved to one canonical identity
across the spellings each source uses.
_Avoid_: payee, vendor, counterparty, entity

### Identity and review

**Account**:
One real financial account held at an Institution, identified by a canonical
id — MoneyBin's own once the account is linked, the source's own key until
then. See **"Account"** under Flagged ambiguities.
_Avoid_: item, login, connection, ledger, profile

**Native reference**:
A source's own identifier for something MoneyBin has its own id for.
_Avoid_: source id, external id, foreign key, raw id

**Link**:
An accepted, reversible binding from one Native reference to one canonical
entity.
_Avoid_: mapping, alias, binding, association

**Match**:
A scored pair or group of rows judged to represent the same real-world event.
_Avoid_: duplicate, dedup, merge, pairing

**Transfer**:
A matched pair of Transactions that are the two sides of one movement between
the user's own accounts.
_Avoid_: internal transaction, contra entry, double entry, self-transfer

**Proposal**:
An inference offered for ratification rather than applied.
_Avoid_: suggestion, candidate, recommendation, draft

**Decision**:
The recorded answer to one Review queue item, carrying who decided and when. It
ratifies a Proposal where one was offered, and otherwise supplies the answer
itself. Whether it can be reversed depends on the queue.
_Avoid_: approval, resolution, verdict, vote

**Review queue**:
The items of one kind still awaiting Decisions. Most carry a Proposal; the
categorization queue carries transactions with no candidate offered.
_Avoid_: inbox, backlog, pending list, worklist

**Confirm**:
The visible ratification step an uncertain inference or a destructive change
must pass before it takes effect. Silence is reserved for near-certain signals
with little at stake.
_Avoid_: prompt, approval, checkpoint, gate

**Actor**:
Who caused a recorded change. Two vocabularies are in use — see **"Actor"**
under Flagged ambiguities.
_Avoid_: user, author, owner, source

### Investments

**Security**:
A tradable instrument, resolved to one canonical identity across the brokers
that report it.
_Avoid_: ticker, symbol, instrument, asset

**Holding**:
A position in one Security in one Account at a point in time.
_Avoid_: position, balance, stake

**Lot**:
One acquisition of a Security, carrying its own acquisition date and its cost
basis where the source supplied one. A Lot opened without one is marked as
such rather than reading as a zero basis.
_Avoid_: tranche, purchase, batch, parcel

**Asset**:
A thing of value priced by appraisal or estimate rather than a market quote — a
house, a car, jewelry. If a market ticker prices it, it is an investment.
_Avoid_: holding, property, possession, item

**Price basis**:
What the publisher of a price series says it did to the numbers: `raw`,
`split_adjusted`, or `split_and_dividend_adjusted`. Declared by whatever
supplied the series, never inferred from the numbers.
_Avoid_: adjustment, price type, series type

### Surfaces

**Surface**:
One of the ways MoneyBin is driven: the CLI, the MCP server, or direct SQL.
_Avoid_: interface, client, frontend, channel, endpoint

**Parity**:
The guarantee that the same outcome is reachable from the CLI and from MCP. It
is functional, not name-for-name, and exempts secret material and hands-on
operator work, which stay CLI-only. See **"Parity"** under Flagged
ambiguities.
_Avoid_: symmetry, feature parity, mirroring, equivalence

**Response envelope**:
The one response shape MCP tool results and CLI `--output json` take, routed
through `render_or_json`. Six operator and operations-metadata commands
(`db query`, `db info`, `db ps`, `stats`, `logs`, `migrate status`) keep their
own JSON shapes by design; the CLI reference names them. Always qualified; bare
"envelope" is not a term.
_Avoid_: envelope, payload, wrapper, result object

**Report**:
A named, re-runnable answer to one money question, with declared columns,
reachable identically from the CLI and from MCP. One either arrives with the
code — MoneyBin's own or an Analysis package's — or is saved by the user from
SQL of their own.
_Avoid_: view, query, dashboard, chart, analysis

**Profile**:
One isolated MoneyBin setup — its own database, its own encryption key, its own
logs and configuration.
_Avoid_: workspace, account, environment, instance, tenant

### Privacy

**Data class**:
What a column actually holds, in privacy terms, declared per column across
`core` and `app`. Columns elsewhere fall back to a floor rather than a
declaration.
_Avoid_: tag, label, PII type, classification

**Sensitivity tier**:
The ordered severity band a Data class belongs to. Always qualified — see
**"Tier"** under Flagged ambiguities.
_Avoid_: tier, level, severity, sensitivity

**Redaction**:
Replacing a value on its way across a Surface boundary, according to its Data
class.
_Avoid_: masking, sanitization, scrubbing, anonymization, obfuscation

**Consent**:
The user's recorded, revocable grant pairing one feature category with the one
AI backend that may receive data for it. It is a ledger of what the user
intends: the gate that withholds data per call is not yet built.
_Avoid_: permission, opt-in, authorization, approval

### Verification

**Invariant**:
A named property of the data that must hold, checked on demand. A check reports
passing, failing, a warning, or skipped. See **"Invariant"** under Flagged
ambiguities.
_Avoid_: check, constraint, assertion, validation

**Doctor**:
The run that executes the Invariants and reports how many hold — the project's
trust artifact. Some sample rather than scan, and some report as skipped.
_Avoid_: health check, diagnostics, lint, validate

**Scenario**:
A whole-system test of one end-to-end behavior against a real database. Most
drive ingestion and judge the result against independently derived
expectations; others exercise infrastructure the pipeline depends on.
_Avoid_: e2e test, integration test, fixture, golden test

**Persona**:
The parameterized description of a synthetic person whose financial life the
generator produces.
_Avoid_: profile, fixture, sample user, archetype

**Ground truth**:
The expectations a Scenario judges against, derived independently of the
pipeline — emitted by the synthetic generator, or hand-authored before the
run.
_Avoid_: expected output, golden data, answer key

### Extension

**Analysis package**:
A separately shipped unit of analysis that extends MoneyBin's warehouse. Always
qualified — see **"Package"** under Flagged ambiguities.
_Avoid_: bare "package", plugin, module, add-on

**Quality scale**:
The declared maturity level an extension ships at, from bronze to platinum.
_Avoid_: tier, level, grade, rating

## Relationships

- An **Institution** holds many **Accounts**; an **Account** has many
  **Transactions**
- A **Provider** ingests one source into **Raw**; **Raw** feeds **Staging**,
  **Staging** feeds **Core**
- A **Report** reads the warehouse: a model in the `reports` schema reads
  **Core** and **App state**, while a saved one may also read **Raw** and
  **Staging**
- **App state** is joined into **Core**, and no derived **Core** value is
  snapshotted back into it
- An ingested row carries a **Source type** from **Raw** onward, with **Source
  origin** alongside it wherever native identifiers need scoping; **Core**
  collapses origin where it merges, keeps the winner's type, and mints a type
  for what it derives. A row with no source carries neither
- Every **Golden record** collapses one or more source rows; **Provenance**
  recovers its contributors, though only **Transactions** have a relation
  dedicated to it
- A **Match** groups source rows; a **Transfer** pairs two **Transactions**
  across two **Accounts**
- A **Link** binds one **Native reference** to one **Account**, **Merchant**,
  or **Security**
- A **Review queue** item waits until a **Decision** resolves it, whether or
  not a **Proposal** was offered; every **Decision** names an **Actor**
- A **Confirm** stands between an uncertain inference or a destructive change
  and its effect
- An **Account** holds many **Holdings**; a **Holding** is composed of **Lots**
  of one **Security**
- A **Profile** owns exactly one database and everything in it
- A **Data class** is declared for each **Core** and **App state** column,
  fixing its **Sensitivity tier** and how **Redaction** treats it
- The MCP server and CLI `--output json` return the same **Response
  envelope**, apart from six operator and operations-metadata commands the
  CLI reference names; direct SQL returns rows
- A **Split** divides one **Transaction**; an unsplit **Transaction** is still
  one **Transaction line**
- An ingestion **Scenario** judges what the pipeline produced against its
  **Ground truth**, which for generated data comes from its **Persona**

## Flagged ambiguities

- **"Provider"** carries four unrelated meanings: the in-tree ingestion
  component (**Provider**), the third-party financial aggregator whose data
  arrives by **Sync**, the AI vendor a **Consent** grant names, and the
  market-data or exchange-rate vendor a price adapter fetches from. Resolved:
  **Provider** keeps the ingestion sense; say **aggregator**, **AI provider**,
  and **market-data vendor** for the other three. Prices fetched from a
  market-data vendor are ingestion, but they arrive by none of **Import**,
  **Sync**, or **Connect**; that pathway has no name yet. Prices that ride in
  on an aggregator's feed arrive by **Sync** like any other row.

- **"Extractor"** and **"Loader"** named the file-driven and sync-driven halves
  of ingestion before one Protocol unified them. Resolved: **Provider** is the
  term, and it covers only what implements the Protocol — a class that merely
  parses a file into an intermediate shape is not one, whatever it is named.
  The `extractors/` directory, the residual `loaders/` package, and the
  Providers' own class names still carry the old term. They are internal
  naming, so they migrate as they are touched.

- **"Adapter"** carries three senses: the `moneybin/adapters/` package, which
  renders a service or orchestration result as the response both surfaces
  emit; the **price adapter** that fetches from a market-data vendor; and the
  loose use in `test_adapter_layering.py` for the surface modules it guards —
  the MCP tools and CLI commands. Resolved: unqualified **Adapter** is the
  rendering package; say **price adapter** and **surface module** for the
  other two.

- **"Account"** carries four meanings across the sources MoneyBin reads: the
  real account at an institution, a source's own identifier for it, a login
  covering several accounts at once, and, loosely, the user's own MoneyBin
  installation. Resolved: **Account** is the real account, named by its
  canonical id — MoneyBin's own once the account is linked, the source's own
  key until then, so it is not unconditionally opaque. Say **Native reference**
  for a source's identifier, **connection** for a login covering several, and
  **Profile** for the installation.

- **"Transaction"** carries three unrelated meanings: the money movement
  (**Transaction**), an entry in the separate investment ledger, and a database
  transaction. Resolved: **Transaction** is the money movement; say
  **investment transaction** for the ledger entry; leave the database sense to
  context, and never abbreviate any of the three to "txn" in prose.

- **"Tier"** carries at least six unrelated meanings: privacy severity
  (**Sensitivity tier**), the AI data-flow bands a **Consent** grant is written
  against, the numbered matcher stages, the two-tier category-source bridge the
  categorizer runs against, extension maturity (**Quality scale**), and the
  high/medium/low confidence band on a smart-import inference. Two of these
  are types literally named `Tier`, one in privacy and one in ingestion, with
  different value sets. Resolved: always qualify. Bare "tier" is not a term.

- **"Audit"** carries three unrelated meanings: a data-invariant check that runs
  against a model, the trail recording **App state** mutations, and the
  general sense of examining something. Resolved: say **Invariant** for the
  check, **audit log** for the trail, and **review** for the general sense.

- **"Invariant"** names both a property of the data that **Doctor** checks and a
  numbered rule of the codebase that reviewers enforce against new code.
  Resolved: **Invariant** is the data property; say **architecture invariant**
  for the codebase rule.

- **"Recipe"** carries four unrelated meanings: the recovery steps offered when
  an **Invariant** fails, the learned deterministic instructions for parsing one
  PDF layout, the curated library of built-in **Reports**, and the contributor
  procedure for turning a bug report into a permanent **Scenario**. Resolved:
  always qualify — **recovery recipe**, **parse recipe**, **report recipe**,
  **bug-report recipe**. For the ordinary how-to sense say **steps**; bare
  "recipe" is not a term.

- **"Seed"** carries four unrelated meanings: reference data shipped in the
  repository, the untyped payload storage a catch-all source writes into, the
  deterministic starting value the synthetic generator uses, and the verb for
  pre-populating anything. Resolved: always qualify — **seed data**, **seed
  payload**, **random seed**. Bare "seed" is not a term.

- **"Source"** carries three meanings that must not collapse: what kind of
  source produced a row (**Source type**), the institution or exporter behind
  it (**Source origin**), and the file it came from. Bare "source" keeps its
  ordinary sense — the external system or dataset being ingested. Resolved:
  qualify it wherever one of the three specific meanings is meant.

- **"Actor"** carries two different value sets: the surface or internal driver
  recorded on an audit-log entry, and the domain actor recorded on a
  **Decision**, where an agent ratifying a **Confirm** counts as the user rather
  than as automation. Resolved: always qualify — **audit actor** and **deciding
  actor**. Whether the two should converge is an open question.

- **"Provenance"** and **"lineage"** are used interchangeably but name different
  things: **Provenance** traces a **Golden record** back to its source rows;
  lineage traces a column back through the SQL that produced it, which is how a
  **Data class** is resolved. Resolved: keep both, never as synonyms. Code
  also calls a **Report**'s upstream tables and a conversion's rate evidence
  "provenance"; the first is lineage, the second is neither.

- **"Package"** names both an **Analysis package** and an ordinary Python
  package, and the repository contains both. Resolved: the analysis sense is
  always qualified; bare "package" means the Python one.

- **"Parity"** and **"symmetry"** are both used for the CLI/MCP guarantee.
  Resolved: **Parity** is the term.

## Open questions

- **Should "Actor" have one vocabulary or two?** The audit-log value set and the
  **Decision** value set answer different questions — which surface drove this,
  versus who owns this judgement — but nothing records that they are meant to
  differ, so a reader meeting the second one first will misread the first.

- **Is "confidence" one thing?** Smart import unified its three channels onto a
  single contract that grades an inference high, medium, or low. Matching
  expresses confidence as a numeric score against its own thresholds instead.
  Whether these are one concept measured two ways or two concepts sharing a
  word is unresolved, and the smart-import band's type name collides with
  **Sensitivity tier**.

- **Is "connection" a first-class term?** It appears in **Source origin**, in
  **Connect**, in the aggregator sense of a login covering several
  **Accounts**, and as a stored Google Sheet connection — the only one of the
  four the code defines. Either it earns an entry or each use
  should name what it actually means.
