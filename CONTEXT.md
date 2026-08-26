# MoneyBin

Personal financial data platform: it ingests a person's financial records from
files and connected services, resolves them into one canonical warehouse, and
answers questions about them through a CLI, an MCP server, and direct SQL.

This is the project's shared vocabulary. Use these terms exactly in new and
edited prose, code, commit messages, issues, and conversation. Where a term is
listed under `_Avoid_`, it means the same thing and should not be used. Prose
and code written before this glossary migrate as they are touched, so the
glossary states what the repository is converging on rather than claiming it
already complies.

MoneyBin is a single context. The same words mean the same things in ingestion,
analysis, and serving; the groupings below are for reading, not boundaries.

## Language

### Ingestion

**Provider**:
An in-tree component that ingests one external source into the Raw layer.
Always qualified when the aggregator sense is meant — see **"Provider"** under
Flagged ambiguities.
_Avoid_: extractor, loader, importer, adapter, connector

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
A saved, reusable description of one source layout, so a file that has been
read once can be read again without asking.
_Avoid_: profile, template, mapping, layout, schema

**Inbox**:
The watched folder where a user drops files for unattended Import.
_Avoid_: watch folder, dropbox, queue, staging folder

**Refresh**:
The pass that brings the warehouse up to date after new data lands, covering
matching, transformation, categorization, identity, and rates.
_Avoid_: rebuild, update, sync, reprocess

**Source type**:
The ingestion pathway a row arrived by. Carried on every row from Raw onward.
_Avoid_: source, format, provider, channel

**Source origin**:
The institution, connection, or exporter that produced a row. It scopes native
identifiers, so two institutions using the same account number stay distinct.
_Avoid_: origin, institution, connection, item

### The warehouse

**Raw**:
The layer holding ingested data untouched, re-importable from the original.
_Avoid_: bronze, landing, source layer, staging

**Staging**:
The layer that cleans, types, and unions each source into a common shape.
_Avoid_: silver, intermediate, cleansing, transform layer

**Core**:
The canonical, deduplicated, multi-source layer: one model per real-world
entity at its primary grain.
_Avoid_: gold, marts, analytics layer, warehouse

**App state**:
User-owned mutable state that no derivation can reproduce from Raw — notes,
tags, rules, settings, decisions.
_Avoid_: metadata, user data, overrides, config

**Grain**:
The one thing a single row of a model stands for.
_Avoid_: granularity, level, key, cardinality

**Golden record**:
The single canonical row that a group of matched source rows collapses to.
_Avoid_: gold record, master record, survivor, winner

**Provenance**:
The recorded trail from a Golden record back to every source row that
contributed to it. Not lineage — see Flagged ambiguities.
_Avoid_: lineage, history, audit trail, source tracking

### Money

**Transaction**:
One posted movement of money against an Account. Investment activity is a
separate ledger — see **"Transaction"** under Flagged ambiguities.
_Avoid_: entry, posting, record, txn, line item

**Sign convention**:
MoneyBin's fixed reading of a signed amount: negative is money leaving,
positive is money arriving.
_Avoid_: polarity, direction, debit/credit convention

**Home currency**:
The single currency the user's own totals are expressed in.
_Avoid_: base currency, default currency, local currency, primary currency

**Display currency**:
The currency one response is converted into for presentation. Stored originals
never change.
_Avoid_: target currency, converted currency, view currency

**Category**:
One node of MoneyBin's own canonical spending taxonomy, referenced by id.
_Avoid_: label, classification, bucket, tag

**Tag**:
A user-authored slug label on a Transaction. Unlike a Category it comes from no
fixed taxonomy, and a Transaction may carry many.
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
One real financial account held at an Institution, identified by an opaque
canonical id that no source supplies. See **"Account"** under Flagged
ambiguities.
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
An inference offered for ratification rather than applied — a candidate Link,
Match, Rule, or Category.
_Avoid_: suggestion, candidate, recommendation, draft

**Decision**:
The recorded, reversible answer to one Proposal, carrying who decided and when.
_Avoid_: approval, resolution, verdict, vote

**Review queue**:
The Proposals of one kind still awaiting Decisions.
_Avoid_: inbox, backlog, pending list, worklist

**Confirm**:
The visible ratification step an uncertain inference must pass before it takes
effect. Silence is reserved for near-certain signals.
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
One acquisition of a Security, carrying its own cost basis and acquisition
date.
_Avoid_: tranche, purchase, batch, parcel

**Asset**:
A thing of value priced by appraisal or estimate rather than a market quote — a
house, a car, jewelry. If a market ticker prices it, it is an investment.
_Avoid_: holding, property, possession, item

**Price basis**:
What the publisher of a price series says it did to the numbers: unadjusted,
split-adjusted, or split-and-dividend-adjusted. Declared by whatever supplied
the series, never inferred from the numbers.
_Avoid_: adjustment, price type, series type

### Surfaces

**Surface**:
One of the ways MoneyBin is driven: the CLI, the MCP server, or direct SQL.
_Avoid_: interface, client, frontend, channel, endpoint

**Parity**:
The guarantee that the same outcome is reachable from the CLI and from MCP. It
is functional, not name-for-name.
_Avoid_: symmetry, feature parity, mirroring, equivalence

**Response envelope**:
The one response shape every MCP tool result and every JSON CLI result takes.
Always qualified; bare "envelope" is not a term.
_Avoid_: envelope, payload, wrapper, result object

**Report**:
A named, re-runnable answer to one money question, with declared columns,
reachable identically from every Surface.
_Avoid_: view, query, dashboard, chart, analysis

**Profile**:
One isolated MoneyBin instance — its own database, its own encryption key, its
own logs and configuration.
_Avoid_: workspace, account, environment, instance, tenant

### Privacy

**Data class**:
What a column actually holds, in privacy terms, assigned per column.
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
The user's recorded, revocable grant allowing one named feature category to
send data to an AI provider.
_Avoid_: permission, opt-in, authorization, approval

### Verification

**Invariant**:
A named property of the data that must hold, checked on demand and reported as
passing or failing. See **"Invariant"** under Flagged ambiguities.
_Avoid_: check, constraint, assertion, validation

**Doctor**:
The run that executes every Invariant and reports how many hold — the project's
trust artifact.
_Avoid_: health check, diagnostics, lint, validate

**Scenario**:
A whole-pipeline test that drives an empty database through ingestion and
judges the result against independently derived expectations.
_Avoid_: e2e test, integration test, fixture, golden test

**Persona**:
The parameterized description of a synthetic person whose financial life the
generator produces.
_Avoid_: profile, fixture, sample user, archetype

**Ground truth**:
The labels the synthetic generator emits alongside the data it generated, which
a Scenario judges against.
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
  **Staging** feeds **Core**, **Core** feeds **Reports**
- **App state** is joined into **Core**, never derived from it
- Every row carries one **Source type** and one **Source origin**
- Many source rows collapse into one **Golden record**; **Provenance** records
  every contributor
- A **Match** groups source rows; a **Transfer** pairs two **Transactions**
  across two **Accounts**
- A **Link** binds one **Native reference** to one **Account**, **Merchant**,
  or **Security**
- A **Proposal** waits in a **Review queue** until a **Decision** resolves it;
  every **Decision** names an **Actor**
- A **Confirm** stands between an uncertain inference and its effect
- An **Account** holds many **Holdings**; a **Holding** is composed of **Lots**
  of one **Security**
- A **Profile** owns exactly one database and everything in it
- Every column carries one **Data class**, which fixes its **Sensitivity tier**
  and how **Redaction** treats it
- Every **Surface** returns the same **Response envelope**
- A **Scenario** judges generated data against the **Ground truth** its
  **Persona** produced

## Flagged ambiguities

- **"Provider"** carries three unrelated meanings: the in-tree ingestion
  component (**Provider**), the third-party financial aggregator whose data
  arrives by **Sync**, and the AI vendor a **Consent** grant names. Resolved:
  **Provider** keeps the ingestion sense; say **aggregator** for the financial
  third party and **AI provider** for the vendor.

- **"Extractor"** and **"Loader"** named the file-driven and sync-driven halves
  of ingestion before one Protocol unified them. Resolved: **Provider** is the
  term. The `extractors/` directory, the `*Extractor` class names, and the
  residual `loaders/` package are unmigrated, not a second pattern; they
  migrate as they are touched.

- **"Account"** carries four meanings across the sources MoneyBin reads: the
  real account at an institution, a source's own identifier for it, a login
  covering several accounts at once, and, loosely, the user's own MoneyBin
  installation. Resolved: **Account** is the real account, named by MoneyBin's
  opaque id. Say **Native reference** for a source's identifier, **connection**
  for a login covering several, and **Profile** for the installation.

- **"Transaction"** carries three unrelated meanings: the posted money movement
  (**Transaction**), an entry in the separate investment ledger, and a database
  transaction. Resolved: **Transaction** is the money movement; say
  **investment transaction** for the ledger entry; leave the database sense to
  context, and never abbreviate any of the three to "txn" in prose.

- **"Tier"** carries at least five unrelated meanings: privacy severity
  (**Sensitivity tier**), the AI data-flow bands a **Consent** grant is written
  against, the numbered matcher stages — whose labels the categorizer reuses
  for its own, unrelated stages — extension maturity (**Quality scale**), and
  the high/medium/low confidence band on a smart-import inference. Two of these
  are types literally named `Tier`, one in privacy and one in ingestion, with
  different value sets. Resolved: always qualify. Bare "tier" is not a term.

- **"Audit"** carries three unrelated meanings: a data-invariant check that runs
  against a model, the trail recording every **App state** mutation, and the
  general sense of examining something. Resolved: say **Invariant** for the
  check, **audit log** for the trail, and **review** for the general sense.

- **"Invariant"** names both a property of the data that **Doctor** checks and a
  numbered rule of the codebase that reviewers enforce against new code.
  Resolved: **Invariant** is the data property; say **architecture invariant**
  for the codebase rule.

- **"Recipe"** carries three unrelated meanings: the recovery steps offered when
  an **Invariant** fails, the learned deterministic instructions for parsing one
  PDF layout, and the curated library of built-in **Reports**. Resolved: always
  qualify — **recovery recipe**, **parse recipe**, **report recipe**. Bare
  "recipe" is not a term.

- **"Seed"** carries four unrelated meanings: reference data shipped in the
  repository, the untyped payload storage a catch-all source writes into, the
  deterministic starting value the synthetic generator uses, and the verb for
  pre-populating anything. Resolved: always qualify — **seed data**, **seed
  payload**, **random seed**. Bare "seed" is not a term.

- **"Source"** carries three meanings that must not collapse: the ingestion
  pathway (**Source type**), the institution or exporter behind a row (**Source
  origin**), and the file a row came from. Resolved: never write bare "source"
  where one of the three is meant.

- **"Actor"** carries two different value sets: the surface or internal driver
  recorded on an audit-log entry, and the domain actor recorded on a
  **Decision**, where an agent ratifying a **Confirm** counts as the user rather
  than as automation. Resolved: always qualify — **audit actor** and **deciding
  actor**. Whether the two should converge is an open question.

- **"Provenance"** and **"lineage"** are used interchangeably but name different
  things: **Provenance** traces a **Golden record** back to its source rows;
  lineage traces a column back through the SQL that produced it, which is how a
  **Data class** is resolved. Resolved: keep both, never as synonyms.

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
  **Connect**, and in the aggregator sense of a login covering several
  **Accounts**, but nothing defines it. Either it earns an entry or each use
  should name what it actually means.
