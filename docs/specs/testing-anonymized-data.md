# Feature: Anonymized Data Generator

## Status
draft

## Goal

Produce a structure-preserving anonymized copy of a real user's MoneyBin database — preserving statistical properties (transaction distributions, account relationships, spending patterns, temporal cadence) while removing personally-identifying detail (real merchant names, exact amounts, exact dates, account identifiers, descriptions). The anonymized output is the **prescribed mechanism for shipping scenario fixtures and bug-report reproductions** without leaking PII.

## Background

MoneyBin is a personal financial data warehouse. Real user data is the most valuable test corpus we have — it captures format quirks, edge cases, and provider behaviors that synthetic personas cannot. But shipping real data is a privacy non-starter. The synthetic data generator ([`testing-synthetic-data.md`](testing-synthetic-data.md)) covers Level 2 realism for life-like financial histories, but cannot reproduce the *specific* bug-triggering data shape that a user encountered.

This spec fills that gap. It is a **peer child spec** of `testing-overview.md` alongside the synthetic generator: different problem (data masking pipeline vs. financial life simulator), same output layer (`synthetic` schema, raw table writes).

**Roadmap placement.** This is an **M1 (Ingestion Core)** deliverable. The anonymized real-data parity check is part of the **Ingestion-Complete gate** (see [`roadmap.md`](../roadmap.md) and `private/testing.md`): it is the enabler that turns pipeline-correctness-against-real-data from a manual, maintainer-only session into a reproducible, agent-runnable regression — the highest-leverage unlock for a single-developer/tester workflow.

### Primary use cases

1. **Automated bug reports.** When a user files a bug, the support flow (manual today, eventually automated via an agent loop) produces an anonymized snapshot of the relevant slice of their database. The snapshot becomes a fixture under `tests/scenarios/data/fixtures/<bug-id>/`. A scenario authored against that fixture per [`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md)'s recipe gives permanent regression coverage.
2. **Format-compatibility seeds.** Anonymized real CSV/OFX exports populate `tests/fixtures/csv_formats/` per the deferred [`testing-csv-fixtures.md`](testing-csv-fixtures.md).
3. **Distribution-faithful evaluation corpora.** Categorization and matching evaluations run against anonymized real data alongside synthetic personas, providing a sanity check that scores aren't gaming the synthetic generator's biases.

### Why this is a separate engine

The synthetic generator builds data from declared persona parameters. The anonymizer transforms existing data through a masking pipeline. They share the output schema but nothing else — different inputs, different operations, different correctness criteria. Co-locating them would couple a creative simulator with a deterministic transformer.

### Related specs

- [`testing-overview.md`](testing-overview.md) — umbrella; this is a child spec.
- [`testing-synthetic-data.md`](testing-synthetic-data.md) — peer; same output layer, different engine.
- [`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md) — primary consumer; defines the bug-report recipe that depends on this anonymizer.
- [`privacy-data-protection.md`](privacy-data-protection.md) — the privacy guarantees this spec must not violate.
- [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) — the redaction primitives some of this work will reuse.

## Design Principles

1. **Statistical similarity, not identity.** Anonymized output must preserve distributions (amount histograms, merchant frequencies, category mix, inter-arrival times, account relationships) within configurable tolerance. It must not preserve any single record's identifying detail.
2. **Re-identification resistance.** No combination of anonymized fields should be sufficient to recover a real merchant, real amount, real date, or real account. Standards target: k-anonymity ≥ k for all quasi-identifier projections; documented threat model.
3. **Round-trippable through the existing pipeline.** Anonymized data writes to the same raw schemas as real imports, so the full `transform → match → categorize` pipeline runs unchanged on it. This is what makes scenario fixtures real-shaped.
4. **Ground-truth preservation where present.** If the source database has user-confirmed categorizations or transfer pairs, the anonymizer preserves those labels (mapped to anonymized IDs) so evaluations have ground truth.
5. **Deterministic given a seed.** Re-running the anonymizer against the same source DB with the same seed produces the same output. Required for reproducible scenario fixtures.

## Requirements

Draft requirements (firmed up at `draft → ready`). The masking pipeline must satisfy the five Design Principles above. Requirement areas:

- Merchant name substitution (canonical merchant catalog → anonymized substitutes preserving category)
- Amount perturbation (distribution-preserving, within configurable noise bounds)
- Date shifting (preserves day-of-week, day-of-month, and inter-arrival distributions)
- Account ID replacement (consistent within a snapshot; preserves account relationships)
- Description scrubbing (PII patterns removed; structure preserved)
- Category and ground-truth label preservation
- Differential privacy bounds on aggregate statistics
- CLI: `moneybin synthetic anonymize --source <profile> --target <profile> --seed N`

## Data Model

Output writes to the existing `synthetic` schema raw tables, identical to the synthetic generator. No new tables.

## Implementation Plan

Sequenced for M1 (Ingestion Core); Stage 1 of this plan is the load-bearing slice for the Ingestion-Complete gate.

**Stage 1 — masking primitives + CLI (the parity-fixture path).**
1. `moneybin synthetic anonymize --source <profile> --target <profile> --seed N` CLI scaffold, writing to the `synthetic` schema raw tables (same output contract as the synthetic generator).
2. Deterministic-given-seed masking primitives: merchant substitution (category-preserving), amount perturbation (distribution-preserving), date shifting (cadence-preserving: day-of-week / day-of-month / inter-arrival), account-ID replacement (relationship-preserving), description scrubbing (reuse the PII patterns from `privacy-and-ai-trust.md`).
3. Ground-truth label preservation: confirmed categorizations and transfer pairs mapped to anonymized IDs so evaluations retain ground truth.
4. Scenario-runner integration: a `setup.persona: from-db` (anonymized-snapshot) path so a scenario can load an anonymized fixture — the mechanism behind the Gate A real-data parity check.
5. Per-primitive unit tests + a round-trip e2e (anonymize → full `transform → match → categorize` → Tier-1 structural invariants).

**Stage 2 — re-identification resistance (hardening, before any corpus leaves the maintainer's machine).**
6. k-anonymity check over quasi-identifier projections; documented threat model.
7. Re-identification attack tests (amount+date join deanonymization; k-anonymity violation).
8. Statistical-similarity tests (KS-test / Wasserstein distance within configured tolerance).

## CLI Interface

```
moneybin synthetic anonymize --source <profile> --target <profile> --seed N [--noise-bounds <preset>]
```

## Testing Strategy

- Unit tests for each masking primitive (merchant substitution, amount perturbation, date shifting)
- Statistical similarity tests: KS-test or Wasserstein distance between source and anonymized distributions, within configured tolerance
- Re-identification tests: attempt known re-identification attacks (k-anonymity violation, deanonymization via amount + date joins)
- End-to-end: anonymize a real DB → run full pipeline → verify outputs are pipeline-valid (FK integrity, sign convention, etc.)

## Dependencies

- `testing-synthetic-data.md` — shared output schema and ground-truth conventions
- `privacy-data-protection.md` — encryption/at-rest guarantees
- `privacy-and-ai-trust.md` — redaction primitives (regex patterns, PII detectors)

## Out of Scope

- Anonymization of derived analytics tables (`core.fct_balances_daily`, etc.) — anonymize raw, let the pipeline derive.
- Cross-user anonymization for community datasets — covered separately under `categorization-overview.md`'s community model.
- Real-time anonymization streams — this is a batch operation against snapshots.
