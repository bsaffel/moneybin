<!-- Last reviewed: 2026-07-13 -->
# Roadmap

MoneyBin is pre-v1. This page describes direction, not dates or release
promises. For what you can rely on today, start with [What Works
Today](features.md). For the dated record of shipped changes, see the
[Changelog](../CHANGELOG.md).

## Today

MoneyBin is a local-first financial data platform for people comfortable with a
CLI, SQL, or an MCP-enabled AI client. It imports financial files, supports
Plaid sync for cash, credit-card, and investment accounts, stores each profile
in an encrypted DuckDB database, and provides auditable reports,
categorization, investment accounting, and data-quality checks.

The Plaid link, sync, and reconcile flow is author-tested against a production
account but still needs non-author validation. The fastest safe way to explore
MoneyBin is the synthetic `moneybin demo` profile. The current install path and
supported workflows are documented in the [guides](guides/README.md).

## Now

- **Make evaluation and installation less developer-only.** Publish a
  reproducible package, improve first-run guidance, and make the demo path a
  reliable way to reach a useful first answer without real financial data.
- **Finish the trust-critical data flows.** Continue hardening account and
  security identity, import confirmation, reconciliation, and Plaid validation
  so a user can understand what was imported, changed, or needs review.
- **Complete investment reporting.** The investment ledger, Plaid ingestion,
  cost basis, and realized-gain calculations exist; market prices and
  net-worth integration remain work in progress.

## Next

- **Deepen analysis and ownership.** Budgeting, recurring-transaction review,
  export bundles, multi-currency support, and richer report lineage build on
  the current warehouse.
- **Add a local visual workflow.** A web interface will focus first on review,
  data quality, accounts, and reports rather than replacing the CLI or MCP
  surfaces.
- **Make customization safer.** Contributor-facing report, package, and
  provider contracts will mature alongside worked examples and validation
  tooling.

## Later

- **Remote and hosted use.** Authenticated remote MCP, optional hosted storage,
  and self-hosted operations require their own explicit security, consent, and
  operational contracts. They are not part of today's local product.
- **Additional data sources and domains.** More connectors, asset tracking, tax
  helpers, and specialized packages will follow demonstrated user need and a
  stable extension model.

## Not a Near-Term Promise

MoneyBin is not currently planning a native mobile app, household-shared
budgets, or a general-purpose accounting system. It also does not claim a
finished self-hosted server or polished consumer experience today. The
[audience guide](audience.md) recommends better-established alternatives when
those needs are primary.

## Engineering Design References

The repository keeps implementation history and current technical contracts in
[`docs/specs/`](specs/) and [`docs/decisions/`](decisions/). Some older design
documents use milestone labels such as `M1` or `M3`; those are engineering
reference labels, not a public delivery calendar. Only specs that define a
durable current or contributor-facing contract should be read as public design
commitments.
