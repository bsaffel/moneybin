# Synthetic Data Generator

Generate realistic, deterministic financial data for testing, demos, and development. Persona-based: each persona represents a different financial life with distinct accounts, spending patterns, and income sources.

## Quick Start

```bash
# Generate data for a persona into a dedicated profile
moneybin synthetic generate --persona basic --profile test-basic

# Switch to the test profile and explore
moneybin --profile test-basic import status
moneybin --profile test-basic db query "SELECT COUNT(*) FROM core.fct_transactions"
```

## Available Personas

| Persona | Description | Accounts | Characteristics |
|---------|-------------|----------|----------------|
| `basic` | Single income, standard expenses | Checking, savings, credit card | Straightforward financial life, regular paycheck |
| `family` | Dual income, kids | Multiple checking/savings, credit cards | Mortgage, childcare, higher spending volume |
| `freelancer` | Variable income, business expenses | Business + personal accounts | Irregular income, multiple revenue streams, business costs |

## What Gets Generated

Each persona produces multi-year transaction histories with:

- **Multiple accounts** — checking, savings, credit cards appropriate to the persona
- **Realistic merchants** — ~200 real merchant names across 14+ spending categories (grocery, dining, transport, subscriptions, etc.)
- **Realistic amounts** — log-normal distributions matching real-world spending patterns
- **Seasonal patterns** — higher spending around holidays, back-to-school, etc.
- **Income** — regular paychecks, freelance invoices, or dual income depending on persona
- **Transfers** — inter-account movements (checking to savings, credit card payments)
- **Recurring transactions** — subscriptions, bills, loan payments at regular intervals

## Deterministic Seeding

Same persona + same seed = same data every time. This makes synthetic data suitable for:

- **Reproducible tests** — test results are deterministic
- **Feature development** — work against a consistent dataset
- **Demos** — show the same data in every demo run

## Ground Truth

Generated data includes a `synthetic.ground_truth` table with known-correct labels:

- **Category labels** — every transaction has a verified category
- **Transfer pairs** — inter-account transfers are labeled as matched pairs

This enables automated accuracy testing for categorization rules, merchant mappings, and transfer detection.

## Commands

```bash
# Generate synthetic data for a persona
moneybin synthetic generate --persona <name> --profile <profile>

# Reset (wipe and regenerate) — refuses to wipe non-generated profiles
moneybin synthetic reset --persona <name> --profile <profile>

# Run scenario verification suites against synthetic fixtures
moneybin synthetic verify --list
moneybin synthetic verify --scenario basic-full-pipeline
moneybin synthetic verify --all --output json
```

`synthetic verify` runs whole-pipeline scenarios — generate → transform → match → categorize — and reports assertions, expectations, and evaluations. See [`testing-scenario-runner`](../specs/testing-scenario-runner.md) for the model and [CONTRIBUTING.md](../../CONTRIBUTING.md) for the developer workflow.

### Safety

- `reset` checks for the `synthetic.ground_truth` table before wiping — it will not destroy real financial data
- All generated data is tagged with `source_origin = 'synthetic_{persona}'` (e.g., `synthetic_basic`, `synthetic_family`) and `source_file` starting with `synthetic://`
- Generated profiles are fully isolated from your real data (separate profile = separate database)

## Data Sources

Synthetic data is written to the same raw tables as real imports:

| Table | Content |
|-------|---------|
| `raw.ofx_accounts` | Account metadata (OFX-format accounts) |
| `raw.ofx_transactions` | Transaction data (OFX-format transactions) |
| `raw.ofx_balances` | Balance snapshots |
| `raw.tabular_accounts` | Account metadata (tabular-format accounts) |
| `raw.tabular_transactions` | Transaction data (tabular-format transactions) |

After generation, run `moneybin transform apply` to build core tables from the synthetic raw data. The full pipeline (staging views, core tables, categorization) works identically on synthetic and real data.

## YAML-Driven Architecture

Personas and merchants are defined in declarative YAML files:

- **Persona configs** (`src/moneybin/testing/synthetic/data/personas/`) — define accounts, income sources, spending budgets, transfer patterns
- **Merchant catalogs** (`src/moneybin/testing/synthetic/data/merchants/`) — define merchants per spending category with realistic name patterns and amount ranges

This makes it straightforward to add new personas or expand the merchant catalog without changing generator code.
