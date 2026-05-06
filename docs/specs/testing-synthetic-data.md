# Feature: Synthetic Data Generator

## Status
<!-- draft | ready | in-progress | implemented -->
implemented

## Goal

Produce realistic, deterministic, multi-year financial histories for fictional personas
— enabling integration testing, demos, feature development, and autonomous verification
without real financial data. Modeled on Beancount's `bean-example`; targets Level 2
realism (real 2026 brand names, log-normal distributions, seasonal patterns) with an
architecture designed for Level 3 graduation through richer seed data, not engine
rewrites.

## Background

- [`testing-overview.md`](testing-overview.md) — parent
  umbrella spec. Defines verification tiers, scenario format, persona catalog, and
  assertion library. This spec implements the generator; the scenario runner is a peer
  concern.
- `private/strategy/core-concerns.md` §10 — original requirements scaffold for testing &
  synthetic data.
- [`smart-import-tabular.md`](smart-import-tabular.md) — defines
  `Database.ingest_dataframe()`, the bulk write primitive the generator uses.
- [`privacy-data-protection.md`](privacy-data-protection.md) — `Database` class; all writes go through
  the encrypted connection.
- [`categorization-overview.md`](categorization-overview.md) — ground-truth category
  labels enable Tier 3 scored evaluation of the categorization pipeline.
- [`matching-overview.md`](matching-overview.md) — ground-truth transfer pair IDs
  enable Tier 3 scored evaluation of transfer detection.
- `CLAUDE.md` "Architecture: Data Layers" — raw/prep/core layering. Generated data
  enters at raw and flows through the full pipeline.
- `private/strategy/mvp-roadmap.md` — M0 deliverable. The generator is an
  infrastructure multiplier: every feature after it has proper test data.

### Companion spec

[`testing-anonymized-data.md`](testing-anonymized-data.md) (forthcoming) defines a
separate engine for structure-preserving anonymization of real databases. Different
problem (data masking pipeline vs. financial life simulator), same output layer
(`synthetic` schema, raw table writes). Designed in the same session to ensure
architectural alignment.

### Competitive context

Most personal finance tools ship with no test data infrastructure. Beancount's
`bean-example` is the gold standard — a single-persona generator that produces realistic
ledger entries. MoneyBin's generator extends this model with multiple personas, ground-
truth labels for ML scoring, deterministic seeding for reproducibility, and a declarative
YAML architecture that separates financial life definitions from engine logic.

---

## Requirements

### Data generation
1. Generate multi-year transaction histories for three v1 personas: `basic` (single
   income, simple spending), `family` (dual income, joint + individual accounts),
   `freelancer` (irregular income, business + personal).
2. All randomness flows through a single seeded `Random` instance per generation run.
   Same persona + seed + years = byte-identical output for a given generator version.
3. Write generated transactions to raw tables (`raw.ofx_transactions`,
   `raw.tabular_transactions`) conforming to existing schemas. `source_file` uses
   synthetic URIs: `synthetic://{persona}/{seed}/{year}` for transactions,
   `synthetic://{persona}/{seed}/{account-name}` for accounts.
4. Write generated account records to raw account tables (`raw.ofx_accounts`,
   `raw.tabular_accounts`).
4a. Write opening balance snapshots: to `raw.ofx_balances` for OFX-sourced accounts,
    and as a populated `balance` column (running balance) on `raw.tabular_transactions`
    for tabular-sourced accounts.
5. Persona accounts declare their `source_type` (`ofx`, `csv`, etc.) to control which
   raw table they write to — exercising the multi-source union in core models.
6. Follow the accounting sign convention: negative = expense, positive = income.
   Amounts are `DECIMAL(18,2)`.

### Ground truth
7. Write ground-truth labels to `synthetic.ground_truth` with expected category and
   transfer pair ID per transaction.
8. Create the `synthetic` schema on demand when the generator runs. The `Database`
   class never creates it during normal initialization.
9. The existence of `synthetic.ground_truth` in a profile serves as the marker that the
   profile was created by the generator.

### Configuration
10. Load persona definitions from YAML config files shipped with the package
    (`src/moneybin/testing/synthetic/data/personas/`).
11. Load merchant catalogs from shared YAML files
    (`src/moneybin/testing/synthetic/data/merchants/`). V1 ships ~200 real 2026 brand
    names across ~14 categories.
12. Validate all YAML config at load time via Pydantic models. Invalid config fails
    fast with clear error messages.

### CLI behavior
13. Auto-derive profile name from persona (`basic`→`alice`, `family`→`bob`,
    `freelancer`→`charlie`). Allow `--profile` override.
14. Refuse to generate into a profile that already has data. Provide actionable guidance
    directing the user to `moneybin synthetic reset`.
15. Run `sqlmesh run` automatically after generation to materialize the full pipeline
    (raw → prep → core).
16. Display a summary after generation: accounts created, transaction count, date range,
    seed used.

### Safety
17. `moneybin synthetic reset` requires explicit `--persona` or `--profile`. No default
    target, no "reset all."
18. Refuse to reset a profile that was not created by the generator (no
    `synthetic.ground_truth` table present). Direct the user to `moneybin db destroy`
    for non-generated profiles.
19. All destructive operations require explicit target specification.
20. Without `--yes`, `reset` prompts for confirmation before destroying data.

### Realism (Level 2 target)
21. Merchant catalogs use real 2026 brand names weighted by popularity, with per-merchant
    amount distributions (log-normal).
22. Bank-statement-style transaction descriptions (`description_prefix` per merchant,
    e.g., "WAL-MART #4521 AUSTIN TX") to exercise merchant normalization.
23. Day-of-week bias (more dining on Friday/Saturday) and seasonal modifiers (holiday
    spending spikes, January belt-tightening, summer travel).
24. Realistic income schedules: biweekly salary with annual raises, freelancer irregular
    invoices with payment delays.
25. Recurring charges on consistent dates with price increases over time (subscription
    economy modeling).
26. Transfer pairs between accounts (checking→savings, credit card payments) with
    realistic timing.

---

## Architecture

### Declarative pipeline

YAML config defines financial lives; a generic engine interprets them. All behavior
comes from data, not code. Adding a persona means adding a YAML file, not writing
Python.

```
YAML Config Layer          Engine Layer              Output Layer
─────────────────         ──────────────            ─────────────
personas/                  GeneratorEngine           raw.ofx_transactions
  basic.yaml               ├─ AccountSetup           raw.tabular_transactions
  family.yaml              ├─ IncomeGenerator        raw.ofx_balances
  freelancer.yaml          ├─ RecurringGenerator     synthetic.ground_truth
                           ├─ SpendingGenerator
merchants/                 └─ TransferGenerator
  grocery.yaml
  dining.yaml
  ...
```

### Package layout

```
src/moneybin/testing/synthetic/
├── __init__.py
├── engine.py              # GeneratorEngine orchestrator
├── generators/
│   ├── __init__.py
│   ├── income.py          # Salary, freelance invoices
│   ├── recurring.py       # Rent, subscriptions, insurance
│   ├── spending.py        # Discretionary by category
│   └── transfers.py       # Account-to-account moves
├── models.py              # Pydantic models for persona/merchant YAML schemas
├── writer.py              # Raw table writer + synthetic.ground_truth writer
├── data/
│   ├── personas/
│   │   ├── basic.yaml
│   │   ├── family.yaml
│   │   └── freelancer.yaml
│   └── merchants/
│       ├── grocery.yaml
│       ├── dining.yaml
│       ├── transport.yaml
│       ├── utilities.yaml
│       ├── entertainment.yaml
│       ├── shopping.yaml
│       ├── health.yaml
│       ├── travel.yaml
│       ├── subscriptions.yaml
│       ├── kids.yaml
│       ├── personal_care.yaml
│       ├── insurance.yaml
│       ├── education.yaml
│       └── gifts.yaml
└── seed.py                # Seeded Random wrapper
```

### Generation pipeline

1. **Init** — `GeneratorEngine.__init__(persona, seed, years)` loads persona YAML,
   loads referenced merchant catalogs, creates seeded `Random` wrapper.
2. **Account setup** — `AccountSetup` creates account records from persona config,
   writes to raw account tables via `Database.ingest_dataframe()`.
3. **Month-by-month loop** — engine walks each month in the date range. For each month:
   - `IncomeGenerator` produces salary deposits or freelance invoices per the persona's
     income schedule. Annual raises applied at year boundaries.
   - `RecurringGenerator` produces fixed-date charges (rent, subscriptions, insurance).
     Price increases applied per schedule.
   - `SpendingGenerator` samples discretionary transactions: selects merchants from
     catalogs using weighted random selection, draws amounts from per-merchant log-normal
     distributions, assigns days using day-of-week weights, applies seasonal modifiers.
   - `TransferGenerator` produces account-to-account transfers (savings deposits,
     credit card payments). Both sides of each transfer share a `transfer_pair_id`.
4. **Write** — `writer.write()` converts generated transactions to Polars DataFrames
   conforming to raw table schemas. Writes to raw tables via
   `Database.ingest_dataframe()`. Writes ground truth to `synthetic.ground_truth`.
5. **Pipeline** — CLI runs `sqlmesh run` to materialize prep and core layers.

### Seeded randomness

All randomness flows through a single `SeededRandom` wrapper initialized with the
user-provided seed. Generator modules receive the wrapper — never import `random`
directly or use ambient state. This makes determinism structural, not by convention.

```python
class SeededRandom:
    """Wrapper around random.Random providing all stochastic operations."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)

    def log_normal(self, mean: float, stddev: float) -> float: ...
    def weighted_choice(self, items: list, weights: list[float]) -> Any: ...
    def poisson(self, lam: float) -> int: ...
    def day_in_month(
        self, year: int, month: int, day_weights: dict[str, float] | None = None
    ) -> int: ...
```

When the generator changes (new merchants, adjusted distributions), golden snapshot
baselines are updated as part of that change. The seed contract is: identical output for
a given generator version. Baseline diffs are the review artifact.

---

## Persona YAML Schema

Persona definitions describe complete financial lives. Each file declares accounts,
income, recurring charges, discretionary spending habits, and transfer patterns.

```yaml
persona: family
profile: bob
description: "Dual-income family, joint + individual accounts, child-related expenses"
years_default: 3

accounts:
  - name: "Our Chase Checking"
    type: checking
    source_type: ofx
    institution: "Chase Bank"
    opening_balance: 4500.00
  - name: "Savings at Ally"
    type: savings
    source_type: ofx
    institution: "Ally Bank"
    opening_balance: 15000.00
  - name: "Alice Costco Visa"
    type: credit_card
    source_type: csv
    institution: "Citi"
    opening_balance: 0.00
  - name: "Bob Amazon Card"
    type: credit_card
    source_type: csv
    institution: "Chase Bank"
    opening_balance: 0.00

income:
  - type: salary
    account: "Our Chase Checking"
    amount: 4200.00
    schedule: biweekly
    pay_day: friday
    annual_raise_pct: 3.0
    description_template: "DIRECT DEP {employer}"
    employer: "Acme Corp"
  - type: salary
    account: "Our Chase Checking"
    amount: 3400.00
    schedule: biweekly
    pay_day: friday
    annual_raise_pct: 2.5
    description_template: "DIRECT DEP {employer}"
    employer: "TechStart Inc"

recurring:
  - category: housing
    description: "Mortgage Payment"
    account: "Our Chase Checking"
    amount: 2100.00
    day_of_month: 1
  - category: utilities
    description: "Electric Company"
    account: "Our Chase Checking"
    amount: { mean: 145.00, stddev: 35.00 }
    day_of_month: 15
  - category: subscriptions
    description: "Netflix"
    account: "Alice Costco Visa"
    amount: 17.99
    day_of_month: 8
    price_increases:
      - after_months: 18
        new_amount: 19.99

spending:
  categories:
    - name: grocery
      merchant_catalog: grocery
      monthly_budget: { mean: 850.00, stddev: 120.00 }
      transactions_per_month: { mean: 10, stddev: 2 }
      accounts: ["Our Chase Checking", "Alice Costco Visa"]
      account_weights: [0.6, 0.4]
      seasonal_modifiers:
        november: 1.3
        december: 1.4
        january: 0.8
    - name: dining
      merchant_catalog: dining
      monthly_budget: { mean: 400.00, stddev: 80.00 }
      transactions_per_month: { mean: 8, stddev: 3 }
      accounts: ["Alice Costco Visa", "Bob Amazon Card"]
      account_weights: [0.5, 0.5]
      day_of_week_weights:
        friday: 2.0
        saturday: 2.0
    - name: kids_activities
      merchant_catalog: kids
      monthly_budget: { mean: 300.00, stddev: 60.00 }
      transactions_per_month: { mean: 4, stddev: 1 }
      accounts: ["Our Chase Checking"]
      seasonal_modifiers:
        june: 1.5
        august: 1.8
        september: 1.3

transfers:
  - from: "Our Chase Checking"
    to: "Savings at Ally"
    amount: 500.00
    schedule: monthly
    day_of_month: 5
    description_template: "TRANSFER TO SAVINGS"
  - from: "Our Chase Checking"
    to: "Alice Costco Visa"
    amount: statement_balance
    schedule: monthly
    day_of_month: 20
    description_template: "ONLINE PAYMENT"
```

### Schema design choices

- **`name`** is the human-readable account label a real person would choose ("Our Chase
  Checking", "Alice Costco Visa"). Used as `account_name` in `raw.tabular_accounts` and
  as display context in OFX. Account names are also how income, spending, and transfer
  sections reference accounts within the persona YAML.
- **`account_id`** is generated deterministically by the engine — a synthetic
  source-system identifier (e.g. `SYN100001`), seeded from the persona seed. This
  parallels what real importers receive: OFX gets institution-assigned `ACCTID`, tabular
  gets a source-system identifier. The generator never derives `account_id` from the
  account name.
- **`institution`** is the financial institution name. Maps to `institution_org` in
  `raw.ofx_accounts` and `institution_name` in `raw.tabular_accounts`.
- **Amounts** can be fixed (`17.99`) or distribution-based (`{ mean: 145, stddev: 35 }`).
  Fixed for predictable charges; distribution for variable ones.
- **`source_type` per account** controls which raw table receives that account's
  transactions. This exercises the multi-source union path in core models — the `family`
  persona has OFX checking/savings and tabular credit cards, just like a real user.
- **`opening_balance`** is the account balance at the start of the generated date range.
  For OFX accounts, written as a balance snapshot to `raw.ofx_balances`. For tabular
  accounts, used to compute the running `balance` column on
  `raw.tabular_transactions`. Not written as a compensating transaction.
- **`seasonal_modifiers`** are multipliers on the base monthly budget. `1.3` = 30% more
  than usual. Applied per-month to the spending generator's budget for that category.
- **`day_of_week_weights`** bias transaction dates within a month. Unspecified days
  default to `1.0`. A weight of `2.0` means that day is twice as likely as default.
- **`merchant_catalog`** references a shared YAML file by name (without extension).
  Catalogs are shared across personas — a `basic` and `family` persona both reference
  the `grocery` catalog.
- **`statement_balance`** is a special value for credit card payment transfers — the
  generator computes the actual accumulated balance on the card.
- **`price_increases`** model subscription economy realism. After the specified number
  of months from generation start, the amount steps to `new_amount`.
- **`description_template`** supports `{employer}` and similar variable substitution
  for realistic bank-statement descriptions.

### Pydantic validation

All YAML is loaded through Pydantic models (`src/moneybin/testing/synthetic/models.py`).
Validation catches errors at load time:

- Unknown persona fields
- Account names referenced in income/spending/transfers that don't exist in `accounts`
- Invalid `source_type` values
- Missing `institution` on accounts
- Merchant catalog references that don't match a file in `data/merchants/`
- Negative amounts, zero weights, invalid schedules

---

## Merchant Catalog Schema

Each spending category gets its own YAML file with weighted merchants reflecting 2026
reality. Merchant catalogs are shared across personas — the engine selects from the
catalog based on the persona's category spending config.

```yaml
category: grocery
merchants:
  - name: "Trader Joe's"
    weight: 15
    amount: { mean: 55.00, stddev: 18.00 }
  - name: "Costco"
    weight: 10
    amount: { mean: 145.00, stddev: 40.00 }
    description_prefix: "COSTCO WHSE"
  - name: "Whole Foods"
    weight: 8
    amount: { mean: 72.00, stddev: 22.00 }
  - name: "Kroger"
    weight: 12
    amount: { mean: 65.00, stddev: 20.00 }
  - name: "Walmart Grocery"
    weight: 14
    amount: { mean: 78.00, stddev: 25.00 }
    description_prefix: "WAL-MART"
  - name: "Target"
    weight: 8
    amount: { mean: 45.00, stddev: 15.00 }
  - name: "Aldi"
    weight: 10
    amount: { mean: 42.00, stddev: 12.00 }
  - name: "Safeway"
    weight: 6
    amount: { mean: 58.00, stddev: 18.00 }
  - name: "Instacart"
    weight: 7
    amount: { mean: 85.00, stddev: 30.00 }
    description_prefix: "INSTACART"
  - name: "Amazon Fresh"
    weight: 5
    amount: { mean: 68.00, stddev: 22.00 }
    description_prefix: "AMZN FRESH"
```

### Design choices

- **`weight`** is relative within the catalog — higher values mean more frequent
  selection. Weights reflect 2026 market share and consumer behavior.
- **Per-merchant `amount`** distributions allow realistic differentiation. Costco trips
  (mean $145, stddev $40) are larger than Aldi trips (mean $42, stddev $12).
- **`description_prefix`** overrides the clean merchant name with a bank-statement-style
  string. When set, the generator produces descriptions like "WAL-MART #4521 AUSTIN TX"
  (with random store numbers and city names appended). When absent, the `name` is used
  directly. This gives the merchant normalization engine realistic, messy input.
- **Merchant-level amounts override category-level budgets** in terms of per-transaction
  sizing. The persona's `monthly_budget` controls total monthly spend in the category;
  the merchant catalog controls how individual transactions are sized within that total.

### V1 catalog coverage (~200 merchants)

| Catalog file | Example merchants | Approx. count |
|---|---|---|
| `grocery.yaml` | Trader Joe's, Costco, Kroger, Instacart | ~15 |
| `dining.yaml` | Chipotle, Starbucks, DoorDash, local restaurants | ~25 |
| `transport.yaml` | Shell, Uber, Lyft, parking, tolls | ~15 |
| `utilities.yaml` | AT&T, Comcast, electric/gas/water utilities | ~10 |
| `entertainment.yaml` | AMC, Spotify, Apple, Steam, live events | ~15 |
| `shopping.yaml` | Amazon, Target, TJ Maxx, Nike, Etsy | ~25 |
| `health.yaml` | CVS, copays, dental, gym memberships | ~15 |
| `travel.yaml` | Airlines, hotels, Airbnb, rental cars | ~15 |
| `subscriptions.yaml` | Netflix, Spotify, iCloud, NYT, gym | ~15 |
| `kids.yaml` | Activity registrations, school supplies, pediatrician | ~15 |
| `personal_care.yaml` | Hair salons, skincare, spa | ~10 |
| `insurance.yaml` | Auto, home, life, health premiums | ~8 |
| `education.yaml` | Udemy, books, student loans | ~8 |
| `gifts.yaml` | Flowers, greeting cards, charitable donations | ~10 |

### Future extension: merchant ground truth

The generator already knows the "true" merchant name for every transaction it produces
— it selected it from the catalog. When a merchant quality/normalization scoring spec is
written, adding ground-truth merchant names requires only a new column in
`synthetic.ground_truth` (`expected_merchant VARCHAR`). No architecture change needed.
The extension point is documented here; the triggering spec is the future merchant
normalization quality initiative.

---

## Data Model

### Output: raw tables

The generator writes to existing raw tables, conforming to their schemas exactly as if
the data came from a real extractor. Per-account `source_type` in the persona YAML
determines which raw table receives the data:

| Persona account `source_type` | Transaction table | Account table | Balance table |
|---|---|---|---|
| `ofx` | `raw.ofx_transactions` | `raw.ofx_accounts` | `raw.ofx_balances` |
| `csv` | `raw.tabular_transactions` | `raw.tabular_accounts` | *(running balance in transaction row)* |

The `source_file` column uses synthetic URIs to prevent collisions with real files and
to enable idempotent re-generation:

```
synthetic://family/42/2024              # transactions: persona=family, seed=42, year=2024
synthetic://family/42/our-chase-checking  # accounts: persona=family, seed=42, account slug
```

### Column mappings

The generator must populate every NOT NULL column and should populate optional columns
where doing so exercises real code paths. Columns not listed default to NULL.

#### `raw.ofx_accounts`

| Column | Generator value |
|---|---|
| `account_id` | Synthetic source-system ID (`SYN100001`, etc.), seeded deterministically |
| `routing_number` | NULL |
| `account_type` | Mapped from persona YAML `type`: `checking`→`CHECKING`, `savings`→`SAVINGS`, `credit_card`→`CREDITLINE` |
| `institution_org` | From persona YAML `institution` field |
| `institution_fid` | NULL |
| `source_file` | `synthetic://{persona}/{seed}/{account-slug}` |
| `extracted_at` | Generation timestamp |

#### `raw.ofx_transactions`

| Column | Generator value |
|---|---|
| `transaction_id` | Seeded deterministic ID, FITID-style (e.g. `SYN20240115001`) |
| `account_id` | Matching `account_id` from `raw.ofx_accounts` |
| `transaction_type` | Contextual: `DEBIT`, `CREDIT`, `DEP`, `DIRECTDEP`, `XFER` |
| `date_posted` | TIMESTAMP from generated date |
| `amount` | Generated amount (negative = expense, positive = income) |
| `payee` | Generated description (bank-statement-style for merchants with `description_prefix`) |
| `memo` | NULL |
| `check_number` | NULL |
| `source_file` | `synthetic://{persona}/{seed}/{year}` |
| `extracted_at` | Generation timestamp |

#### `raw.ofx_balances`

| Column | Generator value |
|---|---|
| `account_id` | Matching `account_id` from `raw.ofx_accounts` |
| `statement_start_date` | Start of generated date range |
| `statement_end_date` | Start of generated date range (initial snapshot) |
| `ledger_balance` | From persona YAML `opening_balance` |
| `ledger_balance_date` | Start of generated date range |
| `available_balance` | NULL |
| `source_file` | `synthetic://{persona}/{seed}/{account-slug}` |
| `extracted_at` | Generation timestamp |

#### `raw.tabular_accounts`

| Column | Generator value |
|---|---|
| `account_id` | Synthetic source-system ID (`SYN100001`, etc.), seeded deterministically |
| `account_name` | From persona YAML `name` field (e.g. "Alice Costco Visa") |
| `account_number` | NULL (real CSVs rarely contain account numbers) |
| `account_number_masked` | NULL |
| `account_type` | From persona YAML `type` |
| `institution_name` | From persona YAML `institution` field |
| `currency` | `USD` |
| `source_file` | `synthetic://{persona}/{seed}/{account-slug}` |
| `source_type` | `csv` |
| `source_origin` | `synthetic` |
| `extracted_at` | Generation timestamp |

#### `raw.tabular_transactions`

| Column | Generator value |
|---|---|
| `transaction_id` | Seeded deterministic ID |
| `account_id` | Matching `account_id` from `raw.tabular_accounts` |
| `transaction_date` | Generated DATE |
| `post_date` | NULL |
| `amount` | Generated amount (negative = expense, positive = income) |
| `original_amount` | String representation of amount |
| `original_date_str` | Date formatted as string |
| `description` | Generated description |
| `memo` | NULL |
| `category` | NULL (exercise categorization pipeline from scratch) |
| `subcategory` | NULL |
| `transaction_type` | NULL |
| `status` | `Posted` |
| `check_number` | NULL |
| `source_transaction_id` | NULL |
| `reference_number` | NULL |
| `balance` | Running balance computed from `opening_balance` + cumulative transactions |
| `currency` | `USD` |
| `member_name` | NULL |
| `source_file` | `synthetic://{persona}/{seed}/{year}` |
| `source_type` | `csv` |
| `source_origin` | `synthetic` |
| `row_number` | Sequential per file |

### Ground truth: `synthetic.ground_truth`

```sql
/* Known-correct labels for scoring categorization and transfer detection accuracy
   against synthetic data */
CREATE TABLE synthetic.ground_truth (
    source_transaction_id VARCHAR NOT NULL, -- joins to raw/core transaction identity
    account_id VARCHAR NOT NULL,           -- synthetic source-system account ID; joins to raw account tables
    expected_category VARCHAR,             -- ground-truth category label; NULL for transfers
    transfer_pair_id VARCHAR,              -- non-NULL for transfer pairs; both sides share the same ID
    persona VARCHAR NOT NULL,              -- which persona generated this row
    seed INTEGER NOT NULL,                 -- seed used for reproducibility
    generated_at TIMESTAMP NOT NULL        -- when this ground truth was produced
);
```

- `source_transaction_id` + `account_id` is the join key back to raw and core tables.
- `expected_category` is NULL for transfers (transfers aren't categorized — they're a
  separate scoring dimension).
- `transfer_pair_id` is NULL for non-transfers. Both sides of a transfer share the same
  ID so transfer detection F1 can be scored by comparing detected pairs against known
  pairs.
- `persona` and `seed` are denormalized for convenience — no separate lookup needed.
- No foreign keys to raw/core — `synthetic` schema exists before `sqlmesh run`
  materializes core.
- The existence of this table in a profile is the marker that the profile was created by
  the generator (safety guard for `moneybin synthetic reset`).

---

## CLI Interface

### `moneybin synthetic generate`

```
moneybin synthetic generate --persona=family [--profile=bob] [--years=3] [--seed=42]
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--persona` | Yes | — | Persona to generate (`basic`, `family`, `freelancer`) |
| `--profile` | No | Auto-derived from persona | Target profile name |
| `--years` | No | Persona's `years_default` | Number of years of history to generate |
| `--seed` | No | Random (displayed in output) | Seed for deterministic output |

**Behavior:**
- Creates the profile if it doesn't exist.
- Errors if the profile already has data — directs user to `moneybin synthetic reset`.
- Writes to raw tables, creates `synthetic.ground_truth`, runs `sqlmesh run`.
- Displays summary: accounts created, transaction count, date range, seed used.

```
$ moneybin synthetic generate --persona=family --seed=42
⚙️  Generating 'family' persona into profile 'bob' (seed=42, 3 years)...
  Created 4 accounts (2 checking/savings, 2 credit cards)
  Generated 4,532 transactions (2023-01-01 to 2025-12-31)
  Wrote ground truth: 4,532 category labels, 156 transfer pairs
⚙️  Running sqlmesh to materialize pipeline...
✅ Profile 'bob' ready. Use --profile=bob with any moneybin command.
```

### `moneybin synthetic reset`

```
moneybin synthetic reset --persona=family [--seed=42] [--years=3] [--yes]
moneybin synthetic reset --profile=bob --persona=family [--seed=42] [--years=3] [--yes]
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--persona` | Yes (or derivable from `--profile`) | — | Persona to regenerate |
| `--profile` | No | Auto-derived from persona | Target profile to reset |
| `--seed` | No | Random | Seed for regeneration |
| `--years` | No | Persona's `years_default` | Years to regenerate |
| `--yes` / `-y` | No | `false` | Skip confirmation prompt |

**Behavior:**
- Requires `--persona` or `--profile` — no default target.
- If `--profile` given without `--persona`, persona must be specified for regeneration.
- Checks for `synthetic.ground_truth` — refuses to reset profiles not created by the
  generator:
  ```
  $ moneybin synthetic reset --profile=default
  ❌ Profile 'default' was not created by the generator. Refusing to reset.
  💡 To destroy a non-generated profile, use 'moneybin db destroy --profile=default'
  ```
- Without `--yes`, prompts: "This will destroy all data in profile 'bob' and regenerate.
  Continue? [y/N]"
- Wipes the profile database, regenerates with specified parameters, runs `sqlmesh run`.

### Non-interactive parity

Both commands are fully automatable with explicit flags. The only interactive prompt is
`reset`'s confirmation, bypassed with `--yes`. Per `.claude/rules/cli.md`, AI agents and
scripts can invoke any operation in a single command.

---

## Realism Roadmap

Four graduated levels. V1 targets Level 2. The declarative architecture (YAML seed
files + generic engine) is designed so Level 3 is achievable by enriching seed data and
adding behavioral rules to generators, not by rewriting the engine.

### Level 1: Functional

Gets tests passing. Generic merchant names ("Grocery Store", "Gas Station"), uniform
random amounts within category ranges, fixed transactions per month, flat biweekly
income. Every month looks identical. No one would mistake it for real data — but
referential integrity holds, amounts are well-formed, and pipeline assertions pass.

### Level 2: Plausible (v1 target)

A human glances at the data and nods. ~200 real 2026 brand names weighted by popularity
— DoorDash, Costco, Spotify, not "General Store #3". Log-normal amount distributions
per merchant (Costco trips are bigger than Aldi trips). Day-of-week bias (more dining on
Friday/Saturday). Seasonal patterns (November-December holiday spending, January belt-
tightening, August back-to-school). Realistic pay schedules with annual raises.
Recurring charges on consistent dates with price increases. Bank-statement-style
descriptions ("WAL-MART #4521 AUSTIN TX").

**Verification:** Manual spot-check + statistical property tests (mean/stddev per
category within expected bounds, day-of-week distribution not uniform, December spending
> January).

### Level 3: Industry Best (future)

Everything from Level 2, plus behavioral correlations (gas purchase → correlated coffee
stop 30% of the time). Merchant loyalty (3-5 anchor merchants per persona with
disproportionate visit frequency). Inflation modeling (amounts drift 3-4% annually for
non-fixed charges). Regional coherence (no mixing HEB and Wegmans). Payday spending
bumps in the 3 days following deposits. Balance-aware spending constraints. Life events
(tax refunds in March/April, insurance renewals).

**Verification:** Compare category proportions against BLS Consumer Expenditure Survey
data for the persona's income bracket. Autocorrelation tests on merchant visit
frequency. Verify no impossible merchant combinations (geographic, temporal).

**Graduation path:** Level 2→3 is primarily richer YAML seed data (merchant loyalty
weights, correlation rules, regional tags) plus a correlation engine added to
`SpendingGenerator`. The YAML-based architecture supports this without structural
changes.

### Level 4: Uncanny Valley (aspirational)

BLS/Census-calibrated spending proportions fitted to actual Consumer Expenditure Survey
data for the persona's demographic (income quintile, household size, age bracket,
region). Micro-behavioral patterns: weekend brunch, "treat yourself" discretionary
spikes, autopay failures and retries, refunds appearing 3-7 days after purchases.
Financial product influence: credit card rewards categories shaping merchant selection.
Social context: family persona has kids' activity fees, school lunch deposits,
pediatrician copays at realistic frequencies.

**Verification:** Blind test — mix synthetic and anonymized-real data, ask reviewers to
identify which is which. >40% misclassification rate = success.

**Graduation path:** Level 3→4 requires BLS data integration and more sophisticated
behavioral modeling — likely the point where Python hooks supplement YAML config for
patterns that can't be expressed declaratively.

---

## Testing Strategy

### Unit tests (generator internals)

- **Determinism:** Generate with the same seed twice, assert identical output. Generate
  with different seeds, assert different output.
- **Pydantic validation:** Invalid YAML configs (missing fields, unknown accounts,
  invalid source_types) fail with clear errors.
- **Amount distributions:** Generated amounts for each merchant fall within expected
  statistical bounds (mean ± 3σ over a large sample).
- **Temporal patterns:** Day-of-week weighted generation produces statistically
  distinguishable distributions (chi-squared test on large samples).
- **Seasonal modifiers:** December spending > January spending for categories with
  holiday modifiers.
- **Transfer pairs:** Every generated transfer has matching opposite-sign entries in the
  correct accounts. Both sides share a `transfer_pair_id`.
- **Income schedules:** Biweekly salary produces 26 deposits/year. Freelancer invoices
  are irregular but within configured bounds.
- **Sign convention:** All expenses negative, all income positive.

### Integration tests (end-to-end)

- Generate `basic` persona → `sqlmesh run` → core tables populated with correct
  referential integrity (every `fct_transactions.account_id` exists in `dim_accounts`).
- `synthetic.ground_truth` rows all join to `core.fct_transactions` after pipeline.
- Synthetic origin identifiable via `source_file` URI prefix (`synthetic://`).
  `source_type` remains `ofx`/`csv` per the persona account config and `source_origin`
  is `synthetic` for tabular accounts — exercises multi-source union paths.
- Running balances on tabular transactions are internally consistent (opening balance +
  cumulative transactions = final balance). OFX opening balance snapshots present in
  `raw.ofx_balances`.
- Profile isolation: generating into profile `alice` has no effect on the default
  profile's database.

### Regression tests (golden snapshots)

- Committed baseline for `basic` persona with `seed=42`, `years=1`.
- Assert exact row count, category distribution summary, and monthly spending totals.
- Baseline updated deliberately when generator changes — the diff is the review
  artifact. This is intentionally brittle: "when a snapshot breaks, you either fix the
  regression or update the baseline" (per umbrella spec).

---

## Dependencies

| Dependency | Type | Status | Notes |
|---|---|---|---|
| `Database.ingest_dataframe()` | Write primitive | Designed (smart-import-tabular.md) | Polars → Arrow → DuckDB via encrypted connection |
| Profile system (`MoneyBinSettings.profile`) | Infrastructure | Implemented | Named profiles for persona isolation |
| Raw table schemas (`raw.ofx_*`, `raw.tabular_*`) | Schema | OFX implemented, tabular designed | Generator output must conform to existing DDL |
| SQLMesh models | Pipeline | Implemented | `sqlmesh run` materializes generated data through prep→core |

### Synthetic data requirements in feature specs

Feature specs that introduce testable behavior should include a "Synthetic Data
Requirements" section describing what the generator should produce to exercise that
feature. This becomes the contract between feature authors and generator maintainers.

The spec template (`_template.md`) includes this as an optional section. See
`spec_implementation.md` for an audit of which specs need and have this section.

---

## Future Extensions

| Extension | Gates on | Notes |
|---|---|---|
| `investor` persona (david) | Investment schema (M3B) | Brokerage, 401k, IRA, dividends, trades, capital gains. Persona YAML needs investment-specific config. |
| `international` persona (eve) | Multi-currency schema (M3C) | Multi-bank across countries, EUR + GBP + USD, forex fees, cross-currency transfers. |
| Merchant ground-truth labels | Future merchant quality spec | Add `expected_merchant` column to `synthetic.ground_truth`. Generator already knows the true name — just emit it. |
| Budget model enrichment | Budget-tracking rewrite (M3C) | `monthly_budget` in persona YAML becomes more expressive to align with budget model. |
| Anonymized generation mode | `testing-anonymized-data.md` | Separate engine, same output layer. Structure-preserving anonymization of real data. |
| Level 3 realism | No blocker | Richer YAML seed data + correlation engine in `SpendingGenerator`. |

---

## Out of Scope

- **Anonymized generation mode** — separate spec (`testing-anonymized-data.md`)
- **Scenario runner** — peer spec; orchestrates generator + assertions + evaluations
- **CSV fixture library** — sibling child spec (`testing-csv-fixtures.md`)
- **Institution-formatted CSV output** — fixture library concern
- **Plaid Sandbox fixtures** — deferred to sync spec (`sync-overview.md`)
- **Investment/multi-currency personas** — gated on schema additions (M3B / M3C)
- **Merchant ground-truth scoring** — documented extension point; awaits merchant
  quality spec
- **CI/CD pipeline configuration** — implementation detail for later
- **MCP tool for generation** — CLI only for v1; MCP wrapper trivial to add later
- **`hypothesis` property-based test generation** — complementary tool for format-
  detection edge cases, not part of this generator

---

## Implementation Plan

### Files to Create

| File | Purpose |
|---|---|
| `src/moneybin/testing/synthetic/__init__.py` | Package init |
| `src/moneybin/testing/synthetic/engine.py` | `GeneratorEngine` orchestrator |
| `src/moneybin/testing/synthetic/seed.py` | `SeededRandom` wrapper |
| `src/moneybin/testing/synthetic/models.py` | Pydantic models for YAML validation |
| `src/moneybin/testing/synthetic/writer.py` | Raw table + ground truth writer |
| `src/moneybin/testing/synthetic/generators/__init__.py` | Generator package init |
| `src/moneybin/testing/synthetic/generators/income.py` | Income generation |
| `src/moneybin/testing/synthetic/generators/recurring.py` | Recurring charge generation |
| `src/moneybin/testing/synthetic/generators/spending.py` | Discretionary spending generation |
| `src/moneybin/testing/synthetic/generators/transfers.py` | Transfer pair generation |
| `src/moneybin/testing/synthetic/data/personas/*.yaml` | Three persona definitions |
| `src/moneybin/testing/synthetic/data/merchants/*.yaml` | ~14 merchant catalogs |
| `src/moneybin/sql/schema/synthetic_ground_truth.sql` | DDL for `synthetic.ground_truth` |
| `src/moneybin/cli/synthetic.py` | CLI commands (`generate`, `reset`) |
| `tests/test_synthetic_generator.py` | Unit + integration tests |
| `tests/baselines/basic_seed42.json` | Golden snapshot baseline |

### Files to Modify

| File | Change |
|---|---|
| `src/moneybin/cli/app.py` | Register `synthetic` subcommand group |
| `src/moneybin/database.py` | Add `create_synthetic_schema()` method (on-demand only) |

### Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Architecture | Declarative YAML pipeline | Personas are data, not code; extensible without engine changes |
| Output target | Raw tables only | Exercises full ETL pipeline; matches how real data enters |
| Ground truth location | `synthetic` schema | Isolated from user-facing schemas; created on demand |
| Determinism | Single `SeededRandom` instance | Structural guarantee, not convention |
| Realism level | Level 2 for v1 | 80% realism for 20% effort; architecture supports Level 3 |
| CLI namespace | `moneybin synthetic` | Groups generation + verification; `synthetic` schema alignment |
| Safety guards | Check `synthetic.ground_truth` existence | Prevents accidental reset of real profiles |
| Account identity | Synthetic `account_id` (`SYN100001`), separate `account_name` | `account_id` = source-system identifier (never derived from name); `account_name` = human-readable label |
| Opening balances | `raw.ofx_balances` + tabular running `balance` column | Matches how real data appears in each format; no compensating transactions |
| `source_origin` | `synthetic` for all generator output | Format-specific testing handled by format compatibility specs |
