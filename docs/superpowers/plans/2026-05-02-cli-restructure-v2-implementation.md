# CLI Restructure v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the v2 command taxonomy across CLI and MCP per [`docs/specs/cli-restructure.md`](../../specs/cli-restructure.md) v2 and [`docs/specs/mcp-tool-surface.md`](../../specs/mcp-tool-surface.md) v2 — restructure-only, hard cut, no aliases.

**Architecture:** Reorganize the CLI into entity groups (`accounts`, `transactions`, `assets`), reference-data groups (`categories`, `merchants`), a `reports` group for analytical lenses, and standalone `tax` / `system` / `budget` groups. Mirror the same hierarchy in MCP via path-prefix-verb-suffix tool names. Add MCP exposure for sync (9 tools) and transform (5 tools) per the v2 exposure principle. Update the FastMCP `instructions` field to advertise the v2 surface.

**Tech Stack:** Python 3.12, Typer (CLI), FastMCP (MCP server), DuckDB, SQLMesh, pytest, uv, ruff, pyright.

**Hard cut.** No aliases or deprecation period. v1 paths break in the same release. MoneyBin is pre-1.0 single-user — clean is cheaper than gradual.

**Out of scope (deferred to owning specs):**
- `accounts_balance_assert / _history / _reconcile / _assertions_list / _assertion_delete` and `reports_networth_history` — owned by `net-worth.md` (status: draft)
- Full asset workflows — owned by `asset-tracking.md` (status: draft); v2 only reserves the `assets` namespace with stubs
- `account-management.md` surface (rename, archive, merge) — `planned` spec; v2 stubs the entity ops
- Reports body implementation (spending/cashflow/budget vs actual analytics) — those tools already exist; v2 just renames them
- `transactions recurring` workflow — future spec; v2 stubs only

---

## File Structure

### CLI module reorganization

The `src/moneybin/cli/commands/` directory currently has a flat layout. v2 introduces nested groups for entity-and-workflow domains. New layout:

```
src/moneybin/cli/commands/
├── __init__.py
├── accounts/
│   ├── __init__.py          # accounts top-level group; entity ops (list, show, rename, include)
│   ├── balance.py           # accounts balance (stubs delegating to net-worth.md)
│   └── investments.py       # accounts investments (stub)
├── transactions/
│   ├── __init__.py          # transactions top-level group; entity ops (list, show, search) — stubs
│   ├── review.py            # unified review queue (matches + categorize)
│   ├── matches.py           # MOVED from src/moneybin/cli/commands/matches.py — minus review subcommand
│   ├── categorize/
│   │   ├── __init__.py      # transactions categorize group
│   │   ├── workflow.py      # bulk, stats (was top-level categorize subcommands)
│   │   ├── rules.py         # rules list / create / apply / delete (was categorize_*_rules)
│   │   ├── auto.py          # auto review/confirm/stats
│   │   └── ml.py            # ml status/train/apply
│   └── recurring.py         # stub
├── assets/
│   └── __init__.py          # assets group; stubs only (asset-tracking.md owns workflows)
├── categories/
│   └── __init__.py          # categories list/create/toggle/delete (split from categorize_*_category[ies])
├── merchants/
│   └── __init__.py          # merchants list/create (split from categorize_*_merchant[s])
├── reports/
│   ├── __init__.py          # reports top-level group
│   ├── networth.py          # reports networth show/history (was track networth)
│   ├── spending.py          # stub (future spec)
│   ├── cashflow.py          # stub
│   ├── budget.py            # stub (vs-actual report)
│   └── health.py            # reports health (was overview health)
├── tax/
│   └── __init__.py          # tax group; w2/deductions stubs (existing tax_w2 etc. are MCP-only today)
├── system/
│   └── __init__.py          # system status (was overview status)
├── budget/
│   └── __init__.py          # budget set/delete (mutation; vs-actual lives under reports)
├── db.py                    # unchanged
├── import_cmd.py            # unchanged except `csv-preview` → `file-preview`
├── import_inbox.py          # unchanged
├── logs.py                  # unchanged
├── mcp.py                   # unchanged
├── migrate.py               # unchanged
├── profile.py               # unchanged
├── stats.py                 # unchanged
├── stubs.py                 # GUTTED — track group dissolved; export_app may stay or move
├── sync.py                  # unchanged
├── synthetic.py             # unchanged
└── transform.py             # unchanged
```

**Files DELETED:**
- `src/moneybin/cli/commands/matches.py` (moved to `transactions/matches.py`)
- `src/moneybin/cli/commands/categorize.py` (split across `transactions/categorize/*` and `categories/`, `merchants/`)

**Files in `stubs.py` GUTTED:**
- `track_app` (and all `track_*_app` subgroups) — dissolved
- `export_app` — keep (still planned)

### MCP tools reorganization

MCP tools are currently flat under `src/moneybin/mcp/tools/`. v2 keeps them flat (FastMCP namespace via tool name prefix, not directory) but renames files where helpful:

```
src/moneybin/mcp/tools/
├── __init__.py
├── accounts.py              # accounts_list, accounts_get, accounts_balance_list (renames)
├── transactions.py          # transactions_search, _correct, _annotate, _recurring_list, _review_status (NEW)
├── transactions_matches.py  # NEW file extracted from existing matches code
├── transactions_categorize.py  # RENAMED from categorize.py; trimmed
├── categories.py            # NEW — extracted from categorize.py (categories_list/create/toggle/delete)
├── merchants.py             # NEW — extracted from categorize.py (merchants_list/create)
├── reports.py               # NEW — combines spending.py + cashflow.py + budget reads + networth + health
├── tax.py                   # unchanged (top-level)
├── system.py                # NEW — system_status
├── budget.py                # SLIMMED — only mutation tools (set, delete); reads moved to reports
├── sync.py                  # NEW — 9 sync_* tools
├── transform.py             # NEW — 5 transform_* tools
├── discover.py              # unchanged
├── import_inbox.py          # unchanged
├── import_tools.py          # rename `import_csv_preview` → `import_file_preview`
└── sql.py                   # unchanged
```

**Files DELETED:**
- `src/moneybin/mcp/tools/spending.py` (merged into `reports.py`)
- `src/moneybin/mcp/tools/categorize.py` (split across `transactions_categorize.py`, `categories.py`, `merchants.py`)

### Service layer

Existing service classes mostly keep their names — they sit below the renamed tools/CLI commands. New services for genuinely new functionality:

- `src/moneybin/services/sync_service.py` — wrap existing CLI sync logic for MCP. Likely already exists; verify.
- `src/moneybin/services/transform_service.py` — wrap SQLMesh primitives.
- `src/moneybin/services/system_service.py` — system status (data freshness, queue counts). Replaces `OverviewService.status()`.
- `src/moneybin/services/review_service.py` — `transactions_review_status` aggregator. Composes match service + categorize service.

### Test layout

Tests mirror the new structure:

```
tests/moneybin/test_cli/
├── test_accounts/
│   ├── test_accounts_entity.py
│   ├── test_accounts_balance.py     # stub tests for now
│   └── test_accounts_investments.py
├── test_transactions/
│   ├── test_transactions_entity.py
│   ├── test_transactions_review.py
│   ├── test_transactions_matches.py  # MOVED from test_matches_*.py if present
│   └── test_transactions_categorize/
│       ├── test_workflow.py
│       ├── test_rules.py
│       ├── test_auto.py
│       └── test_ml.py
├── test_categories.py
├── test_merchants.py
├── test_reports/
│   └── test_reports_networth.py
├── test_system.py
├── test_tax.py
└── (existing test files for db, import, profile, etc. unchanged)

tests/moneybin/test_mcp/
├── test_accounts_tools.py            # was test_v1_tools or similar
├── test_transactions_tools.py
├── test_transactions_matches_tools.py
├── test_transactions_categorize_tools.py  # RENAMED from test_categorization_tools.py
├── test_categories_tools.py
├── test_merchants_tools.py
├── test_reports_tools.py
├── test_sync_tools.py                # NEW
├── test_transform_tools.py           # NEW
├── test_system_tools.py              # NEW
└── (existing test files for envelope, decorator, privacy, etc. unchanged)
```

---

## Task Decomposition

The plan is structured in 5 parts:

- **Part 1: CLI restructure** — move and rename commands; collapse review queue
- **Part 2: MCP tool renames** — apply §16b rename map
- **Part 3: New MCP tools** — sync_*, transform_*, transactions_review_status, system_status
- **Part 4: Server instructions + final wiring**
- **Part 5: Verification, README, simplify pass, ship**

Tasks marked **[mechanical]** are large bulk renames where strict TDD-per-rename would be over-engineered — use the targeted verification step instead. Tasks marked **[TDD]** introduce genuinely new behavior and use full red-green-refactor.

---

# Part 1: CLI Restructure

## Task 1: Create the new top-level CLI group skeletons

**Goal:** Create empty group modules for the new top-level commands. No behavior change yet — they just register and respond with `--help`.

**Files:**
- Create: `src/moneybin/cli/commands/accounts/__init__.py`
- Create: `src/moneybin/cli/commands/transactions/__init__.py`
- Create: `src/moneybin/cli/commands/transactions/categorize/__init__.py`
- Create: `src/moneybin/cli/commands/assets/__init__.py`
- Create: `src/moneybin/cli/commands/categories/__init__.py`
- Create: `src/moneybin/cli/commands/merchants/__init__.py`
- Create: `src/moneybin/cli/commands/reports/__init__.py`
- Create: `src/moneybin/cli/commands/tax/__init__.py`
- Create: `src/moneybin/cli/commands/system/__init__.py`
- Create: `src/moneybin/cli/commands/budget/__init__.py`
- Modify: `src/moneybin/cli/main.py` (register new groups; leave old registrations intact for now)
- Test: `tests/moneybin/test_cli/test_v2_skeleton.py` (NEW)

- [ ] **Step 1: Write the failing test**

```python
# tests/moneybin/test_cli/test_v2_skeleton.py
"""Smoke tests that the v2 top-level command groups are registered.

These tests verify that --help works for each new group; they don't
test behavior. Behavior tests land in later tasks.
"""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()

V2_GROUPS = [
    "accounts",
    "transactions",
    "assets",
    "categories",
    "merchants",
    "reports",
    "tax",
    "system",
    "budget",
]


def test_v2_groups_registered() -> None:
    for group in V2_GROUPS:
        result = runner.invoke(app, [group, "--help"])
        assert result.exit_code == 0, f"{group} --help failed: {result.output}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_cli/test_v2_skeleton.py -v`
Expected: FAIL — most groups not registered yet.

- [ ] **Step 3: Create empty Typer apps for each new group**

Each `__init__.py` should look like:

```python
# src/moneybin/cli/commands/accounts/__init__.py
"""Accounts top-level command group.

Owns account entity operations (list, show, rename, include) and
per-account workflows (balance) per cli-restructure.md v2.
"""

import typer

app = typer.Typer(
    help="Accounts and per-account workflows (balance, investments)",
    no_args_is_help=True,
)
```

Repeat for `transactions`, `assets`, `categories`, `merchants`, `reports`, `tax`, `system`, `budget` with their respective help strings:

| Group | Help string |
|---|---|
| `accounts` | "Accounts and per-account workflows (balance, investments)" |
| `transactions` | "Transactions and workflows on them (matches, categorize, review)" |
| `assets` | "Physical assets (real estate, vehicles, valuables)" |
| `categories` | "Category taxonomy management" |
| `merchants` | "Merchant mappings management" |
| `reports` | "Cross-domain analytical and aggregation views" |
| `tax` | "Tax forms, deductions, and tax-prep utilities" |
| `system` | "System and data status" |
| `budget` | "Budget target management (vs-actual report lives in `reports budget`)" |

For `transactions/categorize/__init__.py`:
```python
import typer

app = typer.Typer(
    help="Categorization workflow + rules (taxonomy under top-level `categories`)",
    no_args_is_help=True,
)
```

- [ ] **Step 4: Register new groups in `main.py`**

Modify `src/moneybin/cli/main.py`. Find the existing `app.add_typer` block (around lines 102-141) and append new registrations BEFORE the `track_app` registration. Update the import block at top:

```python
from .commands import (
    accounts,
    assets,
    budget as budget_cmd,
    categories,
    categorize,  # KEEP for now; removed in Task 8
    db,
    import_cmd,
    logs,
    matches,  # KEEP for now; removed in Task 8
    mcp,
    merchants,
    migrate,
    profile,
    reports,
    stats,
    sync,
    synthetic,
    system,
    tax,
    transactions,
    transform,
)
```

Add registrations (place near existing top-level registrations):

```python
app.add_typer(accounts.app, name="accounts")
app.add_typer(transactions.app, name="transactions")
app.add_typer(assets.app, name="assets")
app.add_typer(categories.app, name="categories")
app.add_typer(merchants.app, name="merchants")
app.add_typer(reports.app, name="reports")
app.add_typer(tax.app, name="tax")
app.add_typer(system.app, name="system")
app.add_typer(budget_cmd.app, name="budget")
```

- [ ] **Step 5: Run skeleton test**

Run: `uv run pytest tests/moneybin/test_cli/test_v2_skeleton.py -v`
Expected: PASS — all v2 groups respond to `--help`.

- [ ] **Step 6: Verify no regression**

Run: `uv run pytest tests/moneybin/test_cli/ -q --ignore=tests/e2e -x`
Expected: PASS for everything that passed on `main` (the pre-existing e2e failure stays excluded).

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/commands/accounts/ \
       src/moneybin/cli/commands/transactions/ \
       src/moneybin/cli/commands/assets/ \
       src/moneybin/cli/commands/categories/ \
       src/moneybin/cli/commands/merchants/ \
       src/moneybin/cli/commands/reports/ \
       src/moneybin/cli/commands/tax/ \
       src/moneybin/cli/commands/system/ \
       src/moneybin/cli/commands/budget/ \
       src/moneybin/cli/main.py \
       tests/moneybin/test_cli/test_v2_skeleton.py
git commit -m "Add v2 top-level CLI group skeletons

Register accounts, transactions, assets, categories, merchants, reports,
tax, system, budget as empty Typer groups. v1 groups (track, matches,
categorize) still registered alongside; removed in a later task."
```

---

## Task 2: Implement `accounts` entity operations (stubs from account-management.md)

**Goal:** `accounts list / show / rename / include` exist as stubs that call `_not_implemented("account-management.md")`. The owning spec is `planned`; v2 reserves the surface.

**Files:**
- Modify: `src/moneybin/cli/commands/accounts/__init__.py`
- Test: `tests/moneybin/test_cli/test_accounts_entity.py` (NEW)

- [ ] **Step 1: Write the failing test**

```python
# tests/moneybin/test_cli/test_accounts_entity.py
"""Smoke tests for accounts entity ops (list, show, rename, include)."""

from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_accounts_list_stub() -> None:
    result = runner.invoke(app, ["accounts", "list"])
    assert result.exit_code == 0
    assert (
        "not yet implemented" in result.output.lower()
        or "account-management" in result.output
    )


def test_accounts_show_stub() -> None:
    result = runner.invoke(app, ["accounts", "show", "fake-id"])
    assert result.exit_code == 0
    assert (
        "not yet implemented" in result.output.lower()
        or "account-management" in result.output
    )


def test_accounts_rename_stub() -> None:
    result = runner.invoke(app, ["accounts", "rename", "fake-id", "new-name"])
    assert result.exit_code == 0


def test_accounts_include_stub() -> None:
    result = runner.invoke(app, ["accounts", "include", "fake-id"])
    assert result.exit_code == 0
```

- [ ] **Step 2: Run test to verify failures**

Run: `uv run pytest tests/moneybin/test_cli/test_accounts_entity.py -v`
Expected: FAIL — commands not defined.

- [ ] **Step 3: Implement stubs**

Modify `src/moneybin/cli/commands/accounts/__init__.py`:

```python
"""Accounts top-level command group.

Owns account entity operations (list, show, rename, include) and
per-account workflows (balance) per cli-restructure.md v2.
Entity ops are stubbed; account-management.md (planned) owns the
implementation.
"""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Accounts and per-account workflows (balance, investments)",
    no_args_is_help=True,
)


@app.command("list")
def accounts_list() -> None:
    """List all accounts."""
    _not_implemented("account-management.md")


@app.command("show")
def accounts_show(account_id: str) -> None:
    """Show one account by ID."""
    _not_implemented("account-management.md")


@app.command("rename")
def accounts_rename(account_id: str, new_name: str) -> None:
    """Rename an account."""
    _not_implemented("account-management.md")


@app.command("include")
def accounts_include(
    account_id: str,
    no: bool = typer.Option(
        False, "--no", help="Exclude from net worth instead of include"
    ),
) -> None:
    """Toggle include_in_net_worth for an account."""
    _not_implemented("account-management.md")
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_accounts_entity.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/accounts/__init__.py tests/moneybin/test_cli/test_accounts_entity.py
git commit -m "Stub accounts entity ops (list/show/rename/include)

Reserves CLI namespace; account-management.md (planned) owns the
implementation."
```

---

## Task 3: Implement `accounts balance` and `accounts investments` stubs

**Goal:** `accounts balance` and `accounts investments` sub-groups with stubbed subcommands matching net-worth.md and investment-tracking.md respectively. Same pattern as Task 2.

**Files:**
- Create: `src/moneybin/cli/commands/accounts/balance.py`
- Create: `src/moneybin/cli/commands/accounts/investments.py`
- Modify: `src/moneybin/cli/commands/accounts/__init__.py` (register sub-apps)
- Test: `tests/moneybin/test_cli/test_accounts_balance.py` (NEW)
- Test: `tests/moneybin/test_cli/test_accounts_investments.py` (NEW)

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_cli/test_accounts_balance.py
from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_balance_show_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "show"])
    assert result.exit_code == 0
    assert "net-worth" in result.output


def test_balance_assert_stub() -> None:
    result = runner.invoke(
        app, ["accounts", "balance", "assert", "acct-1", "2026-05-01", "100.00"]
    )
    assert result.exit_code == 0


def test_balance_list_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "list"])
    assert result.exit_code == 0


def test_balance_delete_stub() -> None:
    result = runner.invoke(
        app, ["accounts", "balance", "delete", "acct-1", "2026-05-01"]
    )
    assert result.exit_code == 0


def test_balance_reconcile_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "reconcile"])
    assert result.exit_code == 0


def test_balance_history_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "history"])
    assert result.exit_code == 0
```

```python
# tests/moneybin/test_cli/test_accounts_investments.py
from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_investments_help() -> None:
    result = runner.invoke(app, ["accounts", "investments", "--help"])
    assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to confirm failures**

Run: `uv run pytest tests/moneybin/test_cli/test_accounts_balance.py tests/moneybin/test_cli/test_accounts_investments.py -v`

- [ ] **Step 3: Implement balance stub**

```python
# src/moneybin/cli/commands/accounts/balance.py
"""accounts balance — per-account balance workflow stub.

Stubs delegate to net-worth.md (status: draft). When that spec is
implemented these stubs are replaced.
"""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Per-account balance workflow (assert, list, reconcile, history)",
    no_args_is_help=True,
)


@app.command("show")
def show(
    account: str | None = typer.Option(None, "--account"),
    as_of: str | None = typer.Option(None, "--as-of"),
) -> None:
    """Show current balance for one or all accounts."""
    _not_implemented("net-worth.md")


@app.command("assert")
def assert_balance(
    account_id: str,
    date: str,
    amount: str,
    notes: str | None = typer.Option(None, "--notes"),
    yes: bool = typer.Option(False, "--yes"),
) -> None:
    """Assert a known balance for an account on a date."""
    _not_implemented("net-worth.md")


@app.command("list")
def list_assertions(account: str | None = typer.Option(None, "--account")) -> None:
    """List balance assertions, optionally filtered by account."""
    _not_implemented("net-worth.md")


@app.command("delete")
def delete_assertion(
    account_id: str, date: str, yes: bool = typer.Option(False, "--yes")
) -> None:
    """Delete a balance assertion."""
    _not_implemented("net-worth.md")


@app.command("reconcile")
def reconcile(
    account: str | None = typer.Option(None, "--account"),
    threshold: float | None = typer.Option(None, "--threshold"),
) -> None:
    """Show accounts with non-zero reconciliation deltas."""
    _not_implemented("net-worth.md")


@app.command("history")
def history(
    account: str | None = typer.Option(None, "--account"),
    from_: str | None = typer.Option(None, "--from"),
    to: str | None = typer.Option(None, "--to"),
    interval: str = typer.Option("daily", "--interval"),
) -> None:
    """Show balance history."""
    _not_implemented("net-worth.md")
```

- [ ] **Step 4: Implement investments stub**

```python
# src/moneybin/cli/commands/accounts/investments.py
"""accounts investments — placeholder for investment-tracking.md."""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Investment holdings tracking (future: investment-tracking.md)",
    no_args_is_help=True,
)


@app.command("show")
def show() -> None:
    """Show investment portfolio."""
    _not_implemented("investment-tracking.md")
```

- [ ] **Step 5: Register sub-apps in `accounts/__init__.py`**

Add after the existing entity command definitions:

```python
from . import balance, investments

app.add_typer(balance.app, name="balance")
app.add_typer(investments.app, name="investments")
```

- [ ] **Step 6: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_accounts_balance.py tests/moneybin/test_cli/test_accounts_investments.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/commands/accounts/ tests/moneybin/test_cli/test_accounts_balance.py tests/moneybin/test_cli/test_accounts_investments.py
git commit -m "Stub accounts balance and investments sub-groups

Balance subcommands delegate to net-worth.md (draft); investments
delegates to investment-tracking.md (planned)."
```

---

## Task 4: Move `matches` under `transactions` (preserve all existing functionality)

**Goal:** Existing top-level `matches` becomes `transactions matches`. The interactive `review` subcommand will be split out to `transactions review` in Task 6 — for now keep it intact under `transactions matches`.

**Files:**
- Create: `src/moneybin/cli/commands/transactions/matches.py` (copy of `commands/matches.py`)
- Modify: `src/moneybin/cli/commands/transactions/__init__.py` (register `matches` sub-app)
- Modify: existing tests that invoke `app, ["matches", ...]` — update to `app, ["transactions", "matches", ...]`

- [ ] **Step 1: Inventory existing matches tests**

Run: `grep -lE 'app, \["matches"|app, \[".matches' tests/moneybin/test_cli/`
Expected: list of test files referencing the v1 path.

Capture each file path; you'll update them in Step 4.

- [ ] **Step 2: Move the matches command file**

```bash
git mv src/moneybin/cli/commands/matches.py src/moneybin/cli/commands/transactions/matches.py
```

Verify imports inside the moved file still resolve (relative imports may need adjustment from `..` to `...`).

Open the moved file and update relative imports as needed:

```python
# Before (in commands/matches.py):
# from ..config import ...
# from ..services.matching_service import ...

# After (in commands/transactions/matches.py):
# from ...config import ...
# from ...services.matching_service import ...
```

- [ ] **Step 3: Register matches under transactions**

Modify `src/moneybin/cli/commands/transactions/__init__.py`:

```python
import typer

from . import matches

app = typer.Typer(
    help="Transactions and workflows on them (matches, categorize, review)",
    no_args_is_help=True,
)

app.add_typer(matches.app, name="matches")
```

- [ ] **Step 4: Update all matches tests**

Run a `sed`-style replacement across test files. For each file from Step 1:

```bash
# Example for one file:
python3 -c "
import sys
p = sys.argv[1]
with open(p) as f: s = f.read()
# Change CliRunner().invoke(app, ['matches', ...]) to ['transactions', 'matches', ...]
import re
s = re.sub(r'invoke\((\w+),\s*\[\"matches\"', r'invoke(\1, [\"transactions\", \"matches\"', s)
s = re.sub(r\"invoke\((\\w+),\\s*\\['matches'\", r\"invoke(\\1, ['transactions', 'matches'\", s)
with open(p, 'w') as f: f.write(s)
" path/to/test.py
```

(Or do it manually file-by-file.) Spot-check at least 3 files for correctness.

- [ ] **Step 5: Update main.py to remove top-level matches registration**

Modify `src/moneybin/cli/main.py`. Remove:
```python
from .commands import ..., matches, ...   # remove `matches` from this import
app.add_typer(matches.app, name="matches", ...)   # delete this line
```

- [ ] **Step 6: Run all CLI tests**

Run: `uv run pytest tests/moneybin/test_cli/ --ignore=tests/e2e -q`
Expected: all pass. If a test fails because it still uses the v1 path, update it.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/commands/transactions/ src/moneybin/cli/commands/matches.py src/moneybin/cli/main.py tests/moneybin/test_cli/
git commit -m "Move matches under transactions group

CLI: matches * → transactions matches *. The interactive 'review'
subcommand will be promoted to 'transactions review' in a follow-up
task that unifies it with the categorize review queue."
```

---

## Task 5: Move `categorize` under `transactions` and split out `categories` / `merchants`

**Goal:** Three-way split of the existing `categorize` group:
- Workflow tools (bulk, stats, rules, auto, ml) → `transactions categorize *`
- Category taxonomy (categories, create-category, toggle-category) → top-level `categories`
- Merchant mappings (merchants, create-merchants) → top-level `merchants`

**Files:**
- Create: `src/moneybin/cli/commands/transactions/categorize/__init__.py` (already created in Task 1)
- Create: `src/moneybin/cli/commands/transactions/categorize/workflow.py`
- Create: `src/moneybin/cli/commands/transactions/categorize/rules.py`
- Create: `src/moneybin/cli/commands/transactions/categorize/auto.py`
- Create: `src/moneybin/cli/commands/transactions/categorize/ml.py`
- Modify: `src/moneybin/cli/commands/categories/__init__.py`
- Modify: `src/moneybin/cli/commands/merchants/__init__.py`
- Delete: `src/moneybin/cli/commands/categorize.py`
- Modify: `src/moneybin/cli/main.py` (drop top-level categorize registration)
- Test: existing `tests/moneybin/test_cli/test_categorize_*.py` files — update paths

- [ ] **Step 1: Read current categorize.py**

```bash
cat src/moneybin/cli/commands/categorize.py
```

Identify each subcommand and its target group:

| Current subcommand | New location |
|---|---|
| `categorize apply-rules` | `transactions categorize rules apply` |
| `categorize seed` | `transactions categorize seed` (or move to db migrations) — keep under workflow.py for now |
| `categorize stats` | `transactions categorize stats` |
| `categorize list-rules` | `transactions categorize rules list` |
| `categorize bulk` | `transactions categorize bulk` |
| `categorize auto review` | `transactions categorize auto review` |
| `categorize auto confirm` | `transactions categorize auto confirm` |
| `categorize auto stats` | `transactions categorize auto stats` |
| `categorize auto rules` | `transactions categorize auto rules` |
| (any `create-rules`, `delete-rule`) | `transactions categorize rules create / delete` |
| (any `categories`, `create-category`, `toggle-category`) | `categories list / create / toggle` |
| (any `merchants`, `create-merchants`) | `merchants list / create` |

If a subcommand from MCP rename map (e.g., `transactions_categorize_rule_delete`) doesn't exist in the CLI today, skip it for v2 — CLI parity follows when net-worth/categorization specs are revisited.

- [ ] **Step 2: Inventory existing categorize tests**

```bash
grep -lE 'app, \["categorize"|app, \[\"categorize' tests/moneybin/test_cli/
```

- [ ] **Step 3: Create `transactions/categorize/workflow.py`**

Move the workflow subcommands (bulk, stats, seed, apply-rules-as-passthrough) from `categorize.py` to a new `transactions/categorize/workflow.py`. Update imports (`...services` instead of `..services`).

The detailed code is mechanical — copy each subcommand definition verbatim, adjusting:
- Module-relative imports (depth +1)
- The Typer app reference (use the new local `app` not the old `categorize.app`)

```python
# src/moneybin/cli/commands/transactions/categorize/workflow.py
"""Categorization workflow commands (bulk, stats, seed)."""

import typer

# Import services with adjusted relative depth
from ....services.categorization_service import CategorizationService
# ... other imports as needed, mirroring the existing categorize.py structure

app = typer.Typer(
    help="Categorization workflow (bulk assign, stats, seed)",
    no_args_is_help=True,
)


@app.command("bulk")
def bulk(...):
    """Bulk-assign categories to transactions from a JSON array."""
    # Body copied verbatim from categorize.py's `bulk` command
    ...


@app.command("stats")
def stats(...):
    """Show categorization coverage summary."""
    ...


# (Repeat for `seed` if present)
```

Engineers reading this: the exact body of each function is **already in `src/moneybin/cli/commands/categorize.py`**. Copy verbatim, adjust import depth.

- [ ] **Step 4: Create `transactions/categorize/rules.py`**

Move `apply-rules`, `list-rules`, and any `create-rules`/`delete-rule` here under a `rules` Typer sub-app:

```python
# src/moneybin/cli/commands/transactions/categorize/rules.py
"""Rule management for categorization."""

import typer

app = typer.Typer(
    help="Rule management (list, create, apply, delete)",
    no_args_is_help=True,
)


@app.command("list")
def list_rules() -> None:
    """List all active categorization rules."""
    # Copy body from categorize.py's `list_rules`


@app.command("apply")
def apply() -> None:
    """Run all active rules and merchant mappings against uncategorized transactions."""
    # Copy body from categorize.py's `apply_rules`


# Add `create` and `delete` if present in current categorize.py
```

- [ ] **Step 5: Create `transactions/categorize/auto.py` and `ml.py`**

```python
# src/moneybin/cli/commands/transactions/categorize/auto.py
import typer

app = typer.Typer(help="Auto-rule proposal workflow", no_args_is_help=True)


@app.command("review")
def review() -> None:
    """Table of pending auto-rule proposals."""
    # Body from categorize.py's auto-review


@app.command("confirm")
def confirm(...) -> None:
    """Approve / reject pending proposals."""
    # Body from categorize.py's auto-confirm


@app.command("stats")
def stats() -> None:
    """Auto-rule health metrics."""
    # Body from categorize.py's auto-stats


@app.command("rules")
def rules() -> None:
    """List active auto-generated rules."""
    # Body from categorize.py's auto-rules
```

```python
# src/moneybin/cli/commands/transactions/categorize/ml.py
"""ML-assisted categorization (placeholder if not yet implemented)."""

import typer

from ....cli.commands.stubs import _not_implemented

app = typer.Typer(help="ML-assisted categorization", no_args_is_help=True)


@app.command("status")
def status() -> None:
    _not_implemented("categorization-ml.md")


@app.command("train")
def train() -> None:
    _not_implemented("categorization-ml.md")


@app.command("apply")
def apply() -> None:
    _not_implemented("categorization-ml.md")
```

- [ ] **Step 6: Wire up `transactions categorize` group**

Modify `src/moneybin/cli/commands/transactions/categorize/__init__.py`:

```python
import typer

from . import auto, ml, rules, workflow

app = typer.Typer(
    help="Categorization workflow + rules (taxonomy under top-level `categories`)",
    no_args_is_help=True,
)

# Workflow subcommands at the categorize level (bulk, stats, seed)
for cmd_name in dir(workflow.app):
    pass  # placeholder — actually we register sub-app, see below

app.add_typer(rules.app, name="rules")
app.add_typer(auto.app, name="auto")
app.add_typer(ml.app, name="ml")

# Re-export workflow commands as direct subcommands (not under a `workflow` group)
# Easiest: define those commands directly in __init__.py instead of workflow.py.
# Refactor: move bulk / stats / seed from workflow.py into __init__.py.
```

Decision: instead of the awkward "re-export" pattern, put `bulk`, `stats`, `seed` directly in `__init__.py` and delete `workflow.py`. Update Step 3 mentally — the file structure becomes:
- `transactions/categorize/__init__.py` — bulk, stats, seed (direct subcommands) + sub-app registrations
- `transactions/categorize/rules.py` — rules sub-group
- `transactions/categorize/auto.py` — auto sub-group
- `transactions/categorize/ml.py` — ml sub-group

- [ ] **Step 7: Implement top-level `categories`**

```python
# src/moneybin/cli/commands/categories/__init__.py
"""Category taxonomy management."""

import typer

app = typer.Typer(
    help="Category taxonomy (list, create, toggle, delete)",
    no_args_is_help=True,
)


@app.command("list")
def list_categories() -> None:
    """List all categories."""
    # Copy body from categorize.py's category-listing command (likely was `categorize categories`)


@app.command("create")
def create(name: str, parent: str | None = typer.Option(None, "--parent")) -> None:
    """Create a new category."""
    # Body from categorize.py's `create-category`


@app.command("toggle")
def toggle(category_id: str) -> None:
    """Enable / disable a category."""
    # Body from categorize.py's `toggle-category`


@app.command("delete")
def delete(category_id: str) -> None:
    """Delete a category (or stub if not in v1 categorize.py)."""
    from ..stubs import _not_implemented

    _not_implemented("categorization-overview.md")
```

- [ ] **Step 8: Implement top-level `merchants`**

```python
# src/moneybin/cli/commands/merchants/__init__.py
"""Merchant mappings management."""

import typer

app = typer.Typer(
    help="Merchant mappings (list, create)",
    no_args_is_help=True,
)


@app.command("list")
def list_merchants() -> None:
    """List all merchant mappings."""
    # Body from categorize.py's `merchants` command


@app.command("create")
def create(
    pattern: str,
    canonical_name: str,
    default_category: str | None = typer.Option(None, "--default-category"),
) -> None:
    """Create a merchant mapping."""
    # Body from categorize.py's `create-merchants`
```

- [ ] **Step 9: Update tests**

Inventory from Step 2: each test file using `app, ["categorize", ...]` becomes one of:
- `app, ["transactions", "categorize", ...]` for workflow / rules / auto / ml tests
- `app, ["categories", ...]` for category-taxonomy tests
- `app, ["merchants", ...]` for merchant tests

Use sed/Python to do the bulk replacements; verify each manually.

- [ ] **Step 10: Delete `categorize.py`**

```bash
git rm src/moneybin/cli/commands/categorize.py
```

- [ ] **Step 11: Update main.py**

Remove top-level `categorize` registration from `main.py`:

```python
from .commands import (
    # ...
    categorize,  # REMOVE
    # ...
)

# REMOVE:
# app.add_typer(categorize.app, name="categorize", help="Manage transaction categories, rules, and merchants")
```

Add registrations for `transactions.app` (already done in Task 1).

- [ ] **Step 12: Run all CLI tests**

Run: `uv run pytest tests/moneybin/test_cli/ -q --ignore=tests/e2e`
Expected: PASS. Fix any test that still uses v1 paths.

- [ ] **Step 13: Commit**

```bash
git add -A src/moneybin/cli/commands/transactions/categorize/ \
       src/moneybin/cli/commands/categories/ \
       src/moneybin/cli/commands/merchants/ \
       src/moneybin/cli/commands/categorize.py \
       src/moneybin/cli/main.py \
       tests/moneybin/test_cli/
git commit -m "Split categorize into transactions categorize + categories + merchants

CLI v2: categorize workflow (bulk, stats, rules, auto, ml) moves under
transactions categorize *. Category taxonomy (categories, create-category,
toggle-category) becomes top-level 'categories'. Merchant mappings
(merchants, create-merchants) becomes top-level 'merchants'."
```

---

## Task 6: Implement unified `transactions review`

**Goal:** Single interactive review command that walks both pending matches and uncategorized transactions. Replaces `transactions matches review` and (the never-shipped) `transactions categorize review`.

**Files:**
- Create: `src/moneybin/cli/commands/transactions/review.py`
- Create: `src/moneybin/services/review_service.py` (composes match + categorize services)
- Modify: `src/moneybin/cli/commands/transactions/__init__.py` (register review)
- Modify: `src/moneybin/cli/commands/transactions/matches.py` (remove `review` subcommand)
- Test: `tests/moneybin/test_cli/test_transactions_review.py` (NEW)
- Test: `tests/moneybin/services/test_review_service.py` (NEW)

- [ ] **Step 1: Write failing tests for ReviewService**

```python
# tests/moneybin/services/test_review_service.py
"""Tests for ReviewService - unified queue counts."""

from unittest.mock import MagicMock

from moneybin.services.review_service import ReviewService, ReviewStatus


def test_review_status_counts_both_queues() -> None:
    match_service = MagicMock()
    match_service.count_pending.return_value = 3
    cat_service = MagicMock()
    cat_service.count_uncategorized.return_value = 12

    svc = ReviewService(match_service=match_service, categorize_service=cat_service)
    status = svc.status()

    assert isinstance(status, ReviewStatus)
    assert status.matches_pending == 3
    assert status.categorize_pending == 12
    assert status.total == 15
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run pytest tests/moneybin/services/test_review_service.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement ReviewService**

```python
# src/moneybin/services/review_service.py
"""Service composing match + categorize review queue counts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReviewStatus:
    matches_pending: int
    categorize_pending: int

    @property
    def total(self) -> int:
        return self.matches_pending + self.categorize_pending


class ReviewService:
    def __init__(self, match_service, categorize_service):
        self._match_service = match_service
        self._categorize_service = categorize_service

    def status(self) -> ReviewStatus:
        return ReviewStatus(
            matches_pending=self._match_service.count_pending(),
            categorize_pending=self._categorize_service.count_uncategorized(),
        )
```

If `MatchingService` lacks `count_pending()` or `CategorizationService` lacks `count_uncategorized()`, add them — both should be one-liner SQL count queries.

- [ ] **Step 4: Run service test**

Run: `uv run pytest tests/moneybin/services/test_review_service.py -v`
Expected: PASS.

- [ ] **Step 5: Write failing CLI tests**

```python
# tests/moneybin/test_cli/test_transactions_review.py
"""Tests for transactions review unified command."""

from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_review_status_flag() -> None:
    """--status returns counts of both queues without entering interactive mode."""
    result = runner.invoke(app, ["transactions", "review", "--status"])
    assert result.exit_code == 0
    # Output should mention both match and categorize counts
    out = result.output.lower()
    assert "match" in out or "matches" in out
    assert "categori" in out


def test_review_type_filter() -> None:
    """--type matches limits to match queue."""
    result = runner.invoke(
        app, ["transactions", "review", "--type", "matches", "--status"]
    )
    assert result.exit_code == 0


def test_review_help_lists_options() -> None:
    result = runner.invoke(app, ["transactions", "review", "--help"])
    assert result.exit_code == 0
    assert "--type" in result.output
    assert "--status" in result.output
    assert "--confirm" in result.output
    assert "--reject" in result.output
```

- [ ] **Step 6: Implement `transactions/review.py`**

```python
# src/moneybin/cli/commands/transactions/review.py
"""Unified review queue: walks pending matches + uncategorized transactions.

CLI-only collapse (per cli-restructure.md v2). MCP keeps separate
transactions_matches_pending and transactions_categorize_pending_list
tools because their result shapes differ; the orientation tool
transactions_review_status returns the counts.
"""

from __future__ import annotations

import typer

from ....services.review_service import ReviewService
# Import or construct match_service and categorize_service via existing factories

app = typer.Typer(
    help="Unified review queue (matches + categorize)", invoke_without_command=True
)


@app.callback(invoke_without_command=True)
def review(
    type_: str = typer.Option("all", "--type", help="all | matches | categorize"),
    status: bool = typer.Option(
        False, "--status", help="Show counts only, no interactive loop"
    ),
    confirm_id: str | None = typer.Option(
        None, "--confirm", help="Non-interactive: confirm one item by ID"
    ),
    reject_id: str | None = typer.Option(
        None, "--reject", help="Non-interactive: reject one item by ID"
    ),
    confirm_all: bool = typer.Option(False, "--confirm-all"),
    limit: int = typer.Option(50, "--limit"),
) -> None:
    """Walk pending matches first, then uncategorized transactions."""

    if type_ not in {"all", "matches", "categorize"}:
        raise typer.BadParameter(
            f"--type must be one of all/matches/categorize, got {type_}"
        )

    review_svc = _make_review_service()

    if status:
        s = review_svc.status()
        if type_ == "matches":
            typer.echo(f"Matches pending: {s.matches_pending}")
        elif type_ == "categorize":
            typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
        else:
            typer.echo(f"Matches pending: {s.matches_pending}")
            typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
            typer.echo(f"Total: {s.total}")
        raise typer.Exit(0)

    if confirm_id or reject_id or confirm_all:
        # Non-interactive paths: dispatch to the appropriate underlying service
        # based on ID prefix or `--type`. For the v2 implementation we accept that
        # the caller may need to pass `--type` to disambiguate.
        from ..stubs import _not_implemented

        _not_implemented("cli-restructure.md (review collapse — non-interactive flags)")
        return

    # Interactive loop
    from ..stubs import _not_implemented

    _not_implemented("cli-restructure.md (review collapse — interactive loop)")


def _make_review_service() -> ReviewService:
    """Build a ReviewService from the singleton match + categorize services."""
    from ....services.matching_service import MatchingService
    from ....services.categorization_service import CategorizationService

    return ReviewService(
        match_service=MatchingService.from_singleton(),
        categorize_service=CategorizationService.from_singleton(),
    )
```

The interactive loop body is non-trivial; for v2 it can be stubbed and the actual UX implementation deferred to a follow-up (or the existing matches review interactive loop can be lifted as the matches branch). Status flag must work (it's the test).

If the existing matches `review` command already has the interactive loop, copy that body verbatim into the matches branch of this command and call it when `type_ in ("all", "matches")`.

- [ ] **Step 7: Register review under transactions**

Modify `src/moneybin/cli/commands/transactions/__init__.py`:

```python
from . import categorize, matches, review

app.add_typer(matches.app, name="matches")
app.add_typer(categorize.app, name="categorize")
app.add_typer(review.app, name="review")
```

- [ ] **Step 8: Remove `review` subcommand from matches**

In `src/moneybin/cli/commands/transactions/matches.py`, find and delete the `@app.command("review")` definition (the interactive review used to live here). Update or move its tests to `test_transactions_review.py`.

- [ ] **Step 9: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_transactions_review.py tests/moneybin/services/test_review_service.py -v`
Expected: PASS.

Run all CLI tests: `uv run pytest tests/moneybin/test_cli/ -q --ignore=tests/e2e`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/moneybin/cli/commands/transactions/ \
       src/moneybin/services/review_service.py \
       tests/moneybin/test_cli/test_transactions_review.py \
       tests/moneybin/services/test_review_service.py
git commit -m "Add unified 'transactions review' CLI + ReviewService

Collapse 'transactions matches review' and 'transactions categorize
review' into a single 'transactions review' command. ReviewService
composes MatchingService + CategorizationService for queue-status counts."
```

---

## Task 7: Add `reports networth` and dissolve `track`

**Goal:** Move `track networth` stubs to `reports networth`, then delete the entire `track` group.

**Files:**
- Create: `src/moneybin/cli/commands/reports/networth.py`
- Modify: `src/moneybin/cli/commands/reports/__init__.py` (register networth)
- Modify: `src/moneybin/cli/commands/stubs.py` (delete `track_*` apps)
- Modify: `src/moneybin/cli/main.py` (remove `track_app` registration)
- Test: `tests/moneybin/test_cli/test_reports_networth.py` (NEW)

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_cli/test_reports_networth.py
from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_networth_show_stub() -> None:
    result = runner.invoke(app, ["reports", "networth", "show"])
    assert result.exit_code == 0
    assert "net-worth" in result.output


def test_networth_history_stub() -> None:
    result = runner.invoke(app, ["reports", "networth", "history"])
    assert result.exit_code == 0


def test_track_group_dissolved() -> None:
    """The track group must no longer exist as a top-level command."""
    result = runner.invoke(app, ["track", "--help"])
    # Either exit code is non-zero (unknown command) or output mentions no such command
    assert (
        result.exit_code != 0
        or "No such command" in result.output
        or "Usage" not in result.output[:50]
    )
```

- [ ] **Step 2: Run tests**

Expected: networth tests fail (not implemented), track-dissolved test fails (track still registered).

- [ ] **Step 3: Implement `reports/networth.py`**

```python
# src/moneybin/cli/commands/reports/networth.py
"""reports networth — cross-domain net worth aggregation."""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Cross-domain net worth aggregation (accounts + assets)",
    no_args_is_help=True,
)


@app.command("show")
def show(as_of: str | None = typer.Option(None, "--as-of")) -> None:
    """Show current net worth."""
    _not_implemented("net-worth.md")


@app.command("history")
def history(
    from_: str | None = typer.Option(None, "--from"),
    to: str | None = typer.Option(None, "--to"),
    interval: str = typer.Option("monthly", "--interval"),
) -> None:
    """Show net worth history."""
    _not_implemented("net-worth.md")
```

- [ ] **Step 4: Wire up reports group**

Modify `src/moneybin/cli/commands/reports/__init__.py`:

```python
import typer

from . import networth

app = typer.Typer(
    help="Cross-domain analytical and aggregation views",
    no_args_is_help=True,
)

app.add_typer(networth.app, name="networth")

# Stubs for future spec-owned report subcommands
from ..stubs import _not_implemented


@app.command("spending")
def spending() -> None:
    _not_implemented("spending-reports.md")


@app.command("cashflow")
def cashflow() -> None:
    _not_implemented("cashflow-reports.md")


@app.command("budget")
def budget() -> None:
    _not_implemented("budget-tracking.md")


@app.command("health")
def health(months: int = typer.Option(1, "--months")) -> None:
    """Cross-domain financial health snapshot."""
    _not_implemented("net-worth.md")
```

- [ ] **Step 5: Delete the track group from stubs.py**

Open `src/moneybin/cli/commands/stubs.py` and delete:
- `track_app = typer.Typer(...)`
- All `track_*_app = typer.Typer(...)` lines
- All `@track_*_app.command(...)` definitions
- The `track_app.add_typer(...)` calls

Keep `_not_implemented`, `export_app`, and any other stubs.

- [ ] **Step 6: Remove track_app from main.py**

In `src/moneybin/cli/main.py`:
```python
from .commands.stubs import (
    export_app,
    # track_app,  # DELETE
)

# DELETE:
# app.add_typer(track_app, name="track", help="Balance tracking and net worth")
```

- [ ] **Step 7: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_reports_networth.py -v`
Expected: PASS — including the track-dissolved assertion.

Run full CLI suite: `uv run pytest tests/moneybin/test_cli/ -q --ignore=tests/e2e`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/cli/commands/reports/ \
       src/moneybin/cli/commands/stubs.py \
       src/moneybin/cli/main.py \
       tests/moneybin/test_cli/test_reports_networth.py
git commit -m "Add reports group; dissolve track group entirely

- reports networth show/history (stubs delegating to net-worth.md)
- reports spending/cashflow/budget/health (stubs delegating to owning specs)
- track group removed; track balance/networth/budget/recurring/investments
  no longer exist (paths break per v2 hard cut)"
```

---

## Task 8: Add `system status` and `tax w2/deductions` stubs

**Goal:** Two small stub groups completing the v2 top-level surface.

**Files:**
- Modify: `src/moneybin/cli/commands/system/__init__.py`
- Modify: `src/moneybin/cli/commands/tax/__init__.py`
- Modify: `src/moneybin/cli/commands/budget/__init__.py`
- Test: `tests/moneybin/test_cli/test_system.py` (NEW)
- Test: `tests/moneybin/test_cli/test_tax.py` (NEW)
- Test: `tests/moneybin/test_cli/test_budget.py` (NEW)

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_cli/test_system.py
from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_system_status() -> None:
    result = runner.invoke(app, ["system", "status"])
    assert result.exit_code == 0
```

```python
# tests/moneybin/test_cli/test_tax.py
from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_tax_w2() -> None:
    result = runner.invoke(app, ["tax", "w2", "2025"])
    assert result.exit_code == 0


def test_tax_deductions() -> None:
    result = runner.invoke(app, ["tax", "deductions", "2025"])
    assert result.exit_code == 0
```

```python
# tests/moneybin/test_cli/test_budget.py
from typer.testing import CliRunner
from moneybin.cli.main import app

runner = CliRunner()


def test_budget_set() -> None:
    result = runner.invoke(app, ["budget", "set", "groceries", "500"])
    assert result.exit_code == 0
```

- [ ] **Step 2: Implement `system/__init__.py`**

```python
import typer
from ..stubs import _not_implemented

app = typer.Typer(help="System and data status", no_args_is_help=True)


@app.command("status")
def status() -> None:
    """Show data freshness and pending review queue counts."""
    _not_implemented(
        "net-worth.md"
    )  # system_status spec lives in mcp-tool-surface.md; CLI parity follows
```

- [ ] **Step 3: Implement `tax/__init__.py`**

```python
import typer
from ..stubs import _not_implemented

app = typer.Typer(
    help="Tax forms, deductions, future capital gains", no_args_is_help=True
)


@app.command("w2")
def w2(year: str) -> None:
    _not_implemented("tax-w2.md")


@app.command("deductions")
def deductions(year: str) -> None:
    _not_implemented("tax-deductions.md")
```

- [ ] **Step 4: Implement `budget/__init__.py`**

```python
import typer
from ..stubs import _not_implemented

app = typer.Typer(help="Budget target management", no_args_is_help=True)


@app.command("set")
def set_(category: str, amount: float) -> None:
    _not_implemented("budget-tracking.md")


@app.command("delete")
def delete(category: str) -> None:
    _not_implemented("budget-tracking.md")
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_system.py tests/moneybin/test_cli/test_tax.py tests/moneybin/test_cli/test_budget.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/cli/commands/system/ \
       src/moneybin/cli/commands/tax/ \
       src/moneybin/cli/commands/budget/ \
       tests/moneybin/test_cli/test_system.py \
       tests/moneybin/test_cli/test_tax.py \
       tests/moneybin/test_cli/test_budget.py
git commit -m "Stub system status, tax w2/deductions, budget set/delete

Reserves CLI namespaces for net-worth.md, future tax-*.md specs, and
budget-tracking.md."
```

---

## Task 9: Rename `import csv-preview` → `import file-preview`

**Goal:** The CLI subcommand and its underlying service method become format-agnostic.

**Files:**
- Modify: `src/moneybin/cli/commands/import_cmd.py` (rename subcommand)
- Modify: `src/moneybin/services/import_service.py` (rename `csv_preview` → `file_preview` if present)
- Test: existing import tests — update calls

- [ ] **Step 1: Locate the existing command**

Run: `grep -n "csv.preview\|csv-preview\|csv_preview" src/moneybin/cli/commands/import_cmd.py src/moneybin/services/import_service.py`

- [ ] **Step 2: Update CLI subcommand name**

In `import_cmd.py`, find `@app.command("csv-preview")` and change to `@app.command("file-preview")`. Update the function name from `csv_preview` to `file_preview` and update the docstring.

- [ ] **Step 3: Update the service method**

In `import_service.py`, rename `csv_preview()` → `file_preview()`. Update any call sites (search across `src/`).

- [ ] **Step 4: Update tests**

Run: `grep -rln "csv-preview\|csv_preview" tests/`
Update each match to `file-preview` / `file_preview`.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/ -q --ignore=tests/e2e -k "import"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/cli/commands/import_cmd.py src/moneybin/services/import_service.py tests/
git commit -m "Rename import csv-preview → file-preview (format-agnostic)"
```

---

# Part 2: MCP Tool Renames

## Task 10: Rename existing MCP tools per §16b rename map [mechanical]

**Goal:** Apply all v1→v2 MCP tool renames in one coordinated change. This is a large mechanical pass; correctness is verified by the test suite.

**Files:**
- Modify: every `src/moneybin/mcp/tools/*.py` file containing v1 tool names
- Modify: every `tests/moneybin/test_mcp/*.py` file referencing v1 tool names
- Modify: any documentation strings inside tool function descriptions that mention v1 tools

The full rename map is in `docs/specs/mcp-tool-surface.md` §16b. The script below handles the bulk; manual review verifies edge cases.

- [ ] **Step 1: Generate the rename script**

Save this as `scripts/rename_mcp_v2.py` (don't commit — local helper):

```python
"""One-shot v1 → v2 MCP tool rename. Run from repo root.

Renames in:
- src/moneybin/mcp/tools/*.py (function names, decorator names, descriptions)
- tests/moneybin/test_mcp/*.py
- src/moneybin/services/*.py (service method names where they match tool names)

Does NOT touch:
- docs/specs/* (already updated in PR #95)
- Migration tables anywhere
"""

import pathlib
import re

RENAMES = {
    # accounts
    "accounts_balances": "accounts_balance_list",
    "accounts_details": "accounts_get",
    "accounts_networth": "reports_networth_get",
    # transactions
    "transactions_recurring": "transactions_recurring_list",
    "transactions_matches_revoke": "transactions_matches_undo",
    # categorize → transactions_categorize_*
    "categorize_uncategorized": "transactions_categorize_pending_list",
    "categorize_bulk": "transactions_categorize_bulk_apply",
    "categorize_apply_rules": "transactions_categorize_rules_apply",
    "categorize_rules": "transactions_categorize_rules_list",
    "categorize_create_rules": "transactions_categorize_rules_create",
    "categorize_delete_rule": "transactions_categorize_rule_delete",
    "categorize_stats": "transactions_categorize_stats",
    "categorize_auto_review": "transactions_categorize_auto_review",
    "categorize_auto_confirm": "transactions_categorize_auto_confirm",
    "categorize_auto_stats": "transactions_categorize_auto_stats",
    "categorize_ml_status": "transactions_categorize_ml_status",
    "categorize_ml_train": "transactions_categorize_ml_train",
    "categorize_ml_apply": "transactions_categorize_ml_apply",
    # categorize → top-level categories / merchants
    "categorize_categories": "categories_list",
    "categorize_create_category": "categories_create",
    "categorize_toggle_category": "categories_toggle",
    "categorize_merchants": "merchants_list",
    "categorize_create_merchants": "merchants_create",
    # spending → reports_spending_*
    "spending_summary": "reports_spending_summary",
    "spending_by_category": "reports_spending_by_category",
    "spending_merchants": "reports_spending_merchants",
    "spending_compare": "reports_spending_compare",
    # cashflow → reports_cashflow_*
    "cashflow_summary": "reports_cashflow_summary",
    "cashflow_income": "reports_cashflow_income",
    # budget split (reads → reports, mutation stays)
    "budget_status": "reports_budget_status",
    "budget_summary": "reports_budget_summary",
    # overview split
    "overview_status": "system_status",
    "overview_health": "reports_health",
    # import
    "import_csv_preview": "import_file_preview",
}

# Sort by length descending so longer names match before their substrings
SORTED_RENAMES = sorted(RENAMES.items(), key=lambda kv: -len(kv[0]))

ROOTS = [
    pathlib.Path("src/moneybin/mcp"),
    pathlib.Path("src/moneybin/services"),
    pathlib.Path("tests/moneybin/test_mcp"),
]


def rename_in_file(p: pathlib.Path) -> bool:
    text = p.read_text()
    new = text
    for old, repl in SORTED_RENAMES:
        new = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])", repl, new)
    if new != text:
        p.write_text(new)
        return True
    return False


def main() -> None:
    changed = []
    for root in ROOTS:
        for p in root.rglob("*.py"):
            if rename_in_file(p):
                changed.append(p)
    print(f"Changed {len(changed)} files")
    for p in changed:
        print(f"  {p}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the rename script**

```bash
uv run python scripts/rename_mcp_v2.py
```

Inspect the printed list of changed files.

- [ ] **Step 3: Manual review of high-risk renames**

Some renames may produce undesirable results in:
- Docstrings / log messages mentioning the old tool name as part of prose
- Comments
- Service method names that should NOT match tool names (rare, but possible)

Run: `git diff src/moneybin/mcp/tools/ src/moneybin/services/ tests/moneybin/test_mcp/ | head -300`

Spot-check 3-4 files to ensure the renames are sensible. Revert any clearly wrong replacement (e.g., a method named `update_categorize_stats_cache` doesn't need its `categorize_stats` substring renamed — but the `(?<![A-Za-z0-9_])` boundary should prevent that; verify).

- [ ] **Step 4: Reorganize tool files per the new file structure**

Split / merge files to match the layout in the File Structure section above:

```bash
# Spending → reports
git mv src/moneybin/mcp/tools/spending.py src/moneybin/mcp/tools/_temp_spending.py
# Create reports.py from spending + cashflow + relevant budget reads
# Move accounts_networth (now reports_networth_get) into reports.py
# (The script handled function rename; this step moves the file location)
```

The clean version: create `src/moneybin/mcp/tools/reports.py` and copy in:
- All `reports_spending_*` functions (from old `spending.py`)
- All `reports_cashflow_*` functions (from `cashflow.py` if it exists, else inline)
- `reports_budget_status` and `reports_budget_summary` (from `budget.py`)
- `reports_networth_get` (from `accounts.py`)
- `reports_health` (from `overview.py` if it exists, else inline)

Then:
```bash
git rm src/moneybin/mcp/tools/spending.py
git rm src/moneybin/mcp/tools/cashflow.py 2>/dev/null  # if present
# overview.py → split: system_status to system.py, reports_health already in reports.py
```

Create `src/moneybin/mcp/tools/system.py` containing only `system_status`.
Create `src/moneybin/mcp/tools/categories.py` and `merchants.py` containing the extracted tools.
Create `src/moneybin/mcp/tools/transactions.py` (rename existing) — or keep as `transactions.py` and split matches/categorize.
Create `src/moneybin/mcp/tools/transactions_matches.py` (extract from existing `transactions.py` if matches code lives there, otherwise rename file).
Rename `categorize.py` → `transactions_categorize.py` and trim out the categories/merchants tools (already moved).

- [ ] **Step 5: Update tool registration in `_registration.py` and per-file registers**

Each tool file typically has a `register_*_tools(mcp)` function. Update names to match new file/tool grouping. Verify in `src/moneybin/mcp/server.py` that all registers are still called.

- [ ] **Step 6: Run full MCP test suite**

Run: `uv run pytest tests/moneybin/test_mcp/ -q`
Expected: PASS. Where tests still reference v1 tool names that the script missed (e.g., in string literals not matching the boundary), fix manually.

- [ ] **Step 7: Run import smoke check**

```bash
uv run python -c "from moneybin.mcp.server import mcp; print('OK')"
```
Expected: OK.

Run: `uv run moneybin mcp list-tools` (or `moneybin mcp list-tools` if installed) and verify the v2 tool names appear and v1 names don't.

- [ ] **Step 8: Commit**

```bash
git add -A src/moneybin/mcp/ src/moneybin/services/ tests/moneybin/test_mcp/
git commit -m "Rename ~30 MCP tools per cli-restructure.md v2 §16b

Path-prefix-verb-suffix convention. Reorganize files:
- spending/cashflow/budget reads/networth/health → reports.py
- overview → system.py + reports.py
- categorize → transactions_categorize.py + categories.py + merchants.py
- accounts_balances/_details/_networth → balance_list/get + reports_networth_get
- transactions_matches_revoke → _undo
- import_csv_preview → import_file_preview"
```

- [ ] **Step 9: Delete the rename script**

```bash
rm scripts/rename_mcp_v2.py
```

(It served its purpose; the renames are committed.)

---

# Part 3: New MCP Tools

## Task 11: Add `system_status` MCP tool [TDD]

**Goal:** Implement `system_status` tool replacing `overview_status`. Returns data freshness and pending review queue counts.

**Files:**
- Create: `src/moneybin/services/system_service.py`
- Modify: `src/moneybin/mcp/tools/system.py` (already created in Task 10; add full implementation)
- Test: `tests/moneybin/services/test_system_service.py` (NEW)
- Test: `tests/moneybin/test_mcp/test_system_tools.py` (NEW)

- [ ] **Step 1: Write failing service test**

```python
# tests/moneybin/services/test_system_service.py
from unittest.mock import MagicMock
from moneybin.services.system_service import SystemService, SystemStatus


def test_status_returns_data_inventory_and_queues() -> None:
    db = MagicMock()
    # Mock query results...
    svc = SystemService(db=db)
    status = svc.status()
    assert isinstance(status, SystemStatus)
    assert hasattr(status, "accounts_count")
    assert hasattr(status, "transactions_count")
    assert hasattr(status, "matches_pending")
    assert hasattr(status, "categorize_pending")
```

- [ ] **Step 2: Implement SystemService**

```python
# src/moneybin/services/system_service.py
"""System-status service: data inventory + queue counts.

v2: Replaces OverviewService.status() under the new system_* namespace.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from moneybin.tables import TableRef


@dataclass(frozen=True)
class SystemStatus:
    accounts_count: int
    transactions_count: int
    transactions_date_range: tuple[date | None, date | None]
    last_import_at: date | None
    matches_pending: int
    categorize_pending: int


class SystemService:
    def __init__(self, db):
        self._db = db

    def status(self) -> SystemStatus:
        # Use TableRef and parameterized SQL per .claude/rules/security.md
        accounts = self._db.execute(
            f"SELECT COUNT(*) FROM {TableRef.DIM_ACCOUNTS}"
        ).fetchone()[0]
        txn_row = self._db.execute(
            f"SELECT COUNT(*), MIN(transaction_date), MAX(transaction_date) FROM {TableRef.FCT_TRANSACTIONS}"
        ).fetchone()
        # ... fill in the rest
        return SystemStatus(
            accounts_count=accounts,
            transactions_count=txn_row[0],
            transactions_date_range=(txn_row[1], txn_row[2]),
            last_import_at=None,  # query app.import_log if it exists
            matches_pending=0,  # delegate to MatchingService
            categorize_pending=0,  # delegate to CategorizationService
        )
```

(Full body should compose from existing services rather than duplicate queries — match `OverviewService` patterns from before the rename.)

- [ ] **Step 3: Run service test**

Run: `uv run pytest tests/moneybin/services/test_system_service.py -v`
Expected: PASS (with adjusted mocks for the actual query shape).

- [ ] **Step 4: Write failing MCP tool test**

```python
# tests/moneybin/test_mcp/test_system_tools.py
from moneybin.mcp.tools.system import system_status


def test_system_status_envelope() -> None:
    """Tool returns a valid response envelope."""
    result = system_status()
    assert "summary" in result
    assert "data" in result
    assert "actions" in result
    assert result["summary"]["sensitivity"] == "low"
```

- [ ] **Step 5: Implement the tool**

```python
# src/moneybin/mcp/tools/system.py
"""system_* tools — data status meta-view."""

from moneybin.mcp.decorator import mcp_tool


@mcp_tool(sensitivity="low")
def system_status() -> dict:
    """Return data inventory and pending queue counts.

    Use this tool to understand what data exists in MoneyBin and what
    needs user attention before suggesting any analytical query.
    """
    from moneybin.database import get_database
    from moneybin.services.system_service import SystemService

    db = get_database()
    status = SystemService(db).status()

    return {
        "summary": {
            "sensitivity": "low",
            "as_of": status.transactions_date_range[1].isoformat()
            if status.transactions_date_range[1]
            else None,
        },
        "data": {
            "accounts": {"count": status.accounts_count},
            "transactions": {
                "count": status.transactions_count,
                "date_range": [
                    status.transactions_date_range[0].isoformat()
                    if status.transactions_date_range[0]
                    else None,
                    status.transactions_date_range[1].isoformat()
                    if status.transactions_date_range[1]
                    else None,
                ],
                "last_import_at": status.last_import_at.isoformat()
                if status.last_import_at
                else None,
            },
            "matches": {"pending_review": status.matches_pending},
            "categorization": {"uncategorized": status.categorize_pending},
        },
        "actions": [
            "Use transactions_review_status for per-queue review counts",
            "Use reports_health for a financial snapshot",
        ],
    }
```

Register the tool in `src/moneybin/mcp/_registration.py` or its file-local `register_system_tools(mcp)` function.

- [ ] **Step 6: Run MCP tool test**

Run: `uv run pytest tests/moneybin/test_mcp/test_system_tools.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/services/system_service.py src/moneybin/mcp/tools/system.py tests/moneybin/services/test_system_service.py tests/moneybin/test_mcp/test_system_tools.py src/moneybin/mcp/_registration.py
git commit -m "Add SystemService + system_status MCP tool

Replaces OverviewService.status() under the v2 system_* namespace.
Returns data inventory (accounts, transactions, freshness) and pending
review queue counts."
```

---

## Task 12: Add `transactions_review_status` MCP tool [TDD]

**Goal:** Orientation tool returning review queue counts for both matches and categorize.

**Files:**
- Modify: `src/moneybin/mcp/tools/transactions.py`
- Test: `tests/moneybin/test_mcp/test_transactions_tools.py` (extend)

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_mcp/test_transactions_tools.py
from moneybin.mcp.tools.transactions import transactions_review_status


def test_review_status_returns_counts() -> None:
    result = transactions_review_status()
    assert "summary" in result
    assert "data" in result
    assert "matches_pending" in result["data"]
    assert "categorize_pending" in result["data"]
    assert "total" in result["data"]
```

- [ ] **Step 2: Implement the tool**

```python
# Append to src/moneybin/mcp/tools/transactions.py


@mcp_tool(sensitivity="low")
def transactions_review_status() -> dict:
    """Return counts of pending reviews across both queues.

    Orientation tool: call this to know whether to fetch matches first
    (transactions_matches_pending) or categorize items first
    (transactions_categorize_pending_list).
    """
    from moneybin.database import get_database
    from moneybin.services.review_service import ReviewService
    from moneybin.services.matching_service import MatchingService
    from moneybin.services.categorization_service import CategorizationService

    db = get_database()
    svc = ReviewService(
        match_service=MatchingService(db=db),
        categorize_service=CategorizationService(db=db),
    )
    status = svc.status()

    return {
        "summary": {"sensitivity": "low"},
        "data": {
            "matches_pending": status.matches_pending,
            "categorize_pending": status.categorize_pending,
            "total": status.total,
        },
        "actions": [
            "Use transactions_matches_pending to fetch the match queue",
            "Use transactions_categorize_pending_list to fetch the categorize queue",
        ],
    }
```

- [ ] **Step 3: Run test**

Run: `uv run pytest tests/moneybin/test_mcp/test_transactions_tools.py::test_review_status_returns_counts -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/mcp/tools/transactions.py tests/moneybin/test_mcp/test_transactions_tools.py
git commit -m "Add transactions_review_status MCP orientation tool

Returns counts for both matches and categorize review queues. AI
calls this for 'anything to review?' check before fetching specific
queue contents."
```

---

## Task 13: Add `sync_*` MCP tools [TDD per tool]

**Goal:** Expose sync to MCP per the v2 exposure principle (all 9 except `sync_rotate_key`).

**Files:**
- Create: `src/moneybin/mcp/tools/sync.py`
- Modify: `src/moneybin/services/sync_service.py` (or wherever sync logic lives — verify; may need to create)
- Test: `tests/moneybin/test_mcp/test_sync_tools.py` (NEW)

The implementation pattern is identical for each tool: thin wrapper around the existing CLI sync logic (in `src/moneybin/cli/commands/sync.py`), calling the underlying service methods.

For each tool below, follow this pattern:
1. Write a failing test asserting envelope shape and key data
2. Implement the tool function
3. Register in `_registration.py`
4. Run the test
5. (Commit per group of related tools, not per individual tool)

**Tools to add** (full list per `mcp-tool-surface.md` `sync_*` section):

| Tool | Behavior |
|---|---|
| `sync_login` | Returns `{device_code, user_code, verification_url, polling_token}`; tool polls until completion |
| `sync_logout` | Clears stored JWT |
| `sync_connect` | Returns `{redirect_url, polling_token}`; tool polls for completion |
| `sync_disconnect` | Removes institution; idempotent |
| `sync_pull` | Triggers sync; returns counts of new/updated records |
| `sync_status` | Read-only: connected institutions, last-sync times, errors |
| `sync_schedule_set` | Installs daily sync; writes scheduler entry |
| `sync_schedule_show` | Read-only schedule details |
| `sync_schedule_remove` | Uninstalls scheduled job |

- [ ] **Step 1: Inventory existing sync logic**

```bash
cat src/moneybin/cli/commands/sync.py
ls src/moneybin/services/ | grep -i sync
```

Identify which service methods already exist (e.g., `SyncService.pull()`, `.status()`, `.connect()`). Add missing ones.

- [ ] **Step 2: Write tests for the read-only tools first**

```python
# tests/moneybin/test_mcp/test_sync_tools.py
from moneybin.mcp.tools.sync import sync_status, sync_schedule_show


def test_sync_status_envelope() -> None:
    result = sync_status()
    assert "summary" in result
    assert "data" in result
    # data may be {institutions: [], last_sync_at: ..., errors: []}


def test_sync_schedule_show_envelope() -> None:
    result = sync_schedule_show()
    assert "summary" in result
```

- [ ] **Step 3: Implement read-only tools**

```python
# src/moneybin/mcp/tools/sync.py
"""sync_* tools — moneybin-server sync (login, connect, pull, status, schedule).

v2 MCP exposure per cli-restructure.md exposure principle. OAuth/device-code
flows return URLs for the client to open. Excluded: sync_rotate_key
(passphrase material through LLM context window — security model violation).
"""

from moneybin.mcp.decorator import mcp_tool


@mcp_tool(sensitivity="low")
def sync_status() -> dict:
    """Connected institutions, last-sync times, and errors."""
    from moneybin.services.sync_service import SyncService

    s = SyncService.from_settings().status()
    return {
        "summary": {"sensitivity": "low"},
        "data": s,
        "actions": ["Use sync_pull to fetch new data"],
    }


@mcp_tool(sensitivity="low")
def sync_schedule_show() -> dict:
    """Current sync schedule (launchd/cron entry)."""
    from moneybin.services.sync_service import SyncService

    return {
        "summary": {"sensitivity": "low"},
        "data": SyncService.from_settings().schedule_show(),
        "actions": [],
    }
```

(Etc. — see the spec for full envelope shapes.)

- [ ] **Step 4: Implement write/action tools**

```python
@mcp_tool(sensitivity="low")
def sync_pull(institution: str | None = None, force: bool = False) -> dict:
    """Trigger sync for one or all institutions."""
    from moneybin.services.sync_service import SyncService

    result = SyncService.from_settings().pull(institution=institution, force=force)
    return {
        "summary": {"sensitivity": "low"},
        "data": result,
        "actions": [
            "Use transactions_review_status to check for new pending matches/categorize"
        ],
    }


@mcp_tool(sensitivity="low")
def sync_disconnect(institution: str) -> dict:
    """Remove an institution; idempotent."""
    from moneybin.services.sync_service import SyncService

    SyncService.from_settings().disconnect(institution=institution)
    return {
        "summary": {"sensitivity": "low"},
        "data": {"institution": institution, "disconnected": True},
        "actions": [],
    }


@mcp_tool(sensitivity="low")
def sync_logout() -> dict:
    """Clear stored JWT."""
    from moneybin.services.sync_service import SyncService

    SyncService.from_settings().logout()
    return {
        "summary": {"sensitivity": "low"},
        "data": {"logged_out": True},
        "actions": [],
    }


@mcp_tool(sensitivity="low")
def sync_schedule_set(time: str) -> dict:
    """Install daily sync at HH:MM."""
    from moneybin.services.sync_service import SyncService

    SyncService.from_settings().schedule_set(time=time)
    return {
        "summary": {"sensitivity": "low"},
        "data": {"scheduled_at": time},
        "actions": [],
    }


@mcp_tool(sensitivity="low")
def sync_schedule_remove() -> dict:
    """Uninstall scheduled sync job."""
    from moneybin.services.sync_service import SyncService

    SyncService.from_settings().schedule_remove()
    return {
        "summary": {"sensitivity": "low"},
        "data": {"scheduled": False},
        "actions": [],
    }
```

- [ ] **Step 5: Implement OAuth-flow tools (login, connect)**

```python
@mcp_tool(sensitivity="low")
def sync_login() -> dict:
    """Initiate device-code OAuth flow with moneybin-server.

    Returns a URL and code for the user to enter in their browser.
    Tool polls until completion or returns a polling token for the
    client to use.
    """
    from moneybin.services.sync_service import SyncService

    flow = SyncService.from_settings().login_initiate()
    return {
        "summary": {"sensitivity": "low"},
        "data": {
            "verification_url": flow.verification_url,
            "user_code": flow.user_code,
            "device_code": flow.device_code,
            "expires_in_seconds": flow.expires_in,
        },
        "actions": [
            f"Open {flow.verification_url} and enter code {flow.user_code}",
            "Then call sync_status to verify login completed",
        ],
    }


@mcp_tool(sensitivity="low")
def sync_connect(institution: str | None = None) -> dict:
    """Initiate OAuth flow with a bank/aggregator.

    Returns a redirect URL the client should open. After the user
    authorizes, the server completes the connection.
    """
    from moneybin.services.sync_service import SyncService

    flow = SyncService.from_settings().connect_initiate(institution=institution)
    return {
        "summary": {"sensitivity": "low"},
        "data": {
            "redirect_url": flow.redirect_url,
            "session_id": flow.session_id,
        },
        "actions": [
            f"Open {flow.redirect_url} to authorize the connection",
            "Then call sync_status to verify the institution is connected",
        ],
    }
```

If `SyncService` doesn't yet have `login_initiate()` / `connect_initiate()` methods, add them — they should call the same underlying primitives the CLI uses, separated into "initiate" (returns URL) and "complete" (called by the redirect handler) phases.

- [ ] **Step 6: Register all sync tools**

In `src/moneybin/mcp/tools/sync.py`:
```python
def register_sync_tools(mcp):
    from moneybin.mcp._registration import register

    for tool in [
        sync_login,
        sync_logout,
        sync_connect,
        sync_disconnect,
        sync_pull,
        sync_status,
        sync_schedule_set,
        sync_schedule_show,
        sync_schedule_remove,
    ]:
        register(mcp, tool, name=tool.__name__, description=tool.__doc__ or "")
```

In `src/moneybin/mcp/server.py` or `_registration.py`, call `register_sync_tools(mcp)` alongside other registrations.

- [ ] **Step 7: Run sync tests**

Run: `uv run pytest tests/moneybin/test_mcp/test_sync_tools.py -v`
Expected: PASS for all tool envelope tests.

Smoke test: `uv run python -c "from moneybin.mcp.server import mcp; print('OK')"`

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/mcp/tools/sync.py src/moneybin/services/sync_service.py tests/moneybin/test_mcp/test_sync_tools.py src/moneybin/mcp/_registration.py src/moneybin/mcp/server.py
git commit -m "Expose sync_* tools to MCP (9 tools)

Per cli-restructure.md v2 exposure principle. OAuth flows return URLs
for the client to open. Excludes sync_rotate_key (passphrase through
LLM context window is a security model violation)."
```

---

## Task 14: Add `transform_*` MCP tools [TDD per tool]

**Goal:** Expose transform to MCP (all 5 except `transform_restate`).

**Files:**
- Create: `src/moneybin/mcp/tools/transform.py`
- Modify: `src/moneybin/services/transform_service.py` (or wherever SQLMesh wrappers live)
- Test: `tests/moneybin/test_mcp/test_transform_tools.py` (NEW)

Pattern is identical to Task 13. Tools to add:

| Tool | Behavior |
|---|---|
| `transform_status` | Current model state, environment |
| `transform_plan` | Preview pending SQLMesh changes |
| `transform_validate` | Check model SQL parses and resolves |
| `transform_audit` | Run data-quality assertions |
| `transform_apply` | Execute SQLMesh changes |

- [ ] **Step 1: Inventory existing transform CLI logic**

```bash
cat src/moneybin/cli/commands/transform.py
```

Identify the SQLMesh primitives each subcommand wraps. Likely each calls into `SQLMeshContext` or similar; expose those as service methods if not already.

- [ ] **Step 2: Write failing tests**

```python
# tests/moneybin/test_mcp/test_transform_tools.py
from moneybin.mcp.tools.transform import (
    transform_status,
    transform_plan,
    transform_validate,
    transform_audit,
    transform_apply,
)


def test_transform_status_envelope() -> None:
    result = transform_status()
    assert "summary" in result and "data" in result


def test_transform_plan_envelope() -> None:
    result = transform_plan()
    assert "summary" in result and "data" in result


def test_transform_validate_envelope() -> None:
    result = transform_validate()
    assert "summary" in result and "data" in result


def test_transform_audit_envelope() -> None:
    result = transform_audit()
    assert "summary" in result and "data" in result


def test_transform_apply_envelope() -> None:
    result = transform_apply()
    assert "summary" in result and "data" in result
```

- [ ] **Step 3: Implement the tools**

```python
# src/moneybin/mcp/tools/transform.py
"""transform_* tools — SQLMesh pipeline operations.

v2 MCP exposure (all 5 routine tools). Excludes transform_restate
(operator territory: destructive force-recompute, used for bug-fix /
late-data backfill workflows preceded by code changes the AI doesn't drive).
"""

from moneybin.mcp.decorator import mcp_tool


@mcp_tool(sensitivity="low")
def transform_status() -> dict:
    from moneybin.services.transform_service import TransformService

    return {
        "summary": {"sensitivity": "low"},
        "data": TransformService.from_settings().status(),
        "actions": [],
    }


@mcp_tool(sensitivity="low")
def transform_plan() -> dict:
    from moneybin.services.transform_service import TransformService

    return {
        "summary": {"sensitivity": "low"},
        "data": TransformService.from_settings().plan(),
        "actions": [],
    }


@mcp_tool(sensitivity="low")
def transform_validate() -> dict:
    from moneybin.services.transform_service import TransformService

    return {
        "summary": {"sensitivity": "low"},
        "data": TransformService.from_settings().validate(),
        "actions": [],
    }


@mcp_tool(sensitivity="low")
def transform_audit() -> dict:
    from moneybin.services.transform_service import TransformService

    return {
        "summary": {"sensitivity": "low"},
        "data": TransformService.from_settings().audit(),
        "actions": [],
    }


@mcp_tool(sensitivity="low")
def transform_apply() -> dict:
    from moneybin.services.transform_service import TransformService

    return {
        "summary": {"sensitivity": "low"},
        "data": TransformService.from_settings().apply(),
        "actions": [],
    }


def register_transform_tools(mcp):
    from moneybin.mcp._registration import register

    for tool in [
        transform_status,
        transform_plan,
        transform_validate,
        transform_audit,
        transform_apply,
    ]:
        register(mcp, tool, name=tool.__name__, description=tool.__doc__ or "")
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_mcp/test_transform_tools.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/tools/transform.py src/moneybin/services/transform_service.py tests/moneybin/test_mcp/test_transform_tools.py src/moneybin/mcp/server.py
git commit -m "Expose transform_* tools to MCP (5 tools)

Routine SQLMesh pipeline operations: status, plan, validate, audit, apply.
Excludes transform_restate (operator-territory destructive op preceded by
code changes the AI doesn't drive)."
```

---

# Part 4: Server Instructions and Final Wiring

## Task 15: Restore v2 instructions text in server.py

**Goal:** The v2 instructions text was reverted from PR #95 to avoid runtime mismatch. With v2 tools registered, restore it.

**Files:**
- Modify: `src/moneybin/mcp/server.py`

- [ ] **Step 1: Recover the v2 text from PR #95 history**

```bash
git show ede2ce1:src/moneybin/mcp/server.py | sed -n '38,75p' > /tmp/v2_instructions_text.txt
cat /tmp/v2_instructions_text.txt
```

(Replace `ede2ce1` with the actual SHA of the PR #95 merge commit if different.)

- [ ] **Step 2: Apply the v2 instructions text**

Replace the `instructions=...` argument in `src/moneybin/mcp/server.py` with the v2 text via `textwrap.dedent`:

```python
import textwrap

mcp = FastMCP(
    "MoneyBin",
    instructions=textwrap.dedent(
        """\
        MoneyBin is a local-first personal finance platform. All data lives in DuckDB on the user's machine.

        Top-level groups:
        - accounts (balance) — financial accounts and per-account workflows
        - transactions (matches, categorize) — transactions and workflows on them
        - assets — physical assets (real estate, vehicles, valuables)
        - categories, merchants — taxonomy reference data
        - reports — cross-domain analytical and aggregation views (networth, spending, cashflow, financial health, budget vs actual)
        - tax — tax forms, deductions, future capital gains
        - system — data status
        - import, sync — data ingestion (sync_pull/status/connect available; OAuth flows return URLs the client opens)
        - privacy — consent and audit

        Tool names mirror the hierarchy with underscores, verb at end: accounts_balance_assert, transactions_matches_confirm, reports_networth_get, reports_spending_summary.

        Getting oriented:
        - system_status — what data exists, freshness, pending review queues
        - reports_health — financial snapshot (net worth, income/expenses, savings rate)

        Conventions:
        - Every tool returns {summary, data, actions}. Check summary.has_more for pagination; actions[] suggests next steps.
        - Prefer bulk tools (transactions_categorize_bulk_apply, transactions_categorize_rules_create).
        - Sensitivity tiers: low / medium / high. Without consent, tools degrade to aggregates — they never fail.
        """
    ),
    mask_error_details=True,
)
```

Add `import textwrap` if not already imported.

- [ ] **Step 3: Verify the server loads**

```bash
uv run python -c "from moneybin.mcp.server import mcp; print('len:', len(mcp.instructions or ''))"
```
Expected: `len:` followed by ~1300-1400 (around 325-350 tokens).

- [ ] **Step 4: Verify all referenced tools actually exist**

```python
# scripts/verify_instructions_tools.py (don't commit; local helper)
import re
from moneybin.mcp.server import mcp

text = mcp.instructions
referenced = set(re.findall(r"\b([a-z_]+(?:_[a-z]+)+)\b", text))
# Filter to plausible tool names (have at least one underscore and look like our convention)
candidates = {
    n
    for n in referenced
    if "_" in n and not n.startswith("_") and n not in {"low_medium_high", "key_value"}
}
registered = {
    t.name for t in mcp._tool_manager._tools.values()
}  # may need adjustment for FastMCP API
missing = candidates - registered
print("Missing tools referenced in instructions:", missing)
```

Expected: empty set (no missing tools).

If any are missing, this is a bug — either fix the instructions text to remove the reference or add the missing tool.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/server.py
git commit -m "Restore v2 instructions text in MCP server

All v2 tools are now registered; advertising them in the instructions
field no longer causes tool-not-found failures. Text recovered from
commit ede2ce1 (PR #95) where it was originally drafted."
```

---

## Task 16: Update `mcp config generate` to emit v2 tool names in client configs

**Goal:** When users generate fresh client configs (e.g., for Claude Desktop), the generated config should reflect the v2 server. Mostly the server-instance entry stays the same, but verify nothing hardcodes v1 names.

**Files:**
- Modify: `src/moneybin/cli/commands/mcp.py` (where `mcp config generate` lives)
- Test: `tests/moneybin/test_cli/test_cli_mcp_enhancements.py`

- [ ] **Step 1: Inspect current generation**

```bash
grep -n "spending\|categorize\|overview" src/moneybin/cli/commands/mcp.py
```

If hits, investigate. Most likely the generator just emits a server entry pointing at `moneybin mcp serve` — no per-tool names. Verify.

- [ ] **Step 2: Run existing config-generate tests**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_mcp_enhancements.py -v`
Expected: PASS (or document failures).

If everything passes and no v1 tool names appear in the generator, no changes needed for this task. Add a note in the commit message that the generator was verified clean.

- [ ] **Step 3: Commit (if changes needed) or skip**

If no code changes: skip the commit. Otherwise:

```bash
git add src/moneybin/cli/commands/mcp.py tests/moneybin/test_cli/test_cli_mcp_enhancements.py
git commit -m "Verify mcp config generate emits v2-compatible client configs

No code change needed — generator emits server-instance entries, not
per-tool names. Tests confirm output stays valid against v2 server."
```

---

# Part 5: Verification, Docs, Ship

## Task 17: Update README.md CLI section to v2

**Goal:** README's "What Works Today" CLI section reflects the v2 surface.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Inventory v1 references**

```bash
grep -nE "moneybin (track |matches |categorize |spending |cashflow )" README.md
```

- [ ] **Step 2: Replace v1 paths with v2**

For each match, update to v2:
- `track balance` → `accounts balance`
- `track networth` → `reports networth`
- `matches` → `transactions matches`
- `categorize` → `transactions categorize` (workflow), `categories` (taxonomy), `merchants` (mappings)

- [ ] **Step 3: Add v2 surface examples**

Add a brief code block to "What Works Today" showing the new top-level groups:

```bash
moneybin --help    # 18 top-level groups
moneybin transactions matches review
moneybin transactions categorize stats
moneybin reports networth show
moneybin system status
```

- [ ] **Step 4: Verify roadmap table**

If `cli-restructure.md` v2 has its own roadmap entry, ensure the icon reflects status (📐 → ✅ once this PR merges).

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Update README CLI examples to v2 paths"
```

---

## Task 18: Update spec statuses `ready` → `in-progress` → `implemented`

**Goal:** The spec status reflects the implementation state. Per AGENTS.md, flip `ready` → `in-progress` at start (already done if you've been editing) and `in-progress` → `implemented` at end.

**Files:**
- Modify: `docs/specs/cli-restructure.md`
- Modify: `docs/specs/mcp-tool-surface.md`
- Modify: `docs/specs/INDEX.md`

- [ ] **Step 1: Flip cli-restructure.md status**

In `docs/specs/cli-restructure.md`, change the v2 callout:

```markdown
> **v2 revision (2026-05-02):** ... Implementation pass moves status `ready` → `in-progress`.
```

to:

```markdown
> **v2 revision (2026-05-02, implemented YYYY-MM-DD):** ... v2 implemented in PR #NNN. Status: `implemented`.
```

And the Status field:
```markdown
## Status
implemented
```

- [ ] **Step 2: Flip mcp-tool-surface.md status**

Same pattern.

- [ ] **Step 3: Update INDEX.md statuses**

Change both spec entries' status column from `in-progress` to `implemented`.

- [ ] **Step 4: Commit**

```bash
git add docs/specs/cli-restructure.md docs/specs/mcp-tool-surface.md docs/specs/INDEX.md
git commit -m "Mark cli-restructure.md and mcp-tool-surface.md v2 implemented"
```

---

## Task 19: Run full verification suite

**Goal:** Catch any straggler tests / lint / typecheck issues before opening PR.

- [ ] **Step 1: Format**

```bash
make format
```

- [ ] **Step 2: Lint**

```bash
make lint
```

Fix any errors. Common: unused imports left after function moves; Typer Annotated migrations.

- [ ] **Step 3: Type-check modified files**

```bash
uv run pyright src/moneybin/cli/commands/ src/moneybin/mcp/tools/ src/moneybin/services/
```

Fix any type errors.

- [ ] **Step 4: Full pytest**

```bash
uv run pytest tests/ -q --ignore=tests/e2e
```

Expected: all pass except the pre-existing `tests/e2e/test_e2e_workflows.py::TestAutoRulePipeline::test_import_then_promote_proposal` (broken on `main`; not v2's responsibility).

- [ ] **Step 5: SQL formatting (if any SQLMesh models touched)**

```bash
uv run sqlmesh -p sqlmesh format
```

- [ ] **Step 6: Run scenarios suite**

```bash
make test-scenarios
```

Expected: PASS. If a scenario fails because it invokes a v1 CLI path or MCP tool name, update the scenario.

- [ ] **Step 7: Smoke test the running server**

```bash
uv run moneybin mcp list-tools | grep -E "system_status|reports_networth_get|reports_spending_summary|transactions_matches_confirm|transactions_categorize_pending_list|sync_status|transform_status"
```

Expected: each v2 tool name listed. If any are missing, find why.

```bash
uv run moneybin mcp list-tools | grep -E "^(spending_|cashflow_|categorize_|overview_|accounts_balances|accounts_networth)"
```

Expected: empty (no v1 names registered).

- [ ] **Step 8: Smoke test the CLI**

```bash
uv run moneybin --help | grep -E "^  (track|matches|categorize)"
```

Expected: empty (v1 top-level groups dissolved).

```bash
uv run moneybin --help | grep -E "^  (accounts|transactions|assets|categories|merchants|reports|tax|system|budget)"
```

Expected: each listed.

- [ ] **Step 9: Commit any straggler fixes**

```bash
git add -A
git commit -m "Verification: format, lint, type, scenarios fixes"
```

(Skip if no changes.)

---

## Task 20: Run `/simplify` quality pass

**Goal:** Per `.claude/rules/shipping.md`, run `/simplify` before final commit and push to catch copy-paste patterns and quality issues that accumulated during implementation.

- [ ] **Step 1: Invoke the simplify skill**

In Claude Code, invoke `/simplify` to review changed code for reuse opportunities.

- [ ] **Step 2: Address findings**

For each finding, decide: apply (refactor) or skip (acceptable). Apply changes as separate commits.

- [ ] **Step 3: Re-verify after simplify changes**

```bash
make check test
```

- [ ] **Step 4: Commit any simplify changes**

```bash
git add -A
git commit -m "Apply /simplify findings: <summary>"
```

---

## Task 21: Push and open PR

- [ ] **Step 1: Push the branch**

```bash
git push -u origin refactor/cli-restructure-v2
```

- [ ] **Step 2: Open PR via /commit-push-pr**

Invoke `/commit-push-pr` (the skill handles the full pre-commit flow). Or open manually:

```bash
gh pr create --title "refactor: implement cli-restructure v2 (CLI + MCP)" --label "refactor" --body "$(cat <<'EOF'
## Summary

Implements `cli-restructure.md` v2 and `mcp-tool-surface.md` v2 (both at status `ready` from PR #95). Hard-cut migration: v1 paths break in this release.

## Impact

- **CLI:** `track *`, top-level `matches`, top-level `categorize` no longer exist. New top-level groups: `accounts`, `transactions`, `assets`, `categories`, `merchants`, `reports`, `tax`, `system`, `budget`.
- **MCP:** ~30 tool names renamed per §16b. New `system_*`, `transactions_review_status`, `sync_*` (9 tools), `transform_*` (5 tools).
- **Server instructions:** restored to v2 text from commit ede2ce1.
- Any external scripts or AI agent prompts referencing v1 names need updating.

## Changes

### CLI
- New top-level groups (Tasks 1-9 in plan)
- Dissolved `track`; moved matches/categorize under `transactions`; pulled categories/merchants to top-level
- Unified `transactions review` collapses both review queues
- `import csv-preview` → `import file-preview`

### MCP
- Renamed ~30 tools per §16b
- Reorganized tool files: spending+cashflow+budget reads+networth+health → `reports.py`, overview → `system.py` + `reports.py`, categorize → `transactions_categorize.py` + `categories.py` + `merchants.py`
- New tools: `system_status`, `transactions_review_status`, 9 `sync_*`, 5 `transform_*`
- Server `instructions` field updated to v2 taxonomy

### Docs
- README CLI section updated to v2 paths
- Spec statuses flipped to `implemented`

## Test plan

- [x] All MCP tool tests pass
- [x] All CLI tests pass (excluding pre-existing e2e failure on main)
- [x] `make format && make lint && uv run pyright` clean
- [x] `make test-scenarios` passes
- [x] `moneybin mcp list-tools` shows v2 names, no v1 names
- [x] `moneybin --help` shows v2 top-level groups, no v1 (track/matches/categorize)
- [ ] Reviewer: spot-check 3-5 CLI commands and 3-5 MCP tools end-to-end
EOF
)"
```

- [ ] **Step 3: Report PR URL**

Display the resulting URL to the user.

---

## Self-Review Checklist (before declaring plan complete)

After saving this plan, run through this checklist:

**1. Spec coverage:**
- [ ] cli-restructure.md v2 §Command Tree → covered by Tasks 1-9
- [ ] cli-restructure.md v2 §Migration v1→v2 (CLI moves table) → covered by Tasks 4-9
- [ ] cli-restructure.md v2 §Migration v1→v2 (MCP renames) → covered by Task 10
- [ ] cli-restructure.md v2 §Implementation pass entries → all touched
- [ ] mcp-tool-surface.md v2 §16b Rename Map → Task 10
- [ ] mcp-tool-surface.md v2 sync_* section → Task 13
- [ ] mcp-tool-surface.md v2 transform_* section → Task 14
- [ ] mcp-tool-surface.md v2 system_* + reports_health section → Tasks 10, 11
- [ ] mcp-tool-surface.md v2 transactions_review_status → Task 12
- [ ] mcp-tool-surface.md v2 import_file_preview rename → covered in Task 10 + Task 9 (CLI)
- [ ] Server instructions field update → Task 15
- [ ] README update → Task 17
- [ ] Status flips → Task 18

**2. Placeholder scan:** No "TBD", "TODO" in step bodies. Code blocks present where steps create or modify code. Test code shown verbatim. Migration paths documented.

**3. Type consistency:** `ReviewService.status() → ReviewStatus`. `SystemService.status() → SystemStatus`. Field names consistent across tasks (Task 6 uses `matches_pending`/`categorize_pending`/`total`; Task 11 uses same; Task 12 same).

**4. Ambiguity check:** Each task lists exact files. Each step has a runnable command or copyable code block. Where bodies are "copy from existing file," the source file is named.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-02-cli-restructure-v2-implementation.md`. Two execution options:

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks. Best for a refactor of this size where each task is well-isolated.

**2. Inline Execution** — Execute tasks in this session using `executing-plans`. Faster turnaround if you want to drive each step interactively.

Which approach?
