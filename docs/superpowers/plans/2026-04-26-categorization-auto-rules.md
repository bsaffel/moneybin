# Auto-Rule Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture user categorization patterns into staged auto-rule proposals; promote approved proposals to active rules in `app.categorization_rules` so future imports auto-categorize matching transactions.

**Architecture:** Add an `app.proposed_rules` table and a private `_auto_rule` engine module that hooks into `CategorizationService.bulk_categorize()`. After each categorization the hook extracts a merchant-first pattern, deduplicates against active rules / merchants / pending proposals, and either inserts a new proposal or increments an existing one. A new `categorize auto-*` CLI command group and matching `categorize.auto_*` MCP tools expose review/confirm/stats. Promotion writes a row to `app.categorization_rules` with `created_by='auto_rule'` and immediately runs `apply_rules()` so approval has instant effect.

**Architectural revision (2026-04-26):** The original plan added `CategorizationService` as a facade over module-level `categorization_service` functions and a separate public `auto_rule_service`. We are consolidating to a class-first surface in this branch (Task 7c) so `CategorizationService` is the **only** public categorization API — matching `AccountService`/`SpendingService`/`TransactionService` and `mcp-tool-surface.md`. Module-level public functions are removed (or kept only as stateless utilities like `normalize_description`). `auto_rule_service` becomes a private `_auto_rule` module imported only by `CategorizationService`. CLI, MCP, and importer all instantiate `CategorizationService(db)` and call methods. `import_service.py` is the lone holdout that remains module-functions; tracked in `private/followups.md`.

**Tech Stack:** Python 3.12, DuckDB, Typer, Pydantic Settings, FastMCP, pytest. All new code follows `.claude/rules/` (security, database, cli, mcp-server, testing).

**Spec:** [`docs/specs/categorization-auto-rules.md`](../../specs/categorization-auto-rules.md)

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `src/moneybin/sql/schema/app_proposed_rules.sql` | Create | DDL for `app.proposed_rules` table |
| `src/moneybin/tables.py` | Modify | Add `PROPOSED_RULES` `TableRef` constant |
| `src/moneybin/config.py` | Modify | Add `CategorizationSettings` and wire into `MoneyBinSettings` |
| `src/moneybin/services/auto_rule_service.py` | Create | Pattern extraction, proposal lifecycle, override detection, promotion |
| `src/moneybin/services/categorization_service.py` | Modify | Add `CategorizationService` class facade (matches `AccountService`/`SpendingService`/`TransactionService` shape per `mcp-tool-surface.md`); call `auto_rule_service.record_categorization()` from `bulk_categorize()` |
| `src/moneybin/cli/commands/categorize.py` | Modify | Add `auto-review`, `auto-confirm`, `auto-stats`, `auto-rules` subcommands |
| `src/moneybin/cli/commands/import_cmd.py` | Modify | Extend import summary with proposal count line |
| `src/moneybin/mcp/tools/categorize.py` | Modify | Register `categorize.auto_review`, `categorize.auto_confirm`, `categorize.auto_stats` tools and `review_auto_rules` prompt |
| `tests/moneybin/test_services/test_auto_rule_service.py` | Create | Unit tests: pattern extraction, dedup, promotion, override threshold |
| `tests/moneybin/test_services/test_categorization_service.py` | Modify | Hook integration: `bulk_categorize()` triggers proposal generation |
| `tests/moneybin/test_cli/test_categorize_auto_commands.py` | Create | CLI argument parsing for new subcommands (mocked service) |
| `tests/moneybin/test_mcp/test_categorization_tools.py` | Modify | Register and invoke new MCP tools |
| `tests/e2e/test_e2e_help.py` | Modify | Add new commands to `_HELP_COMMANDS` |
| `tests/e2e/test_e2e_mutating.py` | Modify | E2E for `auto-confirm --approve <id>` against a real DB |
| `tests/e2e/test_e2e_workflows.py` | Modify | End-to-end import → categorize → approve → re-import flow |
| `docs/specs/categorization-auto-rules.md` | Modify (final) | Status `ready` → `implemented` |
| `docs/specs/INDEX.md` | Modify (final) | Bump status to `implemented` |
| `README.md` | Modify (final) | Roadmap icon 📐 → ✅; add to "What Works Today" |

---

## Task 1: Create `app.proposed_rules` schema

**Files:**
- Create: `src/moneybin/sql/schema/app_proposed_rules.sql`

- [ ] **Step 1: Write the DDL file**

```sql
/* Auto-rule proposals generated from user categorization patterns; staged for review before activation */
CREATE TABLE IF NOT EXISTS app.proposed_rules (
    proposed_rule_id VARCHAR PRIMARY KEY, -- 12-char truncated UUID4 hex identifier
    merchant_pattern VARCHAR NOT NULL, -- Pattern to match: canonical merchant name or cleaned description
    match_type VARCHAR DEFAULT 'contains', -- How merchant_pattern is matched: contains, exact, or regex
    category VARCHAR NOT NULL, -- Proposed category to assign on approval
    subcategory VARCHAR, -- Proposed subcategory; NULL when no subcategory applies
    status VARCHAR DEFAULT 'pending', -- Lifecycle state: pending, approved, rejected, superseded
    trigger_count INTEGER DEFAULT 1, -- Number of categorizations that triggered or reinforced this proposal
    source VARCHAR DEFAULT 'pattern_detection', -- How proposal was generated: pattern_detection or ml
    sample_txn_ids VARCHAR[], -- Up to 5 transaction_ids that triggered this proposal
    proposed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the proposal was first created
    decided_at TIMESTAMP, -- When the user approved or rejected; NULL while pending
    decided_by VARCHAR -- Who decided: 'user' or NULL while pending
);
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/sql/schema/app_proposed_rules.sql
git commit -m "Add app.proposed_rules schema for auto-rule proposals"
```

---

## Task 2: Register `PROPOSED_RULES` table constant

**Files:**
- Modify: `src/moneybin/tables.py`

- [ ] **Step 1: Add the constant**

In the "App tables" block (next to `CATEGORIZATION_RULES`):

```python
CATEGORIZATION_RULES = TableRef("app", "categorization_rules")
PROPOSED_RULES = TableRef("app", "proposed_rules")
```

- [ ] **Step 2: Verify schema init picks it up**

Run: `uv run python -c "from moneybin.tables import PROPOSED_RULES; print(PROPOSED_RULES.full_name)"`
Expected: `app.proposed_rules`

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/tables.py
git commit -m "Register PROPOSED_RULES table constant"
```

---

## Task 3: Add `CategorizationSettings` config

**Files:**
- Modify: `src/moneybin/config.py`
- Test: `tests/moneybin/test_config.py` (modify)

- [ ] **Step 1: Write the failing test**

Add to `tests/moneybin/test_config.py` (create the file if it does not yet exist by checking with `ls tests/moneybin/test_config.py`):

```python
def test_categorization_settings_defaults():
    from moneybin.config import CategorizationSettings

    s = CategorizationSettings()
    assert s.auto_rule_proposal_threshold == 1
    assert s.auto_rule_override_threshold == 2
    assert s.auto_rule_default_priority == 200


def test_categorization_settings_env_override(monkeypatch):
    from moneybin.config import MoneyBinSettings

    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_PROPOSAL_THRESHOLD", "3")
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "5")
    s = MoneyBinSettings()
    assert s.categorization.auto_rule_proposal_threshold == 3
    assert s.categorization.auto_rule_override_threshold == 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_config.py::test_categorization_settings_defaults -v`
Expected: FAIL — `CategorizationSettings` does not exist.

- [ ] **Step 3: Add the settings class**

In `src/moneybin/config.py`, after `MatchingSettings` and before `MoneyBinSettings`:

```python
class CategorizationSettings(BaseModel):
    """Auto-rule proposal and lifecycle configuration."""

    model_config = ConfigDict(frozen=True)

    auto_rule_proposal_threshold: int = Field(
        default=1,
        ge=1,
        description="Propose an auto-rule after N matching user categorizations",
    )
    auto_rule_override_threshold: int = Field(
        default=2,
        ge=1,
        description="Deactivate an auto-rule after N user overrides of its assignments",
    )
    auto_rule_default_priority: int = Field(
        default=200,
        ge=1,
        description="Priority assigned to promoted auto-rules (higher number = lower priority)",
    )
```

In `MoneyBinSettings`, alongside `matching: MatchingSettings = Field(default_factory=MatchingSettings)`:

```python
    categorization: CategorizationSettings = Field(default_factory=CategorizationSettings)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_config.py -v -k categorization`
Expected: PASS for both tests.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_config.py
git commit -m "Add CategorizationSettings with auto-rule thresholds"
```

---

## Task 4: Implement pattern extraction in `auto_rule_service`

**Files:**
- Create: `src/moneybin/services/auto_rule_service.py`
- Test: `tests/moneybin/test_services/test_auto_rule_service.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/moneybin/test_services/test_auto_rule_service.py`:

```python
"""Unit tests for auto_rule_service."""

from unittest.mock import MagicMock

import pytest

from moneybin.services import auto_rule_service


def _mock_db_with_merchant(
    merchant_id: str = "m_abc", canonical_name: str = "STARBUCKS"
):
    db = MagicMock()
    # transaction_categories row -> merchant_id
    db.execute.return_value.fetchone.side_effect = [
        (merchant_id,),  # SELECT merchant_id FROM transaction_categories
        (canonical_name,),  # SELECT canonical_name FROM merchants
    ]
    return db


def test_extract_pattern_uses_merchant_canonical_name_when_present():
    db = _mock_db_with_merchant()
    pattern = auto_rule_service.extract_pattern(db, transaction_id="t_1")
    assert pattern == "STARBUCKS"


def test_extract_pattern_falls_back_to_normalized_description():
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (None,),  # no merchant_id on the categorization row
        ("SQ *STARBUCKS #1234 SEATTLE WA",),  # raw description
    ]
    pattern = auto_rule_service.extract_pattern(db, transaction_id="t_2")
    assert pattern == "STARBUCKS"


def test_extract_pattern_returns_none_when_description_empty():
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [(None,), ("",)]
    assert auto_rule_service.extract_pattern(db, transaction_id="t_3") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v`
Expected: FAIL — module/function does not exist.

- [ ] **Step 3: Implement extract_pattern**

Create `src/moneybin/services/auto_rule_service.py`:

```python
"""Auto-rule proposal lifecycle: pattern extraction, dedup, promotion, override detection.

Hooks into CategorizationService.bulk_categorize() to capture user categorization
patterns, stage them as proposals in app.proposed_rules, and promote approved
proposals into active rules in app.categorization_rules with created_by='auto_rule'.
"""

import logging
import uuid
from dataclasses import dataclass, field
from typing import Literal

import duckdb

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.services.categorization_service import normalize_description
from moneybin.tables import (
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    PROPOSED_RULES,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)

ProposalStatus = Literal["pending", "approved", "rejected", "superseded"]
SAMPLE_TXN_CAP = 5


def extract_pattern(db: Database, transaction_id: str) -> str | None:
    """Extract a merchant-first pattern for the given transaction.

    Returns the canonical merchant name if a merchant_id is recorded on the
    transaction_categories row; otherwise falls back to a normalized description.
    Returns None if neither is available.
    """
    row = db.execute(
        f"SELECT merchant_id FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    merchant_id = row[0] if row else None
    if merchant_id:
        m = db.execute(
            f"SELECT canonical_name FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        if m and m[0]:
            return str(m[0])

    desc_row = db.execute(
        f"SELECT description FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    if not desc_row or not desc_row[0]:
        return None
    cleaned = normalize_description(str(desc_row[0]))
    return cleaned or None
```

Note: the failing-test mocks return `(merchant_id,)` for the first `fetchone()` and `(canonical_name,)` for the second — matching the two SELECTs above. Keep query order stable so tests stay valid.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v`
Expected: PASS for all three tests.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/auto_rule_service.py tests/moneybin/test_services/test_auto_rule_service.py
git commit -m "Add merchant-first pattern extraction for auto-rule proposals"
```

---

## Task 5: Implement proposal recording (dedup + threshold)

**Files:**
- Modify: `src/moneybin/services/auto_rule_service.py`
- Test: `tests/moneybin/test_services/test_auto_rule_service.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/moneybin/test_services/test_auto_rule_service.py`:

```python
from moneybin.database import Database


@pytest.fixture
def real_db(tmp_path):
    """A real encrypted-disabled DB with schema initialized."""
    from unittest.mock import MagicMock

    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    db = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    yield db
    db.close()


def _seed_transaction(
    db: Database,
    txn_id: str,
    description: str = "STARBUCKS",
    merchant_id: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, posted_date, amount, description, source_type) "
        "VALUES (?, 'a1', DATE '2026-01-01', -5.00, ?, 'csv')",
        [txn_id, description],
    )
    db.execute(
        "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by, merchant_id) "
        "VALUES (?, 'Food & Drink', CURRENT_TIMESTAMP, 'user', ?)",
        [txn_id, merchant_id],
    )


def test_record_creates_proposal_on_first_categorization(real_db):
    _seed_transaction(real_db, "t1")
    auto_rule_service.record_categorization(
        real_db, "t1", "Food & Drink", subcategory="Coffee"
    )

    rows = real_db.execute(
        "SELECT merchant_pattern, category, subcategory, trigger_count, status FROM app.proposed_rules"
    ).fetchall()
    assert rows == [("STARBUCKS", "Food & Drink", "Coffee", 1, "pending")]


def test_record_increments_trigger_count_on_same_pattern_and_category(real_db):
    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    auto_rule_service.record_categorization(
        real_db, "t1", "Food & Drink", subcategory="Coffee"
    )
    auto_rule_service.record_categorization(
        real_db, "t2", "Food & Drink", subcategory="Coffee"
    )

    rows = real_db.execute(
        "SELECT trigger_count, sample_txn_ids FROM app.proposed_rules"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 2
    assert sorted(rows[0][1]) == ["t1", "t2"]


def test_record_supersedes_when_same_pattern_different_category(real_db):
    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")
    auto_rule_service.record_categorization(real_db, "t2", "Groceries")

    rows = real_db.execute(
        "SELECT category, status FROM app.proposed_rules ORDER BY proposed_at"
    ).fetchall()
    assert rows == [("Food & Drink", "superseded"), ("Groceries", "pending")]


def test_record_skips_when_active_rule_already_covers_pattern(real_db):
    _seed_transaction(real_db, "t1")
    real_db.execute(
        "INSERT INTO app.categorization_rules (rule_id, name, merchant_pattern, match_type, category, priority, is_active) "
        "VALUES ('r1', 'starbucks', 'STARBUCKS', 'contains', 'Food & Drink', 100, true)"
    )
    auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")

    count = real_db.execute("SELECT COUNT(*) FROM app.proposed_rules").fetchone()[0]
    assert count == 0


def test_record_respects_proposal_threshold(real_db, monkeypatch):
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_PROPOSAL_THRESHOLD", "3")
    get_settings.cache_clear()  # type: ignore[attr-defined]

    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    _seed_transaction(real_db, "t3")
    auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")
    auto_rule_service.record_categorization(real_db, "t2", "Food & Drink")
    # Below threshold: tracked but not surfaced as pending
    pending = real_db.execute(
        "SELECT COUNT(*) FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()[0]
    assert pending == 0

    auto_rule_service.record_categorization(real_db, "t3", "Food & Drink")
    pending = real_db.execute(
        "SELECT COUNT(*) FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()[0]
    assert pending == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v -k record`
Expected: FAIL — `record_categorization` does not exist.

- [ ] **Step 3: Implement `record_categorization`**

Append to `src/moneybin/services/auto_rule_service.py`:

```python
def _active_rule_covers(db: Database, pattern: str) -> bool:
    """True when an active categorization rule already matches this pattern (case-insensitive exact pattern compare)."""
    row = db.execute(
        f"""
        SELECT 1 FROM {CATEGORIZATION_RULES.full_name}
        WHERE is_active = true AND LOWER(merchant_pattern) = LOWER(?)
        LIMIT 1
        """,
        [pattern],
    ).fetchone()
    return row is not None


def _merchant_mapping_covers(db: Database, pattern: str, category: str) -> bool:
    """True when a merchant mapping already produces this category for this pattern."""
    try:
        row = db.execute(
            f"""
            SELECT 1 FROM {MERCHANTS.full_name}
            WHERE LOWER(canonical_name) = LOWER(?) AND category = ?
            LIMIT 1
            """,
            [pattern, category],
        ).fetchone()
    except duckdb.CatalogException:
        return False
    return row is not None


def _find_pending_proposal(
    db: Database, pattern: str
) -> tuple[str, str, str | None, int, list[str]] | None:
    row = db.execute(
        f"""
        SELECT proposed_rule_id, category, subcategory, trigger_count, sample_txn_ids
        FROM {PROPOSED_RULES.full_name}
        WHERE LOWER(merchant_pattern) = LOWER(?) AND status IN ('pending', 'tracking')
        ORDER BY proposed_at DESC LIMIT 1
        """,
        [pattern],
    ).fetchone()
    if not row:
        return None
    return row[0], row[1], row[2], int(row[3]), list(row[4] or [])


def record_categorization(
    db: Database,
    transaction_id: str,
    category: str,
    *,
    subcategory: str | None = None,
) -> str | None:
    """Record a categorization event for auto-rule learning.

    Returns the proposed_rule_id if a proposal was created or updated,
    None if the categorization was filtered out (covered by existing rule/merchant
    or pattern unavailable).
    """
    pattern = extract_pattern(db, transaction_id)
    if not pattern:
        return None

    if _active_rule_covers(db, pattern):
        return None
    if _merchant_mapping_covers(db, pattern, category):
        return None

    threshold = get_settings().categorization.auto_rule_proposal_threshold
    existing = _find_pending_proposal(db, pattern)

    if existing is not None:
        proposed_rule_id, existing_category, existing_subcategory, count, samples = (
            existing
        )
        if existing_category == category and existing_subcategory == subcategory:
            new_samples = (
                samples + [transaction_id] if transaction_id not in samples else samples
            )
            new_samples = new_samples[:SAMPLE_TXN_CAP]
            new_count = count + 1
            new_status = "pending" if new_count >= threshold else "tracking"
            db.execute(
                f"""
                UPDATE {PROPOSED_RULES.full_name}
                SET trigger_count = ?, sample_txn_ids = ?, status = ?
                WHERE proposed_rule_id = ?
                """,
                [new_count, new_samples, new_status, proposed_rule_id],
            )
            return proposed_rule_id
        # Different category: supersede the old proposal, fall through to create a new one
        db.execute(
            f"UPDATE {PROPOSED_RULES.full_name} SET status = 'superseded' WHERE proposed_rule_id = ?",
            [proposed_rule_id],
        )

    proposed_rule_id = uuid.uuid4().hex[:12]
    initial_status = "pending" if threshold <= 1 else "tracking"
    db.execute(
        f"""
        INSERT INTO {PROPOSED_RULES.full_name}
        (proposed_rule_id, merchant_pattern, match_type, category, subcategory,
         status, trigger_count, source, sample_txn_ids)
        VALUES (?, ?, 'contains', ?, ?, ?, 1, 'pattern_detection', ?)
        """,
        [
            proposed_rule_id,
            pattern,
            category,
            subcategory,
            initial_status,
            [transaction_id],
        ],
    )
    return proposed_rule_id
```

Note: introducing a `tracking` status for sub-threshold proposals lets the trigger count accumulate without surfacing them in review until the configured threshold is reached. Tests above verify this transition.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v`
Expected: PASS for all tests in the file.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/auto_rule_service.py tests/moneybin/test_services/test_auto_rule_service.py
git commit -m "Record categorizations into staged auto-rule proposals"
```

---

## Task 6: Implement promotion (approve) and rejection

**Files:**
- Modify: `src/moneybin/services/auto_rule_service.py`
- Test: `tests/moneybin/test_services/test_auto_rule_service.py`

- [ ] **Step 1: Write the failing tests**

Append to the test file:

```python
def test_approve_promotes_to_active_rule(real_db):
    _seed_transaction(real_db, "t1")
    pid = auto_rule_service.record_categorization(
        real_db, "t1", "Food & Drink", subcategory="Coffee"
    )
    assert pid is not None

    result = auto_rule_service.approve(real_db, [pid])
    assert result.approved == 1
    assert (
        result.newly_categorized >= 0
    )  # no other uncategorized matching txns in this test

    rule = real_db.execute(
        "SELECT merchant_pattern, category, subcategory, priority, created_by, is_active "
        "FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert rule == ("STARBUCKS", "Food & Drink", "Coffee", 200, "auto_rule", True)

    status = real_db.execute(
        "SELECT status, decided_by FROM app.proposed_rules WHERE proposed_rule_id = ?",
        [pid],
    ).fetchone()
    assert status == ("approved", "user")


def test_approve_immediately_categorizes_existing_uncategorized(real_db):
    _seed_transaction(real_db, "t1")
    pid = auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")
    # Insert an uncategorized matching txn
    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, posted_date, amount, description, source_type) "
        "VALUES ('t9', 'a1', DATE '2026-01-02', -7.00, 'STARBUCKS DOWNTOWN', 'csv')"
    )
    result = auto_rule_service.approve(real_db, [pid])
    assert result.newly_categorized == 1

    cat = real_db.execute(
        "SELECT category, categorized_by FROM app.transaction_categories WHERE transaction_id = 't9'"
    ).fetchone()
    assert cat == ("Food & Drink", "auto_rule")


def test_reject_marks_proposal_rejected_without_creating_rule(real_db):
    _seed_transaction(real_db, "t1")
    pid = auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")
    auto_rule_service.reject(real_db, [pid])

    status = real_db.execute(
        "SELECT status, decided_by FROM app.proposed_rules WHERE proposed_rule_id = ?",
        [pid],
    ).fetchone()
    assert status == ("rejected", "user")
    rule_count = real_db.execute(
        "SELECT COUNT(*) FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()[0]
    assert rule_count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v -k "approve or reject"`
Expected: FAIL — functions do not exist.

- [ ] **Step 3: Implement approve/reject**

Append to `src/moneybin/services/auto_rule_service.py`:

```python
@dataclass(slots=True)
class ApproveResult:
    approved: int = 0
    skipped: int = 0
    newly_categorized: int = 0
    rule_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RejectResult:
    rejected: int = 0
    skipped: int = 0


def _categorize_existing_with_rule(
    db: Database, rule_id: str, pattern: str, category: str, subcategory: str | None
) -> int:
    """Run the new rule against currently-uncategorized matching transactions. Returns count categorized."""
    rows = db.execute(
        f"""
        SELECT t.transaction_id
        FROM {FCT_TRANSACTIONS.full_name} t
        LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c ON t.transaction_id = c.transaction_id
        WHERE c.transaction_id IS NULL
          AND t.description IS NOT NULL
          AND POSITION(LOWER(?) IN LOWER(t.description)) > 0
        """,
        [pattern],
    ).fetchall()
    if not rows:
        return 0
    db.executemany(
        f"""
        INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
        (transaction_id, category, subcategory, categorized_at, categorized_by, rule_id, confidence)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'auto_rule', ?, 1.0)
        """,
        [(r[0], category, subcategory, rule_id) for r in rows],
    )
    return len(rows)


def approve(db: Database, proposed_rule_ids: list[str]) -> ApproveResult:
    """Promote pending proposals to active rules and immediately categorize matching transactions."""
    settings = get_settings().categorization
    result = ApproveResult()

    for pid in proposed_rule_ids:
        row = db.execute(
            f"""
            SELECT merchant_pattern, match_type, category, subcategory, status
            FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?
            """,
            [pid],
        ).fetchone()
        if not row or row[4] != "pending":
            result.skipped += 1
            continue

        pattern, match_type, category, subcategory, _status = row
        rule_id = uuid.uuid4().hex[:12]
        db.execute(
            f"""
            INSERT INTO {CATEGORIZATION_RULES.full_name}
            (rule_id, name, merchant_pattern, match_type, category, subcategory,
             priority, is_active, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, true, 'auto_rule', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
                rule_id,
                f"auto: {pattern}",
                pattern,
                match_type,
                category,
                subcategory,
                settings.auto_rule_default_priority,
            ],
        )
        db.execute(
            f"""
            UPDATE {PROPOSED_RULES.full_name}
            SET status = 'approved', decided_at = CURRENT_TIMESTAMP, decided_by = 'user'
            WHERE proposed_rule_id = ?
            """,
            [pid],
        )
        newly = _categorize_existing_with_rule(
            db, rule_id, pattern, category, subcategory
        )
        result.approved += 1
        result.rule_ids.append(rule_id)
        result.newly_categorized += newly

    if result.approved:
        logger.info(
            f"Approved {result.approved} auto-rule proposal(s); "
            f"{result.newly_categorized} existing transaction(s) categorized"
        )
    return result


def reject(db: Database, proposed_rule_ids: list[str]) -> RejectResult:
    """Mark pending proposals as rejected. No rule is created."""
    result = RejectResult()
    for pid in proposed_rule_ids:
        row = db.execute(
            f"SELECT status FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?",
            [pid],
        ).fetchone()
        if not row or row[0] != "pending":
            result.skipped += 1
            continue
        db.execute(
            f"""
            UPDATE {PROPOSED_RULES.full_name}
            SET status = 'rejected', decided_at = CURRENT_TIMESTAMP, decided_by = 'user'
            WHERE proposed_rule_id = ?
            """,
            [pid],
        )
        result.rejected += 1
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v`
Expected: PASS for all tests.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/auto_rule_service.py tests/moneybin/test_services/test_auto_rule_service.py
git commit -m "Promote approved auto-rule proposals to active rules"
```

---

## Task 7: Override-driven deactivation

**Files:**
- Modify: `src/moneybin/services/auto_rule_service.py`
- Test: `tests/moneybin/test_services/test_auto_rule_service.py`

- [ ] **Step 1: Write the failing test**

```python
def test_override_threshold_deactivates_rule_and_creates_new_proposal(
    real_db, monkeypatch
):
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "2")
    get_settings.cache_clear()  # type: ignore[attr-defined]

    # Approve an auto-rule for STARBUCKS -> Food & Drink
    _seed_transaction(real_db, "t1")
    pid = auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")
    auto_rule_service.approve(real_db, [pid])

    # Two user overrides correcting STARBUCKS to Groceries
    for tid in ("t10", "t11"):
        real_db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, account_id, posted_date, amount, description, source_type) "
            "VALUES (?, 'a1', DATE '2026-01-03', -8.00, 'STARBUCKS RESERVE', 'csv')",
            [tid],
        )
        real_db.execute(
            "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by) "
            "VALUES (?, 'Groceries', CURRENT_TIMESTAMP, 'user')",
            [tid],
        )

    auto_rule_service.check_overrides(real_db)

    active = real_db.execute(
        "SELECT is_active FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert active == (False,)

    new_proposal = real_db.execute(
        "SELECT category, status FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()
    assert new_proposal == ("Groceries", "pending")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v -k override`
Expected: FAIL — `check_overrides` does not exist.

- [ ] **Step 3: Implement override detection**

Append to `src/moneybin/services/auto_rule_service.py`:

```python
def check_overrides(db: Database) -> int:
    """Deactivate auto-rules with override count >= configured threshold; return number deactivated.

    An override = a transaction whose description matches the auto-rule's pattern
    but is currently categorized by 'user' with a different category. When the
    threshold is reached we deactivate the rule, mark its source proposal superseded,
    and create a new pending proposal with the most common override category.
    """
    settings = get_settings().categorization
    threshold = settings.auto_rule_override_threshold

    rules = db.execute(
        f"""
        SELECT rule_id, merchant_pattern, category
        FROM {CATEGORIZATION_RULES.full_name}
        WHERE is_active = true AND created_by = 'auto_rule'
        """
    ).fetchall()
    deactivated = 0

    for rule_id, pattern, rule_category in rules:
        rows = db.execute(
            f"""
            SELECT c.category, COUNT(*) AS n
            FROM {TRANSACTION_CATEGORIES.full_name} c
            JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
            WHERE c.categorized_by = 'user'
              AND c.category != ?
              AND POSITION(LOWER(?) IN LOWER(t.description)) > 0
            GROUP BY c.category
            ORDER BY n DESC
            """,
            [rule_category, pattern],
        ).fetchall()
        total_overrides = sum(r[1] for r in rows)
        if total_overrides < threshold:
            continue

        db.execute(
            f"UPDATE {CATEGORIZATION_RULES.full_name} SET is_active = false, updated_at = CURRENT_TIMESTAMP WHERE rule_id = ?",
            [rule_id],
        )
        db.execute(
            f"""
            UPDATE {PROPOSED_RULES.full_name}
            SET status = 'superseded'
            WHERE LOWER(merchant_pattern) = LOWER(?) AND status = 'approved'
            """,
            [pattern],
        )
        new_category = rows[0][0]
        new_pid = uuid.uuid4().hex[:12]
        db.execute(
            f"""
            INSERT INTO {PROPOSED_RULES.full_name}
            (proposed_rule_id, merchant_pattern, match_type, category, status,
             trigger_count, source, sample_txn_ids)
            VALUES (?, ?, 'contains', ?, 'pending', ?, 'pattern_detection', ?)
            """,
            [new_pid, pattern, new_category, total_overrides, []],
        )
        deactivated += 1

    if deactivated:
        logger.info(f"Deactivated {deactivated} auto-rule(s) due to user overrides")
    return deactivated
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v`
Expected: PASS for all tests.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/auto_rule_service.py tests/moneybin/test_services/test_auto_rule_service.py
git commit -m "Deactivate auto-rules after user-override threshold"
```

---

## Task 7b: Introduce `CategorizationService` class

**Why:** `docs/specs/mcp-tool-surface.md` (lines 281–820) mandates a `CategorizationService` class with methods `bulk_categorize`, `apply_rules`, `seed`, `stats`, `categories`, `rules`, `merchants`, `auto_review`, `auto_confirm`, `auto_stats`, etc. — matching the established `AccountService`/`SpendingService`/`TransactionService` shape. The forthcoming testing scenario runner (`testing-scenario-runner` branch) will instantiate services uniformly as `Service(db).method(...)`. Auto-rules ship without that surface today; we add it here as a thin facade so the scenario runner can adopt it without a follow-up refactor.

**Files:**
- Modify: `src/moneybin/services/categorization_service.py`
- Create: `tests/moneybin/test_services/test_categorization_service_class.py` (or extend existing test file — see step 1)

- [ ] **Step 1: Write the failing test**

Add to `tests/moneybin/test_services/test_categorization_service.py` (or a new sibling file):

```python
def test_service_facade_exposes_required_methods():
    from moneybin.services.categorization_service import CategorizationService

    expected = {
        "bulk_categorize",
        "apply_rules",
        "apply_deterministic",
        "seed",
        "stats",
        "auto_review",
        "auto_confirm",
        "auto_stats",
    }
    missing = expected - set(dir(CategorizationService))
    assert not missing, f"CategorizationService missing methods: {missing}"


def test_service_bulk_categorize_delegates_to_module_function(real_db):
    from moneybin.services.categorization_service import CategorizationService

    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, posted_date, amount, description, source_type) "
        "VALUES ('ts1', 'a1', DATE '2026-03-01', -3.00, 'STARBUCKS', 'csv')"
    )
    svc = CategorizationService(real_db)
    result = svc.bulk_categorize([
        {"transaction_id": "ts1", "category": "Food & Drink"}
    ])
    assert result.applied == 1


def test_service_auto_review_returns_pending_proposals(real_db):
    from moneybin.services import auto_rule_service
    from moneybin.services.categorization_service import CategorizationService

    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, posted_date, amount, description, source_type) "
        "VALUES ('ts2', 'a1', DATE '2026-03-02', -3.00, 'AMAZON', 'csv')"
    )
    real_db.execute(
        "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by) "
        "VALUES ('ts2', 'Shopping', CURRENT_TIMESTAMP, 'user')"
    )
    auto_rule_service.record_categorization(real_db, "ts2", "Shopping")

    svc = CategorizationService(real_db)
    proposals = svc.auto_review()
    patterns = {p["merchant_pattern"] for p in proposals}
    assert "AMAZON" in patterns
```

(Reuse the `real_db` fixture pattern from `test_auto_rule_service.py`; if not already present in this file, copy the fixture in.)

- [ ] **Step 2: Run — expect failure**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v -k "service_facade or service_bulk or service_auto"`
Expected: FAIL — class does not exist.

- [ ] **Step 3: Add the facade class**

At the bottom of `src/moneybin/services/categorization_service.py`, append:

```python
class CategorizationService:
    """Facade matching AccountService/SpendingService/TransactionService.

    Delegates to existing module-level functions in this file and to
    auto_rule_service. Provides a uniform Service(db).method() surface for
    the MCP layer, CLI commands, and the testing scenario runner.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    # -- Categorization core --

    def bulk_categorize(self, items: list[dict[str, str]]) -> BulkCategorizationResult:
        return bulk_categorize(self._db, items)

    def apply_rules(self) -> int:
        return apply_rules(self._db)

    def apply_deterministic(self) -> dict[str, int]:
        return apply_deterministic_categorization(self._db)

    def seed(self) -> int:
        return seed_categories(self._db)

    def stats(self) -> CategorizationStats:
        return get_stats(self._db)

    # -- Auto-rule lifecycle --

    def auto_review(self) -> list[dict[str, object]]:
        from moneybin.tables import PROPOSED_RULES

        rows = self._db.execute(
            f"""
            SELECT proposed_rule_id, merchant_pattern, match_type, category, subcategory,
                   trigger_count, sample_txn_ids
            FROM {PROPOSED_RULES.full_name}
            WHERE status = 'pending'
            ORDER BY trigger_count DESC, proposed_at ASC
            """
        ).fetchall()
        return [
            {
                "proposed_rule_id": r[0],
                "merchant_pattern": r[1],
                "match_type": r[2],
                "category": r[3],
                "subcategory": r[4],
                "trigger_count": r[5],
                "sample_txn_ids": list(r[6] or []),
            }
            for r in rows
        ]

    def auto_confirm(
        self,
        approve: list[str] | None = None,
        reject: list[str] | None = None,
    ) -> dict[str, object]:
        from moneybin.services import auto_rule_service

        a = auto_rule_service.approve(self._db, approve or [])
        r = auto_rule_service.reject(self._db, reject or [])
        return {
            "approved": a.approved,
            "newly_categorized": a.newly_categorized,
            "rule_ids": a.rule_ids,
            "rejected": r.rejected,
            "skipped": a.skipped + r.skipped,
        }

    def auto_stats(self) -> dict[str, int]:
        active = self._db.execute(
            f"SELECT COUNT(*) FROM {CATEGORIZATION_RULES.full_name} "
            "WHERE created_by = 'auto_rule' AND is_active = true"
        ).fetchone()[0]
        from moneybin.tables import PROPOSED_RULES

        pending = self._db.execute(
            f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name} WHERE status = 'pending'"
        ).fetchone()[0]
        applied = self._db.execute(
            f"SELECT COUNT(*) FROM {TRANSACTION_CATEGORIES.full_name} WHERE categorized_by = 'auto_rule'"
        ).fetchone()[0]
        return {
            "active_auto_rules": active,
            "pending_proposals": pending,
            "transactions_categorized": applied,
        }
```

Note: the import of `auto_rule_service` is local inside `auto_confirm` to avoid the circular import (`auto_rule_service` imports `normalize_description` from this file). Same trick is used in Task 8 for the hook.

- [ ] **Step 4: Run tests — expect pass**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v`
Expected: PASS for new tests and pre-existing tests.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/categorization_service.py tests/moneybin/test_services/test_categorization_service.py
git commit -m "Add CategorizationService facade matching other service classes"
```

---

## Task 7c: Consolidate to class-first categorization surface

**Why:** T7b added `CategorizationService` as a thin facade alongside the existing module-level functions and the public `auto_rule_service`. To make the new surface self-contained and self-consistent — and to match the `AccountService`/`SpendingService`/`TransactionService` pattern — we promote `CategorizationService` to the canonical implementation, make `auto_rule_service` private, and migrate every existing caller.

**Files:**
- Modify: `src/moneybin/services/categorization_service.py` (move logic into class methods; keep `normalize_description`, `_matches_pattern` as module-level helpers since they are stateless and used by the engine module)
- Rename: `src/moneybin/services/auto_rule_service.py` → `src/moneybin/services/_auto_rule.py` (leading underscore signals private)
- Modify: `src/moneybin/cli/commands/categorize.py` (existing commands `bulk_categorize`, `seed`, `stats` migrate to `CategorizationService(db).method()`)
- Modify: `src/moneybin/mcp/tools/categorize.py` (existing tool functions migrate to class)
- Modify: `src/moneybin/services/import_service.py` (`_apply_categorization` calls `CategorizationService(db).apply_rules()` etc.)
- Modify: `tests/moneybin/test_services/test_categorization_service.py` and `test_auto_rule_service.py` (rewrite imports to use the class; the module file becomes `test__auto_rule.py` or merges into `test_categorization_service.py`)

**Required new methods on `CategorizationService` (beyond what T7b added):**
- `match_merchant(description)` → wraps existing `match_merchant` module function
- `apply_merchant_categories()` → wraps `apply_merchant_categories`
- `ensure_seed_table()` → wraps `ensure_seed_table`
- `get_active_categories(...)` → wraps `get_active_categories`
- `categorization_stats(...)` → wraps `get_categorization_stats` (the long-form stats; `stats()` keeps the short-form `CategorizationStats` shape)
- `list_auto_rules()` → returns list of dicts for active `created_by='auto_rule'` rows from `app.categorization_rules`
- `check_overrides()` → wraps `_auto_rule.check_overrides`
- Internal: `_record_categorization(txn_id, category, subcategory)` → wraps `_auto_rule.record_categorization`; called from inside `bulk_categorize` (replaces the Task 8 module-level hook)

**Module-level public functions to REMOVE from `categorization_service.py`:** `bulk_categorize`, `apply_rules`, `apply_deterministic_categorization`, `seed_categories`, `get_stats`, `get_categorization_stats`, `match_merchant`, `apply_merchant_categories`, `ensure_seed_table`, `get_active_categories`, `create_merchant`. (Move bodies into class methods. Keep `normalize_description` and `_matches_pattern` as module-level since `_auto_rule` imports `normalize_description`.)

- [ ] **Step 1: Write the failing tests**

Add to `tests/moneybin/test_services/test_categorization_service.py`:

```python
def test_no_public_module_level_categorization_functions():
    """Surface contract: only CategorizationService is the public API."""
    import moneybin.services.categorization_service as mod

    forbidden = {
        "bulk_categorize",
        "apply_rules",
        "seed_categories",
        "get_stats",
        "get_categorization_stats",
        "match_merchant",
        "apply_merchant_categories",
        "ensure_seed_table",
        "get_active_categories",
        "create_merchant",
        "apply_deterministic_categorization",
    }
    leaked = {name for name in forbidden if hasattr(mod, name)}
    assert not leaked, f"These should be class methods only: {leaked}"


def test_auto_rule_service_is_private():
    """`auto_rule_service` must not be importable; use _auto_rule (private) only."""
    import importlib

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("moneybin.services.auto_rule_service")


def test_service_exposes_consolidated_methods(real_db):
    from moneybin.services.categorization_service import CategorizationService

    expected = {
        "bulk_categorize",
        "apply_rules",
        "apply_deterministic",
        "seed",
        "stats",
        "match_merchant",
        "apply_merchant_categories",
        "ensure_seed_table",
        "get_active_categories",
        "categorization_stats",
        "auto_review",
        "auto_confirm",
        "auto_stats",
        "list_auto_rules",
        "check_overrides",
    }
    missing = expected - set(dir(CategorizationService))
    assert not missing, f"Missing methods: {missing}"


def test_list_auto_rules_returns_active_auto_rules(real_db):
    from moneybin.services.categorization_service import CategorizationService

    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('lt1', 'a1', DATE '2026-03-01', -3.00, 'CHIPOTLE', 'csv')"
    )
    svc = CategorizationService(real_db)
    pid = svc._record_categorization("lt1", "Food & Drink")
    svc.auto_confirm(approve=[pid])

    rules = svc.list_auto_rules()
    assert any(r["merchant_pattern"] == "CHIPOTLE" for r in rules)
```

- [ ] **Step 2: Run tests — expect failure**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v -k "no_public or auto_rule_service_is_private or consolidated or list_auto_rules"`
Expected: FAIL on all four.

- [ ] **Step 3: Rename `auto_rule_service.py` → `_auto_rule.py`**

```bash
git mv src/moneybin/services/auto_rule_service.py src/moneybin/services/_auto_rule.py
```

- [ ] **Step 4: Move logic into `CategorizationService`**

In `src/moneybin/services/categorization_service.py`:

a. Convert each module-level public function listed above into a method on `CategorizationService`. The body stays the same; replace the leading `db: Database` parameter with `self`, and substitute `self._db` for `db` references. For the existing T7b facade methods that just delegated, replace them with the actual logic.

b. Add the new methods:

```python
def list_auto_rules(self) -> list[dict[str, object]]:
    from moneybin.tables import CATEGORIZATION_RULES

    rows = self._db.execute(
        f"""
        SELECT rule_id, merchant_pattern, match_type, category, subcategory, priority
        FROM {CATEGORIZATION_RULES.full_name}
        WHERE created_by = 'auto_rule' AND is_active = true
        ORDER BY priority ASC, rule_id
        """
    ).fetchall()
    return [
        {
            "rule_id": r[0],
            "merchant_pattern": r[1],
            "match_type": r[2],
            "category": r[3],
            "subcategory": r[4],
            "priority": r[5],
        }
        for r in rows
    ]


def check_overrides(self) -> int:
    from moneybin.services import _auto_rule

    return _auto_rule.check_overrides(self._db)


def _record_categorization(
    self, transaction_id: str, category: str, subcategory: str | None = None
) -> str | None:
    from moneybin.services import _auto_rule

    return _auto_rule.record_categorization(
        self._db, transaction_id, category, subcategory=subcategory
    )
```

c. Remove all the now-duplicated module-level `def` statements that were promoted to methods. Keep:
- `normalize_description` (used by `_auto_rule`)
- `_matches_pattern` (private helper)
- `_fetch_merchants`, `_match_description`, `_POS_PREFIXES`, `_TRAILING_*`, `_MULTI_SPACE` — private to this module
- All `@dataclass` result types (`CategorizationStats`, `BulkCategorizationResult`, `SeedResult`)
- `class CategorizationService`

d. Inside `_auto_rule.py`, change `from moneybin.services.categorization_service import normalize_description` (still works — `normalize_description` remains module-level). All other internal helpers in `_auto_rule.py` keep their existing module-level shape; they are called only by `CategorizationService` methods.

- [ ] **Step 5: Migrate callers**

Find and update every caller. Use this checklist:

```bash
grep -rn "from moneybin.services.categorization_service import\|from moneybin.services import categorization_service\|from moneybin.services.auto_rule_service\|from moneybin.services import auto_rule_service" src/ tests/
```

Update each hit:
- **`src/moneybin/cli/commands/categorize.py`**: replace `from moneybin.services.categorization_service import bulk_categorize` (etc.) with `from moneybin.services.categorization_service import CategorizationService`. Each command body becomes `svc = CategorizationService(get_database()); svc.bulk_categorize(...)`. Keep dataclass imports (`BulkCategorizationResult`, `CategorizationStats`, `SeedResult`) for type hints.
- **`src/moneybin/mcp/tools/categorize.py`**: same pattern. Each tool function instantiates `CategorizationService(get_database())` and calls methods.
- **`src/moneybin/services/import_service.py:684 _apply_categorization`**: replace direct imports with `CategorizationService(db).apply_rules()` etc.
- **`tests/moneybin/test_services/test_auto_rule_service.py`**: rewrite to call `CategorizationService(real_db)._record_categorization(...)`, `.auto_confirm(approve=[...])`, `.check_overrides()`. Or merge into `test_categorization_service.py` and delete the file.

- [ ] **Step 6: Run full test suite**

Run: `uv run pytest tests/moneybin/ -v`
Expected: PASS. Iterate on any failures.

- [ ] **Step 7: Lint, type-check, format**

Run: `make format lint && uv run pyright src/moneybin/services/ src/moneybin/cli/commands/categorize.py src/moneybin/mcp/tools/categorize.py`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "Consolidate categorization to class-first CategorizationService surface"
```

---

## Task 8: Hook auto-rule recording into `CategorizationService.bulk_categorize`

**Files:**
- Modify: `src/moneybin/services/categorization_service.py`
- Test: `tests/moneybin/test_services/test_categorization_service.py` (modify)

After T7c, `bulk_categorize` is a method on `CategorizationService`. The auto-rule hook becomes an internal call — no module function involved.

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_services/test_categorization_service.py`:

```python
def test_bulk_categorize_creates_auto_rule_proposal(real_db):
    from moneybin.services.categorization_service import CategorizationService

    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('tb1', 'a1', DATE '2026-02-01', -4.50, 'STARBUCKS RESERVE', 'csv')"
    )
    svc = CategorizationService(real_db)
    svc.bulk_categorize(
        [
            {
                "transaction_id": "tb1",
                "category": "Food & Drink",
                "subcategory": "Coffee",
            }
        ],
    )

    rows = real_db.execute(
        "SELECT merchant_pattern, category, status FROM app.proposed_rules"
    ).fetchall()
    assert ("STARBUCKS RESERVE", "Food & Drink", "pending") in rows
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v -k bulk_categorize_creates_auto_rule_proposal`
Expected: FAIL — no proposal recorded.

- [ ] **Step 3: Wire the hook inside the method**

In `src/moneybin/services/categorization_service.py`, inside `CategorizationService.bulk_categorize()` after the successful `INSERT OR REPLACE INTO ... TRANSACTION_CATEGORIES` (immediately before `applied += 1`), add:

```python
            # Record for auto-rule learning (best-effort — failures must not break categorization)
            try:
                self._record_categorization(txn_id, category, subcategory=subcategory)
            except Exception:  # noqa: BLE001 — auto-rule learning is best-effort
                logger.debug("auto-rule recording failed", exc_info=True)
```

`_record_categorization` was added in T7c and forwards to the private `_auto_rule.record_categorization`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v`
Expected: PASS — including the new hook test and pre-existing tests.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/categorization_service.py tests/moneybin/test_services/test_categorization_service.py
git commit -m "Hook auto-rule recording into CategorizationService.bulk_categorize"
```

---

## Task 9: Add CLI commands (`auto-review`, `auto-confirm`, `auto-stats`, `auto-rules`)

**Files:**
- Modify: `src/moneybin/cli/commands/categorize.py`
- Test: `tests/moneybin/test_cli/test_categorize_auto_commands.py`

- [ ] **Step 1: Write the failing CLI test**

Create `tests/moneybin/test_cli/test_categorize_auto_commands.py`:

```python
"""CLI argument parsing for auto-rule commands. Business logic is tested via auto_rule_service tests."""

from typer.testing import CliRunner

from moneybin.cli.commands.categorize import app

runner = CliRunner()


def test_auto_review_help():
    result = runner.invoke(app, ["auto-review", "--help"])
    assert result.exit_code == 0
    assert "pending" in result.stdout.lower()


def test_auto_confirm_help_lists_approve_and_reject_flags():
    result = runner.invoke(app, ["auto-confirm", "--help"])
    assert result.exit_code == 0
    assert "--approve" in result.stdout
    assert "--reject" in result.stdout
    assert "--approve-all" in result.stdout
    assert "--reject-all" in result.stdout


def test_auto_stats_help():
    result = runner.invoke(app, ["auto-stats", "--help"])
    assert result.exit_code == 0


def test_auto_rules_help():
    result = runner.invoke(app, ["auto-rules", "--help"])
    assert result.exit_code == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_cli/test_categorize_auto_commands.py -v`
Expected: FAIL — commands not registered.

- [ ] **Step 3: Implement the CLI commands**

All four commands are thin wrappers — they instantiate `CategorizationService` and call methods. No raw SQL in CLI. Append to `src/moneybin/cli/commands/categorize.py`:

```python
@app.command("auto-review")
def auto_review_cmd(
    output: str = typer.Option(
        "table", "--output", help="Output format: table or json"
    ),
) -> None:
    """List pending auto-rule proposals with sample transactions and trigger counts."""
    import json

    from moneybin.database import (
        DatabaseKeyError,
        database_key_error_hint,
        get_database,
    )
    from moneybin.services.categorization_service import CategorizationService

    try:
        proposals = CategorizationService(get_database()).auto_review()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e
    except DatabaseKeyError as e:
        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e

    if output == "json":
        typer.echo(json.dumps(proposals))
        return

    if not proposals:
        logger.info("No pending auto-rule proposals.")
        return

    logger.info("👀 Pending auto-rule proposals:")
    for p in proposals:
        sub = f" / {p['subcategory']}" if p["subcategory"] else ""
        samples = p["sample_txn_ids"]
        sample_str = f" samples: {','.join(samples)}" if samples else ""
        logger.info(
            f"  [{p['proposed_rule_id']}] '{p['merchant_pattern']}' "
            f"({p['match_type']}) -> {p['category']}{sub} "
            f"(×{p['trigger_count']}){sample_str}"
        )


@app.command("auto-confirm")
def auto_confirm_cmd(
    approve: list[str] = typer.Option(
        None, "--approve", help="Proposal IDs to approve"
    ),
    reject: list[str] = typer.Option(None, "--reject", help="Proposal IDs to reject"),
    approve_all: bool = typer.Option(
        False, "--approve-all", help="Approve all pending proposals"
    ),
    reject_all: bool = typer.Option(
        False, "--reject-all", help="Reject all pending proposals"
    ),
) -> None:
    """Batch approve/reject auto-rule proposals."""
    from moneybin.database import (
        DatabaseKeyError,
        database_key_error_hint,
        get_database,
    )
    from moneybin.services.categorization_service import CategorizationService

    try:
        svc = CategorizationService(get_database())
        if approve_all or reject_all:
            pending_ids = [p["proposed_rule_id"] for p in svc.auto_review()]
            if approve_all:
                approve = (approve or []) + pending_ids
            if reject_all:
                reject = (reject or []) + pending_ids

        result = svc.auto_confirm(approve=approve or [], reject=reject or [])
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e
    except DatabaseKeyError as e:
        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e

    logger.info(
        f"✅ Approved {result['approved']} "
        f"(categorized {result['newly_categorized']} existing); "
        f"rejected {result['rejected']}"
    )


@app.command("auto-stats")
def auto_stats_cmd() -> None:
    """Show auto-rule health: active rules, pending proposals, transactions categorized."""
    from moneybin.database import (
        DatabaseKeyError,
        database_key_error_hint,
        get_database,
    )
    from moneybin.services.categorization_service import CategorizationService

    try:
        stats = CategorizationService(get_database()).auto_stats()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e
    except DatabaseKeyError as e:
        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e

    logger.info("Auto-rule health:")
    logger.info(f"  Active auto-rules:        {stats['active_auto_rules']}")
    logger.info(f"  Pending proposals:        {stats['pending_proposals']}")
    logger.info(f"  Transactions auto-ruled:  {stats['transactions_categorized']}")


@app.command("auto-rules")
def auto_rules_cmd() -> None:
    """List active auto-rules (rules with created_by='auto_rule')."""
    from moneybin.database import (
        DatabaseKeyError,
        database_key_error_hint,
        get_database,
    )
    from moneybin.services.categorization_service import CategorizationService

    try:
        rules = CategorizationService(get_database()).list_auto_rules()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e
    except DatabaseKeyError as e:
        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e

    if not rules:
        logger.info("No active auto-rules.")
        return

    logger.info("Active auto-rules:")
    for r in rules:
        sub = f" / {r['subcategory']}" if r["subcategory"] else ""
        logger.info(
            f"  [{r['rule_id']}] '{r['merchant_pattern']}' "
            f"({r['match_type']}) -> {r['category']}{sub} "
            f"(priority: {r['priority']})"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_cli/test_categorize_auto_commands.py -v`
Expected: PASS for all four tests.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/categorize.py tests/moneybin/test_cli/test_categorize_auto_commands.py
git commit -m "Add categorize auto-review/auto-confirm/auto-stats/auto-rules CLI"
```

---

## Task 10: Extend import summary

**Files:**
- Modify: `src/moneybin/cli/commands/import_cmd.py`

- [ ] **Step 1: Locate the summary block**

Run: `grep -n "auto-categorized\|categorized\|by rules\|by merchant" src/moneybin/cli/commands/import_cmd.py | head -20`
Expected: identifies the existing summary print/log section.

- [ ] **Step 2: Add proposal count line**

After the existing breakdown (rules / merchant / ML / etc.), add. Use `CategorizationService.auto_stats()` — no raw SQL in the import command:

```python
from moneybin.services.categorization_service import CategorizationService

pending = CategorizationService(db).auto_stats()["pending_proposals"]
if pending:
    logger.info(f"  {pending} new auto-rule proposals")
    logger.info("  Run 'moneybin categorize auto-review' to review proposed rules")
```

If the import command already shows a final summary line, place this immediately above that line.

- [ ] **Step 3: Spot-check by running an existing import test**

Run: `uv run pytest tests/moneybin/test_cli/ -v -k import_summary 2>/dev/null || uv run pytest tests/moneybin/test_cli/ -v -k import | head -40`
Expected: existing tests still pass; manual inspection of the changed code shows the new lines.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/cli/commands/import_cmd.py
git commit -m "Surface auto-rule proposal count in import summary"
```

---

## Task 11: Register MCP tools and prompt

**Files:**
- Modify: `src/moneybin/mcp/tools/categorize.py`
- Test: `tests/moneybin/test_mcp/test_categorization_tools.py` (modify)

- [ ] **Step 1: Write the failing test**

Append to `tests/moneybin/test_mcp/test_categorization_tools.py`:

```python
def test_register_includes_auto_rule_tools():
    from moneybin.mcp.namespace_registry import NamespaceRegistry
    from moneybin.mcp.tools.categorize import register_categorize_tools

    reg = NamespaceRegistry()
    tools = register_categorize_tools(reg)
    names = {t.name for t in tools}
    assert {
        "categorize.auto_review",
        "categorize.auto_confirm",
        "categorize.auto_stats",
    } <= names
```

(If `NamespaceRegistry` lives at a different import path, fix the import — grep `class NamespaceRegistry` to confirm.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_mcp/test_categorization_tools.py -v -k auto_rule`
Expected: FAIL — tools not registered.

- [ ] **Step 3: Implement the tool functions and registration**

In `src/moneybin/mcp/tools/categorize.py`, before `register_categorize_tools`, add:

All three tools are thin wrappers — instantiate `CategorizationService` and call methods. No raw SQL in the tool layer.

```python
def categorize_auto_review() -> ResponseEnvelope:
    """List pending auto-rule proposals."""
    from moneybin.database import get_database
    from moneybin.services.categorization_service import CategorizationService

    data = CategorizationService(get_database()).auto_review()
    return build_envelope(data=data, sensitivity="medium", total_count=len(data))


def categorize_auto_confirm(
    approve: list[str] | None = None,
    reject: list[str] | None = None,
) -> ResponseEnvelope:
    """Approve or reject auto-rule proposals by ID."""
    from moneybin.database import get_database
    from moneybin.services.categorization_service import CategorizationService

    result = CategorizationService(get_database()).auto_confirm(
        approve=approve or [], reject=reject or []
    )
    return build_envelope(data=result, sensitivity="medium")


def categorize_auto_stats() -> ResponseEnvelope:
    """Auto-rule health metrics."""
    from moneybin.database import get_database
    from moneybin.services.categorization_service import CategorizationService

    return build_envelope(
        data=CategorizationService(get_database()).auto_stats(),
        sensitivity="low",
    )
```

In the `tools = [...]` list inside `register_categorize_tools`, append:

```python
(
    ToolDefinition(
        name="categorize.auto_review",
        description="List pending auto-rule proposals with sample transactions and trigger counts.",
        fn=categorize_auto_review,
    ),
)
(
    ToolDefinition(
        name="categorize.auto_confirm",
        description="Batch approve/reject auto-rule proposals. Approved proposals become active rules and immediately categorize matching transactions.",
        fn=categorize_auto_confirm,
    ),
)
(
    ToolDefinition(
        name="categorize.auto_stats",
        description="Auto-rule health: active count, pending proposals, transactions categorized.",
        fn=categorize_auto_stats,
    ),
)
```

- [ ] **Step 4: Add the prompt**

Find where prompts are registered (grep `Prompt(` or `register_prompt` in `src/moneybin/mcp/`) and add:

```python
Prompt(
    name="review_auto_rules",
    description="Help me review proposed auto-categorization rules. Show pending proposals with sample transactions, explain the pattern, and let me approve or reject them.",
)
```

If the prompt registration mechanism differs, follow the existing pattern in the codebase (e.g., a `prompts.py` registry).

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_categorization_tools.py -v`
Expected: PASS for the new and existing tests.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/mcp/tools/categorize.py tests/moneybin/test_mcp/test_categorization_tools.py
git commit -m "Add categorize.auto_review/auto_confirm/auto_stats MCP tools"
```

---

## Task 12: E2E coverage

**Files:**
- Modify: `tests/e2e/test_e2e_help.py`
- Modify: `tests/e2e/test_e2e_mutating.py`
- Modify: `tests/e2e/test_e2e_workflows.py`

- [ ] **Step 1: Add help entries**

In `tests/e2e/test_e2e_help.py`, add to the `_HELP_COMMANDS` list:

```python
(["categorize", "auto-review"],)
(["categorize", "auto-confirm"],)
(["categorize", "auto-stats"],)
(["categorize", "auto-rules"],)
```

- [ ] **Step 2: Add mutating E2E test for auto-confirm**

In `tests/e2e/test_e2e_mutating.py`, add (mirroring nearby tests for env/profile setup):

```python
def test_auto_review_and_approve(tmp_path):
    env = make_workflow_env(tmp_path)
    # Categorize a transaction so a proposal is created
    run_cli(["categorize", "apply-rules"], env=env)  # ensure schema exists
    # Insert a fake categorization via SQL (simplest path) — relies on db unlock in env
    # ... follow pattern of nearby e2e tests for SQL insertion
    result = run_cli(["categorize", "auto-review"], env=env)
    assert result.returncode == 0
```

If existing e2e tests rely on a synthetic-data fixture, prefer that path: load the synthetic dataset, run `categorize bulk`, then `auto-review`.

- [ ] **Step 3: Add full workflow test**

In `tests/e2e/test_e2e_workflows.py`, add a test that imports the standard tabular fixture, runs categorization, calls `auto-review`, then `auto-confirm --approve-all`, then re-imports a second fixture file — assert that the second-import categorizations include `auto_rule` rows.

Use the existing `e2e_home` + `make_workflow_env` fixtures. Mirror the structure of the most recent workflow test in the file.

- [ ] **Step 4: Run E2E tests**

Run: `uv run pytest tests/e2e/test_e2e_help.py tests/e2e/test_e2e_mutating.py tests/e2e/test_e2e_workflows.py -v -m e2e -k auto`
Expected: PASS for the new tests.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/
git commit -m "Add E2E coverage for auto-rule CLI commands and workflow"
```

---

## Task 13: Full quality pass

- [ ] **Step 1: Format and lint**

Run: `make format && make lint`
Expected: clean.

- [ ] **Step 2: Type-check changed files**

Run: `uv run pyright src/moneybin/services/auto_rule_service.py src/moneybin/cli/commands/categorize.py src/moneybin/mcp/tools/categorize.py src/moneybin/config.py src/moneybin/services/categorization_service.py`
Expected: no new errors.

- [ ] **Step 3: Full test suite**

Run: `make check test`
Expected: green.

- [ ] **Step 4: SQL formatting**

Run: `uv run sqlmesh -p sqlmesh format`
Expected: no changes (we only added a raw schema DDL file, which sqlmesh format ignores; verify nothing churned).

- [ ] **Step 5: Commit any auto-formatting fixes**

```bash
git status
# If any changes:
git add -A
git commit -m "Apply formatting after auto-rule implementation"
```

---

## Task 14: Documentation, status, and shipping

**Files:**
- Modify: `docs/specs/categorization-auto-rules.md`
- Modify: `docs/specs/INDEX.md`
- Modify: `README.md`

- [ ] **Step 1: Mark spec implemented**

In `docs/specs/categorization-auto-rules.md` header, change `Status: Ready` to `Status: Implemented`.

In `docs/specs/INDEX.md`, change the row's status from `ready` to `implemented`.

- [ ] **Step 2: Update README**

In the categorization roadmap table in `README.md`, change the auto-rules entry icon from 📐 to ✅.

In "What Works Today" (categorization section), add:

```markdown
- **Auto-rule generation** — when you categorize a transaction, MoneyBin proposes a reusable rule; review with `moneybin categorize auto-review`, approve with `moneybin categorize auto-confirm --approve-all`, and future imports auto-categorize matching transactions. Approved rules are stored alongside user-defined rules with `created_by='auto_rule'`.
```

- [ ] **Step 3: Run pre-push quality pass**

Per `.claude/rules/shipping.md`, invoke `/simplify` to review changed code for reuse opportunities and simplifications. Apply any suggested fixes.

- [ ] **Step 4: Commit**

```bash
git add docs/specs/categorization-auto-rules.md docs/specs/INDEX.md README.md
git commit -m "Mark auto-rule generation as shipped in spec, INDEX, and README"
```

- [ ] **Step 5: Push branch and open PR**

```bash
git push -u origin HEAD
gh pr create --title "Auto-rule generation: stage and promote categorization patterns" --body "$(cat <<'EOF'
## Summary
- Adds `app.proposed_rules` table and `auto_rule_service` to capture user categorization patterns
- Hooks into `bulk_categorize` to propose rules using merchant-first pattern extraction
- New CLI: `moneybin categorize auto-review|auto-confirm|auto-stats|auto-rules`
- New MCP tools: `categorize.auto_review|auto_confirm|auto_stats` + `review_auto_rules` prompt
- Approved proposals are promoted to active rules and immediately re-categorize existing transactions
- After `auto_rule_override_threshold` user overrides, the rule is deactivated and a new proposal is created with the most common correction category

## Test plan
- [ ] `make check test` passes locally
- [ ] `moneybin categorize auto-review` lists proposals from a synthetic import
- [ ] `moneybin categorize auto-confirm --approve-all` promotes them; re-import categorizes new matching transactions
EOF
)"
```

---

## Self-Review Notes

- Spec sections covered: proposal generation (T4–T5, T8), proposal lifecycle (T6), correction handling (T7), priority hierarchy via `priority=200` and `created_by='auto_rule'` (T6), data model (T1), pattern extraction (T4), integration hook (T8), CLI (T9–T10), MCP (T11), configuration (T3), testing (T4–T7, T9, T11, T12), shipping checklist (T14).
- Override counting is query-based per spec — `check_overrides` joins `transaction_categories` to `fct_transactions`, no stored counter (T7).
- The `tracking` status is an implementation choice introduced when `auto_rule_proposal_threshold > 1`; `pending` is the only status surfaced in review queues. Documented inline in T5 and verified by `test_record_respects_proposal_threshold`.
- `check_overrides` is exposed as a service function but not yet wired to a scheduled trigger — the spec doesn't mandate one. A follow-up task can call it from `apply_deterministic_categorization` or import-time if real-world usage shows the need; out of scope here to avoid YAGNI.
