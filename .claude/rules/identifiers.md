---
description: "Identifier generation: content hashes, truncated UUIDs, source-provided IDs, semantic slugs"
paths: ["src/moneybin/**/*.py", "sqlmesh/models/**"]
---

# Identifiers

## Decision Tree

Use the first strategy that applies:

| Priority | Strategy | When | Example |
|---|---|---|---|
| 1 | **Source-provided ID** | Upstream system supplies a stable identifier | OFX `<FITID>`, Plaid `transaction_id` |
| 2 | **Content hash** | No source ID, but identity is determined by content | CSV transactions (no FITID) |
| 3 | **UUID4 (truncated)** | User-created entity with no natural key | Merchants, rules, budgets, user-created categories |
| 4 | **Semantic slug** | Human-authored reference data needing readable IDs | Seed data codes (`INC-SAL`), format names (`chase_credit`) |

## Content Hashes

For records whose identity *is* their content — reimporting the same file must produce the same IDs.

- **Algorithm**: SHA-256, truncated to 16 hex chars (64 bits). 64 bits gives ~1-in-a-billion collision probability at 100k records — sufficient for per-source transaction dedup, and short enough to be readable in logs and debugging. Use full SHA-256 if a single table could exceed ~1M records.
- **Prefix**: Source-specific prefix to prevent cross-source collisions (`csv_`, `pdf_`, etc.).
- **Input**: Pipe-delimited concatenation of the fields that define uniqueness.

```python
raw = f"{date}|{amount}|{description}|{account_id}"
digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
return f"csv_{digest}"
```

## UUID4 (Truncated)

For app-layer entities created by the user or system with no natural key.

- **Length**: 12 hex chars (48 bits) — collision probability ~0.00002% at 10,000 items.
- **Format**: `uuid.uuid4().hex[:12]` (hex only, no hyphens).

```python
merchant_id = uuid.uuid4().hex[:12]
rule_id = uuid.uuid4().hex[:12]
```

Do not truncate shorter than 12 chars. If an entity could plausibly exceed 100,000 records, use the full UUID4 hex (32 chars).

## Source-Provided IDs

Always prefer the upstream system's identifier. Store as-is — do not hash, truncate, or transform. These are stable across re-imports by definition.

## Semantic Slugs

For hand-crafted reference data where readability matters more than uniqueness guarantees. Use short, mnemonic codes (`INC-SAL`, `TRN-INT`) or descriptive names (`chase_credit`, `tiller`). Uniqueness is enforced by the database (PRIMARY KEY or UNIQUE constraint), not by the generation strategy.
