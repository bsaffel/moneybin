# Data Protection Spec — Validation Test Plan

> Purpose: Validate key technical assumptions in the data-protection spec before
> promoting to `ready` status. Run by an agent in a fresh session.
> Spec: `docs/specs/privacy-data-protection.md`
> ADR: `docs/decisions/013-encryption-key-management.md`

## Prerequisites

- DuckDB CLI installed (`brew install duckdb` or equivalent)
- Python environment with `duckdb` package (`uv run python`)

## Test 1: DuckDB Encrypted Database Creation and Access

Validate that DuckDB's encryption extension works as described in the spec.

```bash
# Create a temp directory for testing
TESTDIR=$(mktemp -d)
cd "$TESTDIR"

# 1. Create an encrypted database via the ATTACH pattern
duckdb -c "
  INSTALL httpfs;
  LOAD httpfs;
  ATTACH '${TESTDIR}/encrypted.duckdb' AS edb (TYPE DUCKDB, ENCRYPTION_KEY 'test-key-12345');
  USE edb;
  CREATE TABLE test_data (id INTEGER, name VARCHAR, amount DECIMAL(18,2));
  INSERT INTO test_data VALUES (1, 'Alice', 100.50), (2, 'Bob', 200.75);
  SELECT * FROM test_data;
"

# Expected: Table created, 2 rows returned.
```

**Pass criteria:** Database file created, data inserted and queried successfully.

## Test 2: Encrypted Database Is Unreadable Without Key

```bash
# 2. Try to open the encrypted database WITHOUT the key
duckdb "${TESTDIR}/encrypted.duckdb" -c "SELECT * FROM test_data;" 2>&1

# Expected: Error — cannot read encrypted database without key.
```

**Pass criteria:** Error message (not data). The file is a useless blob without the key.

## Test 3: Encrypted Database Is Readable With Key

```bash
# 3. Open with the correct key via ATTACH
duckdb -c "
  ATTACH '${TESTDIR}/encrypted.duckdb' AS edb (TYPE DUCKDB, ENCRYPTION_KEY 'test-key-12345');
  USE edb;
  SELECT * FROM test_data;
"

# Expected: 2 rows returned (Alice, Bob).
```

**Pass criteria:** Data accessible with the correct key.

## Test 4: `-init` Flag Works With DuckDB CLI

Validate that the `-init` approach works for auto-attaching encrypted databases.

```bash
# 4. Create an init script
cat > "${TESTDIR}/init.sql" <<'INIT'
ATTACH '__TESTDIR__/encrypted.duckdb' AS edb (TYPE DUCKDB, ENCRYPTION_KEY 'test-key-12345');
USE edb;
INIT
sed -i '' "s|__TESTDIR__|${TESTDIR}|g" "${TESTDIR}/init.sql"

# 5. Launch DuckDB CLI with -init
echo "SELECT * FROM test_data;" | duckdb -init "${TESTDIR}/init.sql"

# Expected: 2 rows returned. The -init script auto-attached the encrypted database.
```

**Pass criteria:** `-init` flag executes the ATTACH statement before the query runs.

## Test 5: `-init` Flag Works With `-ui`

This is the critical validation — can we auto-attach an encrypted database in the UI?

```bash
# 6. Launch DuckDB UI with -init (manual test — UI opens in browser)
duckdb -init "${TESTDIR}/init.sql" -ui

# Manual check: In the browser UI, run:
#   SELECT * FROM test_data;
# Expected: 2 rows returned. The encrypted database is available in the UI
# without manually pasting the ATTACH statement.
```

**Pass criteria:** The DuckDB UI opens with the encrypted database already attached
and queryable. If this fails, document the error and fall back to the "print ATTACH
statement" approach in the spec.

## Test 6: Temp Files Are Encrypted

```bash
# 7. Check that DuckDB encrypts temp spill files
duckdb -c "
  ATTACH '${TESTDIR}/encrypted.duckdb' AS edb (TYPE DUCKDB, ENCRYPTION_KEY 'test-key-12345');
  USE edb;
  SET temp_directory='${TESTDIR}/tmp';
  -- Force a spill by creating a large result set
  CREATE TABLE large_test AS
    SELECT i AS id, 'description_' || i AS desc, (random() * 1000)::DECIMAL(18,2) AS amount
    FROM range(100000) t(i);
  SELECT COUNT(*) FROM large_test;
"

# Check if temp directory was created and if files are readable
ls -la "${TESTDIR}/tmp/" 2>/dev/null

# Expected: If temp files were created, they should not be human-readable
# (encrypted). This may not produce temp files for 100k rows — DuckDB may
# handle it in memory. Increase to 10M rows if needed to force spill.
```

**Pass criteria:** Temp files (if created) are not plaintext.

## Test 7: Python API — In-Memory + ATTACH Pattern

Validate the pattern the `Database` class will use.

```python
# Run via: uv run python -c "..."
import duckdb

# Create in-memory connection, then attach encrypted file
conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("""
    ATTACH '/tmp/test_encrypted.duckdb' AS edb
    (TYPE DUCKDB, ENCRYPTION_KEY 'python-test-key')
""")
conn.execute("USE edb")
conn.execute("""
    CREATE TABLE IF NOT EXISTS py_test (id INTEGER, value VARCHAR);
    INSERT INTO py_test VALUES (1, 'hello'), (2, 'world');
""")
result = conn.execute("SELECT * FROM py_test").fetchall()
print(f"Rows: {result}")
# Expected: [(1, 'hello'), (2, 'world')]

# Verify parameterized queries work on attached database
result = conn.execute("SELECT * FROM py_test WHERE id = ?", [1]).fetchall()
print(f"Parameterized: {result}")
# Expected: [(1, 'hello')]

conn.close()
```

**Pass criteria:** In-memory connect + ATTACH + USE + queries all work via Python API.

## Test 8: COPY FROM DATABASE (Key Rotation Pattern)

```bash
# 8. Test the key rotation pattern
duckdb -c "
  LOAD httpfs;
  ATTACH '${TESTDIR}/encrypted.duckdb' AS old_db (TYPE DUCKDB, ENCRYPTION_KEY 'test-key-12345');
  ATTACH '${TESTDIR}/rotated.duckdb' AS new_db (TYPE DUCKDB, ENCRYPTION_KEY 'new-key-67890');
  COPY FROM DATABASE old_db TO new_db;
  USE new_db;
  SELECT * FROM test_data;
"

# Expected: Data copied to new database with new key. 2 rows returned.

# Verify old key doesn't work on new database
duckdb -c "
  ATTACH '${TESTDIR}/rotated.duckdb' AS db (TYPE DUCKDB, ENCRYPTION_KEY 'test-key-12345');
  USE db;
  SELECT * FROM test_data;
" 2>&1

# Expected: Error — wrong key.
```

**Pass criteria:** `COPY FROM DATABASE` works across different encryption keys. Old key
doesn't open the new database.

## Cleanup

```bash
rm -rf "$TESTDIR"
```

## Results Summary

| Test | Description | Status |
|---|---|---|
| 1 | Create encrypted database | PASS |
| 2 | Unreadable without key | PASS |
| 3 | Readable with key | PASS |
| 4 | `-init` flag with CLI | PASS |
| 5 | `-init` flag with `-ui` | PASS |
| 6 | Temp files encrypted | INCONCLUSIVE |
| 7 | Python in-memory + ATTACH | PASS |
| 8 | COPY FROM DATABASE (rotation) | PASS |

## Decision Points

- **If Test 5 fails:** Update `privacy-data-protection.md` to document the fallback approach
  (print `ATTACH` statement for manual paste) as the primary `db ui` UX.
- **If Test 6 is inconclusive:** Note in the spec that temp file encryption is
  documented by DuckDB but not independently verified for small datasets.
- **If all pass:** Spec can be promoted to `ready` after resolving any remaining
  review comments.

## Outcome (2026-04-18)

All 7 functional tests passed. Test 6 (temp file encryption) was inconclusive —
DuckDB handled 10M rows without creating spill files, so temp encryption couldn't
be independently verified. This is consistent with the test plan's prediction and
DuckDB's documented behavior.

**Decision:** Spec promoted to `ready`.
