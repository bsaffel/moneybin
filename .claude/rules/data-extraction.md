---
globs: ["src/moneybin/extractors/**", "src/moneybin/connectors/**", "src/moneybin/loaders/**"]
---

# Data Extraction & Loading

## Day-Boundary Extraction

Only extract complete days. Calculate range from last extraction date to yesterday.

```python
def get_incremental_date_range(
    access_token: str,
) -> tuple[datetime | None, datetime | None]:
    last = get_last_extraction_date(access_token)
    yesterday = datetime.now().date() - timedelta(days=1)
    if last:
        start = last + timedelta(days=1)
        return (start, yesterday) if start <= yesterday else (None, None)
    return (today - timedelta(days=lookback_days), yesterday)
```

## Incremental by Default

- Default to incremental extraction (skip if no new complete days).
- Provide `--force` / `-f` flag for full lookback override.
- Track extraction metadata in DuckDB.

## Dedup Strategy

| Level | Method |
|-------|--------|
| Extraction | Track last extraction dates, skip redundant API calls |
| Loading | Unique IDs (`transaction_id`) prevent duplicate inserts |
| Storage | Allow duplicate raw files; deduplicate at database level |

## Loading Patterns

```sql
-- Incremental: insert only new records
INSERT INTO transactions
SELECT * FROM read_parquet('data/*.parquet')
WHERE transaction_id NOT IN (SELECT transaction_id FROM transactions);

-- Full refresh
CREATE OR REPLACE TABLE transactions AS
SELECT * FROM read_parquet('data/*.parquet');
```

## Adding a New Data Source

1. Create extractor/connector in `src/moneybin/extractors/` or `src/moneybin/connectors/`
2. Create loader in `src/moneybin/loaders/`
3. Add staging models in `sqlmesh/models/prep/`
4. Add CTE + `UNION ALL` in the relevant core model
