"""Transaction matching and dedup engine."""

UNIONED_TABLE = "prep.int_transactions__unioned"


def quote_table_ref(table: str) -> str:
    """Validate and quote a schema.table reference for safe SQL interpolation."""
    from sqlglot import exp

    parts = table.split(".")
    if len(parts) != 2:
        raise ValueError(f"table must be schema.name, got: {table!r}")
    safe_schema = exp.to_identifier(parts[0], quoted=True).sql("duckdb")
    safe_table = exp.to_identifier(parts[1], quoted=True).sql("duckdb")
    return f"{safe_schema}.{safe_table}"
