"""Internal helpers shared across assertion modules."""

from __future__ import annotations

from sqlglot import exp


def quote_ident(ident: str) -> str:
    """Quote a dotted identifier via sqlglot, per .claude/rules/security.md."""
    return ".".join(
        exp.to_identifier(seg, quoted=True).sql("duckdb") for seg in ident.split(".")
    )


def split_table_ident(table: str) -> tuple[str | None, str]:
    """Split an optional schema-qualified table name into (schema, table)."""
    if "." in table:
        s, t = table.split(".", 1)
        return s, t
    return None, table
