"""Internal helpers shared across assertion modules."""

from __future__ import annotations

from sqlglot import exp


def quote_ident(ident: str) -> str:
    """Quote a dotted identifier via sqlglot, per .claude/rules/security.md."""
    return ".".join(
        exp.to_identifier(seg, quoted=True).sql("duckdb") for seg in ident.split(".")
    )
