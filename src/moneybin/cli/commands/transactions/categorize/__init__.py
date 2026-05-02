"""Categorization workflow command group.

Deterministic categorization operations — no LLM dependency.
LLM-based auto-categorization is available through the MCP server.
"""

import typer

app = typer.Typer(
    help="Categorization workflow + rules (taxonomy under top-level `categories`)",
    no_args_is_help=True,
)
