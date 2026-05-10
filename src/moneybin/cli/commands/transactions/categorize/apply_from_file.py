"""Apply LLM-generated categorizations from a JSON file or stdin."""

import json
import logging
import sys
from pathlib import Path

import typer

from moneybin.cli.output import OutputFormat, output_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.protocol.envelope import ResponseEnvelope

logger = logging.getLogger(__name__)

# Keys the BulkCategorizationItem model accepts (extra="forbid"). Used to strip
# export-shape extras (opaque_id, description_redacted, source_type) from rows
# fed to apply-from-file, after opaque_id → transaction_id remap.
_ALLOWED_BULK_ITEM_KEYS = {"transaction_id", "category", "subcategory"}


def categorize_apply_from_file(
    input_path: Path | None = typer.Argument(
        None,
        help="Path to a JSON file produced by export-uncategorized, or '-' to read stdin.",
    ),
    output: OutputFormat = output_option,
) -> None:
    r"""Apply LLM-generated categories from a JSON file to transactions.

    Reads a JSON array where each object has:
      opaque_id, category, and (optionally) subcategory.

    Designed for the export → LLM → apply workflow:

      moneybin transactions categorize export-uncategorized -o todo.json
      # edit todo.json: add category/subcategory to each item
      moneybin transactions categorize apply-from-file todo.json

    Or pipe through LLM tooling:

      moneybin transactions categorize export-uncategorized \
        | llm-tool --fill-categories \
        | moneybin transactions categorize apply-from-file -

    Exit code is 1 if any item failed or was skipped.
    """
    from moneybin.cli.output import render_or_json
    from moneybin.services.categorization_service import (
        CategorizationResult,
        validate_bulk_items,
    )

    use_stdin = str(input_path) == "-"

    if input_path is None:
        typer.echo(
            "Provide a file path or '-' to read from stdin.",
            err=True,
        )
        raise typer.Exit(2)

    try:
        if use_stdin:
            raw = json.load(sys.stdin)
        else:
            with input_path.open(encoding="utf-8") as f:
                raw = json.load(f)
    except FileNotFoundError as e:
        typer.echo(f"❌ File not found: {input_path}", err=True)
        raise typer.Exit(2) from e
    except json.JSONDecodeError as e:
        typer.echo(f"❌ Invalid JSON: {e}", err=True)
        raise typer.Exit(1) from e

    # Map export-shape rows into BulkCategorizationItem-shape rows. The export
    # command emits {opaque_id, description_redacted, source_type} for the LLM
    # to annotate with category/subcategory; the service model is
    # {transaction_id, category, subcategory} with extra="forbid", so we must
    # strip the export-only keys before validation. opaque_id → transaction_id
    # if no transaction_id is already present.
    normalized: object = raw
    if isinstance(raw, list):
        remapped: list[object] = []
        for row in raw:  # pyright: ignore[reportUnknownVariableType]
            if isinstance(row, dict):
                row_dict: dict[str, object] = {str(k): v for k, v in row.items()}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
                if "transaction_id" not in row_dict and "opaque_id" in row_dict:
                    row_dict["transaction_id"] = row_dict["opaque_id"]
                remapped.append({
                    k: v for k, v in row_dict.items() if k in _ALLOWED_BULK_ITEM_KEYS
                })
            else:
                remapped.append(row)  # pyright: ignore[reportUnknownArgumentType]
        normalized = remapped

    try:
        items, parse_errors = validate_bulk_items(normalized)
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if items:
        with handle_cli_errors() as db:
            from moneybin.services.categorization_service import CategorizationService

            result = CategorizationService(db).bulk_categorize(items)
    else:
        result = CategorizationResult(applied=0, skipped=0, errors=0, error_details=[])
    result.merge_parse_errors(parse_errors)

    input_count = len(items) + len(parse_errors)

    def _render_table(_: ResponseEnvelope) -> None:
        logger.info(
            f"✅ Applied {result.applied} | skipped {result.skipped} | errors {result.errors}"
        )
        for err in result.error_details:
            logger.warning(f"⚠️  {err['transaction_id']}: {err['reason']}")

    render_or_json(result.to_envelope(input_count), output, render_fn=_render_table)

    if result.errors > 0 or result.skipped > 0:
        raise typer.Exit(1)
