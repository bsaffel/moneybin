"""CLI adapter coverage for externally-decided categorizations."""

from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.services.categorization import CategorizationItem, CategorizationResult


@pytest.mark.unit
@pytest.mark.parametrize(
    ("command", "input_mode"),
    [
        ("commit", "input"),
        ("commit", "stdin"),
        ("commit-from-file", "path"),
        ("commit-from-file", "stdin"),
    ],
)
def test_commit_routes_preserve_canonical_merchant_name(
    tmp_path: Path, command: str, input_mode: str
) -> None:
    """Dropping the merchant field prevents reviewed rows from teaching the matcher."""
    item = {
        "transaction_id": "txn-1",
        "category": "Subscriptions",
        "canonical_merchant_name": "YouTube",
    }
    if command == "commit-from-file":
        item["description_scrubbed"] = "PAYPAL INST XFER"
    payload = [item]
    input_path = tmp_path / "proposals.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    received: list[CategorizationItem] = []

    class RecordingService:
        def __init__(self, _db: object) -> None:
            pass

        def categorize_items(
            self, items: list[CategorizationItem]
        ) -> CategorizationResult:
            received.extend(items)
            return CategorizationResult(
                applied=1, skipped=0, errors=0, error_details=[]
            )

    if input_mode == "input":
        args = [command, "--input", str(input_path), "--output", "json"]
        input_text = None
    elif input_mode == "path":
        args = [command, str(input_path), "--output", "json"]
        input_text = None
    else:
        args = [command, "-", "--output", "json"]
        if command == "commit":
            args = [command, "-", "--output", "json"]
        input_text = json.dumps(payload)

    with (
        patch(
            "moneybin.cli.commands.transactions.categorize.get_database",
            return_value=nullcontext(object()),
        ),
        patch(
            "moneybin.cli.commands.transactions.categorize.commit_from_file.get_database",
            return_value=nullcontext(object()),
        ),
        patch(
            "moneybin.services.categorization.CategorizationService",
            RecordingService,
        ),
    ):
        result = CliRunner().invoke(app, args, input=input_text)

    assert result.exit_code == 0, result.output
    assert len(received) == 1
    assert received[0].canonical_merchant_name == "YouTube"


@pytest.mark.unit
@pytest.mark.parametrize("input_mode", ["path", "stdin"])
def test_commit_from_file_reports_invalid_merchant_names_per_row(
    tmp_path: Path, input_mode: str
) -> None:
    """Filtering export extras must not erase an invalid merchant-name error."""
    payload = [
        {
            "transaction_id": "txn-valid",
            "category": "Groceries",
            "canonical_merchant_name": "Market",
            "description_scrubbed": "MARKET",
        },
        {
            "transaction_id": "txn-invalid",
            "category": "Groceries",
            "canonical_merchant_name": "",
            "description_scrubbed": "INVALID MARKET",
        },
    ]
    input_path = tmp_path / "proposals.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    received: list[CategorizationItem] = []

    class RecordingService:
        def __init__(self, _db: object) -> None:
            pass

        def categorize_items(
            self, items: list[CategorizationItem]
        ) -> CategorizationResult:
            received.extend(items)
            return CategorizationResult(
                applied=1, skipped=0, errors=0, error_details=[]
            )

    args = ["commit-from-file", str(input_path), "--output", "json"]
    input_text = None
    if input_mode == "stdin":
        args = ["commit-from-file", "-", "--output", "json"]
        input_text = json.dumps(payload)

    with (
        patch(
            "moneybin.cli.commands.transactions.categorize.commit_from_file.get_database",
            return_value=nullcontext(object()),
        ),
        patch(
            "moneybin.services.categorization.CategorizationService",
            RecordingService,
        ),
    ):
        result = CliRunner().invoke(app, args, input=input_text)

    assert result.exit_code == 1, result.output
    assert [item.transaction_id for item in received] == ["txn-valid"]
    assert "canonical_merchant_name" in result.output
