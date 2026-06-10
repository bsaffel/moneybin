"""Tests for the Phase 2b bridge payload + response shapes.

Covers ``build_bridge_request`` (export side) and ``parse_bridge_response``
(apply side). The bridge module is the seam where MoneyBin hands a PDF to the
driving agent and receives a vetted Recipe + rows back; both directions are
data-shape contracts that must stay stable, so each shape gets explicit
coverage.
"""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.extractors.pdf.bridge import (
    TRANSPARENCY_NOTICE,
    BridgeRequest,
    BridgeResponse,
    build_bridge_request,
    parse_bridge_response,
)
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable

_VALID_RECIPE_DICT: dict[str, Any] = {
    "row_region": {"start_anchor": "Date", "end_anchor": "Total:"},
    "row_split": r"\s{2,}",
    "fields": [
        {
            "name": "date",
            "pattern": r"\d{2}/\d{2}",
            "cast": "date",
            "date_format": "%m/%d",
        },
        {"name": "amount", "pattern": r"-?\d+\.\d{2}", "cast": "decimal"},
    ],
    "sign_convention": "negative_is_expense",
    "routing": "transactions",
}


def _doc() -> PdfDocument:
    """Minimal native-text PDF with one table + a few text lines."""
    return PdfDocument(
        source_file="statement.pdf",
        text_lines=[
            "Chase Bank — Checking Statement",
            "Date  Description  Amount",
            "05/01  Coffee  -4.50",
            "05/02  Refund  10.00",
            "Total: 5.50",
        ],
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Amount"],
                rows=[
                    ["05/01", "Coffee", "-4.50"],
                    ["05/02", "Refund", "10.00"],
                    ["05/03", "Lunch", "-12.00"],
                    ["05/04", "Bus", "-2.50"],
                    ["05/05", "Coffee", "-4.50"],
                    ["05/06", "Brunch", "-22.00"],
                    ["05/07", "Lunch", "-15.00"],
                ],
            ),
        ],
    )


def test_build_bridge_request_propose_recipe_returns_typed_request() -> None:
    req = build_bridge_request(_doc(), request_kind="propose_recipe")
    assert isinstance(req, BridgeRequest)
    assert req.request_kind == "propose_recipe"
    assert req.saved_recipe_for_re_derive is None


def test_build_bridge_request_replay_failed_carries_saved_recipe() -> None:
    saved = {"version": 1, "extraction_recipe": _VALID_RECIPE_DICT}
    req = build_bridge_request(
        _doc(),
        request_kind="replay_failed_re_derive",
        saved_recipe_for_re_derive=saved,
    )
    assert req.request_kind == "replay_failed_re_derive"
    assert req.saved_recipe_for_re_derive == saved


def test_bridge_request_includes_transparency_notice() -> None:
    req = build_bridge_request(_doc(), request_kind="propose_recipe")
    assert req.transparency_notice == TRANSPARENCY_NOTICE
    assert "audit_log" in req.transparency_notice
    assert "smart_import_parse" in req.transparency_notice


def test_bridge_request_carries_fingerprint_with_required_keys() -> None:
    req = build_bridge_request(_doc(), request_kind="propose_recipe")
    assert set(req.fingerprint.keys()) >= {"issuer", "headers", "page_bucket"}
    assert isinstance(req.fingerprint["headers"], list)


def test_bridge_request_document_text_preserves_lines() -> None:
    req = build_bridge_request(_doc(), request_kind="propose_recipe")
    assert "Chase Bank" in req.document_text
    assert "Total: 5.50" in req.document_text
    # Newline-joined preserves separation
    assert req.document_text.count("\n") >= 4


def test_bridge_request_tables_preview_caps_rows_per_table() -> None:
    req = build_bridge_request(_doc(), request_kind="propose_recipe")
    assert len(req.tables_preview) == 1
    preview = req.tables_preview[0]
    assert preview["page"] == 1
    assert preview["header"] == ["Date", "Description", "Amount"]
    # The fixture has 7 rows; preview keeps at most 5.
    assert len(preview["rows"]) == 5


def test_parse_bridge_response_returns_typed_recipe_and_rows() -> None:
    rows: list[dict[str, Any]] = [
        {"date": "2026-05-01", "amount": "-4.50"},
        {"date": "2026-05-02", "amount": "10.00"},
    ]
    payload: dict[str, Any] = {"recipe": _VALID_RECIPE_DICT, "rows": rows}
    response = parse_bridge_response(payload)
    assert isinstance(response, BridgeResponse)
    assert response.recipe.routing == "transactions"
    assert response.rows == rows


def test_parse_bridge_response_rejects_non_dict_payload() -> None:
    with pytest.raises(ValueError, match="must be a dict"):
        parse_bridge_response("not-a-dict")  # type: ignore[arg-type]


def test_parse_bridge_response_rejects_missing_recipe_key() -> None:
    with pytest.raises(ValueError, match="recipe"):
        parse_bridge_response({"rows": []})


def test_parse_bridge_response_rejects_missing_rows_key() -> None:
    with pytest.raises(ValueError, match="rows"):
        parse_bridge_response({"recipe": _VALID_RECIPE_DICT})


def test_parse_bridge_response_rejects_recipe_not_a_dict() -> None:
    with pytest.raises(ValueError, match="must be a dict"):
        parse_bridge_response({"recipe": "oops", "rows": []})


def test_parse_bridge_response_rejects_rows_not_a_list() -> None:
    with pytest.raises(ValueError, match="must be a list"):
        parse_bridge_response({"recipe": _VALID_RECIPE_DICT, "rows": "oops"})


def test_parse_bridge_response_rejects_invalid_recipe_shape() -> None:
    # Recipe missing required `row_region` — pydantic ValidationError, wrapped
    # as ValueError so callers have one exception type to catch.
    bad: dict[str, Any] = {"oops": True}
    with pytest.raises(ValueError, match="recipe invalid"):
        parse_bridge_response({"recipe": bad, "rows": []})


def test_parse_bridge_response_raises_bridge_response_error_subtype() -> None:
    """Parse failures raise ``BridgeResponseError`` (a ``ValueError`` subtype).

    Lets the confirm path catch a bad agent response narrowly without also
    swallowing unrelated ValueErrors raised later by extraction/load — those
    would mislabel a malformed-PDF failure as ``bridge_response_invalid``.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    with pytest.raises(BridgeResponseError):
        parse_bridge_response({"rows": []})
