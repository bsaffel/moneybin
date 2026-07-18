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
from moneybin.extractors.pdf.recipe import Recipe

_VALID_RECIPE_DICT: dict[str, Any] = {
    "row_region": {"start_anchor": "Date", "end_anchor": "Total:"},
    "row_split": r"\s{2,}",
    "fields": [
        {
            "name": "date",
            "pattern": r"\d{2}/\d{2}/\d{4}",
            "cast": "date",
            "date_format": "%m/%d/%Y",
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


def test_parse_bridge_response_leaves_the_recipe_unratified() -> None:
    """A bridge-authored recipe is never a human's sign assertion."""
    response = parse_bridge_response({"recipe": _VALID_RECIPE_DICT, "rows": []})
    assert response.recipe.sign_ratified is False


def test_recipe_for_agent_strips_the_ratification_flag() -> None:
    """The saved recipe shown to the agent must not carry the human's ratification.

    The replay-failure request hands the agent the saved recipe to re-derive from.
    Shipping ``sign_ratified`` would both teach the agent the key it would need to
    escalate with and make the contract incoherent — an honest agent echoing the
    recipe back would be rejected for naming a field MoneyBin handed it.
    """
    from moneybin.extractors.pdf.bridge import recipe_for_agent

    ratified = Recipe.model_validate({**_VALID_RECIPE_DICT, "sign_ratified": True})

    payload = recipe_for_agent(ratified)

    assert "sign_ratified" not in payload
    # The patterns the agent actually needs still ship.
    assert payload["row_split"] == _VALID_RECIPE_DICT["row_split"]
    # And what it hands back round-trips through the ingress guard.
    assert (
        parse_bridge_response({"recipe": payload, "rows": []}).recipe.sign_ratified
        is False
    )


def test_parse_bridge_response_rejects_agent_supplied_sign_ratified() -> None:
    """The agent must not be able to self-grant the human's sign ratification.

    ``sign_ratified`` disarms the polarity guard for a format forever, in both
    directions; the apply path skips the sign confirm gate and persists the recipe
    it is handed. An agent that could set the flag through this seam would grant
    itself a permanent, silent ledger inversion — the exact outcome the gate exists
    to prevent. Rejected loudly rather than coerced to False: the attempt is signal.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    hijacked: dict[str, Any] = {**_VALID_RECIPE_DICT, "sign_ratified": True}
    with pytest.raises(BridgeResponseError, match="sign_ratified"):
        parse_bridge_response({"recipe": hijacked, "rows": []})

    # Even the honest-looking value is refused — the key is not the agent's to name.
    with pytest.raises(BridgeResponseError, match="sign_ratified"):
        parse_bridge_response({
            "recipe": {**_VALID_RECIPE_DICT, "sign_ratified": False},
            "rows": [],
        })


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


def test_parse_bridge_response_rejects_uncompilable_regex() -> None:
    """Reject an uncompilable recipe regex at parse time, not at execution.

    Surfaces as ``BridgeResponseError`` (→ ``bridge_response_invalid``) instead
    of a cryptic ``regex.error`` raised later inside ``route_forced_recipe``.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    bad = {**_VALID_RECIPE_DICT, "row_split": "["}  # unterminated character class
    with pytest.raises(BridgeResponseError, match="invalid regex"):
        parse_bridge_response({"recipe": bad, "rows": []})


def test_parse_bridge_response_rejects_recipe_without_amount_field() -> None:
    """A recipe with no amount/debit/credit field is rejected at parse time.

    Without one, confidence passes on the date field alone and a zero-delta
    statement reconciles — loading all-zero rows. Reject as BridgeResponseError.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    no_amount = {
        **_VALID_RECIPE_DICT,
        "fields": [
            {
                "name": "date",
                "pattern": r"\d{2}/\d{2}",
                "cast": "date",
                "date_format": "%m/%d",
            },
        ],
    }
    with pytest.raises(BridgeResponseError, match="amount"):
        parse_bridge_response({"recipe": no_amount, "rows": []})


def test_parse_bridge_response_rejects_recipe_without_primary_date_field() -> None:
    """A recipe with an amount but no primary ``date`` field is rejected.

    The load writes ``row['date']`` into ``transaction_date`` (NOT NULL); a
    recipe with only an amount (or only ``post_date``) passes confidence +
    reconciliation, then fails with a generic DB constraint error instead of a
    clean ``bridge_response_invalid``.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    no_date = {
        **_VALID_RECIPE_DICT,
        "fields": [
            {"name": "amount", "pattern": r"-?\d+\.\d{2}", "cast": "decimal"},
        ],
    }
    with pytest.raises(BridgeResponseError, match="date"):
        parse_bridge_response({"recipe": no_date, "rows": []})


_YEARLESS_FIELDS: list[dict[str, Any]] = [
    {"name": "date", "pattern": r"\d{2}/\d{2}", "cast": "date", "date_format": "%m/%d"},
    {"name": "amount", "pattern": r"-?\d+\.\d{2}", "cast": "decimal"},
]
_PERIOD_ANCHORS: list[dict[str, Any]] = [
    {
        "name": "period_start",
        "pattern": r"Cycle\s+(\d{2}/\d{2}/\d{2})",
        "cast": "date",
    },
    {
        "name": "period_end",
        "pattern": r"Cycle\s+\d{2}/\d{2}/\d{2}\s*-\s*(\d{2}/\d{2}/\d{2})",
        "cast": "date",
    },
]


def test_parse_bridge_response_accepts_yearless_with_default_period_anchors() -> None:
    """A year-less date_format is allowed when metadata_anchors is None.

    None falls back to DEFAULT_ANCHORS, which carry the billing-period patterns the
    executor's ``_resolve_yearless_date`` needs. The executor never writes a 1900
    date — a year-less row with no capturable period raises and is skipped — so the
    bridge admits the recipe rather than pre-rejecting a resolvable one.
    """
    yearless = {**_VALID_RECIPE_DICT, "fields": _YEARLESS_FIELDS}
    response = parse_bridge_response({"recipe": yearless, "rows": []})
    assert response.recipe.fields[0].date_format == "%m/%d"


def test_parse_bridge_response_accepts_yearless_with_declared_period_anchors() -> None:
    """A year-less date_format is allowed when the recipe declares period anchors.

    This is what makes the deterministic path's yearless-no-recognised-period
    decline a PRODUCTIVE bridge escalation: the agent reads the period off a
    non-default label, declares it as period_start/period_end anchors, and the
    statement imports instead of seeding.
    """
    yearless = {
        **_VALID_RECIPE_DICT,
        "fields": _YEARLESS_FIELDS,
        "metadata_anchors": _PERIOD_ANCHORS,
    }
    response = parse_bridge_response({"recipe": yearless, "rows": []})
    assert response.recipe.fields[0].date_format == "%m/%d"


def test_parse_bridge_response_rejects_yearless_without_period_anchors() -> None:
    """A year-less date_format with an explicit anchor list lacking the period is rejected.

    An explicit metadata_anchors list overrides DEFAULT_ANCHORS, so one that omits
    period_start/period_end leaves the executor no billing period to bracket the
    year — every year-less row would fail to cast. Reject at parse time with an
    actionable message rather than silently reconcile-fail the whole statement.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    yearless = {
        **_VALID_RECIPE_DICT,
        "fields": _YEARLESS_FIELDS,
        "metadata_anchors": [
            {"name": "account_id", "pattern": r"Account\s+(\d+)", "cast": "str"},
        ],
    }
    with pytest.raises(BridgeResponseError, match="period_start and period_end"):
        parse_bridge_response({"recipe": yearless, "rows": []})


def test_parse_bridge_response_rejects_non_date_cast_primary_date() -> None:
    """A 'Date'-named field that isn't ``cast='date'`` doesn't satisfy the gate.

    ``_canonical_key`` maps it to ``date`` via its fallback, but ``execute_recipe``
    won't parse it, so the loader would write an unparsed string into the DATE
    ``transaction_date`` column. The gate must also require ``cast == 'date'``.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    bad = {
        **_VALID_RECIPE_DICT,
        "fields": [
            {"name": "Date", "pattern": r"\d{2}/\d{2}", "cast": "str"},
            {"name": "amount", "pattern": r"-?\d+\.\d{2}", "cast": "decimal"},
        ],
    }
    with pytest.raises(BridgeResponseError, match="date"):
        parse_bridge_response({"recipe": bad, "rows": []})


def test_parse_bridge_response_rejects_negative_convention_with_only_debit_credit() -> (
    None
):
    """``negative_is_expense`` with only debit/credit fields (no ``amount``) is rejected.

    ``reconcile``/``_sum_pre_normalization`` reads the canonical ``amount`` key for
    the ``negative_is_*`` conventions; a recipe supplying only debit/credit sums an
    absent key to 0, so a zero-delta statement reconciles and the loader writes every
    amount as 0. The amount-field shape must match the declared ``sign_convention``.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    mismatched = {
        **_VALID_RECIPE_DICT,
        "sign_convention": "negative_is_expense",
        "fields": [
            {
                "name": "date",
                "pattern": r"\d{2}/\d{2}/\d{4}",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {"name": "debit", "pattern": r"\d+\.\d{2}", "cast": "decimal"},
            {"name": "credit", "pattern": r"\d+\.\d{2}", "cast": "decimal"},
        ],
    }
    with pytest.raises(BridgeResponseError, match="sign_convention"):
        parse_bridge_response({"recipe": mismatched, "rows": []})


def test_parse_bridge_response_rejects_split_convention_with_only_amount() -> None:
    """``split_debit_credit`` with only an ``amount`` field (no debit/credit) is rejected.

    ``reconcile`` reads the ``credit``/``debit`` keys for ``split_debit_credit``; a
    recipe supplying only ``amount`` sums absent keys to 0, reconciling a zero-delta
    statement and loading all-zero amounts.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    mismatched = {
        **_VALID_RECIPE_DICT,
        "sign_convention": "split_debit_credit",
        "fields": [
            {
                "name": "date",
                "pattern": r"\d{2}/\d{2}/\d{4}",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {"name": "amount", "pattern": r"-?\d+\.\d{2}", "cast": "decimal"},
        ],
    }
    with pytest.raises(BridgeResponseError, match="sign_convention"):
        parse_bridge_response({"recipe": mismatched, "rows": []})


def test_parse_bridge_response_accepts_split_convention_with_debit_credit() -> None:
    """A ``split_debit_credit`` recipe with debit + credit fields passes the gate.

    Regression guard: the convention-aware shape gate must not over-reject the
    legitimate split layout (the keys ``reconcile`` actually reads for this
    convention).
    """
    rows: list[dict[str, Any]] = [
        {"date": "2026-05-01", "debit": "4.50", "credit": "0.00"},
    ]
    recipe = {
        **_VALID_RECIPE_DICT,
        "sign_convention": "split_debit_credit",
        "fields": [
            {
                "name": "date",
                "pattern": r"\d{2}/\d{2}/\d{4}",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {"name": "debit", "pattern": r"\d+\.\d{2}", "cast": "decimal"},
            {"name": "credit", "pattern": r"\d+\.\d{2}", "cast": "decimal"},
        ],
    }
    response = parse_bridge_response({"recipe": recipe, "rows": rows})
    assert isinstance(response, BridgeResponse)
    assert response.recipe.sign_convention == "split_debit_credit"
