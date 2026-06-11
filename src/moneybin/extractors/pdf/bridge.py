"""Bridge payload + response shapes for Phase 2b PDF escalation (Reqs 8, 9, 14).

When the deterministic rung can't extract confidently from a native-text PDF,
or when a saved recipe stopped reconciling, MoneyBin escalates to the agent
the user is *already* driving the host with. The bridge carries the document
to the agent, the agent proposes a Recipe + rows, and ``import_confirm``
ratifies.

This module owns the two data-shape contracts:

- ``BridgeRequest`` — what MoneyBin sends out (the egress payload). Wraps the
  document text, a per-table preview, the layout fingerprint, the transparency
  notice (Req 14, surfaced explicitly so the agent cannot accidentally elide
  it), and a request kind ("propose_recipe" on first contact vs.
  "replay_failed_re_derive" when a saved recipe broke).
- ``BridgeResponse`` — what the agent sends back: a Pydantic-validated Recipe
  plus the rows it extracted.

Architecture note: this module intentionally does NOT import from the
service layer. The service constructs the egress envelope by wrapping a
``BridgeRequest`` in the channel-agnostic ``BridgePayload`` (from
``services.import_confirmation``) — the boundary keeps extractors free of
service-layer types.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from moneybin.extractors.pdf.fingerprint import compute_fingerprint
from moneybin.extractors.pdf.ir import PdfDocument
from moneybin.extractors.pdf.recipe import Recipe

TRANSPARENCY_NOTICE = (
    "Proceeding will surface this PDF's content to the agent you are "
    "currently driving MoneyBin with. The document text and table headers "
    "are sent verbatim — there is no redacted preview. An entry is written "
    "to app.audit_log (action: smart_import_parse) recording the hand-off."
)

# How many leading rows per table to ship to the agent. Keeps the preview
# compact (the executor only needs to see the pattern shape, not every row)
# and bounds payload size on dense statements.
_TABLE_PREVIEW_ROW_CAP = 5

RequestKind = Literal["propose_recipe", "replay_failed_re_derive"]


class BridgeResponseError(ValueError):
    """The agent's bridge response is malformed or fails the recipe bounds.

    A ``ValueError`` subtype so existing broad catches keep working, but typed
    so the confirm path can catch *parse* failures narrowly — a ValueError
    raised later by PDF extraction or the load must not be mislabeled as an
    invalid bridge response.
    """


@dataclass(frozen=True)
class BridgeRequest:
    """Typed payload contents the bridge ships to the driving agent."""

    transparency_notice: str
    source_file: str
    # text_lines joined with '\n'; agent can re-split as needed.
    document_text: str
    # One entry per table: {page, header, rows[:N]}.
    tables_preview: list[dict[str, Any]]
    fingerprint: dict[str, Any]
    request_kind: RequestKind
    # Populated only for 'replay_failed_re_derive' so the agent can see what
    # the saved recipe was and what specifically failed. None for first-contact
    # 'propose_recipe' requests.
    saved_recipe_for_re_derive: dict[str, Any] | None = None


@dataclass(frozen=True)
class BridgeResponse:
    """The agent's vetted reply: a validated Recipe and extracted rows.

    Recipe is the Pydantic-validated model (security bounds in
    ``Recipe._bound_patterns`` already ran); rows are canonical-shape dicts
    matching the recipe's field names so the apply path treats them
    indistinguishably from deterministic-extracted rows.
    """

    recipe: Recipe
    rows: list[dict[str, Any]]


def build_bridge_request(
    doc: PdfDocument,
    *,
    request_kind: RequestKind,
    saved_recipe_for_re_derive: dict[str, Any] | None = None,
) -> BridgeRequest:
    """Build the typed bridge request envelope from a PDF IR + decision context.

    The service layer wraps the resulting request in ``BridgePayload`` (from
    ``import_confirmation``) before raising ``ImportConfirmationRequiredError``
    — keeping the extractor-layer module free of service-layer types so the
    architecture-layering test stays happy.
    """
    tables_preview = [
        {
            "page": t.page,
            "header": list(t.header),
            "rows": t.rows[:_TABLE_PREVIEW_ROW_CAP],
        }
        for t in doc.tables
    ]
    return BridgeRequest(
        transparency_notice=TRANSPARENCY_NOTICE,
        source_file=doc.source_file,
        document_text="\n".join(doc.text_lines),
        tables_preview=tables_preview,
        fingerprint=compute_fingerprint(doc),
        request_kind=request_kind,
        saved_recipe_for_re_derive=saved_recipe_for_re_derive,
    )


def parse_bridge_response(payload: object) -> BridgeResponse:
    """Validate an agent's response; raise ``BridgeResponseError`` on bad shape.

    Expected payload shape::

        {"recipe": <Recipe-shaped dict>, "rows": [{...}, ...]}

    ``Recipe.model_validate`` enforces the security bounds (Req 9b — pattern
    length + nested-quantifier check) before the apply-side executor ever
    runs against the agent's patterns, so a malicious bridge response cannot
    bypass those guards by going through this seam.
    """
    if not isinstance(payload, dict):
        raise BridgeResponseError("bridge response must be a dict")
    if "recipe" not in payload:
        raise BridgeResponseError("bridge response missing 'recipe' key")
    if "rows" not in payload:
        raise BridgeResponseError("bridge response missing 'rows' key")
    raw_recipe = payload["recipe"]
    if not isinstance(raw_recipe, dict):
        raise BridgeResponseError("bridge response 'recipe' must be a dict")
    raw_rows = payload["rows"]
    if not isinstance(raw_rows, list):
        raise BridgeResponseError("bridge response 'rows' must be a list")
    if not all(isinstance(r, dict) for r in raw_rows):
        raise BridgeResponseError("bridge response 'rows' must be a list of dicts")
    # raw_recipe / raw_rows pass isinstance checks above; cast for pyright since
    # the source dict (untrusted JSON) types its values as object.
    typed_recipe = cast(dict[str, Any], raw_recipe)
    typed_rows = cast(list[dict[str, Any]], raw_rows)
    try:
        recipe = Recipe.model_validate(typed_recipe)
    except Exception as e:  # noqa: BLE001 — pydantic ValidationError + bound-validator ValueErrors
        raise BridgeResponseError(f"bridge recipe invalid: {e}") from e
    # A bridge recipe must extract the two required fields — a primary date and
    # an amount. Without them, confidence + reconciliation can still pass on the
    # field that IS present, but the load then either writes all-zero amounts
    # (no amount field) or NULL into the NOT NULL transaction_date (no date
    # field). Reject here as bridge_response_invalid rather than surfacing a
    # downstream DB error. Same predicates the confidence model and loader use
    # (routing imports nothing from bridge — no cycle).
    from moneybin.extractors.pdf.routing import (
        is_amount_field,
        is_primary_date_field,
    )

    if not any(is_amount_field(f) for f in recipe.fields):
        raise BridgeResponseError(
            "bridge recipe must contain at least one amount, debit, or credit "
            "field — without it, extracted rows have no transaction amount"
        )
    if not any(is_primary_date_field(f) for f in recipe.fields):
        raise BridgeResponseError(
            "bridge recipe must contain a primary transaction date field — "
            "without it, loaded rows have no transaction_date (a post_date "
            "field alone does not qualify)"
        )
    return BridgeResponse(recipe=recipe, rows=typed_rows)
