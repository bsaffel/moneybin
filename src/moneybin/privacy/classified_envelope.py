"""One builder for every runtime-classified response envelope.

A tool that classifies its response per call (``dynamic_classification=True``)
owes four steps before the payload crosses a surface: read the declared data
classes off the contract type, take the max tier across them, redact the
payload, and report the classes for the privacy audit row. Every one of those
steps is security-relevant, and each hand-rolled copy is somewhere the four can
drift apart — a tier taken from the wrong type under-declares the response, a
forgotten ``redact_typed`` ships an unmasked account identifier, and a dropped
class leaves the audit trail claiming less than was returned.

``build_classified_envelope`` performs all four, so a classification-contract
change is one edit instead of sixteen. ``mcp.md``'s "tools contain no privacy
enforcement" is what this makes true.

Redaction deliberately walks ``type(data)`` rather than ``contract_type``:
``contract_type`` names what the *surface* may return (often a union spanning
tiers), while the transform must walk the annotations of the object actually in
hand. Classification uses the wider contract on purpose — an agent reads
``summary.sensitivity`` before it sees the payload, so the declaration has to
cover every arm the tool could have returned.

``actions[]`` is the one field the builder does not redact: the caller composes
it and the builder passes it through untouched. A caller that reads a field's
*value* out of ``data`` to compose a hint is therefore reading it pre-redaction,
which is safe only where that field's class is ``MaskStrength.PASSTHROUGH`` --
anything else reaches the wire unmasked. Pin that assumption with a test derived
from the transform table rather than a comment, the way the gsheet drift hint
does; a comment does not fail when someone changes the class.

Lives under ``moneybin.privacy`` rather than ``moneybin.mcp``: both surfaces
use it, and ``moneybin.privacy.sensitivity`` already imports from here, so the reverse
edge would be a cycle.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

from moneybin.privacy.introspection import PrivacyContractError, extract_data_classes
from moneybin.privacy.redaction import redact_typed
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.protocol.envelope import UNSET, ResponseEnvelope, Unset, build_envelope

type SensitivityLiteral = Literal["low", "medium", "high", "critical"]


def tier_sensitivity(tier: Tier) -> SensitivityLiteral:
    """Return the envelope's ``sensitivity`` string for ``tier``.

    Duplicates ``privacy.sensitivity.tier_to_sensitivity``, which lived under
    ``moneybin.mcp`` when this was written — spelling it here was how
    ``moneybin.privacy`` stayed free of an import edge into the transport
    package. That edge is gone; a test still pins the two together.
    """
    return cast(SensitivityLiteral, tier.name.lower())


@dataclass(frozen=True, slots=True)
class Classification:
    """The data classes one or more contract types declare, and their tier."""

    classes: frozenset[DataClass]
    contract_types: tuple[Any, ...] = ()

    @property
    def classes_returned(self) -> list[str]:
        """Sorted class values for the envelope and the privacy audit row.

        Never raises. Reporting "declares no classes" is honest for a payload
        that declares none — an audit row saying ``[]`` is more useful than no
        row. ``tier`` is where an unclassified payload fails, because that is
        the value a caller would otherwise act on.
        """
        return sorted(data_class.value for data_class in self.classes)

    @property
    def tier(self) -> Tier:
        """Return the max tier across the classes. Fails closed when empty."""
        if not self.classes:
            named = (
                ", ".join(getattr(t, "__name__", repr(t)) for t in self.contract_types)
                or "the given contract"
            )
            raise PrivacyContractError(
                f"{named} has no Annotated[T, DataClass] metadata; every "
                "surface-crossing return type must classify every field."
            )
        return max(data_class.tier for data_class in self.classes)

    @property
    def sensitivity(self) -> SensitivityLiteral:
        """Return the envelope ``sensitivity`` string for the derived tier."""
        return tier_sensitivity(self.tier)


def classify(*contract_types: Any) -> Classification:
    """Return the union of every data class ``contract_types`` declare.

    Several types union rather than compete: a coarse tool that can return
    any of three views declares the classes of all three, because the
    declaration is what an agent trusts before the payload arrives.
    """
    classes = frozenset[DataClass]().union(
        *(extract_data_classes(contract_type) for contract_type in contract_types)
    )
    return Classification(classes=classes, contract_types=tuple(contract_types))


def _declared_types(
    data: Any, contract_type: Any | Sequence[Any] | None
) -> tuple[Any, ...]:
    """Normalize the caller's declared contract into the tuple ``classify`` takes."""
    if contract_type is None:
        return (type(data),)
    if isinstance(contract_type, Sequence) and not isinstance(contract_type, str):
        return tuple(cast("Sequence[Any]", contract_type))
    return (contract_type,)


def build_classified_envelope[T](
    data: T,
    *,
    contract_type: Any | Sequence[Any] | None = None,
    total_count: int | None = None,
    returned_count: int | None = None,
    next_cursor: str | None = None,
    period: str | None = None,
    display_currency: str | None | Unset = UNSET,
    actions: list[str] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
    has_more: bool | None = None,
) -> ResponseEnvelope[T]:
    """Classify, redact, and wrap ``data`` in one envelope.

    Args:
        data: The payload. Redaction walks its runtime type.
        contract_type: The type (or types) the surface declares it may return.
            Defaults to ``type(data)``. Pass a sequence when one tool serves
            several view payloads — the classes union across all of them.
        total_count: Total matching records, when wider than this page.
        returned_count: Rows in this page; defaults to the payload's own shape.
        next_cursor: Opaque pagination token.
        period: Human-readable period covered.
        display_currency: Currency for the amounts, when the payload cannot say.
        actions: Contextual next-step hints.
        degraded: Whether the response is less than the caller asked for.
        degraded_reason: Which degradation applies.
        has_more: Overrides the count-derived ``summary.has_more``. Paginated
            surfaces pass ``next_cursor is not None`` to say "this is the last
            page" even where ``total_count`` exceeds what was returned.

    Returns:
        A redacted envelope whose ``summary.sensitivity`` and
        ``classes_returned`` both come from ``contract_type``.

    Raises:
        PrivacyContractError: ``contract_type`` declares no data classes. No
            envelope is better than one claiming ``low`` for an unknown payload.
    """
    classification = classify(*_declared_types(data, contract_type))
    envelope = cast(
        ResponseEnvelope[T],
        build_envelope(
            data=redact_typed(data, None),
            sensitivity=classification.sensitivity,
            total_count=total_count,
            returned_count=returned_count,
            next_cursor=next_cursor,
            period=period,
            display_currency=display_currency,
            actions=actions,
            degraded=degraded,
            degraded_reason=degraded_reason,
            classes_returned=classification.classes_returned,
        ),
    )
    if has_more is None:
        return envelope
    return replace(envelope, summary=replace(envelope.summary, has_more=has_more))
