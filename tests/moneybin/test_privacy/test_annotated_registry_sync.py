"""Cross-layer drift detection: payload Annotated DataClass matches registry.

Enumerates every dataclass exported from `moneybin.privacy.payloads.*` and
asserts each field whose name matches a registry column carries the same
``DataClass`` the registry assigns to that column. Aggregate / shaped fields
(e.g. ``total``, ``count_archived``) have no registry counterpart — they
must still be classified, but the cross-check is skipped.

The test is parametrized per payload class so failures pinpoint which
payload drifted, and any Phase-5 classification mistake (e.g. ``amount``
typed as ``BALANCE`` when the registry says ``TXN_AMOUNT``) surfaces here.
"""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import fields, is_dataclass
from typing import Annotated, get_args, get_origin, get_type_hints

import pytest

from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass


def _all_payload_classes() -> list[type]:
    """Discover every dataclass exported from ``moneybin.privacy.payloads.*``."""
    import moneybin.privacy.payloads as pkg

    out: list[type] = []
    for mod_info in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"moneybin.privacy.payloads.{mod_info.name}")
        for name in dir(mod):
            obj = getattr(mod, name)
            if (
                isinstance(obj, type)
                and is_dataclass(obj)
                and obj.__module__ == mod.__name__
            ):
                out.append(obj)
    return out


def _all_classified_column_names() -> dict[str, set[DataClass]]:
    """Flatten the registry: ``column_name`` → set of ``DataClass`` it carries.

    The same column name appears across multiple ``(schema, table)`` entries
    (e.g. ``account_id`` is RECORD_ID in every table that has it — spec D6).
    Aggregating into a set lets the cross-check accept any of the registry's
    classifications for a given column name.
    """
    out: dict[str, set[DataClass]] = {}
    for (_schema, _table), columns in CLASSIFICATION.items():
        for col, dc in columns.items():
            out.setdefault(col, set()).add(dc)
    return out


def _find_annotated_meta(hint: object) -> DataClass | None:
    """Walk a type hint and return the first DataClass in ``Annotated`` metadata."""
    origin = get_origin(hint)
    if origin is Annotated:
        for meta in get_args(hint)[1:]:
            if isinstance(meta, DataClass):
                return meta
        return _find_annotated_meta(get_args(hint)[0])
    if origin is not None:
        for arg in get_args(hint):
            found = _find_annotated_meta(arg)
            if found is not None:
                return found
    return None


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload_cls", _all_payload_classes(), ids=lambda c: c.__name__
)
def test_payload_fields_match_registry(payload_cls: type) -> None:
    """Every payload field whose name matches a registry column uses its class.

    A payload field named ``account_id`` MUST use ``DataClass.RECORD_ID``
    because every registry entry for ``account_id`` is RECORD_ID (spec D6:
    the opaque minted canonical surrogate is not PII). Aggregate payloads
    with fields like ``total`` or ``count_archived`` (no registry counterpart)
    are exempt — they must still carry a ``DataClass`` annotation but the
    specific value cannot be cross-checked.
    """
    registry = _all_classified_column_names()
    hints = get_type_hints(payload_cls, include_extras=True)
    for f in fields(payload_cls):
        hint = hints.get(f.name)
        if hint is None:
            continue
        annotated_meta = _find_annotated_meta(hint)
        if annotated_meta is None:
            # Container field with no top-level Annotated (e.g. list[Row]) —
            # the nested Row class is tested in its own parametrize iteration.
            continue
        if f.name not in registry:
            # No registry column for this field name — aggregate / shaped.
            # Already classified, no cross-check possible.
            continue
        expected = registry[f.name]
        assert annotated_meta in expected, (
            f"{payload_cls.__name__}.{f.name}: declared as "
            f"DataClass.{annotated_meta.name}, but the registry says this "
            f"column should be one of "
            f"{sorted(c.name for c in expected)}. Update the payload's "
            "Annotated to match the registry, or update the registry if the "
            "field semantics genuinely changed."
        )


@pytest.mark.unit
def test_pending_txn_age_days_is_a_date_on_both_sides() -> None:
    """``age_days`` is TXN_DATE in the payload AND in the registry — absolutely.

    ``test_payload_fields_match_registry`` only proves the two AGREE, so a
    change that moves both back to AGGREGATE together passes it unchanged. This
    pins the value itself on both sides.

    Nothing else would notice the regression: ``age_days`` shares
    ``PendingTxnRow`` with ``amount`` (TXN_AMOUNT, HIGH), which dominates the
    derived envelope tier, so ``CatPendingPayload`` stays Tier.HIGH whatever
    this field says — while the field itself would have quietly stopped being
    masked at the tiers between.

    Why TXN_DATE and not AGGREGATE: ``age_days`` is computed against
    CURRENT_DATE, which is public, so it is bijective with the transaction date
    (``txn_date = CURRENT_DATE - age_days``). It is a date wearing a count's
    clothing, not an aggregate.
    """
    from moneybin.privacy.payloads.categorize import PendingTxnRow

    hints = get_type_hints(PendingTxnRow, include_extras=True)
    assert _find_annotated_meta(hints["age_days"]) is DataClass.TXN_DATE
    assert (
        CLASSIFICATION[("core", "uncategorized_queue")]["age_days"]
        is DataClass.TXN_DATE
    )
