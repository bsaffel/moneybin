"""Regression guards for redact_typed container reconstruction."""

from dataclasses import dataclass
from typing import Annotated

from moneybin.privacy.redaction import redact_typed
from moneybin.privacy.taxonomy import DataClass


def test_redact_frozenset_reconstructs_type():
    """A frozenset[Annotated[..., CRITICAL]] round-trips to a masked frozenset.

    Pins the container-reconstruction branch in ``_redact`` (list stays a list;
    set/frozenset/tuple are rebuilt via ``type(value)(redacted)``). Without the
    guard, a frozenset field could silently degrade to a list or leave its
    CRITICAL elements unmasked.
    """

    @dataclass(frozen=True)
    class P:
        accts: frozenset[Annotated[str, DataClass.ACCOUNT_IDENTIFIER]]

    out = redact_typed(P(accts=frozenset({"123456789"})), consent=None)
    assert isinstance(out.accts, frozenset)
    assert out.accts == frozenset({"****6789"})
