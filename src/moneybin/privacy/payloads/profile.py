"""Typed payloads for the profile metadata MCP + CLI surfaces.

All fields are LOW-tier: a profile name and a home currency are operational
metadata, not financial data. No balance, amount, or account identifier
appears here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class ProfilePayload:
    """Result of the ``profile`` read.

    ``home_currency`` is ``None`` when the user has not chosen one. That is a
    real answer — consumers must not substitute ``'USD'``, which would
    mislabel a foreign-currency profile's money.

    Carries no ``database_exists`` flag: the tool reads the database to answer
    at all, so a missing one surfaces as a classified error envelope rather
    than a field. A field that can only ever be ``True`` invites an agent to
    branch on it and never take the other branch. The CLI's ``profile show``
    reports it because it can inspect a profile it has not opened.
    """

    name: Annotated[str, DataClass.RECORD_ID]
    home_currency: Annotated[str | None, DataClass.CURRENCY]


@dataclass(frozen=True, slots=True)
class ProfileSetPayload:
    """Effective profile settings after one ``profile_set`` mutation."""

    home_currency: Annotated[str | None, DataClass.CURRENCY]
    # The tool's description promises reversal via system_audit_undo; without
    # the id here the caller would have to search audit history and guess which
    # operation was theirs.
    operation_id: Annotated[str, DataClass.RECORD_ID]
