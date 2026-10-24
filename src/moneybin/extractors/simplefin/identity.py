"""Full, length-framed identities independent of mutable financial content."""

from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
from typing import Literal
from uuid import UUID


def _digest(*values: str) -> str:
    framed = b"".join(
        len(value.encode()).to_bytes(8, "big") + value.encode() for value in values
    )
    return sha256(framed).hexdigest()


def scope_identity_strength(
    protocol_version: str, native_scope: Mapping[str, str]
) -> Literal["strong", "weak"]:
    """A v1 name/domain fallback is insufficient evidence for automatic merging."""
    return (
        "strong"
        if protocol_version == "2.0.0-draft" or native_scope.get("id")
        else "weak"
    )


def source_origin(
    connection_id: UUID, protocol_version: str, native_scope: Mapping[str, str]
) -> str:
    """Scope native identity by broker grant and pinned upstream protocol."""
    if protocol_version == "2.0.0-draft":
        parts = (native_scope.get("conn_id", ""),)
    elif protocol_version == "1.0.7":
        label = next(
            (key for key in ("id", "domain", "name") if native_scope.get(key)), None
        )
        if label is None:
            raise ValueError("Invalid SimpleFIN scope identity")
        parts = (native_scope.get("sfin-url", ""), label, native_scope[label])
    else:
        raise ValueError("Unsupported SimpleFIN protocol")
    if any(not part.strip() for part in parts):
        raise ValueError("Invalid SimpleFIN scope identity")
    return "simplefin_" + _digest(str(connection_id), protocol_version, *parts)


def transaction_reference(
    source_origin: str, account_id: str, transaction_id: str
) -> str:
    """Identify a native event without including its revisable contents."""
    if any(not value.strip() for value in (source_origin, account_id, transaction_id)):
        raise ValueError("Invalid SimpleFIN transaction identity")
    return "simplefin_" + _digest(source_origin, account_id, transaction_id)
