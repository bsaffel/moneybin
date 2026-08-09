"""Helpers for tests that import a file but aren't about account identity."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from moneybin.services.import_confirmation import ImportConfirmationRequiredError

if TYPE_CHECKING:
    from moneybin.services.import_service import (
        BridgeApplyResult,
        ImportResult,
        ImportService,
    )


def _first_contact_bindings(exc: ImportConfirmationRequiredError) -> dict[str, str]:
    """Bind every proposal to "new", or re-raise if identity is really in play.

    Shared by both entry points below so one rule decides what a helper may
    answer on a test's behalf. A proposal carrying merge candidates is never
    answerable here — see ``import_answering_gate``.

    Only reachable now for a source that stated no identity: a stated identity
    with no candidates mints without gating, so most fixtures never enter this
    path at all.
    """
    if exc.outcome.reason != "account_confirmation":
        raise exc
    bindings: dict[str, str] = {}
    for proposal in exc.outcome.account_proposals:
        if proposal["candidates"]:
            raise exc
        bindings[proposal["source_account_key"]] = "new"
    return bindings


def import_answering_gate(
    service: ImportService, file_path: Path, /, **kwargs: Any
) -> ImportResult:
    """Import ``file_path``, answering a first-contact account gate with "new".

    Every channel now stops before load and asks who a never-before-seen account
    belongs to. For the many tests where that account is incidental — OFX batch
    lifecycle, PDF bridge apply, format persistence — the fixture would
    otherwise have to restate a source key it doesn't care about, and would
    break again whenever key derivation changes.

    This answers the gate the way a user would: read the proposals, bind each to
    "new", re-import. It does NOT bypass the gate, and re-raises rather than
    answer when the proposal carries merge candidates — candidates mean account
    identity is genuinely in play, and a test in that position must state its
    own answer instead of letting a helper pick one silently
    (``.claude/rules/testing.md``, "No Shortcuts: Exercise the Real Mechanism").

    Re-calling ``import_file`` is safe precisely because the gate raises before
    any batch opens or any row lands; a residue-leaving gate would break every
    caller of this helper at once.
    """
    try:
        return service.import_file(file_path, **kwargs)
    except ImportConfirmationRequiredError as exc:
        return service.import_file(
            file_path, account_bindings=_first_contact_bindings(exc), **kwargs
        )


def apply_bridge_answering_gate(
    service: ImportService,
    file_path: Path,
    bridge_response: dict[str, Any],
    /,
    **kwargs: Any,
) -> BridgeApplyResult:
    """``apply_pdf_bridge_response`` twin of :func:`import_answering_gate`.

    The bridge raises the same account gate as every other channel, so a test
    about bridge round-tripping needs the same way past an account it never
    cared about.
    """
    try:
        return service.apply_pdf_bridge_response(file_path, bridge_response, **kwargs)
    except ImportConfirmationRequiredError as exc:
        return service.apply_pdf_bridge_response(
            file_path,
            bridge_response,
            account_bindings=_first_contact_bindings(exc),
            **kwargs,
        )
