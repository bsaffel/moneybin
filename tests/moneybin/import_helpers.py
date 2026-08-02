"""Helpers for tests that import a file but aren't about account identity."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from moneybin.services.import_confirmation import ImportConfirmationRequiredError

if TYPE_CHECKING:
    from moneybin.services.import_service import ImportResult, ImportService


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
        if exc.outcome.reason != "account_confirmation":
            raise
        bindings: dict[str, str] = {}
        for proposal in exc.outcome.account_proposals:
            if proposal["candidates"]:
                raise
            bindings[proposal["source_account_key"]] = "new"
        return service.import_file(file_path, account_bindings=bindings, **kwargs)
