"""Cross-transport envelope projection for batch imports.

Lives beside `envelope.py` rather than in `services/import_service.py` because
the projection is a wire concern, not business logic: it reads a typed
`BatchImportResult` and decides the envelope's `status` and `error`. Keeping it
here leaves the service returning only typed business objects, so the service
stays consumable by any transport — and `BatchImportResult` is imported for
typing alone, never for behavior, so the dependency runs adapters → protocol →
service types and never back the other way.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moneybin import error_codes
from moneybin.errors import ErrorDetail
from moneybin.protocol.envelope import ResponseEnvelope

if TYPE_CHECKING:
    from moneybin.services.import_service import BatchImportResult


def mark_total_failure(
    envelope: ResponseEnvelope[Any], batch: BatchImportResult
) -> ResponseEnvelope[Any]:
    """Flip the envelope to status="error" when nothing in the batch imported.

    Partial success is genuine success — a batch with at least one import (or a
    file still awaiting confirmation) stays "ok". Only an all-failed batch is a
    failure, and reporting it as "ok" made a totally failed import look like it
    worked.

    Shared rather than per-adapter because `import_files` reaches the user
    through both the MCP tool and `moneybin import files --output json`. Parity
    is functional: "every file failed" must produce the same status and the same
    non-zero disposition on both surfaces, so the gate is one function both call
    rather than two implementations that can drift.

    `envelope.with_error` rather than assignment: `status` is derived from
    `error` in `__post_init__`, so it only updates when the envelope is rebuilt.
    `build_error_envelope` is deliberately not used here — it forces `data=[]`,
    which would discard the per-file `files` payload precisely when the caller
    needs it to see what broke.
    """
    failed = [r for r in batch.per_file if r.status == "failed"]
    if not failed or len(failed) != len(batch.per_file):
        return envelope

    # The batch's own message names the count instead of hoisting one file's
    # reason: with several distinct causes, the first file's message would
    # over-claim for all of them. Per-file message + code stay in data.files[].
    #
    # The code and hint are hoisted only on unanimity. Taking the first failed
    # file's would make the batch-level classification depend on argument
    # order — the same two files, swapped, would report a different top-level
    # `error.code`, and agents branch on that. Unanimity is the only condition
    # under which one code describes the whole batch.
    codes = {r.error_code for r in failed}
    shared_code = codes.pop() if len(codes) == 1 else None
    # The hint rides with the code: it is advice, so a wrong-but-plausible one
    # is worse than none. Every file's own hint stays in data.files[].
    hints: set[str | None] = {r.hint for r in failed} if shared_code else set()
    shared_hint = hints.pop() if len(hints) == 1 else None
    message = (
        f"Import failed for all {len(failed)} file(s); "
        "see data.files[] for each file's error, error_code, and hint."
    )
    return envelope.with_error(
        # An unclassified or non-unanimous failure has no single code of its
        # own. IMPORT_PARSE_ERROR is the honest fallback: the domain is right
        # and the claim is only that the files could not be processed —
        # data.files[].error carries whatever detail is actually known.
        ErrorDetail(
            message=message,
            code=shared_code or error_codes.IMPORT_PARSE_ERROR,
            hint=shared_hint,
        )
    )
