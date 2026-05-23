"""Per-call operation grouping for audit rows via contextvars.

One MCP tool call or CLI command is one *operation*: every ``app.audit_log``
row it writes shares a single ``operation_id`` so a later undo consumer can
reverse the whole call as a unit. The id is set once at the surface seam (MCP
tool dispatch, CLI command boundary) via :func:`operation` and read at the
``AuditService`` write boundary via :func:`current_operation_id` — repositories
need no changes because the context propagates implicitly.

A :class:`contextvars.ContextVar` (not a thread-local or a threaded parameter)
because the MCP server runs tool bodies across asyncio tasks and worker
threads: asyncio copies the context per task, and ``asyncio.to_thread`` copies
it into the worker thread, so a sync tool body's downstream audit writes see
the id set by the async dispatch wrapper.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

_OP_PREFIX = "op_"

_current_operation_id: ContextVar[str | None] = ContextVar(
    "moneybin_operation_id", default=None
)


def new_operation_id() -> str:
    """Mint a fresh operation id: ``op_<uuid4_hex>`` (full 32-char hex).

    Full UUID4 hex per ``.claude/rules/identifiers.md`` strategy 3 —
    ``app.audit_log`` is sized for >100K rows, so no truncation.
    """
    return f"{_OP_PREFIX}{uuid.uuid4().hex}"


def current_operation_id() -> str:
    """Return the active operation id, minting a fresh one if none is set.

    A mutation outside any :func:`operation` block (a bare repo call in a
    script or test) is its own one-row operation: it still gets a valid
    NOT-NULL id. This does NOT set the var, so two bare writes get distinct
    ids — only writes inside one :func:`operation` block share an id.
    """
    op = _current_operation_id.get()
    return op if op is not None else new_operation_id()


@contextmanager
def operation(operation_id: str | None = None) -> Generator[str, None, None]:
    """Bind one operation id for the duration of the block.

    Every audit row written inside shares ``operation_id``. Pass an explicit
    id for the self-heal form (``op_self_heal_<recipe>_<uuid4_hex>``) or to
    make an undo's own rows share a chosen id; otherwise a fresh
    ``op_<uuid4_hex>`` is minted.
    """
    op = operation_id if operation_id is not None else new_operation_id()
    token = _current_operation_id.set(op)
    try:
        yield op
    finally:
        _current_operation_id.reset(token)
