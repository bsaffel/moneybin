"""MCP tool decorator: sensitivity logging, timeout guard, error classification, envelope guard.

Every decorated tool is exposed as an async coroutine — FastMCP awaits it,
and sync tool bodies run via ``asyncio.to_thread``. On timeout the
decorator drops the singleton DuckDB connection so the next call reopens
a fresh one, releasing any held write lock. Classified domain exceptions
become error envelopes here; anything else propagates to the server's
``mask_error_details`` boundary.

Known limitation — sync tool body continues after timeout: ``asyncio.timeout()``
cancels the awaited task, but the OS thread running the sync body keeps
going until it naturally returns. A tool that mutates filesystem or DB state
(e.g., ``import_inbox_sync``) may finish that work in the background after
the client has already received a ``timed_out`` envelope. Clients that
retry can produce duplicate or conflicting writes. The contract here is
"release the lock and respond within the cap," not "guarantee the
underlying work was undone." Tools doing non-idempotent writes that risk
exceeding the cap must be redesigned (e.g., decomposed into smaller per-
unit calls) — see ``docs/specs/mcp-tool-timeouts.md`` Out of Scope.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Callable
from typing import Any, Literal, cast

from moneybin.database import interrupt_and_reset_database
from moneybin.errors import UserError, classify_user_error
from moneybin.mcp.privacy import Sensitivity, log_tool_call
from moneybin.protocol.envelope import ResponseEnvelope, build_error_envelope

logger = logging.getLogger(__name__)


class _UnsetType:
    """Sentinel for distinguishing 'inherit settings default' from 'explicit None'."""

    _singleton: _UnsetType | None = None

    def __new__(cls) -> _UnsetType:
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
        return cls._singleton

    def __repr__(self) -> str:
        return "_UNSET"


_UNSET = _UnsetType()


def _get_timeout_seconds() -> float:
    """Read the configured timeout. Indirected for test monkeypatching."""
    from moneybin.config import get_settings

    return get_settings().mcp.tool_timeout_seconds


def _get_max_items() -> int | None:
    """Read the configured collection cap. Indirected for test monkeypatching."""
    from moneybin.config import get_settings

    return get_settings().mcp.max_items


def _check_collection_caps(
    fn_name: str,
    list_params: list[str],
    bound_args: dict[str, Any],
    cap: int | None,
) -> ResponseEnvelope | None:
    """Return an error envelope if any list param exceeds ``cap``, else None."""
    if cap is None:
        return None
    for param_name in list_params:
        value = bound_args.get(param_name)
        if value is None:
            continue
        try:
            length = len(value)
        except TypeError:
            continue
        if length > cap:
            err = UserError(
                f"{fn_name}: parameter '{param_name}' has {length} items; max is {cap}",
                code="too_many_items",
                details={
                    "limit": cap,
                    "received": length,
                    "parameter": param_name,
                },
            )
            return build_error_envelope(error=err, sensitivity="low")
    return None


def _find_list_params(fn: Callable[..., Any]) -> list[str]:
    """Return parameter names whose annotation is a list/Sequence/Collection.

    Strings are excluded — ``str`` is technically a ``Sequence[str]``, but
    list-cap semantics don't apply to it. ``X | None`` (Optional) annotations
    are unwrapped before inspection.
    """
    import typing
    from collections.abc import Collection, Sequence

    sig = inspect.signature(fn)
    try:
        type_hints = typing.get_type_hints(fn)
    except Exception:  # noqa: BLE001 — eval failure shouldn't block decoration
        return []
    list_params: list[str] = []

    for param_name in sig.parameters:
        annotation: Any = type_hints.get(param_name)
        if annotation is None:
            continue
        # Unwrap Optional[X] / X | None.
        origin = typing.get_origin(annotation)
        union_origin = type(int | str)
        if origin is typing.Union or origin is union_origin:
            args = [a for a in typing.get_args(annotation) if a is not type(None)]
            if len(args) == 1:
                annotation = args[0]
                origin = typing.get_origin(annotation)
        # str/bytes are explicitly excluded.
        if annotation is str or annotation is bytes:
            continue
        # Direct origin match (covers list[X], Sequence[X], Collection[X], tuple[X, ...]).
        if origin in (list, tuple, Sequence, Collection):
            list_params.append(param_name)
            continue
        # Bare list/tuple type without subscript.
        if (
            isinstance(annotation, type)
            and annotation
            not in (
                str,
                bytes,
            )
            and issubclass(annotation, (list, tuple))
        ):
            list_params.append(param_name)
            continue
        # Generic alias with a non-builtin origin (e.g. custom Sequence subclass).
        if origin is not None and isinstance(origin, type):
            origin_type = cast(type, origin)
            try:
                if origin_type not in (str, bytes) and issubclass(
                    origin_type, (Sequence, Collection)
                ):
                    list_params.append(param_name)
            except TypeError:
                pass

    return list_params


def _check_envelope(fn_name: str, result: Any) -> ResponseEnvelope:
    if not isinstance(result, ResponseEnvelope):
        # mask_error_details=True at the server boundary swallows the TypeError
        # into a generic ToolError, so log the contract violation first.
        msg = f"{fn_name} returned {type(result).__name__}, expected ResponseEnvelope"
        logger.error(msg)
        raise TypeError(msg)
    return result


def _classify_or_raise(fn_name: str, exc: Exception) -> ResponseEnvelope:
    """Convert a classified domain exception to an error envelope, else re-raise."""
    classified = classify_user_error(exc)
    if classified is None:
        raise exc
    logger.error(f"Tool {fn_name} raised {type(exc).__name__}: {classified.code}")
    return build_error_envelope(error=classified, sensitivity="low")


def _build_timeout_envelope(
    fn_name: str, elapsed_s: float, timeout_s: float
) -> ResponseEnvelope:
    err = UserError(
        f"Tool {fn_name} exceeded {timeout_s:.1f}s cap",
        code="timed_out",
        details={
            "tool": fn_name,
            "elapsed_s": round(elapsed_s, 3),
            "timeout_s": timeout_s,
        },
    )
    return build_error_envelope(error=err, sensitivity="low")


def mcp_tool(
    *,
    sensitivity: Literal["low", "medium", "high"],
    domain: str | None = None,
    read_only: bool = True,
    destructive: bool = False,
    idempotent: bool = True,
    open_world: bool = False,
    max_items: int | None | _UnsetType = _UNSET,
) -> Callable[..., Any]:
    """Mark a function as an MCP tool with a sensitivity tier and optional domain.

    Args:
        sensitivity: Sensitivity tier (low/medium/high).
        domain: Optional progressive-disclosure tag.
        read_only: MCP readOnlyHint — default True (most MoneyBin tools are queries).
        destructive: MCP destructiveHint — irreversible state change.
        idempotent: MCP idempotentHint — safe to retry without side effects.
        open_world: MCP openWorldHint — defaults to False (closed-world).
        max_items: Per-tool override for ``MCPConfig.max_items``. ``None``
            disables the cap. Sentinel ``_UNSET`` inherits from settings.

    Tools with a ``domain`` start hidden; ``moneybin_discover`` enables them
    per-session via FastMCP tag visibility. Every tool is wrapped in a
    wall-clock timeout guard — see module docstring.
    """
    tier = Sensitivity(sensitivity)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        is_coro = inspect.iscoroutinefunction(fn)
        if inspect.isasyncgenfunction(fn):
            raise TypeError(
                f"{fn.__name__} is an async generator — MCP tools must be regular coroutines or sync functions"
            )
        if inspect.isgeneratorfunction(fn):
            raise TypeError(
                f"{fn.__name__} is a sync generator — MCP tools must return ResponseEnvelope directly"
            )

        list_params = _find_list_params(fn)
        # Cache the signature at decoration time. inspect.signature is not cheap,
        # and tools with list_params would otherwise rebuild it on every call.
        cached_sig = inspect.signature(fn) if list_params else None

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
            log_tool_call(fn.__name__, tier)
            # Resolve cap: explicit per-tool override wins; otherwise inherit settings.
            cap_attr = cast(
                "int | None | _UnsetType",
                wrapper._mcp_max_items,  # type: ignore[attr-defined]
            )
            if isinstance(cap_attr, _UnsetType):
                cap: int | None = _get_max_items()
            else:
                cap = cap_attr
            if list_params and cached_sig is not None:
                bound: dict[str, Any]
                try:
                    bound = dict(cached_sig.bind_partial(*args, **kwargs).arguments)
                except TypeError:
                    bound = {}
                cap_error = _check_collection_caps(fn.__name__, list_params, bound, cap)
                if cap_error is not None:
                    return cap_error
            timeout_s = _get_timeout_seconds()
            started = time.monotonic()
            # asyncio.timeout()'s .expired() lets us distinguish a cap-fired
            # TimeoutError from one the tool body raised itself (e.g., a
            # downstream HTTP timeout). Without this, both surface as
            # TimeoutError and would be misclassified as cap-fired, causing
            # spurious DB resets and misleading "timed_out" envelopes.
            cm = asyncio.timeout(timeout_s)
            try:
                async with cm:
                    if is_coro:
                        result = await fn(*args, **kwargs)
                    else:
                        result = await asyncio.to_thread(fn, *args, **kwargs)
            except TimeoutError as exc:
                if not cm.expired():
                    return _classify_or_raise(fn.__name__, exc)
                elapsed = time.monotonic() - started
                logger.warning(
                    f"Tool {fn.__name__} timed out after {elapsed:.2f}s "
                    f"(cap {timeout_s:.1f}s); interrupting DB and resetting connection"
                )
                try:
                    interrupt_and_reset_database()
                except Exception as cleanup_exc:  # noqa: BLE001 — cleanup must not raise
                    logger.error(
                        f"interrupt_and_reset_database failed during {fn.__name__} "
                        f"timeout cleanup: {type(cleanup_exc).__name__}"
                    )
                # Brief grace period: the surviving sync-tool thread needs a
                # tick to see the closed DuckDB connection, raise, and unwind
                # any per-tool resources (e.g. InboxService.acquire_lock's
                # flock). Without this, an immediate retry hits inbox_busy
                # while the previous thread is still in its finally block.
                await asyncio.sleep(0.5)
                return _build_timeout_envelope(fn.__name__, elapsed, timeout_s)
            except Exception as exc:
                return _classify_or_raise(fn.__name__, exc)
            return _check_envelope(fn.__name__, result)

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        wrapper._mcp_domain = domain  # type: ignore[attr-defined]
        wrapper._mcp_read_only = read_only  # type: ignore[attr-defined]
        wrapper._mcp_destructive = destructive  # type: ignore[attr-defined]
        wrapper._mcp_idempotent = idempotent  # type: ignore[attr-defined]
        wrapper._mcp_open_world = open_world  # type: ignore[attr-defined]
        wrapper._mcp_max_items = max_items  # type: ignore[attr-defined]
        wrapper._mcp_list_params = list_params  # type: ignore[attr-defined]
        return wrapper

    return decorator
