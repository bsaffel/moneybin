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
import dataclasses
import functools
import inspect
import logging
import time
import typing
from collections.abc import Callable
from typing import Any, cast

from moneybin import error_codes
from moneybin.database import (  # noqa: PLC2701 — private import for per-call tracking
    _write_conn_thread_local,  # pyright: ignore[reportPrivateUsage]
    interrupt_and_reset_database,
)
from moneybin.errors import UserError, classify_user_error
from moneybin.mcp.privacy import Sensitivity, log_tool_call, tier_to_sensitivity
from moneybin.privacy.introspection import (
    PrivacyContractError,
    derive_tier,
    extract_data_classes,
)
from moneybin.privacy.log import build_tool_call_event, write_privacy_event
from moneybin.privacy.redaction import has_active_transform, redact_typed
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
) -> ResponseEnvelope[Any] | None:
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
                code=error_codes.INFRA_TOO_MANY_ITEMS,
                details={
                    "limit": cap,
                    "received": length,
                    "parameter": param_name,
                },
            )
            return build_error_envelope(error=err, sensitivity="low")
    return None


def _find_list_params(fn: Callable[..., Any]) -> list[str]:
    """Return parameter names whose annotation is a list/tuple/Sequence.

    Restricted to ``Sequence`` so that ``dict``/``set``/``frozenset`` —
    which are ``Collection`` but not ``Sequence`` — are not treated as
    list-capped. ``len()`` on a dict counts keys, not items, which would
    surface confusing ``too_many_items`` errors. ``str``/``bytes`` are
    also excluded (technically ``Sequence`` but list-cap semantics don't
    apply). ``X | None`` (Optional) annotations are unwrapped first.
    """
    import typing
    from collections.abc import Sequence
    from types import UnionType

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
        if origin is typing.Union or origin is UnionType:
            args = [a for a in typing.get_args(annotation) if a is not type(None)]
            if len(args) == 1:
                annotation = args[0]
                origin = typing.get_origin(annotation)
        # str/bytes are explicitly excluded.
        if annotation is str or annotation is bytes:
            continue
        # Direct origin match (covers list[X], Sequence[X], tuple[X, ...]).
        if origin in (list, tuple, Sequence):
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
                    origin_type, Sequence
                ):
                    list_params.append(param_name)
            except TypeError:
                pass

    return list_params


def _check_envelope(fn_name: str, result: Any) -> ResponseEnvelope[Any]:
    if not isinstance(result, ResponseEnvelope):
        # mask_error_details=True at the server boundary swallows the TypeError
        # into a generic ToolError, so log the contract violation first.
        msg = f"{fn_name} returned {type(result).__name__}, expected ResponseEnvelope"
        logger.error(msg)
        raise TypeError(msg)
    return result


def _classify_or_raise(fn_name: str, exc: Exception) -> ResponseEnvelope[Any]:
    """Convert a classified domain exception to an error envelope, else re-raise."""
    classified = classify_user_error(exc)
    if classified is None:
        raise exc
    logger.error(f"Tool {fn_name} raised {type(exc).__name__}: {classified.code}")
    return build_error_envelope(error=classified, sensitivity="low")


def _build_unclassified_failure_envelope(fn_name: str) -> ResponseEnvelope[Any]:
    """Synthetic envelope for an audit emit on unclassified exception paths.

    Used only as the row_count source before the original exception
    propagates — never returned to a caller. The exception still re-raises
    so the server boundary's ``mask_error_details`` handler runs.
    """
    err = UserError(
        f"Tool {fn_name} raised unclassified exception",
        code="unclassified_error",
    )
    return build_error_envelope(error=err, sensitivity="low")


def _build_timeout_envelope(
    fn_name: str, elapsed_s: float, timeout_s: float
) -> ResponseEnvelope[Any]:
    err = UserError(
        f"Tool {fn_name} exceeded {timeout_s:.1f}s cap",
        code=error_codes.INFRA_TIMED_OUT,
        details={
            "tool": fn_name,
            "elapsed_s": round(elapsed_s, 3),
            "timeout_s": timeout_s,
        },
    )
    return build_error_envelope(error=err, sensitivity="low")


def _envelope_row_count(envelope: ResponseEnvelope[Any]) -> int:
    """Best-effort row count for the privacy log."""
    return envelope.summary.returned_count


def mcp_tool(
    *,
    domain: str | None = None,
    read_only: bool = True,
    destructive: bool = False,
    idempotent: bool = True,
    open_world: bool = False,
    max_items: int | None | _UnsetType = _UNSET,
    dynamic_classification: bool = False,
    timeout_seconds: float | _UnsetType = _UNSET,
) -> Callable[..., Any]:
    """Mark a function as an MCP tool. Sensitivity is derived from the return type.

    The return type must be ``ResponseEnvelope[T]`` where ``T`` is a
    typed dataclass / Pydantic model / TypedDict whose fields carry
    ``Annotated[..., DataClass.X]`` metadata. Registration fails with
    ``PrivacyContractError`` if the return type lacks classified
    metadata.

    Args:
        domain: Optional namespace tag stored as a FastMCP tag. Dormant
            metadata today — client-driven progressive disclosure was retired
            2026-05-17 (see docs/specs/mcp-architecture.md §3). Preserved for a
            possible future first-party client that does its own schema
            injection.
        read_only: MCP readOnlyHint — default True (most MoneyBin tools are queries).
        destructive: MCP destructiveHint — irreversible state change.
        idempotent: MCP idempotentHint — safe to retry without side effects.
        open_world: MCP openWorldHint — defaults to False (closed-world).
        max_items: Per-tool override for ``MCPConfig.max_items``. ``None``
            disables the cap. Sentinel ``_UNSET`` inherits from settings.
        dynamic_classification: Per-call classification mode for tools whose
            return shape is determined at call time (e.g. dynamic SQL). When
            True, the tool is responsible for setting ``summary.sensitivity``
            and ``classes_returned`` via ``build_envelope``; the decorator
            skips static sensitivity stamping and ``redact_typed`` (the tool
            already redacted via ``redact_records``), and logs the per-call
            values from the envelope instead of static closure constants.
            Use ``sql_query``-style tools that invoke sqlglot lineage and
            ``redact_records`` per call. Static tools must NOT use this flag.
        timeout_seconds: Per-tool override for ``MCPConfig.tool_timeout_seconds``.
            Sentinel ``_UNSET`` inherits from settings. Use this for tools whose
            natural runtime exceeds the default cap (e.g. interactive OAuth
            flows that wait for a user browser click).

    Every tool is wrapped with:
    1. Wall-clock timeout guard (unchanged from prior behavior)
    2. Privacy redaction via ``redact_typed`` (PR 2: CRITICAL masks, unless dynamic_classification)
    3. ``privacy.log.jsonl`` event write per call
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        # Derive sensitivity at registration time from the return type annotation.
        # Raises PrivacyContractError if the return type isn't ResponseEnvelope[T]
        # with classified T — unless dynamic_classification=True is set.
        if dynamic_classification:
            # Per-call classification: can't classify statically. Use HIGH as a
            # placeholder; the actual per-call sensitivity is set by the tool
            # itself and preserved by the decorator (not stamped over).
            sensitivity = Sensitivity.HIGH
            classes_for_log: list[str] = ["unclassified"]
            payload_type_arg: Any = None
            has_critical = False
        else:
            # get_type_hints resolves string annotations (from __future__ import
            # annotations). A payload class defined below the @mcp_tool fn in the
            # same module, or imported lazily/conditionally, raises NameError at
            # decoration time. Re-raise as PrivacyContractError so the failure
            # names the contract instead of surfacing a bare NameError — the same
            # guarding _find_list_params applies to this call.
            try:
                return_hint = typing.get_type_hints(fn).get("return")
            except (NameError, TypeError) as exc:
                raise PrivacyContractError(
                    f"{fn.__name__}: could not resolve return annotation ({exc}); "
                    "ensure the payload type is imported at decoration time"
                ) from exc
            if return_hint is None:
                raise PrivacyContractError(
                    f"{fn.__name__} has no return annotation; "
                    "every @mcp_tool must declare -> ResponseEnvelope[T]"
                )
            # Unwrap ResponseEnvelope[T] → T. Require the origin to be
            # ResponseEnvelope specifically, not merely "some generic" — a
            # `list[Payload]` or `dict[str, Payload]` annotation would otherwise
            # pass and derive sensitivity from the wrong type argument, bypassing
            # the envelope contract.
            if typing.get_origin(return_hint) is not ResponseEnvelope:
                raise PrivacyContractError(
                    f"{fn.__name__} return type must be ResponseEnvelope[T], "
                    f"got {return_hint!r}"
                )
            payload_type_args = typing.get_args(return_hint)
            if not payload_type_args:
                raise PrivacyContractError(
                    f"{fn.__name__} return type ResponseEnvelope must be parameterized "
                    "(e.g. ResponseEnvelope[AccountListPayload])"
                )
            payload_type_arg = payload_type_args[0]
            # derive_tier raises PrivacyContractError naming the *payload type*;
            # re-raise naming the tool too, so a registration-time failure points
            # at which @mcp_tool needs the fix, not just the orphaned payload.
            try:
                tier = derive_tier(payload_type_arg)
            except PrivacyContractError as exc:
                raise PrivacyContractError(f"{fn.__name__}: {exc}") from exc
            sensitivity = tier_to_sensitivity(tier)
            classes_for_log = [
                c.value for c in sorted(extract_data_classes(payload_type_arg))
            ]
            # Skip the redact_typed walk for payloads that would pass through
            # unchanged. Derived from _TRANSFORMS (not `tier == CRITICAL`) so
            # the gate stays correct automatically when PR3 wires real
            # HIGH/MEDIUM transforms: the moment a DataClass stops being
            # _passthrough, every payload carrying it starts being walked. No
            # CRITICAL-only trap. Today this is exactly the CRITICAL set.
            has_critical = has_active_transform(payload_type_arg)

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

        async def _emit_privacy_event(env: ResponseEnvelope[Any]) -> None:
            """Write the per-call privacy.log event off the event loop.

            Called on every return path (success, cap violation, timeout,
            classified error) so the audit trail covers blocked / failed
            invocations, not just successes. The write (lock + file I/O) runs
            via asyncio.to_thread so a slow/NFS log mount can't stall the event
            loop and serialize concurrent tool calls.

            For dynamic_classification tools, reads sensitivity and classes from
            the envelope itself (set by the tool per call); for static tools,
            uses the registration-time closure values.
            """
            if dynamic_classification:
                ev_sensitivity = env.summary.sensitivity
                ev_classes = env.classes_returned or ["unclassified"]
            else:
                ev_sensitivity = sensitivity.value
                ev_classes = classes_for_log
            event = build_tool_call_event(
                actor=f"mcp.{fn.__name__}",
                sensitivity=ev_sensitivity,
                classes_returned=ev_classes,
                # Generic envelope type erased; _envelope_row_count handles Any.
                row_count=_envelope_row_count(env),  # pyright: ignore[reportUnknownArgumentType]
            )
            await asyncio.to_thread(write_privacy_event, event)

        def _stamp_sensitivity(env: ResponseEnvelope[Any]) -> ResponseEnvelope[Any]:
            """Override summary.sensitivity with the decorator-derived tier.

            Error envelopes from build_error_envelope hardcode "low"; without
            this a CRITICAL-tier tool (e.g. accounts_get) that raises would
            report summary.sensitivity="low" in its error response and audit
            row, understating the tier. Applied on every error return path.
            """
            if sensitivity.value == env.summary.sensitivity:
                return env
            updated = dataclasses.replace(
                env.summary,
                sensitivity=sensitivity.value,  # pyright: ignore[reportArgumentType]
            )
            return dataclasses.replace(env, summary=updated)  # pyright: ignore[reportUnknownArgumentType]

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope[Any]:
            log_tool_call(fn.__name__, sensitivity)
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
                    cap_error = _stamp_sensitivity(cap_error)
                    await _emit_privacy_event(cap_error)
                    return cap_error
            timeout_attr = cast(
                "float | _UnsetType",
                wrapper._mcp_timeout_seconds,  # type: ignore[attr-defined]
            )
            if isinstance(timeout_attr, _UnsetType):
                timeout_s = _get_timeout_seconds()
            else:
                timeout_s = timeout_attr
            started = time.monotonic()
            # Per-call holder: the tool's thread stores the write connection
            # here via _write_conn_thread_local so the timeout handler can
            # interrupt *this* call's connection specifically rather than
            # whatever is in the process-global slot (which might belong to a
            # different concurrent tool call if this tool closed its connection
            # mid-function before timing out).
            _conn_for_this_call: list[Any] = [None]
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

                        def _fn_with_conn_tracking(*a: Any, **kw: Any) -> Any:
                            _write_conn_thread_local.conn_holder = _conn_for_this_call
                            try:
                                return fn(*a, **kw)
                            finally:
                                _write_conn_thread_local.conn_holder = None

                        result = await asyncio.to_thread(
                            _fn_with_conn_tracking, *args, **kwargs
                        )
            except TimeoutError as exc:
                if not cm.expired():
                    try:
                        err_env = _classify_or_raise(fn.__name__, exc)
                    except BaseException:
                        # Unclassified — emit a crash audit row, then propagate.
                        await _emit_privacy_event(
                            _build_unclassified_failure_envelope(fn.__name__)
                        )
                        raise
                    err_env = _stamp_sensitivity(err_env)
                    await _emit_privacy_event(err_env)
                    return err_env
                elapsed = time.monotonic() - started
                logger.warning(
                    f"Tool {fn.__name__} timed out after {elapsed:.2f}s "
                    f"(cap {timeout_s:.1f}s); interrupting DB and resetting connection"
                )
                try:
                    interrupt_and_reset_database(_conn_for_this_call[0])
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
                try:
                    await asyncio.sleep(0.5)
                except BaseException:
                    # CancelledError (a BaseException, not Exception) raised here
                    # would escape both the TimeoutError and Exception handlers
                    # with no audit row. Emit the crash event, then propagate.
                    await _emit_privacy_event(
                        _build_unclassified_failure_envelope(fn.__name__)
                    )
                    raise
                timeout_env = _stamp_sensitivity(
                    _build_timeout_envelope(fn.__name__, elapsed, timeout_s)
                )
                await _emit_privacy_event(timeout_env)
                return timeout_env
            except Exception as exc:
                try:
                    err_env = _classify_or_raise(fn.__name__, exc)
                except BaseException:
                    # Unclassified — emit a crash audit row, then propagate
                    # so the server boundary's mask_error_details runs.
                    await _emit_privacy_event(
                        _build_unclassified_failure_envelope(fn.__name__)
                    )
                    raise
                err_env = _stamp_sensitivity(err_env)
                await _emit_privacy_event(err_env)
                return err_env
            except asyncio.CancelledError:
                # External cancellation (client disconnect / server shutdown)
                # raises CancelledError — a BaseException that bypasses the
                # TimeoutError and Exception handlers above. Without this branch
                # the aborted invocation never reaches _emit_privacy_event and is
                # missing from privacy.log.jsonl, leaving a hole in the tool-call
                # audit surface. shield() so the audit write completes even under
                # multiple concurrent cancel() calls (rapid reconnect / shutdown),
                # where a bare await inside the handler would be re-cancelled
                # before the write lands. Then re-raise — cancellation must never
                # be swallowed.
                try:
                    await asyncio.shield(
                        _emit_privacy_event(
                            _build_unclassified_failure_envelope(fn.__name__)
                        )
                    )
                except asyncio.CancelledError:
                    # The shielded emit keeps running in the background; this
                    # re-cancel only interrupts our wait, not the write.
                    pass
                raise
            # _check_envelope raises TypeError when a tool returns a non-
            # ResponseEnvelope. That's an envelope-contract violation that
            # belongs in the audit trail like any other crash path; emit
            # before propagating to the server's mask_error_details boundary.
            try:
                envelope = _check_envelope(fn.__name__, result)
            except BaseException:
                await _emit_privacy_event(
                    _build_unclassified_failure_envelope(fn.__name__)
                )
                raise
            # Stamp summary.sensitivity with the decorator-derived tier so the
            # envelope reflects the statically-derived classification regardless
            # of what build_envelope() defaulted to in the tool body. Same helper
            # the error-return paths use.
            # Skip for dynamic_classification tools: they set their own per-call
            # sensitivity and it must not be overwritten by the static placeholder.
            if not dynamic_classification:
                envelope = _stamp_sensitivity(envelope)
            # Redact CRITICAL fields before returning. Skip the walk entirely
            # for tools whose return type has no CRITICAL field — the result
            # would be value-identical and the dataclass-tree rebuild is the
            # most expensive thing on the hot path. Dynamic-classification tools
            # also skip: they already applied redact_records per call.
            if (
                has_critical
                and not dynamic_classification
                and envelope.error is None
                # ResponseEnvelope.data type param is erased after the
                # dataclasses.replace above; pyright can't see it's narrowable.
                and envelope.data is not None  # pyright: ignore[reportUnknownMemberType]
            ):
                # Same reason: envelope.data is `Unknown` after the generic erase.
                redacted_data = redact_typed(envelope.data, consent=None)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
                envelope = dataclasses.replace(envelope, data=redacted_data)  # pyright: ignore[reportUnknownArgumentType]
            # Write a privacy.log.jsonl event for every tool call.
            await _emit_privacy_event(envelope)
            return envelope

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        wrapper._mcp_domain = domain  # type: ignore[attr-defined]
        wrapper._mcp_read_only = read_only  # type: ignore[attr-defined]
        wrapper._mcp_destructive = destructive  # type: ignore[attr-defined]
        wrapper._mcp_idempotent = idempotent  # type: ignore[attr-defined]
        wrapper._mcp_open_world = open_world  # type: ignore[attr-defined]
        wrapper._mcp_max_items = max_items  # type: ignore[attr-defined]
        wrapper._mcp_timeout_seconds = timeout_seconds  # type: ignore[attr-defined]
        wrapper._mcp_list_params = list_params  # type: ignore[attr-defined]
        return wrapper

    return decorator
