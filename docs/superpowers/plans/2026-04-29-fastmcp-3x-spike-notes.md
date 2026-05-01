# FastMCP 3.x API Spike Notes

> **Working notes for the FastMCP 3.x migration.** Delete at end of migration.
> Installed version: `fastmcp==3.2.4` (alongside `mcp[cli]` — co-existence verified).

Verification: `uv run python -c "import fastmcp; import mcp.server.fastmcp; print(fastmcp.__version__)"` prints `3.2.4`.

---

## FastMCP constructor

**Import:**
```python
from fastmcp import FastMCP, Context
```
(Replaces `from mcp.server.fastmcp import FastMCP`.)

**Constructor signature** (`fastmcp/server/server.py:286`):
```python
FastMCP(
    name: str | None = None,
    instructions: str | None = None,
    *,
    version: str | int | float | None = None,
    auth: AuthProvider | None = None,
    middleware: Sequence[Middleware] | None = None,
    providers: Sequence[Provider] | None = None,
    transforms: Sequence[Transform] | None = None,
    lifespan: LifespanCallable | Lifespan | None = None,
    tools: Sequence[Tool | Callable[..., Any]] | None = None,
    on_duplicate: DuplicateBehavior | None = None,
    mask_error_details: bool | None = None,
    dereference_schemas: bool = True,
    strict_input_validation: bool | None = None,
    list_page_size: int | None = None,
    tasks: bool | None = None,
    session_state_store: AsyncKeyValue | None = None,
    sampling_handler: SamplingHandler | None = None,
    sampling_handler_behavior: Literal["always", "fallback"] | None = None,
    client_log_level: mcp.types.LoggingLevel | None = None,
    **kwargs,
)
```

**Diff vs current usage at `src/moneybin/mcp/server.py:30`:**
- `FastMCP("MoneyBin", instructions=...)` — **same signature, no changes needed.** Both `name` and `instructions` are accepted positionally / by keyword exactly as today.
- New keyword `mask_error_details=True` replaces our `handle_mcp_errors` decorator (see Error handling below).

**Lifespan / startup hooks:**
- `lifespan=` keyword arg accepts an async context manager (`@asynccontextmanager`) or a `Lifespan` instance. The result yielded becomes `ctx.lifespan_context` (typed via `FastMCP[LifespanResultT]` generic).
- For the migration, we don't need a lifespan unless we want startup-time side-effects (e.g. registering tools). Keeping module-level imports + decorator-based registration is fine.

**Run invocation:**
```python
mcp.run(transport="stdio")  # unchanged
```
`run()` is a sync method; transports: `"http" | "stdio" | "sse" | "streamable-http"`. (`fastmcp/server/mixins/transport.py:77`.)

**`mcp.tool(...)` decorator shape:**
```python
@mcp.tool                           # bare
@mcp.tool("custom_name")            # positional name
@mcp.tool(name="x", description="...", tags={"hidden"})  # keyword
mcp.tool(fn, name="x")              # direct call
```
Supported kwargs: `name`, `version`, `title`, `description`, `icons`, `tags`, `output_schema`, `annotations`, `exclude_args` (deprecated), `meta`, `app`, `task`, `timeout`, `auth`. **Crucially, `tags: set[str]` is the hook for the visibility system below.**

The current `mcp.tool(name=tool.name, description=tool.description)(tool.fn)` call at `server.py:168` continues to work verbatim.

---

## Error handling

**There is no `@handle_tool_errors` decorator.** Error masking in FastMCP 3.x is a **server-level constructor argument**, not per-tool.

**Construction:**
```python
mcp = FastMCP("MoneyBin", instructions=..., mask_error_details=True)
```

**Behavior** (`fastmcp/server/server.py:1240-1263`): the `_call_tool` core wraps every tool execution. The exception flow:
- `FastMCPError` subclasses (incl. `ToolError`, `ResourceError`, `ValidationError`, `AuthorizationError`) and `pydantic.ValidationError` → re-raised unmasked. Tools that explicitly `raise ToolError("user-safe message")` always reach the LLM.
- `httpx.HTTPStatusError` with status 429 → wrapped as `ToolError("Rate limited by upstream API, please retry later")` — actionable, always reaches LLM even when masking is on.
- `httpx.TimeoutException` → wrapped as `ToolError("Upstream request timed out, please retry")` — same.
- All other `Exception` subclasses → wrapped as `ToolError(f"Error calling tool {name!r}")` (masked) or `ToolError(f"Error calling tool {name!r}: {e}")` (unmasked) depending on `mask_error_details`.

**Per-tool override:** none. Masking is global.

**Public exception imports:**
```python
from fastmcp.exceptions import (
    FastMCPError,  # base
    ToolError,  # use to raise messages that reach the LLM
    ResourceError,
    PromptError,
    ValidationError,
    AuthorizationError,
    NotFoundError,
    DisabledError,
)
```

**Migration mapping:** today's `handle_mcp_errors` decorator (which catches and rewraps) becomes:
1. Set `mask_error_details=True` on the constructor.
2. Inside tools, raise `ToolError("user-safe message")` for actionable errors. Anything else propagates and is auto-wrapped.

---

## Visibility system

**Decorator-level "hidden" flag — there isn't one for global hiding.** Tools have an `enabled: bool = True` field on the underlying `FunctionTool` model (and a `ToolMeta.enabled` field on the `@tool` decorator metadata at `fastmcp/tools/function_tool.py:87`), but there is no `enabled=False` keyword on the `mcp.tool()` decorator itself.

**The 3.x pattern is tag-based with `Visibility` transforms.** Mark tools with a tag, then add a server-level transform.

**Pattern A — global allowlist (what we want for "core vs extended"):**
```python
from fastmcp.server.transforms import Visibility

# Tag extended tools
@mcp.tool(tags={"extended"})
def categorize_bulk(...): ...

# At server construction, hide everything tagged "extended"
mcp.add_transform(Visibility(False, tags={"extended"}))
```

**`Visibility(...)` signature** (`fastmcp/server/transforms/visibility.py:60`):
```python
Visibility(
    enabled: bool,
    *,
    names: set[str] | None = None,        # match by tool name
    keys: set[str] | None = None,         # match by full key (e.g. "tool:my_tool@v1")
    version: VersionSpec | None = None,
    tags: set[str] | None = None,         # match if tool has any of these tags
    components: set[Literal["tool","resource","template","prompt"]] | None = None,
    match_all: bool = False,              # ignore criteria; match every component
)
```
Stack multiple `Visibility(...)` transforms; later transforms override earlier ones (allowlist via `Visibility(False, match_all=True)` then `Visibility(True, tags={"public"})`).

**Session-scoped enable / disable** — this is the progressive-disclosure primitive. Inside a tool handler:

```python
from fastmcp import Context
from fastmcp.server.transforms.visibility import enable_components, disable_components


@mcp.tool
async def discover(ctx: Context, namespace: str) -> str:
    await enable_components(ctx, tags={namespace})  # this session only
    return f"{namespace} tools enabled"
```

**`enable_components` signature** (`fastmcp/server/transforms/visibility.py:372`):
```python
async def enable_components(
    context: Context,
    *,
    names: set[str] | None = None,
    keys: set[str] | None = None,
    version: VersionSpec | None = None,
    tags: set[str] | None = None,
    components: set[Literal["tool","resource","template","prompt"]] | None = None,
    match_all: bool = False,
) -> None
```
**Note:** parameter is `names: set[str]` (a set), not `*names: str` varargs. `disable_components` has the same signature.

Calling `enable_components` automatically sends `ToolListChangedNotification` (and the resource/prompt equivalents) to that session.

**Context access inside tool handlers:**
```python
from fastmcp import Context


@mcp.tool
async def my_tool(x: int, ctx: Context) -> str:  # any param annotated Context
    ...
```
The framework injects `Context` automatically when it sees the type annotation. Sync tools can also accept `Context` — fastmcp will detect it via parameter inspection.

### Critical security check (CONFIRMED SAFE)

**Hidden tools cannot be called by name** — they raise `ToolError: Unknown tool: '<name>'`.

**Experimental verification** (`/tmp/spike_test.py`, executed against `fastmcp 3.2.4`):
```python
server = FastMCP("Spike")


@server.tool
def public_tool(x: int) -> int:
    return x * 2


@server.tool(tags={"hidden"})
def secret_tool(x: int) -> int:
    return x * 100


server.add_transform(Visibility(False, tags={"hidden"}))

async with Client(server) as client:
    print(await client.list_tools())  # -> ['public_tool']
    await client.call_tool(
        "secret_tool", {"x": 5}
    )  # -> ToolError: Unknown tool: 'secret_tool'
    await client.call_tool("public_tool", {"x": 5})  # -> 10
```

Output verbatim:
```
Visible tools: ['public_tool']
Hidden tool call rejected: ToolError: Unknown tool: 'secret_tool'
Public tool result: 10
```

**Why this works** (`fastmcp/server/server.py:716,1240`): both `list_tools()` and `_call_tool()` filter through `is_enabled()` after applying session transforms. `get_tool()` returns `None` for disabled tools, and `call_tool()` raises `NotFoundError` (which is wrapped as `ToolError` in the public surface) when `get_tool()` returns `None`.

**Session-scoped enable also confirmed** (`/tmp/spike_session.py`):
- Initial `list_tools` excludes globally-hidden `extended_tool`.
- After calling `discover` (which runs `enable_components(ctx, tags={"extended"})`), `list_tools` includes it.
- `call_tool("extended_tool", ...)` succeeds within the same session.

**Conclusion:** the security gate is sufficient. No additional plan steps needed for hidden-tool callability.

---

## Structured outputs

**All three return shapes work** (verified `/tmp/spike_output.py`):

| Return | `result.content` | `result.structured_content` | Recommended |
|---|---|---|---|
| `str` (e.g. `json.dumps(envelope.model_dump())`) | TextContent with the string | `{"result": "<string>"}` (wrapped) | Today's pattern; works but adds a `result` wrapper |
| `dict` | TextContent with JSON | The dict directly | Clean; no schema |
| Pydantic `BaseModel` | TextContent with JSON | The model dict directly | **Preferred** — gives `output_schema` for free |

**Recommendation for the migration:** return `ResponseEnvelope` (or `model_dump()` of it) directly from tool functions instead of `result.to_json()`. FastMCP 3.x serializes Pydantic models to both `content` (text) and `structured_content` (typed dict). This is what `docs/decisions/008-fastmcp-3x-sdk.md` motivates.

Concretely, the `mcp_tool` decorator at `src/moneybin/mcp/decorator.py:50` should change from:
```python
result = fn(*args, **kwargs)
if isinstance(result, ResponseEnvelope):
    return result.to_json()  # str -> wrapped as {"result": "..."}
return result
```
to (no transformation; let fastmcp serialize):
```python
return fn(*args, **kwargs)  # ResponseEnvelope -> structured_content directly
```
…provided `ResponseEnvelope` is a Pydantic model (it is). Tool function return types should be annotated `-> ResponseEnvelope` so fastmcp can derive `output_schema`.

---

## Quick reference for later tasks

| Need | API |
|---|---|
| Server import | `from fastmcp import FastMCP, Context` |
| Tool error type | `from fastmcp.exceptions import ToolError` |
| Mark tool as initially-hidden | `@mcp.tool(tags={"extended"})` + global `Visibility(False, tags={"extended"})` |
| Reveal tools for this session | `await enable_components(ctx, tags={"extended"})` |
| Hide for this session | `await disable_components(ctx, tags={"extended"})` |
| Mask exceptions | `FastMCP(..., mask_error_details=True)` |
| Per-tool error reaching LLM | `raise ToolError("user-safe message")` |
| Run stdio | `mcp.run(transport="stdio")` |
| Test client | `from fastmcp import Client; async with Client(server) as c: await c.call_tool(name, args)` |

---

## Tests xfailed during Task 3 (SDK swap)

These will be un-xfailed by Tasks 4-5, which fold envelope-on-error into `mcp_tool` and switch tool returns from `to_json()` strings to direct `ResponseEnvelope` instances.

- `tests/e2e/test_e2e_mcp.py::TestMCPServerBoot::test_server_invokes_tool` — fastmcp 3.x rejects string returns when an `output_schema` is declared; the failure is `"Tools should wrap non-dict values based on their output_schema"`. Fix: return Pydantic `ResponseEnvelope` directly from tool functions.
