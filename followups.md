# Follow-ups

Deferred work surfaced during implementation. Each entry: what + why + where it came from.

## MCP tool timeouts (feat/mcp-tool-timeouts)

- **Migrate MCP tests to `pytest-asyncio` auto-mode.** The timeout feature made every `@mcp_tool`-decorated function async, so ~17 existing tests across 6 files (`test_decorator.py`, `test_error_handling.py`, `test_tools.py`, `test_v1_tools.py`, `test_categorization_tools.py`, `test_import_inbox_tools.py`) now wrap calls in `asyncio.run(...)`. With `pytest-asyncio` already a project dep, switching to auto-mode + `async def` test bodies would drop the boilerplate. Surfaced by `/simplify` reuse review.
