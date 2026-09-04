"""Render a service or orchestration result as the response both surfaces emit.

One layer, sitting above ``moneybin.orchestration`` and below the transports:
MCP tools and CLI ``--output json`` return the same payload, so the mapping
lives here once rather than twice. ``actions`` stay with the caller, because
MCP names tools and the CLI names commands.

Not ``moneybin.protocol``, which is types only — an adapter reads a
``RefreshResult`` and a connector model, so it depends on the layers those
come from. Not ``moneybin.mcp`` either, which is one transport of the two
that call these.
"""
