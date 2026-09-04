"""Cross-transport protocol types shared across MCP, CLI, and future HTTP.

The response envelope and its pagination, plus the coarse-write request
contracts. Types only, with no dependency on the service or orchestration
layers — the code that renders a domain result into one of these lives in
``moneybin.adapters``, above the pipeline it reads.
"""
