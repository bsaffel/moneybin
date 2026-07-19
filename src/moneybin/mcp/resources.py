# src/moneybin/mcp/resources.py
"""MCP v1 resource definitions.

One ambient context endpoint: ``moneybin://schema``. This resource provides
the curated SQL schema that lets the AI write accurate queries for
``sql_query`` without a separate discovery call. It is the only resource
with unique composition value not already reachable via tools.

The seven resources removed in PR #185 (moneybin://status, moneybin://accounts,
moneybin://privacy, moneybin://tools, accounts://summary, moneybin://recent-curation,
net-worth://summary) were duplicates of tool responses and added context-window
overhead without information gain. Their data remains available via the
corresponding tools.

The operating 45-tool standard registry gives generic and supported
deferred-loading hosts the same tool identities; this resource does not create
another discovery or profile surface.

See ``docs/specs/moneybin-mcp.md`` for the current prompt and resource contract.
"""

from __future__ import annotations

import json
import logging

from moneybin.services.schema_catalog import build_schema_doc

from .server import mcp

logger = logging.getLogger(__name__)


@mcp.resource("moneybin://schema")
def resource_schema() -> str:
    """Curated schema for ad-hoc SQL: interface tables, columns, comments, example queries."""
    logger.info("Resource read: moneybin://schema")
    doc = build_schema_doc()
    return json.dumps(doc, indent=2, default=str)
