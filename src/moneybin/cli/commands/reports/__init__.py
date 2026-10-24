"""Reports top-level command group — cross-domain read-only views.

``list``, ``run``, and ``explain`` span all three registry tiers. The first two
are the CLI twins of the shipped ``reports`` MCP catalog/runner; ``explain`` is
R6's verify surface. ``create`` / ``set`` / ``delete`` / ``reclassify`` are the
lifecycle capability over saved reports. Everything but ``list`` and ``run`` is
CLI-only by design — ``reports-dynamic.md`` names no MCP identity for a lifecycle
verb or for the verify surface.

Every report — including the three net-worth rungs — is generated from an
``@report`` runner in ``moneybin.reports.definitions`` and registered via
``register_reports_cli``. There is no hand-written report command left.
"""

from __future__ import annotations

import typer

from moneybin.reports._framework.registry import register_reports_cli
from moneybin.reports.definitions import ALL_REPORTS

from .user_reports import (
    reports_create,
    reports_delete,
    reports_explain,
    reports_list,
    reports_reclassify,
    reports_run,
    reports_set,
)

app = typer.Typer(
    help="Cross-domain analytical and aggregation views",
    no_args_is_help=True,
)

app.command("list")(reports_list)
app.command("run")(reports_run)
app.command("explain")(reports_explain)
app.command("create")(reports_create)
app.command("set")(reports_set)
app.command("delete")(reports_delete)
app.command("reclassify")(reports_reclassify)
register_reports_cli(ALL_REPORTS, app)
