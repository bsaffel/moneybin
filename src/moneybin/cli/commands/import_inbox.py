"""`moneybin import inbox` — drain, list, and locate the watched inbox."""

from __future__ import annotations

import dataclasses
from typing import Any, cast

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.privacy.payloads.imports import ImportInboxPendingEntry
from moneybin.privacy.redaction import redact_typed
from moneybin.services.inbox_service import InboxService, InboxSyncResult

app = typer.Typer(
    help="Drop files into the inbox and drain them into MoneyBin.",
    no_args_is_help=False,
)


def _masked_pending(entries: list[dict[str, object]]) -> list[dict[str, Any]]:
    """Mask each pending entry by the declaration MCP's payload uses.

    The one masking point for this module, shared by the text renderer and the
    JSON envelope. Neither gets it for free: text never reaches
    ``render_or_json`` at all, and the JSON branch hands it
    ``dataclasses.asdict(result)`` — a bare dict, so the walk's
    ``type(envelope.data)`` gate finds nothing to descend. On the OFX channel
    ``account_proposals[].source_account_key`` is the ``<ACCTID>`` the
    institution issued, and `.claude/rules/cli.md` allows no CLI exemption:
    "never assume CLI users are 'trusted enough to skip redaction'".

    Masking by ``ImportInboxPendingEntry`` rather than a field list here keeps
    one declaration governing this surface and MCP's ``import_inbox_sync``.
    """
    return [
        redact_typed(entry, consent=None, declared_type=ImportInboxPendingEntry)
        for entry in entries
    ]


def _print_sync_text(result: InboxSyncResult) -> None:
    """Render a sync result as human-readable text."""
    processed = result.processed
    failed = result.failed
    pending = _masked_pending(result.pending)
    skipped = result.skipped

    if skipped and any(s.get("reason") == "inbox_busy" for s in skipped):
        typer.echo("⚠️  Another sync is in progress; nothing done.", err=True)
        return

    # Deferred: import_cmd imports this module at its own module level, so a
    # top-level import here would close the cycle. Reused rather than re-rendered
    # because one wrong-account recovery hint is hard enough to keep correct.
    from moneybin.cli.commands.import_cmd import (  # noqa: PLC0415
        echo_accounts_created,
    )

    for item in processed:
        typer.echo(
            f"✓ {item['filename']}  →  imported "
            f"({item.get('transactions', 0)} transactions)"
        )
        raw_created: Any = item.get("accounts_created")
        created: list[dict[str, str]] = (
            cast("list[dict[str, str]]", raw_created)
            if isinstance(raw_created, list)
            else []
        )
        echo_accounts_created(created)
    for item in failed:
        typer.echo(f"✗ {item['filename']}  →  failed ({item['error_code']})", err=True)
        if "sidecar" in item:
            typer.echo(f"   See {item['sidecar']}", err=True)
    for item in pending:
        moved_to = item.get("moved_to", item["filename"])
        tier = item.get("tier", "unknown")
        reason = item.get("reason", "")
        typer.echo(
            f"👀 {item['filename']}  →  pending confirmation (tier={tier})",
            err=True,
        )
        if reason == "account_confirmation":
            typer.echo(
                # Leads with @N, not <source_key>: the key is masked in the
                # listing below (it is the institution's own <ACCTID> on OFX),
                # so the ref is the only referent a reader can copy from here.
                f"   Account identity needed — run 'moneybin import confirm "
                f"{moved_to} --accept --account-binding @N=<account_id|new>' "
                "(@N is the ref beside each proposal below; =account_id adopts "
                "an existing account, =new mints one; or move the file into "
                "inbox/<account-slug>/ and re-sync):",
                err=True,
            )
            raw_props: Any = item.get("account_proposals")
            proposals: list[Any] = raw_props if isinstance(raw_props, list) else []
            for p in proposals:
                # Already masked by _masked_pending — same shape as
                # _echo_account_proposals. The ref is the answer; the masked key
                # only tells two proposals apart.
                typer.echo(
                    f"     {p.get('proposal_ref', '')}  account: "
                    f"{p.get('source_account_key', '<account>')}",
                    err=True,
                )
                raw_cands: Any = p.get("candidates")
                cands: list[Any] = raw_cands if isinstance(raw_cands, list) else []
                for c in cands:
                    typer.echo(
                        f"       {c.get('account_id')}  {c.get('display_name', '')}",
                        err=True,
                    )
        elif tier != "low":
            typer.echo(
                f"   Run 'moneybin import confirm {moved_to} --accept' to ratify "
                "(or re-run with --mapping to override).",
                err=True,
            )
        else:
            # Low-tier mapping confirmation: resolve_or_confirm re-surfaces these
            # on --accept rather than loading them, so --accept would loop. Only
            # the --mapping override path is usable here.
            typer.echo(
                f"   Run 'moneybin import confirm {moved_to} --mapping "
                "field=column' to override (low-confidence detection; "
                "--accept would be rejected).",
                err=True,
            )

    typer.echo(
        f"Done: {len(processed)} imported, {len(failed)} failed, "
        f"{len(pending)} pending.",
        err=True,
    )


@app.callback(invoke_without_command=True)
def inbox_default(
    ctx: typer.Context,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Default action: drain the inbox."""
    if ctx.invoked_subcommand is not None:
        return
    from moneybin.cli.utils import handle_cli_errors  # noqa: PLC0415
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            result = InboxService(db=db, settings=get_settings()).sync()

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        # Pending entries carry detector proposals — account pick-lists whose
        # candidates include account display names (DESCRIPTION/medium). The
        # CLI has no privacy middleware, so the envelope's declared tier is the
        # only sensitivity signal a JSON consumer sees; declare medium when any
        # pending entry exists (mirrors the MCP import_files rule). Processed,
        # failed, skipped, and ignored entries carry only paths and counts (low).
        sensitivity = "medium" if result.pending else "low"
        payload = dataclasses.asdict(result)
        payload["pending"] = _masked_pending(result.pending)
        render_or_json(
            build_envelope(data=payload, sensitivity=sensitivity),
            output,
            cli_actor="inbox_default",
        )
        return
    if quiet:
        return
    _print_sync_text(result)


@app.command("list")
def inbox_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show what a sync would do, without moving anything."""
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors():
        result = InboxService.for_active_profile_no_db().enumerate()

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        render_or_json(
            build_envelope(data=dataclasses.asdict(result), sensitivity="low"),
            output,
            cli_actor="inbox_list",
        )
        return
    if quiet:
        return
    for item in result.would_process:
        hint = f"  [{item['account_hint']}]" if item.get("account_hint") else ""
        typer.echo(f"  {item['filename']}{hint}")
    if not result.would_process:
        typer.echo("(inbox empty)")


@app.command("path")
def inbox_path(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Print the active profile's inbox parent directory."""
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors():
        service = InboxService.for_active_profile_no_db()
        # Materialize the layout so users can immediately copy files into
        # `$(moneybin import inbox path)/inbox/...` on a fresh profile.
        service.ensure_layout()

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        render_or_json(
            build_envelope(
                data={"path": str(service.root), "inbox": str(service.inbox_dir)},
                sensitivity="low",
            ),
            output,
            cli_actor="inbox_path",
        )
        return
    if quiet:
        return
    typer.echo(str(service.root))
