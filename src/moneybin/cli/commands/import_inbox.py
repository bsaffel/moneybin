"""`moneybin import inbox` — drain, list, and locate the watched inbox."""

from __future__ import annotations

import dataclasses
import shlex
from typing import Any, cast

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
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


def _minted_any(processed: list[dict[str, object]]) -> bool:
    """Whether the drain minted an account on any imported file.

    The rows are service-shaped ``dict[str, object]``, so the list is checked
    rather than assumed: a malformed entry must not raise on the drain's own
    success path.
    """
    for entry in processed:
        created = entry.get("accounts_created")
        if isinstance(created, list) and created:
            return True
    return False


def _drain_needs_attention(result: InboxSyncResult) -> bool:
    """Whether requested drain work remains failed, pending, or blocked."""
    return bool(
        result.failed
        or result.pending
        or any(item.get("reason") == "inbox_busy" for item in result.skipped)
    )


def _copyable_recovery() -> object:
    """Direct readers to the complete recovery command below the receipt."""
    from rich.text import Text

    return Text("Recovery: Copy the complete command below.")


def _recovery_command(path: object, arguments: str) -> str | None:
    """Build a shell command only when its exact path is terminal-safe to print."""
    target = str(path)
    if not target.isprintable():
        return None
    return f"moneybin import confirm {shlex.quote(target)} {arguments}"


def _add_recovery_command(
    parts: list[object],
    recovery_commands: list[str],
    *,
    path: object,
    arguments: str,
) -> None:
    """Append a copyable command or explain why an unsafe path has none."""
    command = _recovery_command(path, arguments)
    if command is not None:
        parts.append(_copyable_recovery())
        recovery_commands.append(command)
        return
    parts.append(
        build_summary(
            [
                (
                    "Recovery",
                    "The pending path contains terminal control characters, so no "
                    "shell command was printed. For scripted drains, use moneybin "
                    "import inbox --output json and preserve pending[].moved_to in "
                    "tooling that keeps the exact path.",
                )
            ],
            title="Manual recovery required",
        )
    )


def _sync_text_result(result: InboxSyncResult, recovery_commands: list[str]) -> object:
    """Build one complete, unpaged receipt for an inbox drain."""
    processed = result.processed
    failed = result.failed
    pending = _masked_pending(result.pending)
    skipped = result.skipped

    busy = any(item.get("reason") == "inbox_busy" for item in skipped)
    parts: list[object] = [
        build_summary(
            [
                ("Imported", str(len(processed))),
                ("Failed", str(len(failed))),
                ("Pending confirmation", str(len(pending))),
                ("Skipped", str(len(skipped))),
                ("Ignored", str(len(result.ignored))),
            ],
            title="Inbox drain needs attention"
            if _drain_needs_attention(result)
            else "Inbox drain complete",
        )
    ]
    if busy:
        parts.append(
            build_summary(
                [
                    (
                        "Status",
                        "Another sync is in progress; no inbox files were drained.",
                    )
                ],
                title="Blocked",
            )
        )

    # Deferred: import_cmd imports this module at its own module level, so a
    # top-level import here would close the cycle. Reused rather than re-rendered
    # because one wrong-account recovery hint is hard enough to keep correct.
    from moneybin.cli.commands.import_cmd import (
        format_account_candidate,
    )

    for item in processed:
        parts.append(
            build_summary(
                [
                    ("File", str(item["filename"])),
                    ("Outcome", f"Imported {item.get('transactions', 0)} transactions"),
                ],
                title="Imported file",
            )
        )
        raw_created: Any = item.get("accounts_created")
        created: list[dict[str, str]] = (
            cast("list[dict[str, str]]", raw_created)
            if isinstance(raw_created, list)
            else []
        )
        for account in created:
            parts.append(
                build_summary(
                    [
                        ("Account", str(account.get("display_name", "-"))),
                        ("Account ID", str(account.get("account_id", "-"))),
                        (
                            "Recovery",
                            "Rename with moneybin accounts set <account_id> "
                            "--display-name <name>; inspect identity merges with "
                            "moneybin accounts links run.",
                        ),
                    ],
                    title="Account created",
                )
            )
    for item in failed:
        pairs = [
            ("File", str(item["filename"])),
            ("Outcome", f"Failed ({item['error_code']})"),
        ]
        if "sidecar" in item:
            pairs.append(("Recovery", f"Inspect {item['sidecar']}"))
        parts.append(build_summary(pairs, title="Failed file"))
    for item in pending:
        moved_to = item.get("moved_to", item["filename"])
        tier = item.get("tier", "unknown")
        reason = item.get("reason", "")
        pairs = [
            ("File", str(item["filename"])),
            ("Outcome", f"Pending confirmation (tier={tier})"),
        ]
        if "sidecar" in item:
            pairs.append(("Pending record", str(item["sidecar"])))
        parts.append(build_summary(pairs, title="Needs confirmation"))
        if reason == "account_confirmation":
            subfolder_hint = (
                "; or move the file into inbox/<account-slug>/ and re-sync"
                if item.get("channel") == "tabular"
                else ""
            )
            parts.extend([
                build_summary(
                    [
                        (
                            "Binding",
                            "@N is the proposal ref below; =account_id adopts an "
                            f"existing account, =new mints one{subfolder_hint}.",
                        ),
                    ],
                    title="Account identity needed",
                ),
            ])
            _add_recovery_command(
                parts,
                recovery_commands,
                path=moved_to,
                arguments="--accept --account-binding @N=<account_id|new>",
            )
            raw_props: Any = item.get("account_proposals")
            proposals: list[Any] = raw_props if isinstance(raw_props, list) else []
            for proposal in proposals:
                parts.append(
                    build_summary(
                        [
                            ("Proposal", str(proposal.get("proposal_ref", ""))),
                            (
                                "Account key",
                                str(proposal.get("source_account_key", "<account>")),
                            ),
                        ],
                        title="Account proposal",
                    )
                )
                raw_cands: Any = proposal.get("candidates")
                candidates: list[Any] = raw_cands if isinstance(raw_cands, list) else []
                for candidate in candidates:
                    parts.append(
                        build_summary(
                            [("Candidate", format_account_candidate(candidate))],
                            title="Candidate account",
                        )
                    )
        elif reason == "header_position_ambiguous":
            raw_rows: Any = item.get("header_position_ambiguous_rows")
            rows: list[dict[str, str]] = (
                cast("list[dict[str, str]]", raw_rows)
                if isinstance(raw_rows, list)
                else []
            )
            if rows:
                parts.append(
                    build_rows(
                        list(rows[0]),
                        [[row.get(column, "") for column in rows[0]] for row in rows],
                    )
                )
            parts.extend([
                build_summary([], title="Header position needs confirmation"),
            ])
            _add_recovery_command(
                parts, recovery_commands, path=moved_to, arguments="--accept"
            )
        elif tier != "low":
            parts.extend([
                build_summary([], title="Confirm import"),
            ])
            _add_recovery_command(
                parts, recovery_commands, path=moved_to, arguments="--accept"
            )
        else:
            parts.extend([
                build_summary(
                    [("Why", "Low-confidence detection; --accept would be rejected.")],
                    title="Mapping needed",
                ),
            ])
            _add_recovery_command(
                parts,
                recovery_commands,
                path=moved_to,
                arguments="--mapping field=column",
            )

    for entries in (skipped, result.ignored):
        if entries:
            parts.append(
                build_rows(
                    ["File", "Reason"],
                    [
                        (str(item.get("filename", "-")), str(item.get("reason", "-")))
                        for item in entries
                    ],
                )
            )

    if result.transforms_error:
        parts.append(
            build_summary(
                [
                    (
                        "Imported files",
                        "were saved before the derived-data refresh failed.",
                    ),
                    (
                        "Remaining work",
                        "Run moneybin transform plan to inspect the failure.",
                    ),
                ],
                title="Derived data may be stale",
            )
        )
    return compose_human_result(parts)


@app.callback(invoke_without_command=True)
def inbox_default(
    ctx: typer.Context,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Default action: drain the inbox."""
    if ctx.invoked_subcommand is not None:
        return
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_settings
    from moneybin.database import get_database

    with handle_cli_errors(cli_actor="inbox_default"):
        with get_database(read_only=False) as db:
            result = InboxService(db=db, settings=get_settings()).sync()

    # Ahead of both output branches: -q drops informational output and the JSON
    # branch returns before ever reaching the text path, but a reversal of the
    # user's own decision is neither. The drain is the least supervised surface
    # reaching the reconciliation, so this is the one it can least afford to
    # swallow — and --output json is the mode an unattended caller actually uses.
    from moneybin.adapters.refresh_adapters import (
        refresh_steps_fields,
    )
    from moneybin.cli.utils import (
        warn_refresh_steps,
        warn_transfers_retired,
    )
    from moneybin.matching.reconciliation import (
        RETIRED_SIDES_COLLAPSED,
    )

    warn_transfers_retired(result.transfers_retired, cause=RETIRED_SIDES_COLLAPSED)
    # Ahead of both branches for the same reason, and with the same argument
    # about supervision: the drain reached the network on the user's behalf.
    warn_refresh_steps(result.refresh_steps)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        # Pending entries carry detector proposals — account pick-lists whose
        # candidates include account display names (DESCRIPTION/medium). The
        # CLI has no privacy middleware, so the envelope's declared tier is the
        # only sensitivity signal a JSON consumer sees; declare medium when any
        # pending entry exists (mirrors the MCP import_files rule).
        #
        # A drain that minted an account is medium for the same reason, with
        # nothing pending to raise the tier on its behalf: accounts_created[]
        # .display_name is USER_NOTE. It is counted here rather than derived
        # because the payload below is dataclasses.asdict output — a bare dict,
        # so render_or_json's walk never reaches the annotation and the low
        # fallback would stand in the privacy audit record. Every other
        # processed, failed, skipped, and ignored key is a path or a count.
        sensitivity = (
            "medium" if result.pending or _minted_any(result.processed) else "low"
        )
        # An account gate outranks that: account_proposals[].source_account_key
        # is ACCOUNT_IDENTIFIER — on OFX the <ACCTID> the institution issued —
        # so reading this row for its display names alone under-declares it by
        # two tiers. _masked_pending masks the value, but the asdict payload is
        # a bare dict, so nothing downstream can re-derive either the tier or
        # the audited classes; MCP's typed ImportInboxSyncPayload calls the same
        # bytes critical, and the surfaces must agree (cli.md). Derived from the
        # declarations rather than restated, so they move together.
        classes_returned: list[str] | None = None
        if any(p.get("account_proposals") for p in result.pending):
            from moneybin.privacy.classified_envelope import classify
            from moneybin.privacy.payloads.imports import ImportConfirmationPayload

            # The shared classification primitive the audit path uses, rather
            # than a second inline derivation beside it.
            classification = classify(ImportConfirmationPayload)
            sensitivity = classification.sensitivity
            classes_returned = classification.classes_returned
        payload = dataclasses.asdict(result)
        payload["pending"] = _masked_pending(result.pending)
        # Flattened out of the nested field `asdict` produced, so these read
        # exactly as every other surface spells them. The carrier is internal
        # transport; the envelope is the public contract.
        payload.pop("refresh_steps", None)
        payload.update(refresh_steps_fields(result.refresh_steps))
        render_or_json(
            build_envelope(data=payload, sensitivity=sensitivity),
            output,
            cli_actor="inbox_default",
            classes_returned=classes_returned,
        )
        if _drain_needs_attention(result):
            raise typer.Exit(1)
        return
    from moneybin.cli.utils import get_terminal_policy

    recovery_commands: list[str] = []
    emit_human_result(
        _sync_text_result(result, recovery_commands),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
    for command in recovery_commands:
        typer.echo(command)
    if _drain_needs_attention(result):
        raise typer.Exit(1)


@app.command("list")
def inbox_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # the requested inventory remains visible
    no_pager: bool = no_pager_option,
) -> None:
    """Show what a sync would do, without moving anything."""
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors(cli_actor="inbox_list"):
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
    from moneybin.cli.utils import get_terminal_policy

    parts: list[object] = [
        build_summary(
            [
                ("Would process", str(len(result.would_process))),
                ("Ignored", str(len(result.ignored))),
            ],
            title="Inbox preview",
        )
    ]
    if result.would_process:
        parts.append(
            build_rows(
                ["File", "Account hint"],
                [
                    (str(item["filename"]), str(item.get("account_hint", "-")))
                    for item in result.would_process
                ],
            )
        )
    else:
        parts.append(
            build_summary(
                [("Result", "No files are waiting. Next: moneybin import inbox path")],
                title="Inbox empty",
            )
        )
    if result.ignored:
        parts.append(
            build_rows(
                ["File", "Reason"],
                [
                    (str(item.get("filename", "-")), str(item.get("reason", "-")))
                    for item in result.ignored
                ],
            )
        )
    emit_human_result(
        compose_human_result(parts),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("path")
def inbox_path(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Print the active profile's inbox parent directory."""
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors(cli_actor="inbox_path"):
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
    typer.echo(str(service.root))
