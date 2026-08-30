"""Profile management commands for MoneyBin CLI."""

import logging
import sys
from collections.abc import Mapping
from typing import Annotated

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.config import get_current_profile, set_current_profile
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.protocol.envelope import build_envelope
from moneybin.services.profile_service import (
    ProfileExistsError,
    ProfileNotFoundError,
    ProfileService,
)
from moneybin.services.profile_settings_service import (
    MANAGED_SETTING_KEYS,
    ProfileSettingsService,
)

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage user profiles (create, list, switch, delete, show, set)",
    no_args_is_help=True,
)


@app.command("create")
def profile_create(
    name: Annotated[str, typer.Argument(help="Profile name (will be normalized)")],
    init_inbox: Annotated[
        bool | None,
        typer.Option(
            "--init-inbox/--no-init-inbox",
            help=(
                "Create the import-inbox layout (~/Documents/MoneyBin/<profile>/"
                "{inbox,processed,failed}/). If unset, prompts when interactive "
                "and skips when not."
            ),
        ),
    ] = None,
) -> None:
    """Create a profile, or finish setting up a half-made one.

    Creates the directory structure, config, and encrypted database. A directory
    left unregistered by a bare `db init`, a hand `mkdir`, or an interrupted delete
    is completed in place rather than refused — an existing database is preserved
    untouched. Refuses only when a fully registered profile already exists.
    """
    from moneybin.utils.user_config import normalize_profile_name

    normalized = normalize_profile_name(name)
    if init_inbox is None:
        init_inbox = (
            typer.confirm(
                f"Set up the import inbox at ~/Documents/MoneyBin/{normalized}/?",
                default=True,
            )
            if sys.stdin.isatty()
            else False
        )
    svc = ProfileService()
    # A directory with no config.yaml is completed in place rather than refused, and
    # it may already hold a `db init`'d database. Ask both questions before the call:
    # "Created" would hide the adoption from the person whose data is in there, and
    # claiming we preserved a database that never existed is just as wrong.
    adopting = svc.exists(name)
    preserving_db = adopting and svc.has_database(name)
    try:
        profile_dir = svc.create(name, init_inbox=init_inbox)
        if adopting:
            logger.info(f"✅ Completed setup for profile {normalized} at {profile_dir}")
            if preserving_db:
                logger.info("Existing database left untouched.")
        else:
            logger.info(f"✅ Created profile {normalized} at {profile_dir}")
        if init_inbox:
            logger.info(
                f"Import inbox ready at ~/Documents/MoneyBin/{normalized}/inbox/"
            )
    except ProfileExistsError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"❌ Failed to create profile '{name}': {e}")
        logger.info(f"💡 Run 'moneybin profile create {name}' to retry")
        raise typer.Exit(1) from e


@app.command("list")
def profile_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List all profiles, marking the active one."""
    svc = ProfileService()
    profiles = svc.list()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=profiles, sensitivity="low"),
            output,
            cli_actor="profile_list",
        )
        return

    if not profiles:
        if not quiet:
            logger.info("No profiles found")
            logger.info("💡 Run 'moneybin profile create <name>' to create one")
        return
    for p in profiles:
        marker = " (active)" if p["active"] else ""
        logger.info(f"  {p['name']}{marker}")


@app.command("switch")
def profile_switch(
    name: Annotated[str, typer.Argument(help="Profile name to switch to")],
) -> None:
    """Set a different profile as the active default."""
    svc = ProfileService()
    try:
        svc.switch(name)
        logger.info(f"✅ Switched to profile: {name}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("delete")
def profile_delete(
    name: Annotated[str, typer.Argument(help="Profile name to delete")],
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt"),
    ] = False,
) -> None:
    """Delete a profile and all its data (database, logs, config)."""
    svc = ProfileService()
    if not yes:
        confirm = typer.confirm(
            f"Delete profile '{name}' and ALL its data? This cannot be undone."
        )
        if not confirm:
            return
    try:
        svc.delete(name)
        logger.info(f"✅ Deleted profile: {name}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


def _active_profile_name(svc: ProfileService) -> str | None:
    """The profile this process would act on, or None when there is none.

    `main_callback` deliberately skips lazy profile resolution for the `profile`
    group — `profile create` must run before any profile exists — so module
    state is unset unless `--profile`/`MONEYBIN_PROFILE` was explicit. Falling
    back to the persisted default is what keeps this answer the same one
    `ProfileService` gives; comparing the two as if they were one source of
    truth is how `profile show` silently dropped its settings section.
    """
    try:
        return get_current_profile(auto_resolve=False)
    except RuntimeError:
        return next(
            (str(entry["name"]) for entry in svc.list() if entry["active"]), None
        )


def _read_managed_settings(
    svc: ProfileService, info: Mapping[str, object]
) -> dict[str, object]:
    """Managed settings for a profile, or ``{}`` when its database is unreachable.

    Only the active profile's database is open to this process, and only once
    it exists — `profile show other-profile` legitimately has nothing to read.
    """
    name = str(info["name"])
    if not info["database_exists"] or name != _active_profile_name(svc):
        return {}
    set_current_profile(name)
    with get_database(read_only=True) as db:
        settings = ProfileSettingsService(db).get_settings()
    return {"home_currency": settings.home_currency}


def _set_managed_setting(
    svc: ProfileService,
    target: str,
    key: str,
    value: str,
    *,
    explicit_profile: str | None,
) -> None:
    """Write one managed setting to the target profile's database.

    Unlike ``config.yaml``, which is a plain file this process can write for any
    profile, a managed setting lives in the target's encrypted database. Writing
    the *active* profile's value when the user named another one would be a
    silent wrong-target write, so that case errors instead.
    """
    info = svc.show(target)
    name = str(info["name"])
    if explicit_profile is not None and name != _active_profile_name(svc):
        raise UserError(
            f"{key} is stored in profile {name}'s database, which is not "
            f"the active profile. Switch to it first: "
            f"moneybin profile switch {name}",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if not info["database_exists"]:
        raise UserError(
            f"{key} is stored in the profile's database, which does not exist "
            f"yet. Create it first: moneybin db init",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    # get_database() resolves its path through get_settings(), which reads the
    # activated profile — not `target`. The profile group skips lazy resolution,
    # so without this the ordinary `profile set home_currency EUR` opens nothing
    # and raises an unclassified RuntimeError out of get_settings().
    set_current_profile(name)
    with get_database(read_only=False) as db:
        ProfileSettingsService(db).set_setting(key, value, actor="cli")


@app.command("show")
def profile_show(
    name: Annotated[
        str | None,
        typer.Argument(help="Profile name (defaults to active profile)"),
    ] = None,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — show has no info chatter; only data lines
) -> None:
    """Show resolved settings for a profile."""
    svc = ProfileService()
    if name is None:
        try:
            name = get_current_profile(auto_resolve=False)
        except RuntimeError:
            name = None
    # `show` reads managed settings out of the encrypted database now, so a
    # locked or keyless profile raises DatabaseKeyError here. handle_cli_errors
    # turns that into the "run moneybin db unlock" guidance — a raw traceback on
    # the command a user reaches for when the database is unhealthy is the worst
    # possible moment for one.
    with handle_cli_errors(cli_actor="profile_show"):
        info = svc.show(name)
        info["settings"] = _read_managed_settings(svc, info)
        if output == OutputFormat.JSON:
            render_or_json(
                build_envelope(data=info, sensitivity="low"),
                output,
                cli_actor="profile_show",
            )
            return
        marker = " (active)" if info["active"] else ""
        logger.info(f"Profile: {info['name']}{marker}")
        logger.info(f"  Path:     {info['path']}")
        logger.info(f"  Database: {info['database_path']}")
        db_status = "exists" if info["database_exists"] else "not created"
        logger.info(f"  DB state: {db_status}")
        if info.get("config"):
            logger.info("  Config (config.yaml):")
            for section, values in info["config"].items():  # type: ignore[union-attr]  # narrowed by .get check
                if isinstance(values, dict):
                    for k, v in values.items():
                        logger.info(f"    {section}.{k}: {v}")
        settings: dict[str, object] = info["settings"]  # type: ignore[assignment]  # always set above
        if settings:
            logger.info("  Settings (database):")
            for k, v in settings.items():
                logger.info(f"    {k}: {v if v is not None else '(not set)'}")


@app.command("set")
def profile_set(
    key: Annotated[
        str,
        typer.Argument(
            help="Config key (e.g., logging.level) or managed key (home_currency)"
        ),
    ],
    value: Annotated[str, typer.Argument(help="Value to set")],
    name: Annotated[
        str | None,
        typer.Option("--profile", "-p", help="Profile name (defaults to active)"),
    ] = None,
) -> None:
    """Set a configuration value on a profile.

    Dotted ``section.field`` keys write the profile's ``config.yaml``. Undotted
    managed keys (``home_currency``) write ``app.profile_settings`` in the
    profile's database, where the report guards can read them.
    """
    svc = ProfileService()
    target: str
    if name:
        target = name
    else:
        try:
            target = get_current_profile(auto_resolve=False)
        except RuntimeError:
            profiles = svc.list()
            active = next((p["name"] for p in profiles if p["active"]), None)
            target = str(active) if active else "default"
    # A managed key writes the encrypted database, so this path can now raise
    # DatabaseKeyError / DatabaseLockError alongside the config-file errors.
    with handle_cli_errors(cli_actor="profile_set"):
        try:
            if key in MANAGED_SETTING_KEYS:
                _set_managed_setting(svc, target, key, value, explicit_profile=name)
            else:
                svc.set(target, key, value)
        except (ProfileNotFoundError, ValueError) as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(1) from e
        logger.info(f"✅ Set {key}={value}")
