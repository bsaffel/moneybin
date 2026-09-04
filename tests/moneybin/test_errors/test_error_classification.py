"""Tests for the cross-cutting user-error classifier."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from moneybin import error_codes
from moneybin.database import (
    DatabaseCryptoError,
    DatabaseKeyError,
    DatabaseNotInitializedError,
)
from moneybin.errors import UserError, classify_user_error


def test_classify_database_key_error_returns_user_error() -> None:
    """DatabaseKeyError maps to a UserError carrying the recovery hint."""
    # Patched at its definition, not on `moneybin.errors`: the classifier
    # imports it inside the function to keep `errors.py` import-light.
    with patch(
        "moneybin.database.database_key_error_hint",
        return_value="Run: moneybin db unlock",
    ):
        result = classify_user_error(DatabaseKeyError("locked"))
    assert result is not None
    assert result.code == error_codes.INFRA_WRONG_KEY
    assert "locked" in result.message
    assert result.hint == "Run: moneybin db unlock"


def test_classify_database_crypto_error_returns_user_error() -> None:
    """DatabaseCryptoError maps to a UserError instead of an unclassified crash.

    Raised when the OpenSSL crypto module (httpfs) can't be installed/loaded on
    a write connection — reachable on a machine with no cached httpfs and no
    network on its first encrypted write. Like its Database*Error siblings it
    must reach the CLI/MCP structured-error contract, not surface as a raw
    traceback (CLI) or a generic masked envelope (MCP).
    """
    result = classify_user_error(
        DatabaseCryptoError("needs httpfs for OpenSSL crypto; no network")
    )
    assert result is not None
    assert result.code == error_codes.INFRA_CRYPTO_UNAVAILABLE
    # The exception's own crafted message survives to the surface.
    assert "httpfs" in result.message


def test_classify_file_not_found_returns_user_error() -> None:
    """FileNotFoundError maps to a UserError with no hint."""
    result = classify_user_error(FileNotFoundError("missing.csv"))
    assert result is not None
    assert result.code == error_codes.INFRA_FILE_NOT_FOUND
    assert "missing.csv" in result.message
    assert result.hint is None


def test_classify_permission_error_real_eacces(tmp_path: Path) -> None:
    """A genuine chmod-000 file — real OS behavior, runs on any platform."""
    target = tmp_path / "locked.csv"
    target.write_text("x")
    target.chmod(0o000)
    try:
        with pytest.raises(PermissionError) as exc_info:
            target.read_bytes()
        result = classify_user_error(exc_info.value)
    finally:
        target.chmod(0o644)

    assert result is not None
    assert result.code == error_codes.INFRA_PERMISSION_DENIED
    assert result.hint is not None
    assert "chmod" in result.hint


def test_classify_permission_error_eperm_under_documents() -> None:
    """EPERM + Darwin + protected root gets the Full-Disk-Access hint.

    The PermissionError is CONSTRUCTED, not provoked: a TCC denial cannot be
    reproduced in CI. This asserts our branching, not macOS's behavior — that a
    TCC denial reports errno 1 EPERM (vs. errno 13 EACCES for a mode denial) is
    an observed fact, verified by hand probe 2026-07-18.
    """
    path = Path.home() / "Documents" / "statement.pdf"
    exc = PermissionError(1, "Operation not permitted", str(path))
    with patch("moneybin.errors.platform.system", return_value="Darwin"):
        result = classify_user_error(exc)

    assert result is not None
    assert result.code == error_codes.INFRA_PERMISSION_DENIED
    assert result.hint is not None
    assert "Full Disk Access" in result.hint
    assert result.details is not None
    assert result.details["protected_root"] == "~/Documents"


def test_classify_permission_error_without_filename_ignores_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A filename-less EPERM must not be judged by the working directory.

    Reproduces the cwd fallback deterministically: home is redirected to
    tmp_path and the process runs inside its Documents/, so classifying the
    exception against `Path()` would land in a protected root and mis-fire the
    macOS advice for a path that never failed.
    """
    home = tmp_path.resolve()
    docs = home / "Documents"
    docs.mkdir()
    monkeypatch.chdir(docs)
    exc = PermissionError(1, "Operation not permitted")  # no filename

    with (
        patch("moneybin.errors.platform.system", return_value="Darwin"),
        patch("moneybin.errors.Path.home", return_value=home),
    ):
        result = classify_user_error(exc)

    assert result is not None
    assert result.code == error_codes.INFRA_PERMISSION_DENIED
    assert result.hint is not None
    assert "Full Disk Access" not in result.hint
    assert result.details is not None
    assert result.details.get("protected_root") is None


def test_classify_lookup_error_returns_not_found() -> None:
    """Plain LookupError maps to a UserError with code infra_not_found.

    Use INFRA_NOT_FOUND (prefix-neutral) rather than MUTATION_NOT_FOUND
    because the classifier fires on read paths too (account/category/note
    lookups). MUTATION_NOT_FOUND would mis-signal "write attempt" to
    agents branching on the prefix.
    """
    result = classify_user_error(LookupError("note abc not found"))
    assert result is not None
    assert result.code == error_codes.INFRA_NOT_FOUND
    assert "not found" in result.message


def test_classify_key_error_returns_none() -> None:
    """KeyError (LookupError subclass) propagates as internal error, not not_found."""
    assert classify_user_error(KeyError("bad_key")) is None


def test_classify_index_error_returns_none() -> None:
    """IndexError (LookupError subclass) propagates as internal error, not not_found."""
    assert classify_user_error(IndexError(0)) is None


def test_classify_unknown_exception_returns_none() -> None:
    """Unrecognized exceptions return None so callers can re-raise."""
    assert classify_user_error(RuntimeError("internal bug")) is None


def test_classify_value_error_returns_user_error() -> None:
    """ValueError maps to a UserError with code infra_invalid_input.

    Use INFRA_INVALID_INPUT (prefix-neutral) rather than MUTATION_INVALID_INPUT
    because ValueError fires on read paths too (date/decimal parsing in
    reports, query filters). MUTATION_INVALID_INPUT would mis-signal
    "write attempt" to agents branching on the prefix.
    """
    result = classify_user_error(ValueError("bad input"))
    assert result is not None
    assert result.code == error_codes.INFRA_INVALID_INPUT
    assert "bad input" in result.message


def test_classify_match_run_error_withholds_the_cause() -> None:
    """The matcher's partial-run carrier is classified, and its message dropped.

    ``MatchRunError.__init__`` passes ``str(cause)`` to ``Exception``, so the
    carrier's own message is whatever DuckDB or a repository raised — binder
    text, a file path, a row value. Every surface renders a classified
    message verbatim (``❌ {message}`` on the CLI, the MCP error envelope), so
    the branch has to answer with MoneyBin's own words rather than pass
    ``str(exc)`` through the way most branches here legitimately do.
    """
    from moneybin.matching.engine import MatchResult, MatchRunError

    exc = MatchRunError(
        RuntimeError("Binder Error: no column 'amt' in /Users/x/moneybin.duckdb"),
        partial=MatchResult(auto_merged=4),
    )

    result = classify_user_error(exc)

    assert result is not None, "an unclassified carrier propagates as a traceback"
    assert result.code == error_codes.REFRESH_MATCH_FAILED
    assert "Binder Error" not in result.message
    assert "/Users/x" not in result.message
    assert result.hint is not None and "Binder Error" not in result.hint


def test_user_error_to_dict_omits_none_hint() -> None:
    """UserError.to_dict drops the hint field when not set."""
    err = UserError("m", code="c")
    assert err.to_dict() == {"message": "m", "code": "c"}


def test_user_error_to_dict_serializes_recovery_actions() -> None:
    """UserError.to_dict includes recovery_actions when populated."""
    from moneybin.errors import RecoveryAction

    err = UserError(
        "m",
        code=error_codes.MUTATION_NOT_FOUND,
        recovery_actions=[
            RecoveryAction(
                tool="system_audit_undo",
                arguments={"operation_id": "op_test"},
                rationale="Restore pre-mutation state",
                confidence="certain",
                idempotent=True,
            )
        ],
    )
    d = err.to_dict()
    assert "recovery_actions" in d
    assert d["recovery_actions"][0]["tool"] == "system_audit_undo"
    assert d["recovery_actions"][0]["confidence"] == "certain"


def test_user_error_to_dict_omits_recovery_actions_when_none() -> None:
    """UserError.to_dict omits recovery_actions when not set.

    Preserves backward compat — to_dict shape unchanged for callers that
    aren't aware of recovery_actions.
    """
    err = UserError("m", code=error_codes.MUTATION_NOT_FOUND)
    assert "recovery_actions" not in err.to_dict()


def test_user_error_to_dict_includes_hint() -> None:
    """UserError.to_dict includes the hint when populated."""
    err = UserError("m", code="c", hint="h")
    assert err.to_dict() == {"message": "m", "code": "c", "hint": "h"}


def test_classify_database_not_initialized_error() -> None:
    from moneybin.database import DatabaseNotInitializedError
    from moneybin.errors import classify_user_error

    err = DatabaseNotInitializedError("db missing")
    result = classify_user_error(err)
    assert result is not None
    assert "db init" in (result.message + (result.hint or "")).lower()
    assert result.code == error_codes.INFRA_DATABASE_NOT_INITIALIZED


def test_classify_database_lock_error() -> None:
    from moneybin.database import DatabaseLockError
    from moneybin.errors import classify_user_error

    err = DatabaseLockError("Could not acquire write lock after 5s")
    result = classify_user_error(err)
    assert result is not None
    assert result.code == error_codes.INFRA_DATABASE_LOCKED


@pytest.fixture(autouse=True)
def _clean_active_profile() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Reset the process-wide active profile so DB-not-init guidance is deterministic."""
    from moneybin import config

    original = config._current_profile  # pyright: ignore[reportPrivateUsage]
    config._current_profile = None  # pyright: ignore[reportPrivateUsage]
    try:
        yield
    finally:
        config._current_profile = original  # pyright: ignore[reportPrivateUsage]


class TestDatabaseNotInitializedAdvice:
    """The `profile create` vs `db init` verb is decided at raise time.

    `get_database` answers "did this profile finish setup?" while it still
    holds resolved settings; the classifier only reads the answer. These
    tests cover both halves — the reading, and the answering.
    """

    def test_unregistered_profile_points_at_profile_create(self) -> None:
        result = classify_user_error(
            DatabaseNotInitializedError("missing", profile_registered=False)
        )
        assert result is not None
        assert "profile create" in result.message
        assert result.code == error_codes.INFRA_DATABASE_NOT_INITIALIZED

    def test_registered_profile_points_at_db_init(self) -> None:
        result = classify_user_error(
            DatabaseNotInitializedError("missing", profile_registered=True)
        )
        assert result is not None
        assert "profile create" not in result.message
        assert "db init" in result.message.lower()

    def test_unanswered_setup_state_points_at_db_init(self) -> None:
        """An instance nobody annotated falls back to the safe verb."""
        result = classify_user_error(DatabaseNotInitializedError("missing"))
        assert result is not None
        assert "profile create" not in result.message
        assert "db init" in result.message.lower()

    def test_classification_reads_no_config_and_builds_no_service(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Classification runs on the failure path — it must touch neither.

        The pre-MB-51 classifier instantiated `ProfileService` and read the
        active profile from config here, behind a bare `except Exception` that
        turned any config or filesystem failure into silently wrong advice.
        Both reaches are now poisoned: the test fails loudly if either returns.
        """
        from moneybin import config
        from moneybin.services import profile_service

        def _forbidden(*args: object, **kwargs: object) -> object:
            raise AssertionError("classification must not reach into config/services")

        monkeypatch.setattr(config, "get_current_profile", _forbidden)
        monkeypatch.setattr(profile_service.ProfileService, "__init__", _forbidden)

        for state, expected in (
            (False, "profile create"),
            (True, "db init"),
            (None, "db init"),
        ):
            result = classify_user_error(
                DatabaseNotInitializedError("missing", profile_registered=state)
            )
            assert result is not None
            assert expected in result.message.lower()


class TestDatabaseNotInitializedAnnotation:
    """`get_database` records the profile's setup state on the exception."""

    def test_unregistered_profile_is_marked_and_advised(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from moneybin.config import set_current_profile
        from moneybin.database import get_database

        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        # An active profile with no config.yaml anywhere under tmp_path.
        set_current_profile("ghost")

        with pytest.raises(DatabaseNotInitializedError) as excinfo:
            get_database(read_only=True)

        assert excinfo.value.profile_registered is False
        classified = classify_user_error(excinfo.value)
        assert classified is not None
        assert "profile create" in classified.message

    def test_bare_directory_is_unregistered(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A directory that exists but was never registered. This used to be steered
        # at `db init` because `profile create` refused on the bare directory — a
        # dead end dressed up as advice: `db init` would build a database into a
        # profile that `profile list` still hides and that has no inbox. `create()`
        # completes the directory in place, so the guidance names the verb that
        # actually finishes setup.
        from moneybin.config import set_current_profile
        from moneybin.database import get_database

        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        (tmp_path / "profiles" / "bare").mkdir(parents=True)  # no config.yaml, no db
        set_current_profile("bare")

        with pytest.raises(DatabaseNotInitializedError) as excinfo:
            get_database(read_only=True)

        assert excinfo.value.profile_registered is False
        classified = classify_user_error(excinfo.value)
        assert classified is not None
        assert "profile create" in classified.message

    def test_registered_profile_is_marked_and_advised(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A fully registered profile whose database is missing (deleted, or never
        # init'd). Setup is done; only the database is absent — so `db init` is the
        # right verb, and `profile create` would (correctly) refuse.
        from moneybin.config import set_current_profile
        from moneybin.database import get_database
        from moneybin.services.profile_service import ProfileService

        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        with patch.object(ProfileService, "_init_database"):  # no keychain here
            ProfileService().create("registered")
        set_current_profile("registered")

        with pytest.raises(DatabaseNotInitializedError) as excinfo:
            get_database(read_only=True)

        assert excinfo.value.profile_registered is True
        classified = classify_user_error(excinfo.value)
        assert classified is not None
        assert "profile create" not in classified.message
        assert "db init" in classified.message.lower()

    def test_denied_registration_read_leaves_the_state_unanswered(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An EPERM on the registration probe must not replace the error.

        `Path.exists` swallows only ENOENT-class errors, so a denied read
        (macOS TCC, a locked-down profile root) propagates. If that escaped it
        would turn "database not found" into an unrelated PermissionError —
        the error path is exactly where a second failure must stay quiet.
        """
        from moneybin.config import get_settings, set_current_profile
        from moneybin.database import get_database

        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        set_current_profile("denied")
        get_settings()  # warm the cache before the probe starts raising

        # Only the registration probe is denied. `Database.__init__` reaches
        # `db_path.exists()` first, and denying that too would raise before the
        # annotation ever runs — proving nothing about the guard under test.
        real_exists = Path.exists

        def _denied(self: Path) -> bool:
            if self.name == "config.yaml":
                raise PermissionError(1, "Operation not permitted", str(self))
            return real_exists(self)

        monkeypatch.setattr(Path, "exists", _denied)

        with pytest.raises(DatabaseNotInitializedError) as excinfo:
            get_database(read_only=True)

        assert excinfo.value.profile_registered is None
        classified = classify_user_error(excinfo.value)
        assert classified is not None
        assert "db init" in classified.message.lower()


class TestSecretFamilyClassification:
    """`moneybin.secrets` raises past several boundaries; none of it may crash.

    `moneybin db info` and `db unlock` call `SecretStore.get_key` directly, so
    before MB-51 a missing or keychain-denied key reached `handle_cli_errors`
    unclassified and printed a raw traceback.
    """

    def test_missing_secret_is_setup_guidance(self) -> None:
        from moneybin.secrets import SecretNotFoundError

        result = classify_user_error(SecretNotFoundError("Secret 'X' not found."))
        assert result is not None
        assert result.code == error_codes.INFRA_SETUP_REQUIRED
        assert "not found" in result.message

    def test_denied_keychain_read_is_a_permission_failure(self) -> None:
        """Checked before its `SecretNotFoundError` base, or it would be masked."""
        from moneybin.secrets import SecretUnavailableError

        result = classify_user_error(SecretUnavailableError("keychain locked"))
        assert result is not None
        assert result.code == error_codes.INFRA_PERMISSION_DENIED

    def test_absent_keyring_backend_is_setup_guidance(self) -> None:
        from moneybin.secrets import SecretStorageUnavailableError

        result = classify_user_error(SecretStorageUnavailableError("no backend"))
        assert result is not None
        assert result.code == error_codes.INFRA_SETUP_REQUIRED
