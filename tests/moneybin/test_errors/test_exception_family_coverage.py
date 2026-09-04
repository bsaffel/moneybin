"""Every domain exception family reaches a user as a message, not a traceback.

`UserError` is the taxonomy the CLI and MCP surfaces know how to present.
Domain modules deliberately raise their *own* families instead — a caller that
wants to react to a locked database catches `DatabaseLockError`, not a string
code. That second pattern only works while `classify_user_error` has a branch
for each family: a family it does not recognize propagates unclassified, which
on the CLI is a raw traceback and on MCP is `infra_unclassified_error`.

Nothing forced that correspondence before, and `moneybin.secrets` had drifted
out of it. This test enumerates the families off the modules themselves, so a
class added later fails here rather than in front of a user.

The constructor map is checked by set equality against what the modules
actually define — a new exception class cannot be classified-by-omission.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from types import ModuleType

import pytest

from moneybin import database, secrets
from moneybin.errors import classify_user_error
from moneybin.matching import application as matching_application

# One constructor per family member. Values are the arguments each class needs;
# the message text is irrelevant here, only that classification recognizes it.
FAMILY_CONSTRUCTORS: dict[str, Callable[[], BaseException]] = {
    "DatabaseCryptoError": lambda: database.DatabaseCryptoError("no crypto module"),
    "DatabaseKeyError": lambda: database.DatabaseKeyError("key unreadable"),
    "DatabaseLockError": lambda: database.DatabaseLockError("lock held"),
    "DatabaseNotInitializedError": lambda: database.DatabaseNotInitializedError(
        "no database"
    ),
    "SchemaDriftError": lambda: database.SchemaDriftError("stale snapshot"),
    "SecretNotFoundError": lambda: secrets.SecretNotFoundError("no such secret"),
    "SecretStorageUnavailableError": lambda: secrets.SecretStorageUnavailableError(
        "no keyring backend"
    ),
    "SecretUnavailableError": lambda: secrets.SecretUnavailableError("keychain locked"),
    "MatchDecisionNotFoundError": lambda: (
        matching_application.MatchDecisionNotFoundError("match-1")
    ),
    "MatchDecisionStateError": lambda: matching_application.MatchDecisionStateError(
        "match-1", "accepted", "rejected"
    ),
}

GUARDED_MODULES = (database, secrets, matching_application)


def _families_defined_in(module: ModuleType) -> set[str]:
    """Exception classes the module itself defines, not ones it imports."""
    return {
        name
        for name, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, BaseException) and obj.__module__ == module.__name__
    }


def test_constructor_map_covers_exactly_the_declared_families() -> None:
    """The map is an enumeration, not an allowlist — drift fails here first."""
    declared: set[str] = set()
    for module in GUARDED_MODULES:
        declared |= _families_defined_in(module)
    assert declared == set(FAMILY_CONSTRUCTORS), (
        "the exception families in "
        f"{[m.__name__ for m in GUARDED_MODULES]} changed. Add the new class to "
        "FAMILY_CONSTRUCTORS (and give it a classify_user_error branch), or drop "
        "the entry for a class that no longer exists."
    )


@pytest.mark.parametrize("family", sorted(FAMILY_CONSTRUCTORS))
def test_family_is_classified(family: str) -> None:
    classified = classify_user_error(FAMILY_CONSTRUCTORS[family]())
    assert classified is not None, (
        f"{family} reaches the CLI as a raw traceback and MCP as "
        "infra_unclassified_error. Give it a classify_user_error branch."
    )
    assert classified.message, f"{family} classifies to an empty message"
    assert classified.code, f"{family} classifies without a code"
