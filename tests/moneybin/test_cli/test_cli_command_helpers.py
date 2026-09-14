"""Unit tests for the shared ``assert_published_commands_resolve`` helper.

The helper itself has one behavioral edge worth pinning directly, rather
than only through the doctor-service and recovery-hint tests that consume
it: a resolved command GROUP is a failure only when invoking it bare does
nothing. Most groups in this codebase are ``no_args_is_help=True`` and
print help on their own — but a few (``import inbox``, ``mcp config``) are
``invoke_without_command=True`` and run a real default action instead, and
the helper must not reject those.
"""

from __future__ import annotations

import pytest

from tests.cli_command_helpers import assert_published_commands_resolve


def test_a_real_leaf_command_passes() -> None:
    assert_published_commands_resolve("`moneybin transform apply`")


def test_a_bare_group_with_no_default_action_fails() -> None:
    with pytest.raises(AssertionError, match="command GROUP"):
        assert_published_commands_resolve("`moneybin transform`")


def test_a_group_that_runs_bare_passes() -> None:
    """``import inbox`` is ``invoke_without_command=True`` — it drains the inbox."""
    assert_published_commands_resolve("`moneybin import inbox`")
