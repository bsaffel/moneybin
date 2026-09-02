"""The suite must not read the developer's own ``MONEYBIN_*`` configuration.

``MoneyBinSettings`` resolves every field from ``MONEYBIN_``-prefixed
environment variables, so a var exported in the developer's shell (or their
launch wrapper) reaches any test that constructs settings without an explicit
override. Those tests then assert against the developer's machine rather than
against the shipped defaults, and they pass in CI — where no such var
exists — while failing locally, or the reverse.

``tests/conftest.py`` clears the whole prefix at import time and re-adds only
the vars the harness itself owns. This guard pins that set: a new harness-owned
var must be added here deliberately, and a leaked ambient var fails loudly with
the offending names.
"""

from __future__ import annotations

import os

# Set by tests/conftest.py: a per-xdist-worker MoneyBin home, and the import
# inbox root beneath it.
HARNESS_OWNED = {"MONEYBIN_HOME", "MONEYBIN_IMPORT___INBOX_ROOT"}


def test_only_harness_owned_moneybin_env_vars_are_visible() -> None:
    """No ambient ``MONEYBIN_*`` var survives into the test process."""
    present = {name for name in os.environ if name.upper().startswith("MONEYBIN_")}
    assert present == HARNESS_OWNED
