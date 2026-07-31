"""Scenario: re-import identity follows a file's bytes, not the name it has.

Duplicate detection used to key on the file's *path*. Two ordinary habits broke
it in opposite directions, and each needs its own assertion here because a
fixture that only moves one way would leave the other regression uncaught:

* saving the same statement twice (``statement (1).qfx``) read as a brand-new
  file and imported the same transactions again;
* reusing one filename across months — saving July's statement over June's —
  read as already-imported and was refused.

Both run through the real ``ImportService.import_file``, so this covers the
digest actually reaching ``raw.import_log`` and being compared on the next
import, which the unit tests around ``find_existing_import`` cannot show.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from moneybin.services.import_service import ImportService
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env

_FIXTURES = (
    Path(__file__).parent / "data" / "fixtures" / "account-identity-cross-source"
)


@pytest.mark.scenarios
@pytest.mark.slow
def test_reimport_identity_follows_content_not_path(tmp_path: Path) -> None:
    """Same bytes are one document anywhere; new bytes are new everywhere."""
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, _env):
        svc = ImportService(db)

        downloaded = tmp_path / "statement.qfx"
        shutil.copy(_FIXTURES / "wf_checking.qfx", downloaded)
        svc.import_file(downloaded, refresh=False)

        # Same bytes, second download -> the browser's "(1)" suffix. A path key
        # calls this new and double-imports; content identity recognizes it.
        second_download = tmp_path / "statement (1).qfx"
        shutil.copy(_FIXTURES / "wf_checking.qfx", second_download)
        with pytest.raises(ValueError, match="already imported"):
            svc.import_file(second_download, refresh=False)

        # Next month's statement saved over the same filename. A path key
        # refuses this as a duplicate; the bytes say otherwise.
        shutil.copy(_FIXTURES / "wf_savings.qfx", downloaded)
        svc.import_file(downloaded, refresh=False)

        # Two distinct documents landed, and the redundant download did not.
        digests = db.execute(
            "SELECT COUNT(DISTINCT file_sha256) FROM raw.import_log "
            "WHERE status = 'complete'"
        ).fetchone()
        assert digests is not None and digests[0] == 2, digests
