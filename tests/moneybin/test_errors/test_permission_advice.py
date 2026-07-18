"""Table-driven tests for the permission-failure advice policy.

Split deliberately from OS behavior: this file tests OUR branching given
(errno, platform, path). The claim that macOS reports EPERM for a TCC denial
is an observed fact recorded in the design doc, verified by hand — it is not
asserted here, because a test that fabricates the input cannot verify the OS.
"""

from pathlib import Path

import pytest

from moneybin.errors import permission_advice

EACCES = 13
EPERM = 1


def test_eacces_advises_ownership_and_mode() -> None:
    hint, details = permission_advice(EACCES, "Darwin", Path.home() / "Documents/a.pdf")
    assert "chmod" in hint or "ownership" in hint
    assert "Full Disk Access" not in hint
    assert details["errno"] == EACCES


def test_eperm_under_protected_root_on_macos_advises_full_disk_access() -> None:
    hint, details = permission_advice(EPERM, "Darwin", Path.home() / "Documents/a.pdf")
    assert "Full Disk Access" in hint
    assert "Privacy & Security" in hint
    assert details["protected_root"] == "~/Documents"
    assert details["platform"] == "Darwin"


@pytest.mark.parametrize("root", ["Documents", "Desktop", "Downloads"])
def test_all_three_protected_roots_are_recognized(root: str) -> None:
    hint, details = permission_advice(EPERM, "Darwin", Path.home() / root / "a.pdf")
    assert "Full Disk Access" in hint
    assert details["protected_root"] == f"~/{root}"


def test_eperm_outside_protected_root_stays_generic() -> None:
    """Not every EPERM is TCC — immutable flags and sandboxes produce it too.

    The path sits under home but outside the three gated roots, so it fails the
    root check alone — a path off home entirely would also fail it for being
    off home, proving less about which condition fired.
    """
    hint, details = permission_advice(EPERM, "Darwin", Path.home() / "Music/a.pdf")
    assert "Full Disk Access" not in hint
    assert details.get("protected_root") is None


def test_eperm_without_a_path_stays_generic() -> None:
    """An exception carrying no filename must not be judged by the cwd.

    Falling back to `Path()` would hand the Full-Disk-Access advice to any
    EPERM that happened to be raised while the working directory sat under a
    protected root — advice aimed at a path that never failed.
    """
    hint, details = permission_advice(EPERM, "Darwin", None)
    assert "Full Disk Access" not in hint
    assert details.get("protected_root") is None


def test_eperm_on_linux_stays_generic() -> None:
    hint, _ = permission_advice(EPERM, "Linux", Path.home() / "Documents/a.pdf")
    assert "Full Disk Access" not in hint
    assert "System Settings" not in hint
