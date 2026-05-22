"""Property test: every UserError raise populates recovery_actions or opts out explicitly.

This test starts SKIPPED in PR 2 and is enabled per-domain as PRs 9a-N
retrofit each error site. Domains flip ENABLED_DOMAINS to True as their retrofit PR lands.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

# Domains migrate one at a time. Until a domain is True, its UserError
# raises are not checked.
ENABLED_DOMAINS: dict[str, bool] = {
    "import": False,  # PR 9a
    "categorize": False,  # PR 9b
    "accounts": False,  # PR 9c
    "curation": False,  # PR 9d
    "budgets": False,  # PR 9e
    "transform": False,  # PR 9f
}


SRC_ROOT = pathlib.Path(__file__).parent.parent.parent.parent / "src" / "moneybin"


def _user_error_call_sites() -> list[tuple[pathlib.Path, int, ast.Call]]:
    """Walk the source tree, return every (file, line, AST node) for UserError(...) calls."""
    sites: list[tuple[pathlib.Path, int, ast.Call]] = []
    for path in SRC_ROOT.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "UserError"
            ):
                sites.append((path, node.lineno, node))
    return sites


def _site_domain(path: pathlib.Path) -> str | None:
    """Map a file path to one of ENABLED_DOMAINS keys.

    Scans MCP tools (`mcp/tools/*.py`), service modules
    (`services/*.py` and `services/<group>/*.py`), and CLI commands
    (`cli/commands/*.py` and `cli/commands/<group>/*.py`). Domain
    classification is by filename/parent-directory prefix.

    Returns None for files outside the scoped trees (e.g. extractors,
    matching, repositories) — those error sites are not yet gated by
    this property test.
    """
    parts = path.parts
    stem = path.stem
    parent = path.parent.name

    def _classify(name: str) -> str | None:
        if name.startswith("import"):
            return "import"
        if name.startswith("transactions_categorize") or name.startswith("categoriz"):
            return "categorize"
        if name.startswith("account"):
            return "accounts"
        if "tags" in name or "notes" in name or "splits" in name or "curation" in name:
            return "curation"
        if name.startswith("budget"):
            return "budgets"
        if name.startswith("transform"):
            return "transform"
        return None

    # MCP tools: src/moneybin/mcp/tools/*.py
    if "mcp" in parts and "tools" in parts:
        return _classify(stem)

    # Services: src/moneybin/services/*.py (flat) or
    #           src/moneybin/services/<group>/*.py (packaged, e.g. categorization/)
    if "services" in parts:
        return _classify(stem) or _classify(parent)

    # CLI commands: src/moneybin/cli/commands/*.py or
    #               src/moneybin/cli/commands/<group>/*.py
    if "cli" in parts and "commands" in parts:
        return _classify(stem) or _classify(parent)

    return None


def _has_recovery_actions_kwarg(call: ast.Call) -> bool:
    return any(kw.arg == "recovery_actions" for kw in call.keywords)


def _code_kwarg(call: ast.Call) -> str | None:
    for kw in call.keywords:
        if kw.arg == "code":
            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                return kw.value.value
            if isinstance(kw.value, ast.Attribute):
                # error_codes.SOMETHING — return the name
                return kw.value.attr.lower()
            if isinstance(kw.value, ast.Name):
                # Bare-name reference (e.g. `from moneybin.error_codes import
                # RECOVERY_NO_PATH; ... code=RECOVERY_NO_PATH`). Lower the
                # constant name and trust the project's NAME == VALUE.upper()
                # convention enforced by test_error_codes.
                return kw.value.id.lower()
    return None


@pytest.mark.parametrize(
    "domain", [d for d, enabled in ENABLED_DOMAINS.items() if enabled]
)
def test_every_user_error_site_handles_recovery(domain: str) -> None:
    """Within enabled domains, every UserError raise either has recovery_actions or uses RECOVERY_NO_PATH."""
    violations: list[str] = []
    for path, line, call in _user_error_call_sites():
        if _site_domain(path) != domain:
            continue
        if _has_recovery_actions_kwarg(call):
            continue
        code = _code_kwarg(call)
        if code == "recovery_no_path":
            continue
        violations.append(f"{path.relative_to(SRC_ROOT)}:{line} (code={code!r})")

    assert not violations, (
        f"UserError raises in domain '{domain}' missing recovery_actions and "
        f"not using RECOVERY_NO_PATH:\n  " + "\n  ".join(violations)
    )


def test_at_least_one_domain_enabled_someday() -> None:
    """Reminder: as PRs 9a-N land, flip ENABLED_DOMAINS entries to True."""
    if not any(ENABLED_DOMAINS.values()):
        pytest.skip("No domains enabled yet — flip in PRs 9a-N as retrofits land")
