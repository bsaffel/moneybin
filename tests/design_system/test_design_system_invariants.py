"""Mechanical invariants for ``design-system/``.

The design system is authored by hand and verified in a browser, so nothing here
is covered by the rest of the suite. Every assertion below encodes a defect that
actually shipped to ``main`` and survived until someone happened to look:

- a component that self-registered on a ``window`` global instead of exporting,
  so it bundled but never resolved (Icon, PR #319);
- a component missing its preview or ``docsMap`` entry, which silently drops it
  from the bundle;
- an exported name list that disagreed with its own ``.d.ts`` union, so the type
  lied about the runtime (``Icon.names`` returned 44 glyphs, the union typed 19);
- specimen cards and docs fetching React, Babel, and fonts from ``unpkg.com`` and
  ``fonts.googleapis.com`` — 17 references, while ``tokens/typography.css`` says
  "no font CDNs in a no-telemetry product";
- a card hardcoding a token's *dark* hex, freezing it there and breaking it under
  ``[data-theme="light"]``;
- doc surfaces enumerating the components without the newest one, so the agents
  that read them never learn it exists and keep inlining one-off SVGs.

Each of those is mechanically checkable and none of them needed judgment to
catch. They shipped because nothing checked.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parents[2]
_DS = _REPO_ROOT / "design-system"
_COMPONENTS = _DS / "components"
_GUIDELINES = _DS / "guidelines"
_CONFIG = _DS / ".design-sync" / "config.json"
_PREVIEWS = _DS / ".design-sync" / "previews"

# Doc surfaces that enumerate the component set. Each is read by an agent — the
# design agent (conventions.md, inlined into its system prompt) or a local one
# (the rest) — and an agent cannot use a component it is never told exists.
_ENUMERATING_DOCS = (
    _DS / ".design-sync" / "conventions.md",
    _DS / "readme.md",
    _DS / "CLAUDE.md",
    _REPO_ROOT / ".claude" / "skills" / "moneybin-design" / "SKILL.md",
)

_CDN_HOSTS = (
    "unpkg.com",
    "fonts.googleapis.com",
    # Google Fonts serves its CSS from googleapis and the font binaries from
    # gstatic — a regression could reach the asset host without naming the other.
    "fonts.gstatic.com",
    "cdn.jsdelivr.net",
    "cdnjs.cloudflare.com",
)

_HEX = re.compile(r"#[0-9A-Fa-f]{3,8}\b")

# A hex is *applied* when it paints something — in a ``style`` attribute or an SVG
# presentation attribute. A hex merely printed as text (a swatch's own label) is
# inert; it is the applied value that freezes a card to one theme.
_PRESENTATION_ATTRS = frozenset({
    "stroke",
    "fill",
    "color",
    "stop-color",
    "flood-color",
    "lighting-color",
})

# The escape hatch. Two cards legitimately apply literal colors: a paint chip whose
# whole job is to *be* the value it documents, and the brand plates, which render the
# mark on the dark surface and the light surface at once (a token resolves to one
# value per theme, so it structurally cannot express both). Earlier revisions of this
# gate tried to *infer* which literals were legitimate from page structure — is the
# element textless? does its cell print the value? — and leaked four different ways,
# once hiding a real theme-freeze bug. Intent is not recoverable from structure, so
# the card declares it: every literal color is spelled out on the element that paints
# it, and an undeclared literal is a failure. Nothing is inferred, so nothing leaks.
_DECLARATION_ATTR = "data-literal-color"


class _CardColors(HTMLParser):
    """Audit a card's literal colors against what each element declares."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.undeclared: set[str] = set()  # painted, never declared
        self.stale: set[str] = set()  # declared, no longer painted
        self.in_stylesheet: set[str] = set()  # painted from a <style> block
        self._in_style = False

    def _audit(self, attrs: list[tuple[str, str | None]]) -> None:
        painted: set[str] = set()
        declared: set[str] = set()
        for key, value in attrs:
            if value is None:
                continue
            name = key.lower()
            if name == "style" or name in _PRESENTATION_ATTRS:
                painted |= {h.upper() for h in _HEX.findall(value)}
            elif name == _DECLARATION_ATTR:
                declared |= {h.upper() for h in _HEX.findall(value)}
        self.undeclared |= painted - declared
        self.stale |= declared - painted

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "style":
            self._in_style = True
        self._audit(attrs)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._audit(attrs)  # self-closing (SVG <line/>) — no children to descend into

    def handle_endtag(self, tag: str) -> None:
        if tag == "style":
            self._in_style = False

    def handle_data(self, data: str) -> None:
        # A <style> block paints elements it never names, so no element can declare
        # its colors. Tokens only — there is no escape hatch here by construction.
        if self._in_style:
            self.in_stylesheet |= {h.upper() for h in _HEX.findall(data)}


def _card_colors(html: str) -> _CardColors:
    audit = _CardColors()
    audit.feed(html)
    audit.close()
    return audit


def _component_jsx() -> list[Path]:
    """Every component source, e.g. components/core/Icon.jsx."""
    return sorted(p for p in _COMPONENTS.glob("*/*.jsx"))


def _component_names() -> list[str]:
    return [p.stem for p in _component_jsx()]


def test_components_exist() -> None:
    """Guard the discovery helper itself — an empty glob would vacuously pass every test below."""
    names = _component_names()
    assert len(names) >= 9, f"expected the full component set, found {names}"
    assert "Icon" in names


def test_guideline_cards_exist() -> None:
    """Same guard for the cards: a parametrized test over an empty glob passes silently."""
    cards = sorted(p.name for p in _GUIDELINES.glob("*.html"))
    assert len(cards) >= 29, (
        f"expected the full specimen set, found {len(cards)}: {cards}"
    )
    assert "icons-grammar.html" in cards


@pytest.mark.parametrize("jsx", _component_jsx(), ids=lambda p: p.stem)
def test_component_ships_the_full_quartet(jsx: Path) -> None:
    """A component needs all four artifacts or it silently drops out of the bundle.

    Missing ``.d.ts``/``.prompt.md`` leaves the design agent without an API contract
    or usage docs; a missing preview or ``docsMap`` entry means the converter never
    emits the component at all — with no error.
    """
    name = jsx.stem
    docs_map = json.loads(_CONFIG.read_text())["docsMap"]

    assert jsx.with_suffix(".d.ts").is_file(), f"{name}: missing {name}.d.ts"
    assert (jsx.parent / f"{name}.prompt.md").is_file(), (
        f"{name}: missing {name}.prompt.md"
    )
    assert (_PREVIEWS / f"{name}.tsx").is_file(), (
        f"{name}: missing .design-sync/previews/{name}.tsx — without it the converter "
        f"does not bundle {name}"
    )
    assert name in docs_map, (
        f"{name}: missing a docsMap entry in .design-sync/config.json — without it "
        f"{name}.prompt.md never reaches the design agent"
    )
    # The *value* matters, not just the key: the converter reads the mapped path to
    # decide which docs reach the design agent, so a copy-pasted `"Icon":
    # "components/core/Button.prompt.md"` would hand the agent the wrong API — the
    # same silent failure, one layer down.
    expected_docs = jsx.relative_to(_DS).with_suffix(".prompt.md").as_posix()
    assert docs_map[name] == expected_docs, (
        f"{name}: docsMap points at {docs_map[name]!r}, not {expected_docs!r} — the "
        f"design agent would read the wrong component's usage docs"
    )


@pytest.mark.parametrize("jsx", _component_jsx(), ids=lambda p: p.stem)
def test_component_is_an_esm_export(jsx: Path) -> None:
    """Components must ``export``, not self-register on a window global.

    Source authored in the design tool registers itself on ``window.<Something>``
    and has no ESM export. It bundles without complaint and then never resolves as
    ``MoneyBinDS.<Name>`` — a silent failure. (Icon arrived exactly this way.)
    """
    source = jsx.read_text()
    name = jsx.stem

    # Named export specifically: the converter promotes PascalCase *named* exports
    # onto the bundled global, so `export default function Icon` would land at
    # MoneyBinDS.default — the same "bundles but never resolves" failure.
    assert re.search(rf"export\s+function\s+{name}\b", source), (
        f"{name}: no `export function {name}` — a default export or a window-global "
        f"registration bundles but never resolves as MoneyBinDS.{name}"
    )
    assert "window.MoneyBin" not in source, (
        f"{name}: registers on a window global; the bundler wraps globals itself"
    )


def test_icon_names_match_the_typed_union() -> None:
    """``Icon.names`` must equal the ``IconName`` union — or the type lies about the runtime.

    Icon deliberately carries a dormant reserve set beyond the shipped vocabulary.
    The reserve renders if asked for, but must stay out of both the public
    enumeration and the type; when it leaked into ``Icon.names`` the array returned
    44 names while the union declared 19.
    """
    jsx = (_COMPONENTS / "core" / "Icon.jsx").read_text()
    # Strip `//` comments first: a comment in the union reads "disclosure; rotate via
    # direction", and that semicolon would end the match early, silently truncating it.
    dts = re.sub(r"//[^\n]*", "", (_COMPONENTS / "core" / "Icon.d.ts").read_text())

    # Pin the PUBLIC export, not just the vetted list behind it. `Icon.names` is what
    # callers read; if that assignment regressed to `Object.keys(GLYPHS)` while
    # CORE_NAMES stayed correct, the runtime would hand back all 44 glyphs again and
    # a CORE_NAMES-only check would sail through — the original bug, returning green.
    export = re.search(r"Icon\.names\s*=\s*([^;\n]+)", jsx)
    assert export, "Icon.jsx: no `Icon.names = ...` export found"
    assert export.group(1).strip() == "CORE_NAMES", (
        f"Icon.names is assigned {export.group(1).strip()!r}, not CORE_NAMES — the "
        f"public vocabulary must be the vetted list, not every drawn glyph"
    )

    core_block = re.search(r"const CORE_NAMES\s*=\s*\[(.*?)\]", jsx, re.S)
    assert core_block, "Icon.jsx: CORE_NAMES not found"
    core_names = set(re.findall(r"'([a-z-]+)'", core_block.group(1)))

    union_block = re.search(r"export type IconName\s*=(.*?);", dts, re.S)
    assert union_block, "Icon.d.ts: IconName union not found"
    union_names = set(re.findall(r"'([a-z-]+)'", union_block.group(1)))

    assert core_names == union_names, (
        "Icon.names and the IconName union disagree — promote a reserve glyph into "
        f"BOTH or neither. only in CORE_NAMES: {sorted(core_names - union_names)}; "
        f"only in IconName: {sorted(union_names - core_names)}"
    )


def test_no_cdn_fetches() -> None:
    """Nothing in the design system may fetch from a CDN at render time.

    ``tokens/typography.css`` states the rule outright — "no font CDNs in a
    no-telemetry product" — yet six tracked files pulled React, Babel, and fonts
    from unpkg and Google Fonts. Opening any of them hit the network.
    """
    offenders: list[str] = []
    for path in _DS.rglob("*"):
        # Skip build output and vendored converter tooling. `ds-bundle*` (not an
        # exact name) because the build falls back to `ds-bundle-out/` when the
        # primary dir is locked, and the vendored React in there is full of CDN
        # strings that are not ours.
        if not path.is_file() or ".ds-sync" in path.parts:
            continue
        if any(part.startswith("ds-bundle") for part in path.parts):
            continue
        if path.suffix not in {
            ".html",
            ".css",
            ".js",
            ".jsx",
            ".ts",
            ".tsx",
            ".md",
            ".json",
        }:
            continue
        text = path.read_text(errors="ignore")
        for host in _CDN_HOSTS:
            # NOTES.md documents *why* the CDN fetches were removed; prose may name them.
            if host in text and path.name != "NOTES.md":
                offenders.append(f"{path.relative_to(_REPO_ROOT)} -> {host}")

    assert not offenders, (
        "design-system must render offline from local assets (no-telemetry rule). "
        f"CDN references found: {offenders}"
    )


@pytest.mark.parametrize(
    "card", sorted(_GUIDELINES.glob("*.html")), ids=lambda p: p.name
)
def test_guideline_card_contract(card: Path) -> None:
    """Every specimen card declares itself and renders its glyphs correctly.

    The ``@dsCard`` first line is what registers the card in the Design System pane.
    Without ``<meta charset>`` the signed amounts and glyphs the system depends on
    (``−`` ``·`` ``▲▼`` ``▸_``) render as mojibake.
    """
    text = card.read_text()
    first_line = text.splitlines()[0] if text.splitlines() else ""

    assert first_line.lstrip().startswith("<!-- @dsCard"), (
        f"{card.name}: first line must be the @dsCard marker — the pane builds its "
        f"card index from it"
    )
    assert '<meta charset="utf-8">' in text, (
        f'{card.name}: missing <meta charset="utf-8"> — −·▲▼▸ render as mojibake'
    )


@pytest.mark.parametrize(
    "card", sorted(_GUIDELINES.glob("*.html")), ids=lambda p: p.name
)
def test_card_declares_every_literal_color(card: Path) -> None:
    """A card paints with tokens, or it declares the literal it paints with.

    An undeclared literal freezes the card at one theme. ``icons-grammar.html``
    hardcoded ``#A39C90`` — ``--text-secondary`` at its *dark* value — so it kept
    rendering dark-on-light under ``[data-theme="light"]``; ``colors-brass.html``
    hardcoded ``color:#141311`` on a brass fill, contradicting the very ``Button`` it
    demonstrates (which reads ``--on-accent-brass``, and inverts on light theme).

    A few literals are legitimate — a paint chip *is* the value it documents, and the
    brand plates render dark and light at once. Rather than infer which ones those are
    (four leaks and counting), the card names them: ``data-literal-color`` on the
    element that paints them, listing exactly the hexes in its own ``style``. Stale
    declarations fail too, so editing a color forces you to re-affirm it is deliberate.
    """
    audit = _card_colors(card.read_text())

    assert not audit.undeclared, (
        f"{card.name}: paints {sorted(audit.undeclared)} with no {_DECLARATION_ATTR} "
        f"declaration. Use var(--*) or currentColor — a literal freezes the element "
        f'under [data-theme="light"]. If the literal is deliberate (a swatch chip, a '
        f'brand plate), declare it: {_DECLARATION_ATTR}="{sorted(audit.undeclared)[0]}".'
    )
    assert not audit.stale, (
        f"{card.name}: declares {sorted(audit.stale)} via {_DECLARATION_ATTR} but no "
        f"longer paints it — drop the stale declaration so the next edit still has to "
        f"justify its literals."
    )
    assert not audit.in_stylesheet, (
        f"{card.name}: its <style> block paints {sorted(audit.in_stylesheet)}. A "
        f"stylesheet rule styles elements it never names, so no element can declare "
        f"the literal — use var(--*) here, always."
    )


@pytest.mark.parametrize("doc", _ENUMERATING_DOCS, ids=lambda p: p.name)
def test_docs_enumerate_every_component(doc: Path) -> None:
    """Every doc that lists the components must list *all* of them.

    These are the surfaces an agent reads to learn what exists, and an agent cannot
    use what it is never told about: while Icon was absent from these lists, agents
    kept inlining one-off SVGs — the exact practice Icon was added to end. Wordmark
    sat missing from one of them for weeks, unnoticed.
    """
    text = doc.read_text()
    # Word-boundary, not substring: `"Icon" in text` is satisfied by the prose word
    # "Iconography" alone, so a doc could drop its real `Icon` reference and still
    # pass — silently reintroducing the very staleness this test exists to catch.
    missing = [
        name for name in _component_names() if not re.search(rf"\b{name}\b", text)
    ]
    assert not missing, (
        f"{doc.relative_to(_REPO_ROOT)} does not mention {missing}. An agent reading "
        f"this file will not know {'they exist' if len(missing) > 1 else 'it exists'}."
    )
