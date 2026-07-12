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
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parents[2]
_DS = _REPO_ROOT / "design-system"
_COMPONENTS = _DS / "components"
_GUIDELINES = _DS / "guidelines"
_TOKENS = _DS / "tokens"
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

# Two — and only two — reasons a card may apply a literal hex. They are kept
# apart on purpose: a single blanket per-file exemption is what let a real
# theme-freeze bug hide inside colors-brass.html (a CTA sample hardcoding
# `color:#141311` on a brass fill, which stays near-black on light theme's darker
# brass instead of following --on-accent-brass).

# 1. Swatch cards: a chip painted with the literal value it is documenting. The
#    hex is the *subject*, so the card must also print it — that is what makes it
#    a swatch rather than a hardcoded style. Anything applied but NOT printed is
#    ordinary UI chrome and must use tokens like any other card.
_SWATCH_CARDS = frozenset({
    "colors-brass.html",
    "colors-chart.html",
    "colors-dark.html",
    "colors-light.html",
    "colors-semantic.html",
})

# 2. Dual-plate brand cards: they render the mark on the dark plate AND the light
#    plate side by side in one card. A token resolves to exactly one value per
#    theme, so it structurally cannot express "both at once".
_DUAL_PLATE_CARDS = frozenset({
    "brand-logo.html",
    "brand-duckkey.html",
})

_CDN_HOSTS = (
    "unpkg.com",
    "fonts.googleapis.com",
    # Google Fonts serves its CSS from googleapis and the font binaries from
    # gstatic — a regression could reach the asset host without naming the other.
    "fonts.gstatic.com",
    "cdn.jsdelivr.net",
    "cdnjs.cloudflare.com",
)

# A hex is "applied" when it is *rendered* — inside a style attribute, a <style>
# block, or an SVG presentation attribute. A hex that is merely displayed (a
# swatch's own text label) is fine; it is the applied value that freezes a card
# to one theme.
#
# Match on structure, not on a property-name list: `border:1px solid #2A2723`
# puts the hex three tokens after the colon, and attributes may be single-quoted,
# so a "property, then colon, then hex" pattern silently misses both.
_STYLE_ATTR = re.compile(r"""style\s*=\s*(["'])(.*?)\1""", re.I | re.S)
_STYLE_BLOCK = re.compile(r"<style[^>]*>(.*?)</style>", re.I | re.S)
_PRESENTATION_ATTR = re.compile(
    r"""\b(?:stroke|fill|color|stop-color|flood-color|lighting-color)\s*=\s*"""
    r"""(["'])\s*(#[0-9A-Fa-f]{3,8})\s*\1""",
    re.I,
)
_HEX = re.compile(r"#[0-9A-Fa-f]{3,8}\b")


def _applied_hex(html: str) -> list[str]:
    """Every hex color the card actually renders with (not ones it merely prints)."""
    applied: list[str] = []
    for _quote, body in _STYLE_ATTR.findall(html):
        applied += _HEX.findall(body)
    for body in _STYLE_BLOCK.findall(html):
        applied += _HEX.findall(body)
    applied += [hex_value for _quote, hex_value in _PRESENTATION_ATTR.findall(html)]
    return applied


@dataclass
class _Frame:
    """One open element: the hexes it applies, and whether it carries text."""

    tag: str
    hexes: set[str]
    has_text: bool = False


class _SwatchAudit(HTMLParser):
    """Attribute each applied hex to the element applying it, and note if it has text.

    A swatch *chip* paints the literal value it documents and carries no text of its
    own — the label lives beside it. So a hex applied to an element that has text is
    UI chrome, no matter what the rest of the card prints.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._stack: list[_Frame] = []
        self.chip_hexes: set[str] = set()  # applied by an element with no text
        self.chrome_hexes: set[str] = set()  # applied by an element that has text
        self.printed: set[str] = set()  # rendered as visible text

    def _hexes(self, attrs: list[tuple[str, str | None]]) -> set[str]:
        values = " ".join(v for _k, v in attrs if v)
        return {h.upper() for h in _HEX.findall(values)}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._stack.append(_Frame(tag=tag, hexes=self._hexes(attrs)))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # Self-closing (SVG <line/>, <rect/>) can never hold text, so it is a chip.
        self.chip_hexes |= self._hexes(attrs)

    def handle_data(self, data: str) -> None:
        if not self._stack:
            return
        # A <style>/<script> body is code, not a printed label — and a hex in there
        # styles the whole card, so it is chrome by definition.
        if any(f.tag in {"style", "script"} for f in self._stack):
            self.chrome_hexes |= {h.upper() for h in _HEX.findall(data)}
            return
        if data.strip():
            self._stack[-1].has_text = True
            self.printed |= {h.upper() for h in _HEX.findall(data)}

    def _close_frame(self, frame: _Frame) -> None:
        if frame.has_text:
            self.chrome_hexes |= frame.hexes
        else:
            self.chip_hexes |= frame.hexes

    def handle_endtag(self, tag: str) -> None:
        while self._stack:
            frame = self._stack.pop()
            self._close_frame(frame)
            if frame.tag == tag:
                break

    def finish(self) -> None:
        """Drain any unclosed tags (void elements like <br>) — none of them hold text."""
        self.close()
        while self._stack:
            self._close_frame(self._stack.pop())


def _swatch_violations(html: str) -> tuple[set[str], set[str]]:
    """(chrome, undocumented) — hexes on a text-bearing element, and chips never printed."""
    audit = _SwatchAudit()
    audit.feed(html)
    audit.finish()
    return audit.chrome_hexes, audit.chip_hexes - audit.printed


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
    "card",
    sorted(
        p
        for p in _GUIDELINES.glob("*.html")
        if p.name not in _SWATCH_CARDS and p.name not in _DUAL_PLATE_CARDS
    ),
    ids=lambda p: p.name,
)
def test_guideline_card_uses_tokens_not_hardcoded_hex(card: Path) -> None:
    """An applied hex freezes a card at one theme.

    A hardcoded ``#A39C90`` is not merely a style violation: it is ``--text-secondary``
    at its *dark* value, so the card keeps rendering dark-on-light under
    ``[data-theme="light"]``.
    """
    applied = _applied_hex(card.read_text())
    assert not applied, (
        f"{card.name}: applies hardcoded hex {sorted(set(applied))} — use var(--*) or "
        f'currentColor, or the card breaks under [data-theme="light"]'
    )


@pytest.mark.parametrize(
    "card", sorted(_GUIDELINES.glob("*.html")), ids=lambda p: p.name
)
def test_swatch_card_only_hardcodes_the_value_it_documents(card: Path) -> None:
    """A swatch may paint its own literal value — and nothing else.

    Exempting a whole *file* is too coarse: it hides ordinary UI chrome that happens
    to sit in a color card. ``colors-brass.html`` shipped a CTA sample hardcoding
    ``color:#141311`` on a ``var(--accent-brass)`` fill; on light theme the brass
    darkens to ``#8A6A1C`` while the text stayed near-black, so the sample
    contradicted the very ``Button`` it demonstrates (which reads
    ``--on-accent-brass``).

    The exemption is bound to the swatch *chip*, not to the file. A file-wide rule
    ("this hex is printed somewhere in the card") is still too loose: the swatches
    print ``#C79B3B``, so a CTA sample doing ``background:#C79B3B`` would inherit
    their documentation and freeze to one theme anyway. A chip is an element that
    paints the literal value and carries no text of its own; anything with text is
    chrome and must use tokens.

    Dual-plate brand cards are the one true whole-file exemption.
    """
    if card.name in _DUAL_PLATE_CARDS:
        pytest.skip(
            "renders the dark and light plates together; no token expresses both"
        )

    chrome, undocumented = _swatch_violations(card.read_text())
    assert not chrome, (
        f"{card.name}: {sorted(chrome)} is applied to an element that has text, so it "
        f"is UI chrome, not a swatch chip. Use var(--*) — a hardcoded value freezes it "
        f'under [data-theme="light"].'
    )
    assert not undocumented, (
        f"{card.name}: applies {sorted(undocumented)} without printing it, so it is "
        f"styling, not a swatch. Use var(--*)."
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
