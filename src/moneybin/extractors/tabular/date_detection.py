"""Date format detection, DD/MM disambiguation, and number format detection.

Handles the nuances of international date and number conventions that
trip up every CSV importer. Uses positional value analysis for date
disambiguation and convention scoring for number format detection.
"""

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Sequence

    from moneybin.extractors.tabular.formats import (
        ConfidenceType,
        NumberFormatType,
    )

_CURRENCY_SYMBOLS = re.compile(
    r"[$€£¥₩₹₽₺₫kr\s]|CHF|R\$|kr\b|SEK|NOK|DKK", re.IGNORECASE
)

# Currency symbols only — no \s, so thousands-separator spaces are preserved
# for swiss_french number format detection.
_CURRENCY_SYMBOLS_NO_SPACE = re.compile(
    r"[$€£¥₩₹₽₺₫]|CHF|R\$|kr\b|SEK|NOK|DKK", re.IGNORECASE
)

_DATE_FORMATS: list[str] = [
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%m/%d/%y",
    "%m-%d-%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%d-%b-%Y",
    "%d-%B-%Y",
    "%b %d, %Y",
]

_MIN_YEAR = 1970

# Share of non-empty values a detected candidate must read. This is a
# confidence bar for *guessing*: the detector picks among candidates with no
# input from the caller, so it should only choose one that reads almost
# everything.
_MIN_PARSE_RATE = 0.9

# The bar a caller-supplied --date-format clears instead. Deliberately lower,
# because it answers a different question: not "am I confident enough to pick
# this unasked" but "does this format read the column at all". The transform
# imports valid rows and records the rest in rows_rejected, which `import
# status` surfaces — so partial parsing is a visible outcome, and holding an
# explicit override to the detector's bar made a file with 8 good dates in 10
# unimportable by either route. Below a majority the format is more likely
# wrong than the data dirty, which is the zero-row import this gate exists to
# stop.
_MIN_OVERRIDE_PARSE_RATE = 0.5


def _max_year() -> int:
    return datetime.now().year + 1


def format_parses(values: "Sequence[str | None]", fmt: str) -> bool:
    """Report whether `fmt` reads enough of `values` to be usable.

    The detector carries a fixed candidate list, so a real format it has no
    entry for (`%Y%m%d`) can only arrive as a caller override. Checking it here
    keeps that escape hatch from becoming a second route to a zero-row import:
    a format nothing parses is refused rather than carried into the loader.

    Held to `_MIN_OVERRIDE_PARSE_RATE`, not the detector's own bar — see that
    constant for why the two differ. A file whose date column is merely dirty
    still imports, with the unparsed rows counted in `rows_rejected`.
    """
    clean = [value.strip() for value in values if value and value.strip()]
    if not clean:
        return False
    parsed = 0
    for value in clean:
        try:
            datetime.strptime(value, fmt)
        except ValueError:
            continue
        except re.error:
            # strptime compiles the format into a regex, so a format that
            # repeats a directive ("%Y %Y") fails as a duplicate group name —
            # re.error, which is NOT a ValueError. Catching only ValueError let
            # a malformed caller format escape this check and surface as an
            # internal traceback instead of IMPORT_INVALID_DATE_FORMAT. No
            # value can rescue the format, so stop rather than retry each one.
            return False
        parsed += 1
    return parsed / len(clean) >= _MIN_OVERRIDE_PARSE_RATE


def detect_date_format(
    values: list[str | None],
    *,
    declared_format: str | None = None,
) -> tuple[str | None, "ConfidenceType"]:
    """Detect the date format from sample values.

    Tries each candidate format and scores on parse rate and date range
    reasonableness. Handles DD/MM vs MM/DD disambiguation.

    Args:
        values: Sample date strings (may include None/empty).
        declared_format: A caller-declared format (e.g. an explicit
            ``--date-format`` override) to score FIRST, before the fixed
            ``_DATE_FORMATS`` scan and before DD/MM disambiguation -- a
            caller who named the format already resolved the ambiguity a
            positional scan exists to guess at, and a format outside
            ``_DATE_FORMATS`` entirely (``%Y%m%d``, a time-bearing
            ``%Y-%m-%d %H:%M:%S``) would otherwise never be recognized at
            all, however cleanly it reads the column. Scored with the SAME
            parse-rate/range rule as every candidate below (``_MIN_PARSE_
            RATE``, not ``format_parses``'s lower override bar -- that bar
            answers a different question, whether the loader can import
            with it; this one decides what detection believes). Falls
            through to the normal scan, unchanged, when the declaration
            doesn't clear the bar -- a genuinely wrong override still fails
            here and is caught by import-time validation, never silently
            wins.

    Returns:
        Tuple of (format string, confidence: "high" | "medium" | "low").
        Format is None if no candidate passes the threshold.
    """
    clean = [v.strip() for v in values if v and v.strip()]
    if not clean:
        return None, "low"

    if declared_format is not None:
        parse_count = 0
        reasonable_count = 0
        for val in clean:
            try:
                dt = datetime.strptime(val, declared_format)
            except ValueError:
                continue
            except re.error:
                # Malformed declared format (e.g. a repeated directive) --
                # every value will raise identically, so nothing can
                # rescue it. Fall through to the normal scan below.
                parse_count = 0
                break
            parse_count += 1
            if _MIN_YEAR <= dt.year <= _max_year():
                reasonable_count += 1
        parse_rate = parse_count / len(clean)
        if parse_rate >= _MIN_PARSE_RATE:
            range_score = reasonable_count / max(parse_count, 1)
            confidence = (
                "high" if parse_rate >= 0.95 and range_score >= 0.95 else "medium"
            )
            return declared_format, confidence
        # Doesn't clear the bar -- fall through unchanged rather than
        # forcing a declaration the data doesn't actually support.

    scores: list[tuple[str, float, float]] = []
    for fmt in _DATE_FORMATS:
        parse_count = 0
        reasonable_count = 0
        for val in clean:
            try:
                dt = datetime.strptime(val, fmt)
                parse_count += 1
                if _MIN_YEAR <= dt.year <= _max_year():
                    reasonable_count += 1
            except ValueError:
                continue
        parse_rate = parse_count / len(clean) if clean else 0
        range_score = reasonable_count / max(parse_count, 1)
        if parse_rate >= _MIN_PARSE_RATE:
            scores.append((fmt, parse_rate, range_score))
            # Early exit on perfect match with unambiguous format
            if (
                parse_rate == 1.0
                and range_score == 1.0
                and fmt not in {"%m/%d/%Y", "%d/%m/%Y"}
            ):
                return fmt, "high"

    if not scores:
        return None, "low"

    dd_mm_fmts = {"%d/%m/%Y"}
    mm_dd_fmts = {"%m/%d/%Y"}
    has_dd_mm = any(s[0] in dd_mm_fmts for s in scores)
    has_mm_dd = any(s[0] in mm_dd_fmts for s in scores)

    if has_dd_mm and has_mm_dd:
        resolved_fmt, confidence = _disambiguate_dd_mm(clean, scores)
        if resolved_fmt:
            return resolved_fmt, confidence

    scores.sort(key=lambda s: s[1] * s[2], reverse=True)
    best_fmt, best_parse, best_range = scores[0]

    confidence = "high" if best_parse >= 0.95 and best_range >= 0.95 else "medium"
    return best_fmt, confidence


def _disambiguate_dd_mm(
    values: list[str],
    scores: list[tuple[str, float, float]],
) -> tuple[str | None, "ConfidenceType"]:
    """Disambiguate DD/MM vs MM/DD using positional value analysis."""
    sep_pattern = re.compile(r"[/\-.]")
    pos1_max = 0
    pos2_max = 0

    for val in values:
        parts = sep_pattern.split(val)
        if len(parts) >= 2:
            try:
                p1 = int(parts[0])
                p2 = int(parts[1])
                pos1_max = max(pos1_max, p1)
                pos2_max = max(pos2_max, p2)
            except ValueError:
                continue

    if pos1_max > 12 and pos2_max <= 12:
        return "%d/%m/%Y", "high"
    if pos2_max > 12 and pos1_max <= 12:
        return "%m/%d/%Y", "high"
    if pos1_max > 12 and pos2_max > 12:
        return None, "low"

    dd_mm_score = next((s[1] * s[2] for s in scores if s[0] == "%d/%m/%Y"), 0)
    mm_dd_score = next((s[1] * s[2] for s in scores if s[0] == "%m/%d/%Y"), 0)
    if mm_dd_score > dd_mm_score:
        return "%m/%d/%Y", "medium"
    if dd_mm_score > mm_dd_score:
        return "%d/%m/%Y", "medium"
    return "%m/%d/%Y", "medium"


def detect_number_format(
    values: list[str | None],
) -> "NumberFormatType":
    """Detect the number format convention from sample values.

    Args:
        values: Sample amount strings.

    Returns:
        One of NumberFormatType literal values.
    """
    clean = [v.strip() for v in values if v and v.strip()]
    if not clean:
        return "us"

    # Use the space-preserving regex so swiss_french thousands separators survive.
    stripped = [_CURRENCY_SYMBOLS_NO_SPACE.sub("", v).strip() for v in clean]
    stripped = [v.lstrip("-").strip("()").strip() for v in stripped]

    convention_scores: dict[str, int] = {
        "us": 0,
        "european": 0,
        "swiss_french": 0,
        "zero_decimal": 0,
    }

    for val in stripped:
        if not val:
            continue
        has_period = "." in val
        has_comma = "," in val
        has_space = " " in val

        if has_period and has_comma:
            last_period = val.rfind(".")
            last_comma = val.rfind(",")
            if last_period > last_comma:
                convention_scores["us"] += 1
            else:
                convention_scores["european"] += 1

        elif has_space and has_comma:
            convention_scores["swiss_french"] += 1

        elif has_period and not has_comma:
            after_period = val[val.rfind(".") + 1 :]
            if len(after_period) <= 3 and after_period.isdigit():
                convention_scores["us"] += 1
            else:
                convention_scores["european"] += 1

        elif has_comma and not has_period:
            after_comma = val[val.rfind(",") + 1 :]
            if len(after_comma) == 2 and after_comma.isdigit():
                convention_scores["european"] += 1
            elif len(after_comma) == 3 and after_comma.isdigit():
                convention_scores["zero_decimal"] += 1
            else:
                convention_scores["us"] += 1

        else:
            convention_scores["us"] += 1

    best = max(convention_scores, key=lambda k: convention_scores[k])
    if convention_scores[best] == 0:
        return "us"
    return cast("NumberFormatType", best)


def parse_amount_str(value: str, number_format: "NumberFormatType") -> Decimal | None:
    """Parse an amount string using the specified number format convention.

    Handles currency symbols, parentheses-as-negative, DR/CR suffixes.

    Args:
        value: Raw amount string.
        number_format: One of NumberFormatType literal values.

    Returns:
        Parsed Decimal, or None if the string is empty/unparseable.
    """
    if not value or not value.strip():
        return None

    s = value.strip()

    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1].strip()
    if s.startswith("-"):
        is_negative = True
        s = s[1:].strip()

    s_upper = s.upper().rstrip()
    if s_upper.endswith(" DR"):
        is_negative = True
        s = s[:-3].strip()
    elif s_upper.endswith(" CR"):
        is_negative = False
        s = s[:-3].strip()

    s = _CURRENCY_SYMBOLS.sub("", s).strip()

    if not s:
        return None

    if number_format == "european":
        s = s.replace(".", "").replace(",", ".")
    elif number_format == "swiss_french":
        s = s.replace(" ", "").replace(",", ".")
    elif number_format == "zero_decimal":
        s = s.replace(",", "")
    else:  # us
        s = s.replace(",", "")

    try:
        result = Decimal(s)
        return -result if is_negative else result
    except (ValueError, InvalidOperation):
        return None
