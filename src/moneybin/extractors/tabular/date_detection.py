"""Date format detection, DD/MM disambiguation, and number format detection.

Handles the nuances of international date and number conventions that
trip up every CSV importer. Uses positional value analysis for date
disambiguation and convention scoring for number format detection.
"""

import re
from datetime import datetime

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
_MAX_YEAR = datetime.now().year + 1


def detect_date_format(
    values: list[str | None],
) -> tuple[str | None, str]:
    """Detect the date format from sample values.

    Tries each candidate format and scores on parse rate and date range
    reasonableness. Handles DD/MM vs MM/DD disambiguation.

    Args:
        values: Sample date strings (may include None/empty).

    Returns:
        Tuple of (format string, confidence: "high" | "medium" | "low").
        Format is None if no candidate passes the threshold.
    """
    clean = [v.strip() for v in values if v and v.strip()]
    if not clean:
        return None, "low"

    scores: list[tuple[str, float, float]] = []
    for fmt in _DATE_FORMATS:
        parse_count = 0
        reasonable_count = 0
        for val in clean:
            try:
                dt = datetime.strptime(val, fmt)
                parse_count += 1
                if _MIN_YEAR <= dt.year <= _MAX_YEAR:
                    reasonable_count += 1
            except ValueError:
                continue
        parse_rate = parse_count / len(clean) if clean else 0
        range_score = reasonable_count / max(parse_count, 1)
        if parse_rate >= 0.9:
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
) -> tuple[str | None, str]:
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


def detect_number_format(values: list[str | None]) -> str:
    """Detect the number format convention from sample values.

    Args:
        values: Sample amount strings.

    Returns:
        One of: "us", "european", "swiss_french", "zero_decimal".
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
    return best


def parse_amount_str(value: str, number_format: str) -> float | None:
    """Parse an amount string using the specified number format convention.

    Handles currency symbols, parentheses-as-negative, DR/CR suffixes.

    Args:
        value: Raw amount string.
        number_format: Convention: us, european, swiss_french, zero_decimal.

    Returns:
        Parsed float, or None if the string is empty/unparseable.
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
        result = float(s)
        return -result if is_negative else result
    except ValueError:
        return None
