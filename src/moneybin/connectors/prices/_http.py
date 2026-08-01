"""One request path shared by every price adapter.

Both adapters need the same three behaviours — back off on rate limiting only,
map a status code to a typed error, and never let a float touch a price — so
they share one implementation rather than growing two that drift apart.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal

import httpx

from moneybin.connectors.prices.errors import (
    PriceFeedAPIError,
    PriceFeedAuthError,
    PriceFeedNotFoundError,
    PriceFeedRateLimitError,
    PriceFeedRequestRejectedError,
    PriceFeedUnreachableError,
)

# Three attempts absorbs a burst against a per-minute quota without turning one
# unlucky security into a minute of stalled wall clock across a 40-security run.
RETRY_MAX = 3
RETRY_BACKOFF_BASE = 2.0
DEFAULT_TIMEOUT = httpx.Timeout(20.0, connect=10.0)


def fetch_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, str],
    headers: dict[str, str] | None = None,
    sleep: Callable[[float], object],
) -> object:
    """GET `url` and return its parsed body, retrying rate limits only.

    Returns ``object``, not ``Any``: a provider's payload is untrusted input, and
    ``Any`` would let a caller index into it without narrowing, so a shape change
    upstream would surface as a traceback in the middle of a refresh instead of a
    named failure for one security.

    Prices are parsed with ``parse_float=Decimal`` so a quote never becomes a
    float. ``json.loads`` would otherwise read 212.55 as
    212.55000000000001136868377216160297393798828125, and that value would land
    in a DECIMAL(28,10) column on an append-only table where it cannot be
    corrected in place.
    """
    last_error: PriceFeedRateLimitError | None = None
    for attempt in range(RETRY_MAX):
        try:
            response = client.get(url, params=params, headers=headers or {})
        except httpx.RequestError as exc:
            # Wrap so classify_user_error surfaces a clean message rather than an
            # httpx traceback. str(exc) can contain the full request URL, which
            # for Tiingo would be fine (the token rides a header) but is noise.
            raise PriceFeedUnreachableError(
                f"price feed unreachable: {type(exc).__name__}"
            ) from exc
        if response.status_code == 429:
            last_error = PriceFeedRateLimitError("price feed rate limit exceeded (429)")
            sleep(RETRY_BACKOFF_BASE**attempt)
            continue
        _raise_for_status(response)
        try:
            return json.loads(response.text, parse_float=Decimal)
        except json.JSONDecodeError as exc:
            raise PriceFeedAPIError(
                f"price feed returned a body that is not JSON ({response.status_code})"
            ) from exc
    # Loop invariant: only a 429 continues, so last_error is populated here.
    if last_error is None:  # pragma: no cover — defensive
        raise RuntimeError("retry loop exited without raising")
    raise last_error


def _raise_for_status(response: httpx.Response) -> None:
    """Map a non-2xx status onto the typed hierarchy.

    Deliberately does not echo the response body. A provider error page can
    quote the request it received, and pasting that into a message or a stored
    failure reason is how a credential ends up in a log.
    """
    if response.status_code in (401, 403):
        raise PriceFeedAuthError(
            f"price feed rejected the credential ({response.status_code})"
        )
    if response.status_code == 404:
        # Describes one security rather than the run: the provider answered, and
        # its answer is that it does not know this symbol.
        raise PriceFeedNotFoundError(
            f"price feed does not know this symbol ({response.status_code})"
        )
    if response.status_code == 400:
        # Also potentially about one security: the request itself was malformed.
        # Whether that is per-security depends on whether the caller varies a
        # parameter per security, so only the adapters that do catch this — see
        # PriceFeedRequestRejectedError. The provider's own explanation is not
        # quoted: a 400 body routinely echoes the request that produced it.
        raise PriceFeedRequestRejectedError(
            f"price feed rejected the request ({response.status_code})"
        )
    if response.status_code >= 400:
        raise PriceFeedAPIError(f"price feed returned {response.status_code}")
