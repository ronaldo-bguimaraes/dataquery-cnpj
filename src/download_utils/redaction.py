from __future__ import annotations

import re

import aiohttp

_URL_PATTERN = re.compile(r"https?://[^\s'\"<>]+", re.IGNORECASE)


def redact_urls(text: str) -> str:
    """Replace every URL in ``text`` with ``<url>``.

    The RFB WebDAV hostname embeds the public access token in the URL path, so URLs must
    never reach logs, XCom or exception messages.
    """

    return _URL_PATTERN.sub("<url>", text)


def describe_exception(exc: BaseException) -> str:
    """Return a short, sanitized description of an exception."""

    if isinstance(exc, aiohttp.ClientResponseError):
        reason = (exc.message or "").strip()
        return f"HTTP {exc.status} {reason}".strip()
    if isinstance(exc, aiohttp.ClientError):
        return type(exc).__name__

    message = redact_urls(str(exc)).strip()
    if not message:
        return type(exc).__name__
    return f"{type(exc).__name__}: {message}"
