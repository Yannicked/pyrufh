"""Utilities for parsing and building RUFH (Resumable Uploads for HTTP) headers.

Implements parsing for structured header fields as defined in RFC 9651
(Structured Field Values for HTTP):
  - Upload-Offset  (Item, Integer)
  - Upload-Complete (Item, Boolean)
  - Upload-Length  (Item, Integer)
  - Upload-Limit   (Dictionary)
  - Upload-Draft-Interop-Version (Item, Integer)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx

# The current draft interop version as defined in Appendix B of the spec.
DRAFT_INTEROP_VERSION = 8

CONTENT_TYPE_PARTIAL_UPLOAD = "application/partial-upload"


@dataclass
class UploadLimits:
    """Limits communicated by the server via the Upload-Limit header field."""

    max_size: int | None = None
    min_size: int | None = None
    max_append_size: int | None = None
    min_append_size: int | None = None
    max_age: int | None = None

    @classmethod
    def from_header(cls, value: str) -> UploadLimits:
        """Parse the Upload-Limit header dictionary.

        The header is a Structured Fields Dictionary (RFC 9651 §3.2), e.g.:
            max-size=1234567890, max-append-size=10000000
        """
        limits = cls()
        # Simple hand-rolled parser for SF Dictionary of Integers.
        # We do not pull in a full SF library to keep the dependency footprint
        # small - the dict only contains known Integer members.
        for part in value.split(","):
            part = part.strip()
            if "=" not in part:
                continue
            key, _, raw = part.partition("=")
            key = key.strip().lower()
            raw = raw.strip()
            # SF Integers are plain decimal strings (possibly with params after ';').
            raw_int = raw.split(";")[0].strip()
            try:
                int_val = int(raw_int)
            except ValueError:
                # Unknown or malformed value - ignore per spec (unknown keys
                # MUST be ignored; wrong type MUST cause entire field to be ignored,
                # but we apply best-effort here on a per-member basis).
                continue
            if key == "max-size":
                limits.max_size = int_val
            elif key == "min-size":
                limits.min_size = int_val
            elif key == "max-append-size":
                limits.max_append_size = int_val
            elif key == "min-append-size":
                limits.min_append_size = int_val
            elif key == "max-age":
                limits.max_age = int_val
            # Unknown keys are ignored per spec.
        return limits


@dataclass
class UploadState:
    """State returned by the server for an upload resource (offset retrieval)."""

    offset: int
    complete: bool
    length: int | None = None
    limits: UploadLimits | None = None


def parse_upload_offset(headers: httpx.Headers) -> int | None:
    """Parse the Upload-Offset header field (Item, Integer)."""
    value = headers.get("upload-offset")
    if value is None:
        return None
    # SF Item Integer: plain decimal, possibly with parameters.
    raw = value.split(";")[0].strip()
    try:
        result = int(raw)
        return result if result >= 0 else None
    except ValueError:
        return None


def parse_upload_complete(headers: httpx.Headers) -> bool | None:
    """Parse the Upload-Complete header field (Item, Boolean).

    SF Booleans are represented as ?1 (true) or ?0 (false).
    """
    value = headers.get("upload-complete")
    if value is None:
        return None
    raw = value.split(";")[0].strip()
    if raw == "?1":
        return True
    if raw == "?0":
        return False
    return None


def parse_upload_length(headers: httpx.Headers) -> int | None:
    """Parse the Upload-Length header field (Item, Integer)."""
    value = headers.get("upload-length")
    if value is None:
        return None
    raw = value.split(";")[0].strip()
    try:
        result = int(raw)
        return result if result >= 0 else None
    except ValueError:
        return None


def parse_upload_limits(headers: httpx.Headers) -> UploadLimits | None:
    """Parse the Upload-Limit header field (Dictionary)."""
    value = headers.get("upload-limit")
    if value is None:
        return None
    return UploadLimits.from_header(value)


def parse_location(headers: httpx.Headers) -> str | None:
    """Return the Location header field value, if present."""
    return headers.get("location")


def build_upload_complete_header(complete: bool) -> str:
    """Encode a boolean as an SF Boolean string."""
    return "?1" if complete else "?0"


def build_upload_offset_header(offset: int) -> str:
    """Encode an integer offset as an SF Integer string."""
    return str(offset)


def build_upload_length_header(length: int) -> str:
    """Encode an integer length as an SF Integer string."""
    return str(length)


def draft_interop_headers() -> dict[str, str]:
    """Return the Upload-Draft-Interop-Version header required by draft implementations."""
    return {"Upload-Draft-Interop-Version": str(DRAFT_INTEROP_VERSION)}
