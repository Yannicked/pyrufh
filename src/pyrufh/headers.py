"""Utilities for parsing and building RUFH (Resumable Uploads for HTTP) headers.

Implements parsing for structured header fields as defined in RFC 9651
(Structured Field Values for HTTP):
  - Upload-Offset  (Item, Integer)
  - Upload-Complete (Item, Boolean)
  - Upload-Length  (Item, Integer)
  - Upload-Limit   (Dictionary)
  - Upload-Draft-Interop-Version (Item, Integer)

Also implements parsing for Digest Fields as defined in RFC 9530:
  - Content-Digest (Dictionary with base64-encoded digests)
  - Repr-Digest (Dictionary with base64-encoded digests)
  - Want-Content-Digest (Dictionary with integer weights)
  - Want-Repr-Digest (Dictionary with integer weights)
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    from collections.abc import Mapping

ALGORITHM_KEYS = {
    "sha-512": "sha512",
    "sha-256": "sha256",
    "sha": "sha1",
    "md5": "md5",
    "unixsum": "sum",
    "unixcksum": "cksum",
    "adler": "adler",
    "crc32c": "crc32c",
}

DIGEST_ALGORITHMS = list(ALGORITHM_KEYS.keys())

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


def parse_upload_offset(headers: Mapping[str, str]) -> int | None:
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


def parse_upload_complete(headers: Mapping[str, str]) -> bool | None:
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


def parse_upload_length(headers: Mapping[str, str]) -> int | None:
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


def parse_upload_limits(headers: Mapping[str, str]) -> UploadLimits | None:
    """Parse the Upload-Limit header field (Dictionary)."""
    value = headers.get("upload-limit")
    if value is None:
        return None
    return UploadLimits.from_header(value)


def parse_location(headers: Mapping[str, str]) -> str | None:
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


def parse_digest(headers: Mapping[str, str], header_name: str) -> dict[str, bytes] | None:
    """Parse a Content-Digest or Repr-Digest header (RFC 9530).

    Returns a dictionary mapping algorithm names to decoded digest bytes.
    For example: {"sha-256": b"\\x12\\x34..."}

    Returns None if the header is absent.
    """
    value = headers.get(header_name.lower())
    if value is None:
        return None

    result: dict[str, bytes] = {}
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            continue
        alg, _, raw_b64 = part.partition("=")
        alg = alg.strip().lower()
        raw_b64 = raw_b64.strip()
        if raw_b64.startswith(":") and raw_b64.endswith(":"):
            raw_b64 = raw_b64[1:-1]
        try:
            result[alg] = base64.b64decode(raw_b64)
        except Exception:
            continue
    return result if result else None


def parse_content_digest(headers: Mapping[str, str]) -> dict[str, bytes] | None:
    """Parse the Content-Digest header (RFC 9530 §2)."""
    return parse_digest(headers, "content-digest")


def parse_repr_digest(headers: Mapping[str, str]) -> dict[str, bytes] | None:
    """Parse the Repr-Digest header (RFC 9530 §3)."""
    return parse_digest(headers, "repr-digest")


def parse_want_digest(headers: Mapping[str, str], header_name: str) -> dict[str, int] | None:
    """Parse a Want-Content-Digest or Want-Repr-Digest header (RFC 9530 §4).

    Returns a dictionary mapping algorithm names to preference weights (1-10).
    A weight of 0 means "not acceptable".
    """
    value = headers.get(header_name.lower())
    if value is None:
        return None

    result: dict[str, int] = {}
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            continue
        alg, _, raw_weight = part.partition("=")
        alg = alg.strip().lower()
        try:
            weight = int(raw_weight.strip())
            if 0 <= weight <= 10:
                result[alg] = weight
        except ValueError:
            continue
    return result if result else None


def parse_want_content_digest(headers: Mapping[str, str]) -> dict[str, int] | None:
    """Parse the Want-Content-Digest header (RFC 9530 §4)."""
    return parse_want_digest(headers, "want-content-digest")


def parse_want_repr_digest(headers: Mapping[str, str]) -> dict[str, int] | None:
    """Parse the Want-Repr-Digest header (RFC 9530 §4)."""
    return parse_want_digest(headers, "want-repr-digest")


def build_digest_header(digests: dict[str, bytes]) -> str:
    """Build a Content-Digest or Repr-Digest header value (RFC 9530).

    Takes a dictionary mapping algorithm names to digest bytes.
    Returns the header string value (e.g., "sha-256=:abc...:").
    """
    parts = []
    for alg in sorted(digests.keys()):
        b64 = base64.b64encode(digests[alg]).decode("ascii")
        parts.append(f"{alg}=:{b64}:")
    return ", ".join(parts)


def build_content_digest_header(digests: dict[str, bytes]) -> str:
    """Build a Content-Digest header value (RFC 9530 §2)."""
    return build_digest_header(digests)


def build_repr_digest_header(digests: dict[str, bytes]) -> str:
    """Build a Repr-Digest header value (RFC 9530 §3)."""
    return build_digest_header(digests)


def build_want_digest_header(preferences: dict[str, int]) -> str:
    """Build a Want-Content-Digest or Want-Repr-Digest header value (RFC 9530 §4).

    Takes a dictionary mapping algorithm names to preference weights (1-10).
    """
    parts = []
    for alg in sorted(preferences.keys()):
        parts.append(f"{alg}={preferences[alg]}")
    return ", ".join(parts)


def build_want_content_digest_header(preferences: dict[str, int]) -> str:
    """Build a Want-Content-Digest header value (RFC 9530 §4)."""
    return build_want_digest_header(preferences)


def build_want_repr_digest_header(preferences: dict[str, int]) -> str:
    """Build a Want-Repr-Digest header value (RFC 9530 §4)."""
    return build_want_digest_header(preferences)


def compute_digest(algorithm: str, data: bytes | BinaryIO) -> bytes:
    """Compute a digest for the given algorithm and data.

    Parameters
    ----------
    algorithm:
        The algorithm name (e.g., "sha-256", "sha-512").
    data:
        The bytes or file-like object to digest. If a file-like object,
        it will be read in chunks to avoid loading the entire content
        into memory.

    Returns
    -------
    bytes
        The raw digest bytes.

    Raises
    ------
    ValueError
        If the algorithm is not supported.
    """
    alg_key = ALGORITHM_KEYS.get(algorithm.lower())
    if alg_key is None:
        raise ValueError(f"Unsupported digest algorithm: {algorithm}")

    if alg_key in ("sha512", "sha256", "sha1", "md5"):
        return _compute_hash_stream(alg_key, data)
    elif alg_key == "sum":
        if isinstance(data, bytes):
            return _unix_sum(data)
        return _unix_sum_stream(data)  # type: ignore[arg-type]
    elif alg_key == "cksum":
        if isinstance(data, bytes):
            return _unix_cksum(data)
        return _unix_cksum_stream(data)  # type: ignore[arg-type]
    elif alg_key == "adler":
        if isinstance(data, bytes):
            return _adler32(data)
        return _adler32_stream(data)  # type: ignore[arg-type]
    elif alg_key == "crc32c":
        if isinstance(data, bytes):
            return _crc32c(data)
        return _crc32c_stream(data)  # type: ignore[arg-type]
    else:
        raise ValueError(f"Unsupported digest algorithm: {algorithm}")


def _compute_hash_stream(alg_key: str, data: bytes | BinaryIO) -> bytes:
    """Compute a hash digest using an update loop for memory efficiency."""
    if isinstance(data, bytes):
        if alg_key == "sha512":
            return hashlib.sha512(data).digest()
        elif alg_key == "sha256":
            return hashlib.sha256(data).digest()
        elif alg_key == "sha1":
            return hashlib.sha1(data).digest()
        else:
            return hashlib.md5(data).digest()

    if alg_key == "sha512":
        h = hashlib.sha512()
    elif alg_key == "sha256":
        h = hashlib.sha256()
    elif alg_key == "sha1":
        h = hashlib.sha1()
    else:
        h = hashlib.md5()

    while True:
        chunk = data.read(65536)
        if not chunk:
            break
        h.update(chunk)
    return h.digest()


def _unix_sum(data: bytes) -> bytes:
    """Compute UNIX-style sum checksum (sum -r)."""
    s = 0
    for byte in data:
        s = (s >> 1) + ((s & 1) << 15)
        s = (s + byte) & 0xFFFF
    return s.to_bytes(2, "big")


def _unix_sum_stream(data: BinaryIO) -> bytes:
    """Compute UNIX-style sum checksum from a file-like object."""
    s = 0
    while True:
        chunk = data.read(65536)
        if not chunk:
            break
        for byte in chunk:
            s = (s >> 1) + ((s & 1) << 15)
            s = (s + byte) & 0xFFFF
    return s.to_bytes(2, "big")


def _unix_cksum(data: bytes) -> bytes:
    """Compute UNIX-style CRC (cksum)."""
    return _crc32(data).to_bytes(4, "big")


def _unix_cksum_stream(data: BinaryIO) -> bytes:
    """Compute UNIX-style CRC from a file-like object."""
    import zlib

    s = 0
    while True:
        chunk = data.read(65536)
        if not chunk:
            break
        s = zlib.crc32(chunk, s)
    return (s & 0xFFFFFFFF).to_bytes(4, "big")


def _adler32(data: bytes) -> bytes:
    """Compute Adler-32 checksum."""
    import zlib

    return zlib.adler32(data).to_bytes(4, "big")


def _adler32_stream(data: BinaryIO) -> bytes:
    """Compute Adler-32 checksum from a file-like object."""
    a = 1
    b = 0
    while True:
        chunk = data.read(65536)
        if not chunk:
            break
        for byte in chunk:
            a = (a + byte) & 0xFFFF
            b = (b + a) & 0xFFFF
    return ((b << 16) | a).to_bytes(4, "big")


def _crc32(data: bytes) -> int:
    """Compute CRC32."""
    import zlib

    return zlib.crc32(data) & 0xFFFFFFFF


def _crc32c(data: bytes) -> bytes:
    """Compute CRC32c (Castagnoli)."""
    import zlib

    return zlib.crc32(data).to_bytes(4, "big")


def _crc32c_stream(data: BinaryIO) -> bytes:
    """Compute CRC32c from a file-like object."""
    import zlib

    s = 0
    while True:
        chunk = data.read(65536)
        if not chunk:
            break
        s = zlib.crc32(chunk, s)
    return (s & 0xFFFFFFFF).to_bytes(4, "big")
