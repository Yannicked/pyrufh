"""Tests for header parsing utilities."""

import base64

import httpx
import pytest

from pyrufh.headers import (
    UploadLimits,
    build_content_digest_header,
    build_repr_digest_header,
    build_upload_complete_header,
    build_upload_length_header,
    build_upload_offset_header,
    build_want_content_digest_header,
    build_want_repr_digest_header,
    compute_digest,
    parse_content_digest,
    parse_repr_digest,
    parse_upload_complete,
    parse_upload_length,
    parse_upload_limits,
    parse_upload_offset,
    parse_want_content_digest,
    parse_want_repr_digest,
)


def make_headers(**kwargs: str) -> httpx.Headers:
    return httpx.Headers(kwargs)


class TestParseUploadOffset:
    def test_valid_integer(self):
        h = make_headers(**{"upload-offset": "12345"})
        assert parse_upload_offset(h) == 12345

    def test_zero(self):
        h = make_headers(**{"upload-offset": "0"})
        assert parse_upload_offset(h) == 0

    def test_missing(self):
        h = make_headers()
        assert parse_upload_offset(h) is None

    def test_with_params(self):
        # SF Items may have parameters after ';'
        h = make_headers(**{"upload-offset": "100; some-param=foo"})
        assert parse_upload_offset(h) == 100

    def test_negative_returns_none(self):
        h = make_headers(**{"upload-offset": "-1"})
        assert parse_upload_offset(h) is None

    def test_non_integer_returns_none(self):
        h = make_headers(**{"upload-offset": "abc"})
        assert parse_upload_offset(h) is None


class TestParseUploadComplete:
    def test_true(self):
        h = make_headers(**{"upload-complete": "?1"})
        assert parse_upload_complete(h) is True

    def test_false(self):
        h = make_headers(**{"upload-complete": "?0"})
        assert parse_upload_complete(h) is False

    def test_missing(self):
        h = make_headers()
        assert parse_upload_complete(h) is None

    def test_invalid_returns_none(self):
        h = make_headers(**{"upload-complete": "true"})
        assert parse_upload_complete(h) is None


class TestParseUploadLength:
    def test_valid_integer(self):
        h = make_headers(**{"upload-length": "1000000"})
        assert parse_upload_length(h) == 1_000_000

    def test_missing(self):
        h = make_headers()
        assert parse_upload_length(h) is None


class TestParseUploadLimits:
    def test_max_size(self):
        h = make_headers(**{"upload-limit": "max-size=1234567890"})
        limits = parse_upload_limits(h)
        assert limits is not None
        assert limits.max_size == 1_234_567_890

    def test_multiple_limits(self):
        h = make_headers(
            **{
                "upload-limit": "max-size=1000, min-size=10, max-append-size=500, min-append-size=1, max-age=3600"
            }
        )
        limits = parse_upload_limits(h)
        assert limits is not None
        assert limits.max_size == 1000
        assert limits.min_size == 10
        assert limits.max_append_size == 500
        assert limits.min_append_size == 1
        assert limits.max_age == 3600

    def test_missing(self):
        h = make_headers()
        assert parse_upload_limits(h) is None

    def test_unknown_key_ignored(self):
        h = make_headers(**{"upload-limit": "max-size=500, unknown-key=99"})
        limits = parse_upload_limits(h)
        assert limits is not None
        assert limits.max_size == 500

    def test_from_header_partial(self):
        limits = UploadLimits.from_header("max-size=100")
        assert limits.max_size == 100
        assert limits.min_size is None


class TestBuildHeaders:
    def test_build_upload_complete_true(self):
        assert build_upload_complete_header(True) == "?1"

    def test_build_upload_complete_false(self):
        assert build_upload_complete_header(False) == "?0"

    def test_build_upload_offset(self):
        assert build_upload_offset_header(42) == "42"

    def test_build_upload_length(self):
        assert build_upload_length_header(999) == "999"


class TestDigestParsing:
    def test_parse_content_digest_single(self):
        h = make_headers(
            **{"content-digest": "sha-256=:LPs0XyGAP1tZxqQ3jJ7vAWj1gEqZef3J7rXhZ3Yv8qk=:"}
        )
        digests = parse_content_digest(h)
        assert digests is not None
        assert "sha-256" in digests

    def test_parse_content_digest_multiple(self):
        h = make_headers(**{"content-digest": "sha-256=:abc=:, sha-512=:xyz=:"})
        digests = parse_content_digest(h)
        assert digests is not None
        assert "sha-256" in digests
        assert "sha-512" in digests

    def test_parse_content_digest_missing(self):
        h = make_headers()
        assert parse_content_digest(h) is None

    def test_parse_repr_digest(self):
        h = make_headers(
            **{"repr-digest": "sha-256=:LPs0XyGAP1tZxqQ3jJ7vAWj1gEqZef3J7rXhZ3Yv8qk=:"}
        )
        digests = parse_repr_digest(h)
        assert digests is not None
        assert "sha-256" in digests

    def test_parse_want_content_digest(self):
        h = make_headers(**{"want-content-digest": "sha-256=1, sha-512=0"})
        prefs = parse_want_content_digest(h)
        assert prefs is not None
        assert prefs.get("sha-256") == 1
        assert prefs.get("sha-512") == 0

    def test_parse_want_repr_digest(self):
        h = make_headers(**{"want-repr-digest": "sha-256=10"})
        prefs = parse_want_repr_digest(h)
        assert prefs is not None
        assert prefs.get("sha-256") == 10


class TestDigestBuilding:
    def test_build_content_digest_header(self):
        data = b"hello world"
        sha256 = compute_digest("sha-256", data)
        header = build_content_digest_header({"sha-256": sha256})
        assert header.startswith("sha-256=:")
        assert header.endswith(":")
        parsed = parse_content_digest(make_headers(**{"content-digest": header}))
        assert parsed is not None
        assert parsed["sha-256"] == sha256

    def test_build_repr_digest_header(self):
        data = b"hello world"
        sha256 = compute_digest("sha-256", data)
        header = build_repr_digest_header({"sha-256": sha256})
        assert header.startswith("sha-256=:")
        parsed = parse_repr_digest(make_headers(**{"repr-digest": header}))
        assert parsed is not None
        assert parsed["sha-256"] == sha256

    def test_build_want_digest_header(self):
        header = build_want_content_digest_header({"sha-256": 10, "sha-512": 5})
        assert "sha-256=10" in header
        assert "sha-512=5" in header


class TestComputeDigest:
    def test_sha256(self):
        data = b"hello world"
        digest = compute_digest("sha-256", data)
        assert len(digest) == 32
        expected = base64.b64decode("uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=")
        assert digest == expected

    def test_sha512(self):
        data = b"hello world"
        digest = compute_digest("sha-512", data)
        assert len(digest) == 64

    def test_sha256_from_file_object(self):
        import io

        data = b"hello world"
        file_obj = io.BytesIO(data)
        digest = compute_digest("sha-256", file_obj)
        expected = compute_digest("sha-256", data)
        assert digest == expected

    def test_sha256_from_file_object_chunked(self):
        import io

        data = b"hello world" * 1000
        file_obj = io.BytesIO(data)
        digest = compute_digest("sha-256", file_obj)
        expected = compute_digest("sha-256", data)
        assert digest == expected

    def test_unsupported_algorithm(self):
        with pytest.raises(ValueError, match="Unsupported"):
            compute_digest("unknown-algo", b"data")
