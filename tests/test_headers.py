"""Tests for header parsing utilities."""

import httpx

from pyrufh.headers import (
    UploadLimits,
    build_upload_complete_header,
    build_upload_length_header,
    build_upload_offset_header,
    parse_upload_complete,
    parse_upload_length,
    parse_upload_limits,
    parse_upload_offset,
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
